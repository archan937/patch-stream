mod bindings {
    wit_bindgen::generate!({
        world: "patch-producer-ts",
        path: "../wit",
        generate_all,
        async: [
            "export:wasmcloud:patch-stream/patches@0.1.0#subscribe",
            "import:wasi:http/client@0.3.0-rc-2026-03-15#send",
        ],
    });
}

use bindings::exports::wasmcloud::patch_stream::patches::Guest;
use bindings::wasi::cli::environment;
use bindings::wasi::http::client;
use bindings::wasi::http::types::{ErrorCode, Fields, Method, Request, RequestOptions, Scheme};
use rquickjs::{async_with, AsyncContext, AsyncRuntime, Function};
use rquickjs::prelude::{Async, MutFn};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::task::Waker;
use wit_bindgen::{StreamReader, StreamWriter};

const PAGE_RUNTIME_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/page_runtime.js"));

// ---------------------------------------------------------------------------
// HTTP chunk queue — fed by a wasip3 spawn task, drained by __httpStreamRead
// ---------------------------------------------------------------------------

struct ChunkQueue {
    chunks: RefCell<VecDeque<Option<String>>>, // None = end of stream
    waker: RefCell<Option<Waker>>,
}

impl ChunkQueue {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            chunks: RefCell::new(VecDeque::new()),
            waker: RefCell::new(None),
        })
    }

    fn push(&self, chunk: Option<String>) {
        self.chunks.borrow_mut().push_back(chunk);
        if let Some(w) = self.waker.borrow_mut().take() {
            w.wake();
        }
    }
}

struct NextChunk(Rc<ChunkQueue>);

impl std::future::Future for NextChunk {
    type Output = Option<String>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<String>> {
        if let Some(chunk) = self.0.chunks.borrow_mut().pop_front() {
            return std::task::Poll::Ready(chunk);
        }
        *self.0.waker.borrow_mut() = Some(cx.waker().clone());
        std::task::Poll::Pending
    }
}

// Active chunk queue: set by __httpStreamStart, polled by __httpStreamRead.
type ActiveCq = Rc<RefCell<Option<Rc<ChunkQueue>>>>;

// ---------------------------------------------------------------------------
// Inter-task patch queue
// ---------------------------------------------------------------------------

struct PatchQueue {
    queue: RefCell<VecDeque<String>>,
    waker: RefCell<Option<Waker>>,
    done: Cell<bool>,
}

impl PatchQueue {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            queue: RefCell::new(VecDeque::new()),
            waker: RefCell::new(None),
            done: Cell::new(false),
        })
    }

    fn push(&self, s: String) {
        self.queue.borrow_mut().push_back(s);
        if let Some(w) = self.waker.borrow_mut().take() {
            w.wake();
        }
    }

    fn finish(&self) {
        self.done.set(true);
        if let Some(w) = self.waker.borrow_mut().take() {
            w.wake();
        }
    }
}

struct NextPatch(Rc<PatchQueue>);

impl std::future::Future for NextPatch {
    type Output = Option<String>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<String>> {
        if let Some(s) = self.0.queue.borrow_mut().pop_front() {
            return std::task::Poll::Ready(Some(s));
        }
        if self.0.done.get() {
            return std::task::Poll::Ready(None);
        }
        *self.0.waker.borrow_mut() = Some(cx.waker().clone());
        std::task::Poll::Pending
    }
}

// ---------------------------------------------------------------------------
// WIT guest
// ---------------------------------------------------------------------------

struct Component;

impl Guest for Component {
    async fn subscribe(prompt: String) -> StreamReader<u8> {
        eprintln!("[patch-producer-ts] subscribe prompt={:?}", &prompt[..prompt.len().min(40)]);
        let (writer, reader) = bindings::wit_stream::new::<u8>();
        wit_bindgen::spawn(run_generate(prompt, writer));
        reader
    }
}

async fn run_generate(prompt: String, mut writer: StreamWriter<u8>) {
    eprintln!("[patch-producer-ts] run_generate started");
    let queue = PatchQueue::new();
    let queue_finish = queue.clone();

    // Drain task: forwards patches from the queue to the WIT stream writer.
    {
        let q = queue.clone();
        wit_bindgen::spawn(async move {
            while let Some(patch) = NextPatch(q.clone()).await {
                let mut line = patch.into_bytes();
                line.push(b'\n');
                writer.write_all(line).await;
            }
        });
    }

    let rt = AsyncRuntime::new().expect("AsyncRuntime::new");
    rt.set_max_stack_size(1024 * 1024).await;
    let ctx = AsyncContext::full(&rt).await.expect("AsyncContext::full");

    // Active chunk queue slot: __httpStreamStart stores a new Rc<ChunkQueue> here;
    // __httpStreamRead takes chunks from it.
    let active_cq: ActiveCq = Rc::new(RefCell::new(None));

    async_with!(ctx => |ctx| {
        ctx.eval::<(), _>(PAGE_RUNTIME_JS).unwrap_or_else(|_| {
            let msg = ctx
                .catch()
                .as_exception()
                .and_then(|e| e.message())
                .unwrap_or_default();
            panic!("page_runtime eval failed: {msg}");
        });

        let api_key = environment::get_environment()
            .into_iter()
            .find(|(k, _)| k == "ANTHROPIC_API_KEY")
            .map(|(_, v)| v)
            .unwrap_or_default();
        eprintln!("[patch-producer-ts] api_key present={}", !api_key.is_empty());
        let setup = format!(
            "globalThis.process = {{ env: {{ ANTHROPIC_API_KEY: {} }} }};",
            serde_json::to_string(&api_key).unwrap()
        );
        ctx.eval::<(), _>(setup.as_str()).expect("set ANTHROPIC_API_KEY");

        // __httpStreamStart: sync — creates a ChunkQueue and spawns a wasip3 task
        // that makes the WASI HTTP request and feeds chunks into the queue.
        // Running the HTTP send in a proper wasip3 task (not in the rquickjs
        // executor) ensures wasmtime's async HTTP future is polled by the wasip3
        // concurrent runtime rather than rquickjs's internal spawner, which would
        // break the waker chain and cause the request to hang.
        {
            let slot = active_cq.clone();
            ctx.globals().set(
                "__httpStreamStart",
                Function::new(
                    ctx.clone(),
                    move |url: String, headers_json: String, body: String| {
                        let cq = ChunkQueue::new();
                        *slot.borrow_mut() = Some(cq.clone());
                        wit_bindgen::spawn(async move {
                            eprintln!("[patch-producer-ts] http_task: sending to {}", &url[..url.len().min(60)]);
                            match wasi_http_send(&url, &headers_json, &body).await {
                                Ok(mut reader) => {
                                    loop {
                                        match read_next_chunk(&mut reader).await {
                                            Some(chunk) => cq.push(Some(chunk)),
                                            None => { cq.push(None); break; }
                                        }
                                    }
                                    eprintln!("[patch-producer-ts] http_task: response complete");
                                }
                                Err(e) => {
                                    eprintln!("[patch-producer-ts] http_task error: {e:?}");
                                    cq.push(None);
                                }
                            }
                        });
                    },
                ),
            )?;
        }

        // __httpStreamRead: async — returns a JS Promise<string|null>.
        // Awaits the next chunk from the ChunkQueue populated by the wasip3 HTTP task.
        {
            let slot = active_cq.clone();
            ctx.globals().set(
                "__httpStreamRead",
                Function::new(
                    ctx.clone(),
                    Async(MutFn::new(move || {
                        let cq = slot.borrow().clone();
                        async move {
                            match cq {
                                Some(cq) => NextChunk(cq).await,
                                None => None,
                            }
                        }
                    })),
                ),
            )?;
        }

        // onPatch: sync — pushes each JSON patch string into the drain queue.
        let on_patch = {
            let q = queue.clone();
            Function::new(ctx.clone(), move |patch: String| {
                q.push(patch);
            })?
        };

        eprintln!("[patch-producer-ts] calling generatePage");
        let generate_fn: Function = ctx.globals().get("generatePage")?;
        let promise: rquickjs::Promise = generate_fn.call((prompt, on_patch))?;
        eprintln!("[patch-producer-ts] generatePage returned promise, awaiting...");
        let result = promise.into_future::<()>().await;
        eprintln!("[patch-producer-ts] generatePage done, ok={}", result.is_ok());
        result.ok();

        Ok::<(), rquickjs::Error>(())
    })
    .await
    .ok();

    rt.run_gc().await;
    queue_finish.finish();
}

// ---------------------------------------------------------------------------
// WASI HTTP helpers — called from wit_bindgen::spawn tasks, not from rquickjs
// ---------------------------------------------------------------------------

async fn wasi_http_send(
    url: &str,
    headers_json: &str,
    body: &str,
) -> Result<StreamReader<u8>, ErrorCode> {
    let headers_map: std::collections::HashMap<String, String> =
        serde_json::from_str(headers_json).unwrap_or_default();

    let fields = Fields::new();
    for (k, v) in &headers_map {
        fields
            .append(&k.to_string(), &v.as_bytes().to_vec())
            .map_err(|_| ErrorCode::InternalError(Some("header error".into())))?;
    }
    let has_content_length = headers_map
        .keys()
        .any(|k| k.eq_ignore_ascii_case("content-length"));
    if !has_content_length {
        fields
            .append(
                &"content-length".to_string(),
                &body.len().to_string().into_bytes(),
            )
            .map_err(|_| ErrorCode::InternalError(Some("content-length header error".into())))?;
    }
    eprintln!(
        "[patch-producer-ts] http: {} headers (cl_from_js={}), body_len={}, body_preview={:?}",
        headers_map.len(),
        has_content_length,
        body.len(),
        &body[..body.len().min(300)]
    );

    let body_bytes = body.as_bytes().to_vec();
    let (mut body_tx, body_rx) = bindings::wit_stream::new::<u8>();

    let (_trailers_tx, trailers_rx) = bindings::wit_future::new(
        || Ok::<Option<bindings::wasi::http::types::Trailers>, ErrorCode>(None),
    );

    let options = RequestOptions::new();
    let (req, _req_result) = Request::new(fields, Some(body_rx), trailers_rx, Some(options));

    req.set_method(&Method::Post)
        .map_err(|()| ErrorCode::InternalError(None))?;

    if let Some((scheme_str, rest)) = url.split_once("://") {
        let wasi_scheme = if scheme_str.eq_ignore_ascii_case("https") {
            Scheme::Https
        } else {
            Scheme::Http
        };
        req.set_scheme(Some(&wasi_scheme))
            .map_err(|()| ErrorCode::InternalError(None))?;

        let (authority, path) = rest
            .split_once('/')
            .map(|(a, p)| (a, format!("/{p}")))
            .unwrap_or((rest, "/".to_string()));

        req.set_authority(Some(authority))
            .map_err(|()| ErrorCode::InternalError(None))?;
        req.set_path_with_query(Some(&path))
            .map_err(|()| ErrorCode::InternalError(None))?;
    }

    let write_fut = async move {
        body_tx.write_all(body_bytes).await;
        drop(body_tx);
    };
    let ((), response_result) = futures::join!(write_fut, client::send(req));
    let response = response_result?;
    let status = response.get_status_code();
    eprintln!("[patch-producer-ts] http response status={}", status);

    let (_done_tx, done_rx) = bindings::wit_future::new(|| Ok::<(), ErrorCode>(()));
    let (mut body_stream, _trailers) =
        bindings::wasi::http::types::Response::consume_body(response, done_rx);

    if status != 200 {
        let mut body = Vec::new();
        while let Some(b) = body_stream.next().await {
            body.push(b);
        }
        eprintln!("[patch-producer-ts] error body: {}", String::from_utf8_lossy(&body));
        return Err(ErrorCode::InternalError(Some(format!("status {status}"))));
    }

    Ok(body_stream)
}

async fn read_next_chunk(reader: &mut StreamReader<u8>) -> Option<String> {
    let mut buf = Vec::with_capacity(1024);
    while let Some(byte) = reader.next().await {
        buf.push(byte);
        if byte == b'\n' {
            break;
        }
    }
    if buf.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&buf).into_owned())
    }
}

bindings::export!(Component with_types_in bindings);
