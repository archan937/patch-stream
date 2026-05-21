mod bindings {
    wit_bindgen::generate!({
        world: "patch-consumer",
        path: "../wit",
        generate_all,
        async: [
            "import:wasmcloud:patch-stream/patches@0.1.0#subscribe",
            "export:wasi:http/handler@0.3.0-rc-2026-03-15#handle",
            "import:wasi:http/types@0.3.0-rc-2026-03-15#[static]request.consume-body",
        ],
    });
}

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::patch_stream::patches;

struct Component;

impl Handler for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        // Extract prompt from request body JSON: {"prompt":"..."}
        let prompt = extract_prompt(request).await.unwrap_or_default();

        let headers = Fields::new();
        let _ = headers.append(
            &"content-type".to_string(),
            &b"application/x-ndjson".to_vec(),
        );

        let patches_rx = patches::subscribe(prompt).await;
        let (_trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));

        let (response, _result) = Response::new(headers, Some(patches_rx), trailers_rx);
        response
            .set_status_code(200)
            .map_err(|()| ErrorCode::InternalError(Some("set_status failed".into())))?;
        Ok(response)
    }
}

async fn extract_prompt(request: Request) -> Option<String> {
    let (_res_tx, res_rx) =
        bindings::wit_future::new(|| Ok::<(), ErrorCode>(()));
    let (body_reader, _trailers) = Request::consume_body(request, res_rx).await;

    let bytes = body_reader.collect().await;
    let body = String::from_utf8(bytes).ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    v.get("prompt")?.as_str().map(|s| s.to_owned())
}

bindings::export!(Component with_types_in bindings);
