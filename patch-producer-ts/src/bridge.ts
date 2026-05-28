// bridge.ts — generic Web API polyfills + HTTP streaming bridge for rquickjs.
//
// Zero app-specific imports. Copy this file into any rquickjs-based WASM/Rust
// component to get the full Web API surface and the HTTP streaming bridge.
//
// Optional host globals (gracefully skipped when not registered):
//   __print            — maps console.* to the host print function
//   __httpStreamAwaitPrev — async; serialises concurrent HTTP tasks (WASI P3)
//
// Required host globals (must be registered by the Rust host):
//   __httpStreamStart(url, headersJson, body)  — starts an HTTP task
//   __httpStreamRead() → string|null           — reads next SSE chunk

// console — rquickjs has no built-in console; wire it to __print (set by Rust).
if (!(globalThis as any).console) {
  const _print: (s: string) => void =
    (globalThis as any).__print ?? ((_: string) => {});
  const fmt = (...args: any[]) =>
    args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ");
  (globalThis as any).console = {
    log: (...a: any[]) => _print(fmt(...a)),
    info: (...a: any[]) => _print(fmt(...a)),
    warn: (...a: any[]) => _print("[warn] " + fmt(...a)),
    error: (...a: any[]) => _print("[error] " + fmt(...a)),
    debug: (...a: any[]) => _print(fmt(...a)),
  };
}

class QJSReadableStream {
  private _bytes: Uint8Array;
  constructor(bytes: Uint8Array) {
    this._bytes = bytes;
  }
  getReader() {
    let done = false;
    const bytes = this._bytes;
    return {
      read(): Promise<{ done: boolean; value?: Uint8Array }> {
        if (done) return Promise.resolve({ done: true });
        done = true;
        return Promise.resolve({ done: false, value: bytes });
      },
      cancel() {
        done = true;
      },
      releaseLock() {},
    };
  }
}

class QJSTextEncoder {
  encode(str: string): Uint8Array {
    const bytes: number[] = [];
    for (let i = 0; i < str.length; i++) {
      const c = str.charCodeAt(i);
      if (c < 0x80) bytes.push(c);
      else if (c < 0x800)
        bytes.push((c >> 6) | 0xc0, (c & 0x3f) | 0x80);
      else
        bytes.push(
          (c >> 12) | 0xe0,
          ((c >> 6) & 0x3f) | 0x80,
          (c & 0x3f) | 0x80
        );
    }
    return new Uint8Array(bytes);
  }
}

class QJSHeaders {
  private _data: Record<string, string> = {};
  constructor(init?: Record<string, string> | [string, string][]) {
    if (Array.isArray(init))
      init.forEach(([k, v]) => {
        this._data[k.toLowerCase()] = v;
      });
    else if (init)
      Object.entries(init).forEach(([k, v]) => {
        this._data[k.toLowerCase()] = v;
      });
  }
  get(k: string) {
    return this._data[k.toLowerCase()] ?? null;
  }
  set(k: string, v: string) {
    this._data[k.toLowerCase()] = v;
  }
  has(k: string) {
    return k.toLowerCase() in this._data;
  }
  forEach(fn: (v: string, k: string) => void) {
    Object.entries(this._data).forEach(([k, v]) => fn(v, k));
  }
  entries() {
    return Object.entries(this._data)[Symbol.iterator]();
  }
}

class QJSRequest {
  url: string;
  method: string;
  headers: QJSHeaders;
  body: string | null;
  constructor(url: string | { toString(): string }, init?: any) {
    this.url = url.toString();
    this.method = init?.method ?? "GET";
    this.headers = new QJSHeaders(init?.headers);
    this.body = init?.body ?? null;
  }
}

class QJSBlob {
  private _data: string;
  constructor(parts?: any[], _opts?: any) {
    this._data = (parts ?? []).map((p) => String(p)).join("");
  }
  text() { return Promise.resolve(this._data); }
  get size() { return this._data.length; }
}

class QJSFile extends QJSBlob {
  name: string;
  constructor(parts: any[], name: string, opts?: any) {
    super(parts, opts);
    this.name = name;
  }
}

class QJSFormData {
  private _data: [string, string][] = [];
  append(k: string, v: string) { this._data.push([k, v]); }
  get(k: string) { return this._data.find(([key]) => key === k)?.[1] ?? null; }
  has(k: string) { return this._data.some(([key]) => key === k); }
  entries() { return this._data[Symbol.iterator](); }
}

class QJSURLSearchParams {
  private _data: [string, string][] = [];
  constructor(init?: string | Record<string, string> | [string, string][]) {
    if (typeof init === "string") {
      const s = init.startsWith("?") ? init.slice(1) : init;
      s.split("&").forEach((pair) => {
        const [k, v = ""] = pair.split("=");
        if (k) this._data.push([decodeURIComponent(k), decodeURIComponent(v)]);
      });
    } else if (Array.isArray(init)) {
      this._data = [...init];
    } else if (init) {
      Object.entries(init).forEach(([k, v]) => this._data.push([k, v]));
    }
  }
  get(k: string) { return this._data.find(([key]) => key === k)?.[1] ?? null; }
  set(k: string, v: string) {
    const i = this._data.findIndex(([key]) => key === k);
    if (i >= 0) this._data[i][1] = v;
    else this._data.push([k, v]);
  }
  has(k: string) { return this._data.some(([key]) => key === k); }
  append(k: string, v: string) { this._data.push([k, v]); }
  delete(k: string) { this._data = this._data.filter(([key]) => key !== k); }
  toString() {
    return this._data
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join("&");
  }
  entries() { return this._data[Symbol.iterator](); }
  forEach(fn: (v: string, k: string) => void) {
    this._data.forEach(([k, v]) => fn(v, k));
  }
}

class QJSURL {
  href: string;
  origin: string;
  protocol: string;
  hostname: string;
  host: string;
  port: string;
  pathname: string;
  search: string;
  hash: string;
  searchParams: QJSURLSearchParams;

  constructor(url: string, base?: string) {
    let full = url;
    if (base && !url.match(/^[a-z][a-z+\-.]*:/i)) {
      full = base.replace(/\/$/, "") + "/" + url.replace(/^\//, "");
    }
    this.href = full;
    const m = full.match(
      /^(([a-z][a-z+\-.]*):\/\/([^/:?#]*)(?::(\d+))?)(\/[^?#]*)?(\?[^#]*)?(#.*)?$/i
    );
    if (m) {
      this.protocol = m[2] + ":";
      this.hostname = m[3];
      this.port = m[4] ?? "";
      this.host = this.hostname + (this.port ? ":" + this.port : "");
      this.origin = m[1];
      this.pathname = m[5] ?? "/";
      this.search = m[6] ?? "";
      this.hash = m[7] ?? "";
    } else {
      this.protocol = "";
      this.hostname = "";
      this.port = "";
      this.host = "";
      this.origin = "";
      this.pathname = full;
      this.search = "";
      this.hash = "";
    }
    this.searchParams = new QJSURLSearchParams(
      this.search ? this.search.slice(1) : ""
    );
  }

  toString() { return this.href; }
}

class QJSAbortController {
  signal: { aborted: boolean; onabort: null; addEventListener: () => void; removeEventListener: () => void };
  constructor() {
    this.signal = {
      aborted: false,
      onabort: null,
      addEventListener() {},
      removeEventListener() {},
    };
  }
  abort() {
    this.signal.aborted = true;
  }
}

class QJSTextDecoder {
  constructor(_enc = "utf-8") {}
  decode(input?: Uint8Array | ArrayBuffer | null): string {
    if (!input) return "";
    const bytes =
      input instanceof ArrayBuffer ? new Uint8Array(input) : (input as Uint8Array);
    let s = "";
    for (let i = 0; i < bytes.length; ) {
      const b = bytes[i++];
      if (b < 0x80) {
        s += String.fromCharCode(b);
      } else if ((b & 0xe0) === 0xc0) {
        s += String.fromCharCode(((b & 0x1f) << 6) | (bytes[i++] & 0x3f));
      } else if ((b & 0xf0) === 0xe0) {
        const b2 = bytes[i++];
        const b3 = bytes[i++];
        s += String.fromCharCode(((b & 0x0f) << 12) | ((b2 & 0x3f) << 6) | (b3 & 0x3f));
      } else {
        // surrogate pair for 4-byte UTF-8
        const b2 = bytes[i++];
        const b3 = bytes[i++];
        const b4 = bytes[i++];
        const cp =
          (((b & 0x07) << 18) | ((b2 & 0x3f) << 12) | ((b3 & 0x3f) << 6) | (b4 & 0x3f)) -
          0x10000;
        s += String.fromCharCode(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff));
      }
    }
    return s;
  }
}

if (!(globalThis as any).TextEncoder)
  (globalThis as any).TextEncoder = QJSTextEncoder;
if (!(globalThis as any).TextDecoder)
  (globalThis as any).TextDecoder = QJSTextDecoder;
if (!(globalThis as any).Headers) (globalThis as any).Headers = QJSHeaders;
if (!(globalThis as any).ReadableStream)
  (globalThis as any).ReadableStream = QJSReadableStream;
if (!(globalThis as any).Request) (globalThis as any).Request = QJSRequest;
if (!(globalThis as any).Response) (globalThis as any).Response = Object;
if (!(globalThis as any).Blob) (globalThis as any).Blob = QJSBlob;
if (!(globalThis as any).File) (globalThis as any).File = QJSFile;
if (!(globalThis as any).FormData) (globalThis as any).FormData = QJSFormData;
if (!(globalThis as any).AbortController)
  (globalThis as any).AbortController = QJSAbortController;

// setTimeout/clearTimeout: the Anthropic SDK uses setTimeout to schedule a
// request-timeout abort AND to delay retries. We defer via 2 microtask hops so
// that .finally() handlers (which call clearTimeout) run first — preventing the
// SDK's internal AbortController from being aborted before clearTimeout fires.
if (!(globalThis as any).setTimeout) {
  let _timerId = 0;
  const _cancelled = new Set<number>();
  (globalThis as any).setTimeout = function (fn: () => void, _ms?: number): number {
    const id = ++_timerId;
    Promise.resolve().then(() => {
      Promise.resolve().then(() => {
        if (!_cancelled.has(id)) fn();
        _cancelled.delete(id);
      });
    });
    return id;
  };
  (globalThis as any).clearTimeout = function (id: number) {
    _cancelled.add(id);
  };
}
if (!(globalThis as any).URL) (globalThis as any).URL = QJSURL;
if (!(globalThis as any).URLSearchParams)
  (globalThis as any).URLSearchParams = QJSURLSearchParams;

(globalThis as any).fetch = async function (
  url: string | { toString(): string },
  options?: { method?: string; headers?: any; body?: string }
): Promise<any> {
  const headersObj: Record<string, string> = {};
  const h = options?.headers;
  if (h) {
    if (typeof h.forEach === "function")
      h.forEach((v: string, k: string) => {
        headersObj[k] = v;
      });
    else Object.assign(headersObj, h);
  }

  // Wait for the previous HTTP task to exit (drops its WASI StreamReader) before
  // opening a new one.  Serialising reads avoids a wasmCloud P3 runtime crash:
  // "cannot read from stream after being notified that the writable end dropped".
  // Optional: not all hosts register __httpStreamAwaitPrev (e.g. stream-rs Rust server).
  await (globalThis as any).__httpStreamAwaitPrev?.();
  (globalThis as any).__httpStreamStart(
    url.toString(),
    JSON.stringify(headersObj),
    options?.body ?? ""
  );

  const encoder = new QJSTextEncoder();

  // __httpStreamRead may return a string|null (native Rust: sync via block_in_place)
  // or a Promise<string|null> (WASM: async via rquickjs Async<F> wrapper).
  function readChunk(): Promise<string | null> {
    const r = (globalThis as any).__httpStreamRead();
    if (r !== null && typeof r === "object" && typeof r.then === "function")
      return r as Promise<string | null>;
    return Promise.resolve(r as string | null);
  }

  function readAll(): Promise<string> {
    const parts: string[] = [];
    function step(): Promise<string> {
      return readChunk().then((chunk) => {
        if (chunk === null) return parts.join("");
        parts.push(chunk);
        return step();
      });
    }
    return step();
  }

  return Promise.resolve({
    ok: true,
    status: 200,
    statusText: "OK",
    headers: new QJSHeaders(),
    body: {
      getReader() {
        return {
          read(): Promise<{ done: boolean; value?: Uint8Array }> {
            return readChunk().then((chunk) => {
              if (chunk === null) return { done: true };
              return { done: false, value: encoder.encode(chunk) };
            });
          },
          cancel() {},
          releaseLock() {},
        };
      },
    },
    text: () => readAll(),
    json: () => readAll().then((t) => JSON.parse(t)),
    arrayBuffer: () => readAll().then((t) => encoder.encode(t).buffer),
    clone() {
      return this;
    },
  });
};
