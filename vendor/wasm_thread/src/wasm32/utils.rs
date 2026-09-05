use std::{
    io,
    num::NonZeroUsize,
    sync::{LockResult, Mutex, MutexGuard, TryLockError},
};

use wasm_bindgen::prelude::*;
use web_sys::{Blob, Url, WorkerGlobalScope};

pub fn available_parallelism() -> io::Result<NonZeroUsize> {
    if let Some(window) = web_sys::window() {
        return Ok(NonZeroUsize::new(window.navigator().hardware_concurrency() as usize).unwrap());
    }

    // `js_sys::global()` is `self`; a page with a `script-src` CSP without `'unsafe-eval'`
    // rejects evaluating it as script.
    if let Ok(worker) = js_sys::global().dyn_into::<WorkerGlobalScope>() {
        return Ok(NonZeroUsize::new(worker.navigator().hardware_concurrency() as usize).unwrap());
    }

    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "hardware_concurrency unsupported",
    ))
}

pub fn is_web_worker_thread() -> bool {
    js_sys::global().dyn_into::<WorkerGlobalScope>().is_ok()
}

/// Name of the global the embedding page may set to the URL of the `wasm_bindgen` generated
/// .js shim script, so no stack-trace heuristic is needed to find it.
pub const SHIM_URL_GLOBAL: &str = "__zsBindgenShimUrl";

/// Extracts path of the `wasm_bindgen` generated .js shim script.
///
/// Reads `globalThis.__zsBindgenShimUrl` ([`SHIM_URL_GLOBAL`]) when the page set it; otherwise
/// generates a javascript exception to obtain a stacktrace containing the current script URL
/// (the same heuristic as before, without `eval`).
pub fn get_wasm_bindgen_shim_script_path() -> String {
    if let Some(url) = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str(SHIM_URL_GLOBAL))
        .ok()
        .and_then(|value| value.as_string())
        .filter(|url| !url.is_empty())
    {
        return url;
    }
    script_path_from_stack().expect(
        "wasm_thread: cannot find the wasm-bindgen shim URL; set globalThis.__zsBindgenShimUrl or Builder::wasm_bindgen_shim_url",
    )
}

/// The first `<url>:<line>:<column>` frame of a fresh `Error().stack`, which is the shim
/// script that called into wasm.
fn script_path_from_stack() -> Option<String> {
    let error = js_sys::Error::new("");
    let stack = js_sys::Reflect::get(&error, &JsValue::from_str("stack")).ok()?.as_string()?;
    let matches = js_sys::RegExp::new(r"(?:\(|@)(\S+):\d+:\d+", "").exec(&stack)?;
    matches.get(1).as_string()
}

/// Generates worker entry script as URL encoded blob
pub fn get_worker_script(wasm_bindgen_shim_url: Option<String>) -> String {
    // Cache URL so that subsequent calls are less expensive
    static CACHED_URL: Mutex<Option<String>> = Mutex::new(None);

    if let Some(url) = CACHED_URL.lock_spin().unwrap().clone() {
        return url;
    }

    // If wasm bindgen shim url is not provided, try to obtain one automatically
    let wasm_bindgen_shim_url = wasm_bindgen_shim_url.unwrap_or_else(get_wasm_bindgen_shim_script_path);

    // Generate script from template
    #[cfg(feature = "es_modules")]
    let template = include_str!("js/web_worker_module.js");
    #[cfg(not(feature = "es_modules"))]
    let template = include_str!("js/web_worker.js");

    let script = template.replace("WASM_BINDGEN_SHIM_URL", &wasm_bindgen_shim_url);

    // Create url encoded blob
    let arr = js_sys::Array::new();
    arr.set(0, JsValue::from_str(&script));
    let blob = Blob::new_with_str_sequence(&arr).unwrap();
    let url = Url::create_object_url_with_blob(
        &blob
            .slice_with_f64_and_f64_and_content_type(0.0, blob.size(), "text/javascript")
            .unwrap(),
    )
    .unwrap();

    *CACHED_URL.lock_spin().unwrap() = Some(url.clone());

    url
}

/// A spin lock mutex extension.
///
/// Atomic wait panics in wasm main thread so we can't use `Mutex::lock()`.
/// This is a helper, which implement spinlock by calling `Mutex::try_lock()` in a loop.
/// Care must be taken not to introduce deadlocks when using this trait.
pub trait SpinLockMutex {
    type Inner;

    fn lock_spin<'a>(&'a self) -> LockResult<MutexGuard<'a, Self::Inner>>;
}

impl<T> SpinLockMutex for Mutex<T> {
    type Inner = T;

    fn lock_spin<'a>(&'a self) -> LockResult<MutexGuard<'a, Self::Inner>> {
        loop {
            match self.try_lock() {
                Ok(guard) => break Ok(guard),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(e)) => break Err(e),
            }
        }
    }
}
