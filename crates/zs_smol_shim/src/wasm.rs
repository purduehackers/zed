//! The wasm32-unknown-unknown half of the `smol` facade: stubs for the OS-bound modules and a
//! `spawn` that runs on the installed `smol::runtime::Runtime`.

pub mod fs;
pub mod net;
pub mod process;
mod spawn;
pub use spawn::spawn;

use std::sync::atomic::{AtomicBool, Ordering};

use wasm_bindgen::JsCast as _;

/// `ErrorKind::Unsupported`, "<what> is not available in the browser; use the Fs trait / the
/// remote protocol".
pub(crate) fn unsupported(what: &'static str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("{what} is not available in the browser; use the Fs trait / the remote protocol"),
    )
}

/// `globalThis.setTimeout(callback, millis)` through js-sys (no web-sys features). Logs once at
/// `warn` if called off the main thread: gpui_web workers never return to their event loop.
pub(crate) fn js_set_timeout(callback: Box<dyn FnOnce() + 'static>, millis: i32) {
    use wasm_bindgen::JsValue;
    use wasm_bindgen::closure::Closure;

    static WARNED_OFF_MAIN_THREAD: AtomicBool = AtomicBool::new(false);

    let global = js_sys::global();
    let on_main_thread =
        js_sys::Reflect::has(&global, &JsValue::from_str("document")).unwrap_or(false);
    if !on_main_thread && !WARNED_OFF_MAIN_THREAD.swap(true, Ordering::Relaxed) {
        log::warn!(
            "smol::spawn used off the main thread before smol::runtime::install_runtime; \
             the setTimeout fallback only fires when this worker yields to its event loop"
        );
    }

    let set_timeout = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .ok()
        .and_then(|value| value.dyn_into::<js_sys::Function>().ok());
    let Some(set_timeout) = set_timeout else {
        log::error!("smol: globalThis.setTimeout is missing; dropping a scheduled runnable");
        return;
    };
    let closure = Closure::once_into_js(callback);
    if let Err(error) = set_timeout.call2(&global, &closure, &JsValue::from(millis)) {
        log::error!("smol: setTimeout failed: {error:?}");
    }
}
