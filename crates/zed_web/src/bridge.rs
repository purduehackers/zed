//! JS ↔ Rust bridge: the `ZsHost` callbacks the shell passes to `start()`
//! (CONTRACTS.md §8.4), boot progress, the session-refresh hook the transport calls before
//! every reconnect dial, the settings/keymap save-back, lifecycle forwarding, error
//! reporting and the `{ code, message }` rejection objects.
//!
//! JS values are main-thread only. Every function here runs on the GPUI foreground executor
//! (the main thread); the panic hook is the one exception and checks the thread itself.

use std::{
    cell::{Cell, RefCell},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context as _, Result, anyhow};
use gpui::{AsyncApp, Task};
use js_sys::{Function, Object, Promise, Reflect};
use project::lifecycle::LifecycleKind;
use remote::{RefreshError, RefreshReason, WebSocketSession, WebSocketSessionRefresh};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use zed_web_core::{BootError, BootStage, ConnectInfo, DocumentKind};

/// The `ZsHost` object: five required callbacks plus the optional `onClosed`.
pub struct JsHost {
    this: JsValue,
    boot_progress: Function,
    refresh_connect_info: Function,
    save_document: Function,
    report_error: Function,
    on_lifecycle: Function,
    on_closed: Option<Function>,
}

/// Which terminal refresh error the host last answered with; read next to the close code
/// when a session ends so the `stopped` detail names `workspace_stopped`/`unauthorized`
/// regardless of the close frame the transport synthesized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshErrorKind {
    /// `refreshConnectInfo` rejected with `{ code: "unauthorized" }`.
    Unauthorized,
    /// `refreshConnectInfo` rejected with `{ code: "stopped" }`.
    Stopped,
}

thread_local! {
    static HOST: RefCell<Option<JsHost>> = const { RefCell::new(None) };
    static SESSION: RefCell<Option<ConnectInfo>> = const { RefCell::new(None) };
    static LAST_REFRESH_ERROR: Cell<Option<RefreshErrorKind>> = const { Cell::new(None) };
    static READY_EMITTED: Cell<bool> = const { Cell::new(false) };
    static BOOTED: Cell<bool> = const { Cell::new(false) };
    /// The last `(stage, detail)` handed to `host.bootProgress` (read by the test hooks).
    static LAST_PROGRESS: RefCell<Option<(BootStage, String)>> = const { RefCell::new(None) };
}

/// Panic messages raised on `wasm_thread` workers, where JS host values are unreachable;
/// drained by the main thread on its next tick ([`report_pending_worker_panics`]).
static WORKER_PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static PANIC_HOOK_INSTALLED: AtomicBool = AtomicBool::new(false);

impl JsHost {
    /// Validates the host object: every required member must be a function.
    pub fn from_js(value: JsValue) -> Result<Self> {
        anyhow::ensure!(value.is_object(), "host is not an object");
        let required = |name: &str| -> Result<Function> {
            Reflect::get(&value, &JsValue::from_str(name))
                .ok()
                .and_then(|member| member.dyn_into::<Function>().ok())
                .ok_or_else(|| anyhow!("host.{name} is not a function"))
        };
        let optional = |name: &str| -> Option<Function> {
            Reflect::get(&value, &JsValue::from_str(name))
                .ok()
                .and_then(|member| member.dyn_into::<Function>().ok())
        };
        Ok(Self {
            boot_progress: required("bootProgress")?,
            refresh_connect_info: required("refreshConnectInfo")?,
            save_document: required("saveDocument")?,
            report_error: required("reportError")?,
            on_lifecycle: required("onLifecycle")?,
            on_closed: optional("onClosed"),
            this: value,
        })
    }

    /// Makes this the process-wide host.
    pub fn install(self) {
        HOST.with(|host| *host.borrow_mut() = Some(self));
    }
}

fn with_host<R>(f: impl FnOnce(&JsHost) -> R) -> Option<R> {
    HOST.with(|host| host.borrow().as_ref().map(f))
}

fn log_call_error(name: &str, result: Result<JsValue, JsValue>) {
    if let Err(error) = result {
        log::error!("host.{name} threw: {}", describe_js_value(&error));
    }
}

/// Reports a boot stage to the shell. `Ready` and `Reconnecting` also maintain the
/// once-per-episode bookkeeping of [`ready_once`].
pub fn progress(stage: BootStage, detail: &str) {
    match stage {
        BootStage::Ready => {
            READY_EMITTED.with(|ready| ready.set(true));
            BOOTED.with(|booted| booted.set(true));
        }
        BootStage::Reconnecting => READY_EMITTED.with(|ready| ready.set(false)),
        _ => {}
    }
    log::info!("boot progress: {} {detail}", stage.as_str());
    LAST_PROGRESS.with(|last| *last.borrow_mut() = Some((stage, detail.to_string())));
    with_host(|host| {
        log_call_error(
            "bootProgress",
            host.boot_progress.call2(
                &host.this,
                &JsValue::from_str(stage.as_str()),
                &JsValue::from_str(detail),
            ),
        )
    });
}

/// The last stage and detail reported through [`progress`], if any.
#[cfg(feature = "test-hooks")]
pub fn last_progress() -> Option<(BootStage, String)> {
    LAST_PROGRESS.with(|last| last.borrow().clone())
}

/// Emits `Ready` unless it was already emitted for the current connection episode (a
/// reconnect starts a new one).
pub fn ready_once() {
    if !READY_EMITTED.with(|ready| ready.get()) {
        progress(BootStage::Ready, "");
    }
}

/// Whether boot has reached `Ready` at least once; afterwards every transport status
/// belongs to a reconnect episode rather than to the boot's `connecting` stage.
pub fn booted() -> bool {
    BOOTED.with(|booted| booted.get())
}

/// The latest `/connect` result (the boot one, then every refresh).
pub fn current_session() -> Option<ConnectInfo> {
    SESSION.with(|session| session.borrow().clone())
}

/// Records a `/connect` result as the current session.
pub fn set_session(info: ConnectInfo) {
    SESSION.with(|session| *session.borrow_mut() = Some(info));
}

/// The last terminal refresh error, if the host answered one.
pub fn last_refresh_error() -> Option<RefreshErrorKind> {
    LAST_REFRESH_ERROR.with(|error| error.get())
}

/// Seconds before `sessionExpiresAt` at which an HTTP caller refreshes first.
const TOKEN_REFRESH_MARGIN_SECS: i64 = 60;

/// Refreshes the session through the host when the current token expires within
/// [`TOKEN_REFRESH_MARGIN_SECS`]; HTTP callers (`/files`, extension assets) call it before
/// each request. The socket itself survives token expiry.
pub fn ensure_fresh_token(cx: &mut AsyncApp) -> Task<Result<()>> {
    let expires_soon = current_session()
        .and_then(|session| session.session_expires_at)
        .and_then(|expires_at| {
            time::OffsetDateTime::parse(&expires_at, &time::format_description::well_known::Rfc3339)
                .ok()
        })
        .is_some_and(|expires_at| {
            (expires_at - time::OffsetDateTime::now_utc()).whole_seconds()
                <= TOKEN_REFRESH_MARGIN_SECS
        });
    if !expires_soon {
        return Task::ready(Ok(()));
    }
    cx.spawn(async move |_| {
        refresh_connect_info()
            .await
            .map(|_| ())
            .map_err(|error| anyhow!("{error}"))
    })
}

/// The transport's refresh hook over `host.refreshConnectInfo()` (b1
/// `WebSocketSessionRefresh`). The `workspace_id` argument is ignored: the host already
/// knows its workspace.
pub struct JsSessionRefresh;

impl WebSocketSessionRefresh for JsSessionRefresh {
    fn refresh(
        &self,
        _workspace_id: &str,
        reason: RefreshReason,
        cx: &mut AsyncApp,
    ) -> Task<Result<WebSocketSession, RefreshError>> {
        log::info!("refreshing the session: {reason:?}");
        cx.spawn(async move |_| refresh_connect_info().await)
    }
}

async fn refresh_connect_info() -> Result<WebSocketSession, RefreshError> {
    let call = with_host(|host| host.refresh_connect_info.call0(&host.this))
        .ok_or_else(|| RefreshError::Other(anyhow!("no host is installed")))?;
    let promise = match call {
        Ok(value) => value,
        Err(error) => return Err(map_refresh_rejection(error)),
    };
    let promise: Promise = promise.dyn_into().map_err(|_| {
        RefreshError::Other(anyhow!("host.refreshConnectInfo did not return a promise"))
    })?;
    match JsFuture::from(promise).await {
        Ok(value) => {
            let info = connect_info_from_js(&value).map_err(RefreshError::Other)?;
            let session = WebSocketSession {
                url: info.ws_url.clone(),
                token: info.token.clone(),
                session_id: info.session_id.clone(),
            };
            set_session(info);
            Ok(session)
        }
        Err(error) => Err(map_refresh_rejection(error)),
    }
}

fn map_refresh_rejection(error: JsValue) -> RefreshError {
    match js_string_property(&error, "code").as_deref() {
        Some("unauthorized") => {
            LAST_REFRESH_ERROR.with(|last| last.set(Some(RefreshErrorKind::Unauthorized)));
            RefreshError::Unauthorized
        }
        Some("stopped") => {
            LAST_REFRESH_ERROR.with(|last| last.set(Some(RefreshErrorKind::Stopped)));
            RefreshError::Stopped
        }
        _ => RefreshError::Other(anyhow!(
            "host.refreshConnectInfo rejected: {}",
            describe_js_value(&error)
        )),
    }
}

/// Parses a `connect` object (the boot config's shape) handed back by the host.
pub fn connect_info_from_js(value: &JsValue) -> Result<ConnectInfo> {
    let json = js_sys::JSON::stringify(value)
        .map_err(|error| anyhow!("connect info is not JSON: {}", describe_js_value(&error)))?;
    let json: String = json.into();
    serde_json::from_str(&json).context("invalid connect info")
}

/// Saves a user document through `host.saveDocument(kind, json)`.
pub async fn save_document(kind: DocumentKind, json: String) -> Result<()> {
    let call = with_host(|host| {
        host.save_document.call2(
            &host.this,
            &JsValue::from_str(kind.as_str()),
            &JsValue::from_str(&json),
        )
    })
    .ok_or_else(|| anyhow!("no host is installed"))?;
    let promise =
        call.map_err(|error| anyhow!("host.saveDocument threw: {}", describe_js_value(&error)))?;
    let promise: Promise = promise
        .dyn_into()
        .map_err(|_| anyhow!("host.saveDocument did not return a promise"))?;
    JsFuture::from(promise)
        .await
        .map(|_| ())
        .map_err(|error| anyhow!("host.saveDocument rejected: {}", describe_js_value(&error)))
}

/// Forwards a lifecycle notice to `host.onLifecycle(kind, seconds)` (snake_case kinds, D29).
pub fn lifecycle(kind: LifecycleKind, seconds: u32) {
    with_host(|host| {
        log_call_error(
            "onLifecycle",
            host.on_lifecycle.call2(
                &host.this,
                &JsValue::from_str(kind.as_str()),
                &JsValue::from_f64(f64::from(seconds)),
            ),
        )
    });
}

/// Forwards the server's last close frame to the optional `host.onClosed({ code, reason })`.
pub fn on_closed(code: u16, reason: &str) {
    with_host(|host| {
        let Some(on_closed) = &host.on_closed else {
            return;
        };
        let info = Object::new();
        Reflect::set(
            &info,
            &JsValue::from_str("code"),
            &JsValue::from_f64(f64::from(code)),
        )
        .ok();
        Reflect::set(
            &info,
            &JsValue::from_str("reason"),
            &JsValue::from_str(reason),
        )
        .ok();
        log_call_error("onClosed", on_closed.call1(&host.this, &info));
    });
}

/// Reports a panic or boot failure through `host.reportError(kind, message, stack)`.
pub fn report_error(kind: &str, message: &str, stack: &str) {
    with_host(|host| {
        log_call_error(
            "reportError",
            host.report_error.call3(
                &host.this,
                &JsValue::from_str(kind),
                &JsValue::from_str(message),
                &JsValue::from_str(stack),
            ),
        )
    });
}

/// Whether this is the document's main thread (workers have no `Window` global).
pub fn is_main_thread() -> bool {
    js_sys::global().is_instance_of::<web_sys::Window>()
}

/// Chains a reporting hook onto the console hook `web_init` installed. On the main thread
/// the panic goes to `host.reportError("panic", ..)` directly; on a worker it is queued for
/// [`report_pending_worker_panics`]. Idempotent.
pub fn install_panic_hook() {
    if PANIC_HOOK_INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        previous(info);
        let message = info.to_string();
        if is_main_thread() {
            report_error("panic", &message, "");
        } else if let Ok(mut pending) = WORKER_PANICS.lock() {
            pending.push(message);
        }
    }));
}

/// Reports panics queued by worker threads; called from a foreground tick.
pub fn report_pending_worker_panics() {
    let pending: Vec<String> = match WORKER_PANICS.lock() {
        Ok(mut pending) => pending.drain(..).collect(),
        Err(_) => return,
    };
    for message in pending {
        report_error("panic", &message, "");
    }
}

/// Maps an `anyhow::Error` to the `{ code, message }` rejection object.
pub fn boot_error(code: &'static str) -> impl Fn(anyhow::Error) -> JsValue {
    move |error| js_error(BootError::new(code, format!("{error:#}")))
}

/// Builds the `{ code, message }` rejection object.
pub fn js_error(error: BootError) -> JsValue {
    let object = Object::new();
    Reflect::set(
        &object,
        &JsValue::from_str("code"),
        &JsValue::from_str(error.code),
    )
    .ok();
    Reflect::set(
        &object,
        &JsValue::from_str("message"),
        &JsValue::from_str(&error.message),
    )
    .ok();
    object.into()
}

fn js_string_property(value: &JsValue, name: &str) -> Option<String> {
    Reflect::get(value, &JsValue::from_str(name))
        .ok()
        .and_then(|property| property.as_string())
}

/// A readable rendering of a thrown value: its `message` when it has one, else its string form.
pub fn describe_js_value(value: &JsValue) -> String {
    js_string_property(value, "message")
        .or_else(|| value.as_string())
        .or_else(|| js_sys::JSON::stringify(value).ok().map(String::from))
        .unwrap_or_else(|| "unknown error".to_string())
}
