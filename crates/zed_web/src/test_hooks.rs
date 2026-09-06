//! `window.__zs_test`: asynchronous browser test hooks over the real workspace, editor and
//! terminal entities, compiled only with the `test-hooks` cargo feature. Never part of a
//! production bundle: `script/build-web --test-hooks` is the only thing that enables the
//! feature, and it emits to a separate `<build>-test` directory.
//!
//! Every hook is a JS function returning a `Promise`. The work runs on the GPUI foreground
//! executor (the main thread) through the app's `AsyncApp`, exactly like the shell-facing
//! exports of `zed_web.rs`; nothing here touches JS values off the main thread. Hooks that
//! need the workspace window reject until the boot has opened it, so a test waits on
//! `waitIdle()` (or polls `connectionState()`) instead of sleeping.
//!
//! Surface (`apps/web/tests/e2e-browser/hooks.ts` mirrors it):
//! `connectionState()`, `connectionEvents()`, `waitIdle(timeoutMs?)`, `hostOs()`,
//! `openFile(path)`, `openItems()`, `bufferText(path)`, `activeBufferText()`,
//! `insertText(text)`, `moveCursorEnd()`, `save()`, `isDirty(path)`, `languageServers()`,
//! `spawnTerminal(cwd?)`, `terminals()`, `terminalInput(id, text)`,
//! `terminalScrollback(id)`, `lifecycleEvents()`, `completionsVisible()`, `contextMenu()`,
//! `triggerCompletion()`, `clientStateVersion()`, `clientStateStatus()`,
//! `clientStateEvents()`, `visibilityEvents()`, `aiKeys()`, `forceDisconnect()`,
//! `executorProbe()`, `sharedContentionProbe()`, `workspaceLayout()`, `closeDocks()`.
//!
//! The `*Events()` hooks are logs the wiring in `boot` and `connect` appends to as things
//! happen (connection-state transitions, `Reconnected`/`Disconnected`, client-state saves,
//! `set_hidden` calls and their flushes), so a test asserts on what happened rather than
//! sampling a transient state.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    path::PathBuf,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result};
use credentials_provider::CredentialsProvider as _;
use db::client_state::ClientStateEvent;
use editor::{
    Editor,
    actions::{MoveToEnd, ShowCompletions},
};
use gpui::{App, AsyncApp, Entity, WindowHandle};
use js_sys::{Array, Object, Promise, Reflect};
use language::Buffer;
use language_model::LanguageModelRegistry;
use project::lifecycle::LifecycleKind;
use remote::{CloseInfo, ConnectionState, RemoteConnectionOptions};
use task::RevealStrategy;
use terminal::Terminal;
use terminal_view::{TerminalView, terminal_panel::TerminalPanel};
use wasm_bindgen::{JsValue, prelude::Closure};
use wasm_bindgen_futures::future_to_promise;
use workspace::{MultiWorkspace, SaveIntent, Workspace};
use zed_web_core::{BootStage, HostOs, ai_proxy};

use crate::{ai::ProxyCredentialsProvider, boot, bridge, connect};

mod shared_contention;

/// Default for `waitIdle()`: past the boot's own 90 s budget (`boot::BOOT_TIMEOUT`), so a
/// slow-but-legal boot reports its own `boot_timeout` instead of this hook's deadline; a
/// caller passes a shorter or longer `timeoutMs` when it knows better.
const WAIT_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// How long `spawnTerminal()` waits for the server to assign the remote terminal id once
/// the spawn task itself succeeded.
const TERMINAL_ID_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);

struct LifecycleEvent {
    kind: String,
    seconds: u32,
    at_ms: f64,
}

struct ConnectionEvent {
    /// `state` (a `ConnectionState` transition), `reconnected` or `disconnected`
    /// (`RemoteClientEvent`).
    kind: &'static str,
    from: Option<&'static str>,
    to: Option<&'static str>,
    close_code: Option<u16>,
    close_reason: Option<String>,
    at_ms: f64,
}

struct ClientStateRecord {
    /// `saved`, `stale`, `save_failed` or `read_only`.
    kind: &'static str,
    version: Option<u64>,
    /// Whether the store reported the tab hidden when the event fired.
    hidden: bool,
    detail: String,
    at_ms: f64,
}

struct VisibilityRecord {
    hidden: bool,
    at_ms: f64,
    /// Filled in when the store's `set_hidden` task resolved.
    flushed_ms: Option<f64>,
    flush_ok: Option<bool>,
    flush_error: String,
    version_after: Option<u64>,
}

thread_local! {
    static LIFECYCLE: RefCell<Vec<LifecycleEvent>> = const { RefCell::new(Vec::new()) };
    static CONNECTION: RefCell<Vec<ConnectionEvent>> = const { RefCell::new(Vec::new()) };
    static CLIENT_STATE: RefCell<Vec<ClientStateRecord>> = const { RefCell::new(Vec::new()) };
    static VISIBILITY: RefCell<Vec<VisibilityRecord>> = const { RefCell::new(Vec::new()) };
    static HOST_OS: Cell<Option<HostOs>> = const { Cell::new(None) };
    static AI: RefCell<Option<(String, Arc<ProxyCredentialsProvider>)>> =
        const { RefCell::new(None) };
}

/// Records the page origin and the AI-proxy credentials provider `ai::install` returned, so
/// `aiKeys()` can probe the live one rather than a second instance; called from
/// `init::init_before_connect`.
pub fn record_ai_credentials(origin: &str, provider: Arc<ProxyCredentialsProvider>) {
    AI.with(|ai| *ai.borrow_mut() = Some((origin.to_owned(), provider)));
}

fn ai_credentials() -> Result<(String, Arc<ProxyCredentialsProvider>)> {
    AI.with(|ai| ai.borrow().clone())
        .context("the AI proxy credentials provider is not installed yet")
}

/// Records a lifecycle notice for `lifecycleEvents()`; called from `boot::handle_lifecycle`.
pub fn record_lifecycle(kind: LifecycleKind, seconds: u32) {
    LIFECYCLE.with(|events| {
        events.borrow_mut().push(LifecycleEvent {
            kind: kind.as_str().to_string(),
            seconds,
            at_ms: js_sys::Date::now(),
        })
    });
}

/// The host OS the keymap layer was told (`keymap::host_os`); called from `boot_in_app`.
pub fn record_host_os(os: HostOs) {
    HOST_OS.with(|slot| slot.set(Some(os)));
}

/// A `ConnectionState` transition of the session's `RemoteClient`; called from
/// `connect::observe`.
pub fn record_connection_transition(from: ConnectionState, to: ConnectionState) {
    CONNECTION.with(|events| {
        events.borrow_mut().push(ConnectionEvent {
            kind: "state",
            from: Some(connection_state_name(from)),
            to: Some(connection_state_name(to)),
            close_code: None,
            close_reason: None,
            at_ms: js_sys::Date::now(),
        })
    });
}

/// A `RemoteClientEvent` (`reconnected` or `disconnected`, with the last close frame when
/// there is one); called from `connect::observe`.
pub fn record_connection_event(kind: &'static str, close: Option<&CloseInfo>) {
    CONNECTION.with(|events| {
        events.borrow_mut().push(ConnectionEvent {
            kind,
            from: None,
            to: None,
            close_code: close.map(|close| close.code),
            close_reason: close.map(|close| close.reason.clone()),
            at_ms: js_sys::Date::now(),
        })
    });
}

/// A `ClientStateEvent` of the global store; called from the boot's store subscription.
pub fn record_client_state_event(event: &ClientStateEvent, hidden: bool) {
    let (kind, version, detail) = match event {
        ClientStateEvent::Saved { version } => ("saved", Some(*version), String::new()),
        ClientStateEvent::Stale { server_version } => {
            ("stale", Some(*server_version), String::new())
        }
        ClientStateEvent::SaveFailed(error) => ("save_failed", None, error.clone()),
        ClientStateEvent::ReadOnly(reason) => ("read_only", None, reason.clone()),
    };
    CLIENT_STATE.with(|events| {
        events.borrow_mut().push(ClientStateRecord {
            kind,
            version,
            hidden,
            detail,
            at_ms: js_sys::Date::now(),
        })
    });
}

/// A `set_hidden(hidden)` call from the shell; returns the record's index for
/// [`record_visibility_flush`]. Called from `boot::set_hidden`.
pub fn record_visibility(hidden: bool) -> usize {
    VISIBILITY.with(|events| {
        let mut events = events.borrow_mut();
        events.push(VisibilityRecord {
            hidden,
            at_ms: js_sys::Date::now(),
            flushed_ms: None,
            flush_ok: None,
            flush_error: String::new(),
            version_after: None,
        });
        events.len() - 1
    })
}

/// The outcome of the store's `set_hidden` task for the record at `index` (the hidden
/// flush, or the immediate `Ok` of the visible case) and the store version afterwards.
pub fn record_visibility_flush(index: usize, result: &Result<()>, version_after: Option<u64>) {
    VISIBILITY.with(|events| {
        if let Some(record) = events.borrow_mut().get_mut(index) {
            record.flushed_ms = Some(js_sys::Date::now());
            record.flush_ok = Some(result.is_ok());
            record.flush_error = result
                .as_ref()
                .err()
                .map(|error| format!("{error:#}"))
                .unwrap_or_default();
            record.version_after = version_after;
        }
    });
}

/// Installs `window.__zs_test`. Idempotent; called once from `boot::run`.
pub fn install() {
    let Some(window) = web_sys::window() else {
        return;
    };
    let hooks = Object::new();
    hook0(&hooks, "connectionState", connection_state);
    hook0(&hooks, "connectionEvents", connection_events);
    hook1(&hooks, "waitIdle", wait_idle);
    hook0(&hooks, "hostOs", host_os);
    hook1(&hooks, "openFile", open_file);
    hook0(&hooks, "openItems", open_items);
    hook1(&hooks, "bufferText", buffer_text);
    hook1(&hooks, "bufferSyntax", buffer_syntax);
    hook0(&hooks, "commandPaletteVisible", command_palette_visible);
    hook0(&hooks, "activeBufferText", active_buffer_text);
    hook1(&hooks, "insertText", insert_text);
    hook0(&hooks, "moveCursorEnd", move_cursor_end);
    hook0(&hooks, "save", save);
    hook1(&hooks, "isDirty", is_dirty);
    hook0(&hooks, "languageServers", language_servers);
    hook1(&hooks, "spawnTerminal", spawn_terminal);
    hook0(&hooks, "terminals", terminals);
    hook2(&hooks, "terminalInput", terminal_input);
    hook1(&hooks, "terminalScrollback", terminal_scrollback);
    hook0(&hooks, "lifecycleEvents", lifecycle_events);
    hook0(&hooks, "completionsVisible", completions_visible);
    hook0(&hooks, "contextMenu", context_menu);
    hook0(&hooks, "triggerCompletion", trigger_completion);
    hook0(&hooks, "clientStateVersion", client_state_version);
    hook0(&hooks, "clientStateStatus", client_state_status);
    hook0(&hooks, "clientStateEvents", client_state_events);
    hook0(&hooks, "visibilityEvents", visibility_events);
    hook0(&hooks, "aiKeys", ai_keys);
    hook0(&hooks, "forceDisconnect", force_disconnect);
    hook0(&hooks, "executorProbe", executor_probe);
    hook0(&hooks, "sharedContentionProbe", shared_contention::probe);
    hook0(&hooks, "workspaceLayout", workspace_layout);
    hook0(&hooks, "closeDocks", close_docks);
    Reflect::set(&window, &JsValue::from_str("__zs_test"), &hooks).ok();
    log::info!("test hooks installed on window.__zs_test");
}

fn hook0<F, Fut>(target: &Object, name: &str, f: F)
where
    F: Fn() -> Fut + 'static,
    Fut: Future<Output = Result<JsValue>> + 'static,
{
    let closure = Closure::<dyn Fn() -> Promise>::new(move || {
        let future = f();
        future_to_promise(async move { future.await.map_err(js_err) })
    });
    Reflect::set(target, &JsValue::from_str(name), closure.as_ref()).ok();
    closure.forget();
}

fn hook1<F, Fut>(target: &Object, name: &str, f: F)
where
    F: Fn(JsValue) -> Fut + 'static,
    Fut: Future<Output = Result<JsValue>> + 'static,
{
    let closure = Closure::<dyn Fn(JsValue) -> Promise>::new(move |a: JsValue| {
        let future = f(a);
        future_to_promise(async move { future.await.map_err(js_err) })
    });
    Reflect::set(target, &JsValue::from_str(name), closure.as_ref()).ok();
    closure.forget();
}

fn hook2<F, Fut>(target: &Object, name: &str, f: F)
where
    F: Fn(JsValue, JsValue) -> Fut + 'static,
    Fut: Future<Output = Result<JsValue>> + 'static,
{
    let closure =
        Closure::<dyn Fn(JsValue, JsValue) -> Promise>::new(move |a: JsValue, b: JsValue| {
            let future = f(a, b);
            future_to_promise(async move { future.await.map_err(js_err) })
        });
    Reflect::set(target, &JsValue::from_str(name), closure.as_ref()).ok();
    closure.forget();
}

fn js_err(error: anyhow::Error) -> JsValue {
    js_sys::Error::new(&format!("{error:#}")).into()
}

fn set(target: &Object, key: &str, value: impl Into<JsValue>) {
    Reflect::set(target, &JsValue::from_str(key), &value.into()).ok();
}

fn opt_str(value: Option<&str>) -> JsValue {
    value.map(JsValue::from_str).unwrap_or(JsValue::NULL)
}

fn opt_num(value: Option<u64>) -> JsValue {
    value
        .map(|value| JsValue::from_f64(value as f64))
        .unwrap_or(JsValue::NULL)
}

fn string_arg(value: &JsValue, name: &str) -> Result<String> {
    value
        .as_string()
        .with_context(|| format!("{name} must be a string"))
}

fn optional_string_arg(value: &JsValue) -> Option<String> {
    value.as_string().filter(|value| !value.is_empty())
}

/// A remote terminal id is a random `u64` (b3), which does not fit a JS number's exact
/// integer range, so the hooks carry it as a decimal string both ways; a number is accepted
/// only while it is still exact.
fn id_arg(value: &JsValue) -> Result<u64> {
    if let Some(text) = value.as_string() {
        return text
            .trim()
            .parse::<u64>()
            .with_context(|| format!("id {text:?} is not a u64"));
    }
    let number = value.as_f64().context("id must be a decimal string")?;
    anyhow::ensure!(
        number >= 0.0 && number <= 9_007_199_254_740_992.0 && number.fract() == 0.0,
        "id {number} is not an exact integer; pass the decimal string the hooks returned"
    );
    Ok(number as u64)
}

fn opt_id(value: Option<u64>) -> JsValue {
    value
        .map(|id| JsValue::from_str(&id.to_string()))
        .unwrap_or(JsValue::NULL)
}

/// The app, the workspace window and the workspace; rejects until the boot opened them.
fn app_window() -> Result<(AsyncApp, WindowHandle<MultiWorkspace>, Entity<Workspace>)> {
    let cx = boot::async_app()?;
    let window = boot::window().context("the workspace window is not open yet")?;
    let workspace = boot::workspace().context("the workspace is not open yet")?;
    Ok((cx, window, workspace))
}

/// The actual dock visibility and active panels, plus the live merged title-bar settings
/// consumed by `title_bar::TitleBarSettings::from_settings` (that type is crate-private).
/// Reading the store, rather than the default JSON, includes every settings override.
async fn workspace_layout() -> Result<JsValue> {
    let (mut cx, window, workspace) = app_window()?;
    // `ready` reports the window opened, but panel loading can still be pending. Await
    // the same task the native entry point uses before exposing dock state to a test.
    if let Some(panels) =
        cx.update(|cx| workspace.update(cx, |workspace, _| workspace.take_panels_task()))
    {
        panels.await.context("loading workspace panels")?;
    }
    let state = window.update(&mut cx, |_, window, cx| {
        let title_bar = cx
            .global::<settings::SettingsStore>()
            .merged_settings()
            .title_bar
            .as_ref()
            .context("the title-bar settings are not installed")?;
        anyhow::Ok(serde_json::json!({
            "docks": workspace.read(cx).capture_dock_state(window, cx),
            "titleBar": title_bar,
        }))
    })??;
    js_sys::JSON::parse(&state.to_string())
        .map_err(|error| anyhow::anyhow!("serializing workspace layout: {error:?}"))
}

/// Closes the docks through the normal workspace operation and waits for its local DB
/// serialization. The test still drives the shell's real visibility-change path to flush
/// that image to the server before reloading.
async fn close_docks() -> Result<JsValue> {
    let (mut cx, window, workspace) = app_window()?;
    let task = window.update(&mut cx, |_, window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.close_all_docks(window, cx);
            workspace.flush_serialization(window, cx)
        })
    })?;
    task.await;
    Ok(JsValue::TRUE)
}

fn connection_state_name(state: ConnectionState) -> &'static str {
    match state {
        ConnectionState::Connecting => "connecting",
        ConnectionState::Connected => "connected",
        ConnectionState::HeartbeatMissed => "heartbeat_missed",
        ConnectionState::Reconnecting => "reconnecting",
        ConnectionState::Disconnected => "disconnected",
    }
}

/// `{ phase, detail, booted, connection, epoch, resumed, closeCode, closeReason, closeDetail }`.
/// Works before the app starts (`connection: "none"`). `closeDetail` is the product's own
/// `close_code_detail` name for the last close frame (`connect.rs`: the `stopped` detail the
/// shell is told), never a table of this module's.
async fn connection_state() -> Result<JsValue> {
    let out = Object::new();
    let (stage, detail) = bridge::last_progress()
        .map(|(stage, detail)| (stage.as_str(), detail))
        .unwrap_or(("none", String::new()));
    set(&out, "phase", stage);
    set(&out, "detail", detail.as_str());
    set(&out, "booted", bridge::booted());
    set(&out, "connection", "none");
    set(&out, "epoch", JsValue::NULL);
    set(&out, "resumed", JsValue::NULL);
    set(&out, "closeCode", JsValue::NULL);
    set(&out, "closeReason", JsValue::NULL);
    set(&out, "closeDetail", JsValue::NULL);
    let (Ok(cx), Some(remote)) = (boot::async_app(), boot::remote_client()) else {
        return Ok(out.into());
    };
    cx.update(|cx| {
        let remote = remote.read(cx);
        set(
            &out,
            "connection",
            connection_state_name(remote.connection_state()),
        );
        if let RemoteConnectionOptions::WebSocket(options) = remote.connection_options() {
            if let Some(info) = options.server_info() {
                set(&out, "epoch", JsValue::from_f64(info.epoch as f64));
                set(&out, "resumed", info.resumed);
            }
            if let Some(close) = options.last_close() {
                set(&out, "closeCode", JsValue::from_f64(f64::from(close.code)));
                set(&out, "closeReason", close.reason.as_str());
                let (detail, _) =
                    connect::close_code_detail(Some(&close), bridge::last_refresh_error());
                set(&out, "closeDetail", detail);
            }
        }
    });
    Ok(out.into())
}

/// `[{ kind, from, to, closeCode, closeReason, at }]`, oldest first: every
/// `ConnectionState` transition (`kind: "state"`) and every `RemoteClientEvent`
/// (`reconnected`, `disconnected`) since the dial.
async fn connection_events() -> Result<JsValue> {
    let out = Array::new();
    CONNECTION.with(|events| {
        for event in events.borrow().iter() {
            let entry = Object::new();
            set(&entry, "kind", event.kind);
            set(&entry, "from", opt_str(event.from));
            set(&entry, "to", opt_str(event.to));
            set(
                &entry,
                "closeCode",
                event
                    .close_code
                    .map(|code| JsValue::from_f64(f64::from(code)))
                    .unwrap_or(JsValue::NULL),
            );
            set(
                &entry,
                "closeReason",
                opt_str(event.close_reason.as_deref()),
            );
            set(&entry, "at", JsValue::from_f64(event.at_ms));
            out.push(&entry);
        }
    });
    Ok(out.into())
}

/// `"mac" | "windows" | "linux"` as the keymap layer saw it (D12), `null` before the boot
/// decided.
async fn host_os() -> Result<JsValue> {
    Ok(opt_str(HOST_OS.with(|slot| slot.get()).map(HostOs::as_str)))
}

fn is_connected(cx: &AsyncApp) -> bool {
    boot::remote_client()
        .map(|remote| {
            cx.update(|cx| remote.read(cx).connection_state() == ConnectionState::Connected)
        })
        .unwrap_or(false)
}

/// The boot's terminal outcome, when it has one: `failed` (a boot error, `boot_timeout`
/// included) or `stopped` (a terminal disconnect). `waitIdle()` rejects on it at once.
fn terminal_progress() -> Option<(BootStage, String)> {
    match bridge::last_progress() {
        Some((stage @ (BootStage::Failed | BootStage::Stopped), detail)) => Some((stage, detail)),
        _ => None,
    }
}

/// A JS `setTimeout` as a future: the only clock available before the GPUI app exists
/// (the hooks are installed at the top of `boot::run`, ahead of `run_embedded`).
async fn js_sleep(duration: Duration) {
    let promise = Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            window
                .set_timeout_with_callback_and_timeout_and_arguments_0(
                    &resolve,
                    duration.as_millis() as i32,
                )
                .ok();
        }
    });
    wasm_bindgen_futures::JsFuture::from(promise).await.ok();
}

/// Resolves once the boot reached `ready` at least once, the workspace is open, the transport
/// is `Connected` (again, after a reconnect) and the foreground executor has drained what the
/// boot queued on it. Callable as soon as the hooks exist: it waits for the app itself first.
/// State-driven: it rejects as soon as the boot reports `failed` or `stopped` (the product's
/// own error is the message), and only otherwise on the deadline (`timeoutMs`, default
/// [`WAIT_IDLE_TIMEOUT`]).
///
/// Every wait here is on [`js_sleep`] (a main-thread `setTimeout`), never on a GPUI timer: a
/// wedged executor, or a wedged hop back to the main thread, is exactly what this hook exists
/// to report, and a deadline tested only after a GPUI timer resolved is unreachable in that
/// state.
async fn wait_idle(timeout_ms: JsValue) -> Result<JsValue> {
    let timeout = timeout_ms
        .as_f64()
        .filter(|ms| ms.is_finite() && *ms > 0.0)
        .map(|ms| Duration::from_millis(ms as u64))
        .unwrap_or(WAIT_IDLE_TIMEOUT);
    let started = web_time::Instant::now();
    let cx = loop {
        if let Some((stage, detail)) = terminal_progress() {
            anyhow::bail!(
                "waitIdle: the boot ended with {} {detail} before the app started",
                stage.as_str()
            );
        }
        match boot::async_app() {
            Ok(cx) => break cx,
            Err(_) => {
                if started.elapsed() > timeout {
                    anyhow::bail!(
                        "waitIdle: the app did not start within {}s (last progress {:?})",
                        timeout.as_secs(),
                        bridge::last_progress()
                    );
                }
                js_sleep(POLL).await;
            }
        }
    };
    loop {
        if let Some((stage, detail)) = terminal_progress() {
            anyhow::bail!(
                "waitIdle: the session ended with {} {detail}",
                stage.as_str()
            );
        }
        if bridge::booted() && boot::workspace().is_some() && is_connected(&cx) {
            break;
        }
        if started.elapsed() > timeout {
            anyhow::bail!(
                "waitIdle: not idle within {}s (last progress {:?})",
                timeout.as_secs(),
                bridge::last_progress()
            );
        }
        js_sleep(POLL).await;
    }
    // Settle. The boot's last work (the pane restoring its items) runs on the *foreground*
    // executor, so the settle is two round trips on that executor - a task queued now runs
    // after everything queued there before it, and the second trip covers one level of
    // follow-up task - rather than a fixed sleep on an executor the work never touches.
    for _ in 0..2 {
        foreground_turn(&cx, started, timeout).await?;
    }
    if let Some((stage, detail)) = terminal_progress() {
        anyhow::bail!(
            "waitIdle: the session ended with {} {detail}",
            stage.as_str()
        );
    }
    Ok(JsValue::from_f64(started.elapsed().as_millis() as f64))
}

/// Queues a no-op on the foreground executor and waits, on the main-thread clock, until it
/// ran: the caller is then ordered behind everything queued there before it. Bounded by the
/// caller's own deadline, since a foreground executor that never runs the task is the stall
/// `executorProbe()` diagnoses.
async fn foreground_turn(
    cx: &AsyncApp,
    started: web_time::Instant,
    timeout: Duration,
) -> Result<()> {
    let ran = Rc::new(Cell::new(false));
    let flag = ran.clone();
    cx.spawn(async move |_| flag.set(true)).detach();
    while !ran.get() {
        if started.elapsed() > timeout {
            anyhow::bail!(
                "waitIdle: the foreground executor did not run a queued task within {}s (last progress {:?})",
                timeout.as_secs(),
                bridge::last_progress()
            );
        }
        js_sleep(POLL).await;
    }
    Ok(())
}

fn project_path_for(
    workspace: &Entity<Workspace>,
    path: &str,
    cx: &App,
) -> Result<project::ProjectPath> {
    workspace
        .read(cx)
        .project()
        .read(cx)
        .find_project_path(path, cx)
        .with_context(|| format!("{path} is not inside an open worktree"))
}

/// Opens `path` (absolute, inside a worktree) in the active pane and focuses it.
async fn open_file(path: JsValue) -> Result<JsValue> {
    let path = string_arg(&path, "path")?;
    let (mut cx, window, workspace) = app_window()?;
    let task = window.update(&mut cx, |_, window, cx| {
        let project_path = project_path_for(&workspace, &path, cx)?;
        anyhow::Ok(workspace.update(cx, |workspace, cx| {
            workspace.open_path(project_path, None, true, window, cx)
        }))
    })??;
    let item = task.await?;
    let out = Object::new();
    set(&out, "path", path.as_str());
    let (kind, dirty) = cx.update(|cx| {
        let kind = if item.act_as::<Editor>(cx).is_some() {
            "editor"
        } else {
            "other"
        };
        (kind, item.is_dirty(cx))
    });
    set(&out, "kind", kind);
    set(&out, "dirty", dirty);
    Ok(out.into())
}

/// `[{ path, dirty, active }]` for the active pane's items.
async fn open_items() -> Result<JsValue> {
    let (cx, _window, workspace) = app_window()?;
    let items = cx.update(|cx| {
        let workspace = workspace.read(cx);
        let pane = workspace.active_pane().read(cx);
        let active = pane.active_item().map(|item| item.item_id());
        pane.items()
            .map(|item| {
                (
                    item.project_path(cx)
                        .map(|project_path| project_path.path.as_unix_str().to_string()),
                    item.is_dirty(cx),
                    Some(item.item_id()) == active,
                )
            })
            .collect::<Vec<_>>()
    });
    let out = Array::new();
    for (path, dirty, active) in items {
        let entry = Object::new();
        set(&entry, "path", opt_str(path.as_deref()));
        set(&entry, "dirty", dirty);
        set(&entry, "active", active);
        out.push(&entry);
    }
    Ok(out.into())
}

fn open_buffer_for(
    workspace: &Entity<Workspace>,
    path: &str,
    cx: &mut App,
) -> Result<gpui::Task<Result<Entity<Buffer>>>> {
    let project_path = project_path_for(workspace, path, cx)?;
    let project = workspace.read(cx).project().clone();
    if let Some(buffer) = project
        .read(cx)
        .buffer_store()
        .read(cx)
        .get_by_path(&project_path)
    {
        return Ok(gpui::Task::ready(Ok(buffer)));
    }
    Ok(project.update(cx, |project, cx| project.open_buffer(project_path, cx)))
}

/// The text of the buffer at `path`, opening it (without an editor) when needed.
async fn buffer_text(path: JsValue) -> Result<JsValue> {
    let path = string_arg(&path, "path")?;
    let (cx, _window, workspace) = app_window()?;
    let task = cx.update(|cx| open_buffer_for(&workspace, &path, cx))?;
    let buffer = task.await?;
    let text = cx.update(|cx| buffer.read(cx).text());
    Ok(JsValue::from_str(&text))
}

async fn buffer_syntax(path: JsValue) -> Result<JsValue> {
    let path = string_arg(&path, "path")?;
    let (cx, _, workspace) = app_window()?;
    let buffer = cx
        .update(|cx| open_buffer_for(&workspace, &path, cx))?
        .await?;
    let (name, highlighted) = cx.update(|cx| {
        let snapshot = buffer.read(cx).snapshot();
        let name = snapshot
            .language()
            .map(|language| language.name().to_string());
        let highlighted = snapshot
            .chunks(
                0..snapshot.len(),
                language::LanguageAwareStyling {
                    tree_sitter: true,
                    diagnostics: false,
                },
            )
            .filter(|chunk| chunk.syntax_highlight_id.is_some())
            .count();
        (name, highlighted)
    });
    let result = Object::new();
    set(&result, "language", opt_str(name.as_deref()));
    set(
        &result,
        "highlightedChunks",
        JsValue::from_f64(highlighted as f64),
    );
    Ok(result.into())
}

async fn command_palette_visible() -> Result<JsValue> {
    let (cx, _, workspace) = app_window()?;
    Ok(JsValue::from_bool(cx.update(|cx| {
        workspace
            .read(cx)
            .active_modal::<command_palette::CommandPalette>(cx)
            .is_some()
    })))
}

fn active_editor(workspace: &Entity<Workspace>, cx: &App) -> Result<Entity<Editor>> {
    workspace
        .read(cx)
        .active_item_as::<Editor>(cx)
        .context("the active item is not an editor")
}

async fn active_buffer_text() -> Result<JsValue> {
    let (cx, _window, workspace) = app_window()?;
    let text = cx.update(|cx| {
        let editor = active_editor(&workspace, cx)?;
        anyhow::Ok(editor.read(cx).text(cx))
    })?;
    Ok(JsValue::from_str(&text))
}

/// Types `text` into the active editor at its selections.
async fn insert_text(text: JsValue) -> Result<JsValue> {
    let text = string_arg(&text, "text")?;
    let (mut cx, window, workspace) = app_window()?;
    window.update(&mut cx, |_, window, cx| {
        let editor = active_editor(&workspace, cx)?;
        editor.update(cx, |editor, cx| editor.insert(&text, window, cx));
        anyhow::Ok(())
    })??;
    Ok(JsValue::TRUE)
}

async fn move_cursor_end() -> Result<JsValue> {
    let (mut cx, window, workspace) = app_window()?;
    window.update(&mut cx, |_, window, cx| {
        let editor = active_editor(&workspace, cx)?;
        editor.update(cx, |editor, cx| editor.move_to_end(&MoveToEnd, window, cx));
        anyhow::Ok(())
    })??;
    Ok(JsValue::TRUE)
}

/// Saves the active item (`SaveIntent::Save`) and resolves when the write completed.
async fn save() -> Result<JsValue> {
    let (mut cx, window, workspace) = app_window()?;
    let task = window.update(&mut cx, |_, window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.save_active_item(SaveIntent::Save, window, cx)
        })
    })?;
    task.await?;
    Ok(JsValue::TRUE)
}

/// Whether the open buffer at `path` is dirty; `false` when no buffer is open for it.
async fn is_dirty(path: JsValue) -> Result<JsValue> {
    let path = string_arg(&path, "path")?;
    let (cx, _window, workspace) = app_window()?;
    let dirty = cx.update(|cx| {
        let project_path = project_path_for(&workspace, &path, cx)?;
        let project = workspace.read(cx).project().read(cx);
        anyhow::Ok(
            project
                .buffer_store()
                .read(cx)
                .get_by_path(&project_path)
                .is_some_and(|buffer| buffer.read(cx).is_dirty()),
        )
    })?;
    Ok(JsValue::from_bool(dirty))
}

/// `[{ id, name, language }]`: the language servers the project reports running (the
/// completions test waits for one before asking for a menu).
async fn language_servers() -> Result<JsValue> {
    let (cx, _window, workspace) = app_window()?;
    let servers = cx.update(|cx| {
        workspace
            .read(cx)
            .project()
            .read(cx)
            .language_server_statuses(cx)
            .map(|(id, status)| {
                (
                    id.0 as f64,
                    status.name.to_string(),
                    status
                        .language_name
                        .as_ref()
                        .map(|language| language.to_string()),
                )
            })
            .collect::<Vec<_>>()
    });
    let out = Array::new();
    for (id, name, language) in servers {
        let entry = Object::new();
        set(&entry, "id", JsValue::from_f64(id));
        set(&entry, "name", name.as_str());
        set(&entry, "language", opt_str(language.as_deref()));
        out.push(&entry);
    }
    Ok(out.into())
}

struct TerminalInfo {
    id: Option<u64>,
    title: String,
    cwd: Option<String>,
    entity: Entity<Terminal>,
}

fn terminal_js(info: &TerminalInfo) -> JsValue {
    let out = Object::new();
    set(&out, "id", opt_id(info.id));
    set(&out, "title", info.title.as_str());
    set(&out, "cwd", opt_str(info.cwd.as_deref()));
    out.into()
}

/// Every terminal view in the terminal panel and the center panes.
fn terminal_list(workspace: &Entity<Workspace>, cx: &App) -> Vec<TerminalInfo> {
    let workspace = workspace.read(cx);
    let mut views: Vec<Entity<TerminalView>> = Vec::new();
    if let Some(panel) = workspace.panel::<TerminalPanel>(cx) {
        for pane in panel.read(cx).panes() {
            views.extend(pane.read(cx).items_of_type::<TerminalView>());
        }
    }
    for pane in workspace.panes() {
        views.extend(pane.read(cx).items_of_type::<TerminalView>());
    }
    views
        .into_iter()
        .map(|view| {
            let entity = view.read(cx).terminal().clone();
            let terminal = entity.read(cx);
            TerminalInfo {
                id: terminal.remote_terminal_id(),
                title: terminal.title(false),
                cwd: terminal
                    .remote_working_directory()
                    .map(|path| path.to_string_lossy().into_owned()),
                entity: entity.clone(),
            }
        })
        .collect()
}

fn terminal_by_id(workspace: &Entity<Workspace>, id: u64, cx: &App) -> Result<Entity<Terminal>> {
    terminal_list(workspace, cx)
        .into_iter()
        .find(|info| info.id == Some(id))
        .map(|info| info.entity)
        .with_context(|| format!("no terminal with id {id}"))
}

/// Opens a shell in the terminal panel (`cwd`, else the first worktree root) and resolves
/// with `{ id, title, cwd }` once the server assigned the remote terminal id (`id` is its
/// decimal string). The panel's spawn task is awaited directly, so a `SpawnTerminal` the
/// server refuses (a bad cwd, a stopped session) rejects with that error instead of a
/// timeout.
async fn spawn_terminal(cwd: JsValue) -> Result<JsValue> {
    let cwd = optional_string_arg(&cwd);
    let (mut cx, window, workspace) = app_window()?;
    let task = window.update(&mut cx, |_, window, cx| {
        workspace.update(cx, |workspace, cx| {
            let working_directory = match cwd {
                Some(cwd) => PathBuf::from(cwd),
                None => workspace
                    .project()
                    .read(cx)
                    .visible_worktrees(cx)
                    .next()
                    .map(|worktree| worktree.read(cx).abs_path().to_path_buf())
                    .context("no worktree is open")?,
            };
            let panel = workspace
                .panel::<TerminalPanel>(cx)
                .context("the terminal panel is not installed")?;
            anyhow::Ok(panel.update(cx, |panel, cx| {
                panel.add_terminal_shell(
                    false,
                    Some(working_directory),
                    RevealStrategy::Always,
                    window,
                    cx,
                )
            }))
        })
    })??;
    let terminal = task
        .await
        .context("spawnTerminal: the shell did not spawn")?;
    let started = web_time::Instant::now();
    loop {
        let found = cx.update(|cx| {
            let terminal = terminal.upgrade()?;
            let info = terminal_list(&workspace, cx)
                .into_iter()
                .find(|info| info.entity == terminal)?;
            info.id.map(|_| terminal_js(&info))
        });
        if let Some(found) = found {
            return Ok(found);
        }
        anyhow::ensure!(
            terminal.upgrade().is_some(),
            "spawnTerminal: the terminal was dropped before the server acknowledged it"
        );
        if started.elapsed() > TERMINAL_ID_TIMEOUT {
            anyhow::bail!(
                "spawnTerminal: the shell spawned but the server assigned no terminal id within {}s",
                TERMINAL_ID_TIMEOUT.as_secs()
            );
        }
        // The main-thread clock, for the same reason as `wait_idle`: a deadline reached only
        // after a GPUI timer resolved cannot fire when the executor is what stalled.
        js_sleep(POLL).await;
    }
}

/// `[{ id, title, cwd }]`; `id` is the remote terminal id as a decimal string, `null` until
/// the server assigned one.
async fn terminals() -> Result<JsValue> {
    let (cx, _window, workspace) = app_window()?;
    let list = cx.update(|cx| {
        terminal_list(&workspace, cx)
            .iter()
            .map(terminal_js)
            .collect::<Vec<_>>()
    });
    let out = Array::new();
    for entry in list {
        out.push(&entry);
    }
    Ok(out.into())
}

/// Writes `text` to the remote pty of terminal `id` (no newline is added).
async fn terminal_input(id: JsValue, text: JsValue) -> Result<JsValue> {
    let id = id_arg(&id)?;
    let text = string_arg(&text, "text")?;
    let (cx, _window, workspace) = app_window()?;
    cx.update(|cx| {
        let terminal = terminal_by_id(&workspace, id, cx)?;
        terminal.update(cx, |terminal, _| terminal.input(text.into_bytes()));
        anyhow::Ok(())
    })?;
    Ok(JsValue::TRUE)
}

/// The visible grid plus scrollback of terminal `id`, as text.
async fn terminal_scrollback(id: JsValue) -> Result<JsValue> {
    let id = id_arg(&id)?;
    let (cx, _window, workspace) = app_window()?;
    let content = cx.update(|cx| {
        let terminal = terminal_by_id(&workspace, id, cx)?;
        anyhow::Ok(terminal.read(cx).get_content())
    })?;
    Ok(JsValue::from_str(&content))
}

/// `[{ kind, seconds, at }]` (snake_case kinds, D29), oldest first.
async fn lifecycle_events() -> Result<JsValue> {
    let out = Array::new();
    LIFECYCLE.with(|events| {
        for event in events.borrow().iter() {
            let entry = Object::new();
            set(&entry, "kind", event.kind.as_str());
            set(
                &entry,
                "seconds",
                JsValue::from_f64(f64::from(event.seconds)),
            );
            set(&entry, "at", JsValue::from_f64(event.at_ms));
            out.push(&entry);
        }
    });
    Ok(out.into())
}

/// Whether the active editor's *completions* menu is up. `Editor::context_menu_visible()` is
/// true for a code-action menu too, which a test asserting that completions appeared must not
/// accept; [`context_menu`] reports which one it is.
async fn completions_visible() -> Result<JsValue> {
    let (cx, _window, workspace) = app_window()?;
    let menu = cx.update(|cx| {
        let editor = active_editor(&workspace, cx)?;
        anyhow::Ok(editor.read(cx).visible_context_menu())
    })?;
    Ok(JsValue::from_bool(matches!(menu, Some(("completions", _)))))
}

/// `{ kind, rows }` of the active editor's visible code context menu (`kind`: `completions`
/// or `code_actions`), or `null` when none is up.
async fn context_menu() -> Result<JsValue> {
    let (cx, _window, workspace) = app_window()?;
    let menu = cx.update(|cx| {
        let editor = active_editor(&workspace, cx)?;
        anyhow::Ok(editor.read(cx).visible_context_menu())
    })?;
    let Some((kind, rows)) = menu else {
        return Ok(JsValue::NULL);
    };
    let out = Object::new();
    set(&out, "kind", kind);
    set(&out, "rows", JsValue::from_f64(rows as f64));
    Ok(out.into())
}

/// `editor::ShowCompletions` on the active editor.
async fn trigger_completion() -> Result<JsValue> {
    let (mut cx, window, workspace) = app_window()?;
    window.update(&mut cx, |_, window, cx| {
        let editor = active_editor(&workspace, cx)?;
        editor.update(cx, |editor, cx| {
            editor.show_completions(&ShowCompletions::default(), window, cx)
        });
        anyhow::Ok(())
    })??;
    Ok(JsValue::TRUE)
}

/// The client-state store's current version (`null` before the store exists).
async fn client_state_version() -> Result<JsValue> {
    let Ok(cx) = boot::async_app() else {
        return Ok(JsValue::NULL);
    };
    let version =
        cx.update(|cx| db::client_state::global_store(cx).map(|store| store.read(cx).version()));
    Ok(opt_num(version))
}

/// `{ version, hidden, intervalMs, dirty, readOnly, stopped }` of the client-state store
/// (`null` before it exists): the deterministic side of D7's triggers (`set_hidden` drops
/// the interval to 5 s, visible restores 15 s).
async fn client_state_status() -> Result<JsValue> {
    let Ok(cx) = boot::async_app() else {
        return Ok(JsValue::NULL);
    };
    let status = cx.update(|cx| {
        db::client_state::global_store(cx).map(|store| {
            let store = store.read(cx);
            (
                store.version(),
                store.is_hidden(),
                store.interval().as_millis() as f64,
                store.is_dirty(),
                store.is_read_only(),
                store.is_stopped(),
            )
        })
    });
    let Some((version, hidden, interval_ms, dirty, read_only, stopped)) = status else {
        return Ok(JsValue::NULL);
    };
    let out = Object::new();
    set(&out, "version", JsValue::from_f64(version as f64));
    set(&out, "hidden", hidden);
    set(&out, "intervalMs", JsValue::from_f64(interval_ms));
    set(&out, "dirty", dirty);
    set(&out, "readOnly", read_only);
    set(&out, "stopped", stopped);
    Ok(out.into())
}

/// `[{ kind, version, hidden, detail, at }]`, oldest first: every `ClientStateEvent` the
/// store emitted (`saved`, `stale`, `save_failed`, `read_only`) with whether the tab was
/// hidden at the time.
async fn client_state_events() -> Result<JsValue> {
    let out = Array::new();
    CLIENT_STATE.with(|events| {
        for event in events.borrow().iter() {
            let entry = Object::new();
            set(&entry, "kind", event.kind);
            set(&entry, "version", opt_num(event.version));
            set(&entry, "hidden", event.hidden);
            set(&entry, "detail", event.detail.as_str());
            set(&entry, "at", JsValue::from_f64(event.at_ms));
            out.push(&entry);
        }
    });
    Ok(out.into())
}

/// `[{ hidden, at, flushedAt, flushOk, flushError, versionAfter }]`, oldest first: every
/// `set_hidden` the shell called and, once the store's task resolved, the outcome of the
/// hidden flush (`flushOk`, `versionAfter`); a visible call resolves at once.
async fn visibility_events() -> Result<JsValue> {
    let out = Array::new();
    VISIBILITY.with(|events| {
        for event in events.borrow().iter() {
            let entry = Object::new();
            set(&entry, "hidden", event.hidden);
            set(&entry, "at", JsValue::from_f64(event.at_ms));
            set(
                &entry,
                "flushedAt",
                event
                    .flushed_ms
                    .map(JsValue::from_f64)
                    .unwrap_or(JsValue::NULL),
            );
            set(
                &entry,
                "flushOk",
                event
                    .flush_ok
                    .map(JsValue::from_bool)
                    .unwrap_or(JsValue::NULL),
            );
            set(&entry, "flushError", event.flush_error.as_str());
            set(&entry, "versionAfter", opt_num(event.version_after));
            out.push(&entry);
        }
    });
    Ok(out.into())
}

/// How long each leg of [`executor_probe`] may take before it counts as stuck.
const PROBE_TIMEOUT: Duration = Duration::from_millis(2_500);

/// Runs `future` against a JS-clock deadline; `Some(ms)` when it completed in time.
async fn probe_leg<F: Future<Output = ()>>(future: F) -> Option<f64> {
    let started = web_time::Instant::now();
    let future = std::pin::pin!(future);
    let deadline = std::pin::pin!(js_sleep(PROBE_TIMEOUT));
    match futures::future::select(future, deadline).await {
        futures::future::Either::Left(_) => Some(started.elapsed().as_secs_f64() * 1000.0),
        futures::future::Either::Right(_) => None,
    }
}

/// `{ foregroundTask, backgroundTask, backgroundTimer, backgroundThenForeground }`: the
/// milliseconds each executor path took to complete, or `null` when it did not within
/// [`PROBE_TIMEOUT`]. A browser where background workers never run tasks (or never wake the
/// main thread back up) shows `null` for every background leg while the foreground leg
/// completes; nothing in the boot before `connecting` waits on a background task, so this is
/// the first thing to ask when the boot stalls there.
async fn executor_probe() -> Result<JsValue> {
    let cx = boot::async_app()?;
    let out = Object::new();
    let foreground = probe_leg(async {
        cx.spawn(async move |_| ()).await;
    })
    .await;
    set(
        &out,
        "foregroundTask",
        foreground.map(JsValue::from_f64).unwrap_or(JsValue::NULL),
    );
    let background = probe_leg(async {
        cx.background_executor()
            .spawn(async {
                // Surfaces on the worker's console: proof the workers run tasks at all, even
                // when the hop back to the main thread is what is stuck.
                log::warn!(
                    "executor probe: a background task ran on {:?}",
                    std::thread::current().id()
                );
            })
            .await;
    })
    .await;
    set(
        &out,
        "backgroundTask",
        background.map(JsValue::from_f64).unwrap_or(JsValue::NULL),
    );
    let timer = probe_leg(async {
        cx.background_executor()
            .timer(Duration::from_millis(20))
            .await;
    })
    .await;
    set(
        &out,
        "backgroundTimer",
        timer.map(JsValue::from_f64).unwrap_or(JsValue::NULL),
    );
    let hop = probe_leg(async {
        let executor = cx.background_executor().clone();
        cx.spawn(async move |_| executor.spawn(async {}).await)
            .await;
    })
    .await;
    set(
        &out,
        "backgroundThenForeground",
        hop.map(JsValue::from_f64).unwrap_or(JsValue::NULL),
    );
    Ok(out.into())
}

/// `{ origin, configured, providers }` for the AI proxy (b11 §6.6):
///
/// - `configured` is every proxied provider id whose `<origin>/api/ai/<id>` URL the real
///   [`ProxyCredentialsProvider`] answers a credential for. It goes through
///   `read_credentials`, so it exercises the inventory request, the single-flight cache and
///   the placeholder — the whole path a broken install ordering would silently disable.
/// - `providers` is what `LanguageModelRegistry` holds after `language_models::init`, with
///   each provider's `authenticated` flag: a test asserts a seeded provider reads
///   authenticated, and that the `localhost` providers (`ollama`, `lmstudio`, `llama.cpp`)
///   are absent on wasm, which no host test can observe.
///
/// Rejects until `init_before_connect` has run `ai::install`.
async fn ai_keys() -> Result<JsValue> {
    let (origin, credentials) = ai_credentials()?;
    let cx = boot::async_app()?;

    let configured = Array::new();
    for id in ai_proxy::PROXIED_LANGUAGE_MODEL_PROVIDERS
        .iter()
        .chain(ai_proxy::PROXIED_EDIT_PREDICTION_PROVIDERS)
    {
        let url = ai_proxy::proxy_api_url(&origin, id);
        if credentials.read_credentials(&url, &cx).await?.is_some() {
            configured.push(&JsValue::from_str(id));
        }
    }

    let providers = cx.update(|cx| {
        LanguageModelRegistry::global(cx)
            .read(cx)
            .providers()
            .into_iter()
            .map(|provider| (provider.id().0.to_string(), provider.is_authenticated(cx)))
            .collect::<Vec<_>>()
    });
    let provider_list = Array::new();
    for (id, authenticated) in providers {
        let entry = Object::new();
        set(&entry, "id", id.as_str());
        set(&entry, "authenticated", JsValue::from_bool(authenticated));
        provider_list.push(&entry);
    }

    let out = Object::new();
    set(&out, "origin", origin.as_str());
    set(&out, "configured", configured);
    set(&out, "providers", provider_list);
    Ok(out.into())
}

/// Kills the transport from the client side (`RemoteClient::force_disconnect`), which
/// starts the ordinary reconnect path; the server-side drop is the local backend's route.
async fn force_disconnect() -> Result<JsValue> {
    let cx = boot::async_app()?;
    let remote = boot::remote_client().context("not connected yet")?;
    let task = cx.update(|cx| remote.update(cx, |remote, cx| remote.force_disconnect(cx)));
    task.await?;
    Ok(JsValue::TRUE)
}
