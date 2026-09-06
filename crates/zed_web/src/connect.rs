//! Dialing the sandbox over b1's WebSocket transport, the one `RemoteClient` of the session,
//! the reconnect/disconnect observers that drive the shell's overlay and the client-state
//! store, and opening the workspace around the client (D16).

use std::{path::PathBuf, sync::Arc};

use db::client_state::ClientStateStore;
use futures::channel::oneshot;
use gpui::{App, AsyncApp, Entity};
use remote::{
    CloseInfo, ConnectionIdentifier, ConnectionState, RemoteClient, RemoteClientDelegate,
    RemoteClientEvent, RemoteConnectionOptions, WebSocketClientDelegate,
    WebSocketConnectionOptions, websocket_wire,
};
use workspace::{AppState, OpenedRemoteProject};
use zed_web_core::{BootConfig, BootError, BootStage, ConnectInfo};

use crate::bridge::{self, RefreshErrorKind};

/// Dials (refresh and backoff happen inside the connection pool) and creates the one
/// `RemoteClient` of the session.
///
/// D1: the identity (`Hash`/`Eq`, the workspace-persistence row, the pool key) is
/// `workspace_id` = `BootConfig.workspace.id`; `session_id` is the per-connect id from
/// `/connect` and informational. `ConnectionIdentifier::setup()` only feeds
/// `Hello.identifier`, which the server logs and never keys on.
pub async fn connect(
    workspace_id: &str,
    info: &ConnectInfo,
    cx: &mut AsyncApp,
) -> Result<Entity<RemoteClient>, BootError> {
    let refresh: Arc<dyn remote::WebSocketSessionRefresh> = Arc::new(bridge::JsSessionRefresh);
    cx.update(|cx| remote::set_session_refresh_provider(cx, refresh.clone()));

    let options = WebSocketConnectionOptions::new(
        info.ws_url.clone(),
        workspace_id.to_string(),
        info.session_id.clone(),
        info.token.clone(),
    )
    .with_refresh(refresh);

    // The transport reports every dial through `set_status`: the first one is the boot's
    // `connecting` stage, every later one belongs to a reconnect episode (the stage
    // vocabulary of CONTRACTS.md §8.4 has no boot stage after `ready`).
    let delegate: Arc<dyn RemoteClientDelegate> =
        Arc::new(WebSocketClientDelegate::new(|status, _cx| {
            let stage = if bridge::booted() {
                BootStage::Reconnecting
            } else {
                BootStage::Connecting
            };
            bridge::progress(stage, status.unwrap_or(""));
        }));

    let connection = match remote::connect(options.clone().into(), delegate.clone(), cx).await {
        Ok(connection) => connection,
        Err(error) => {
            return Err(first_dial_error(
                options.last_close(),
                bridge::last_refresh_error(),
                error,
            ));
        }
    };

    // The cancellation sender lives for the app: forgetting it never fires the receiver.
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    std::mem::forget(cancel_tx);

    let client = cx
        .update(|cx| {
            RemoteClient::new(
                ConnectionIdentifier::setup(),
                connection,
                cancel_rx,
                delegate,
                cx,
            )
        })
        .await
        .map_err(|error| {
            first_dial_error(options.last_close(), bridge::last_refresh_error(), error)
        })?;
    client.ok_or_else(BootError::cancelled)
}

fn first_dial_error(
    close: Option<CloseInfo>,
    refresh: Option<RefreshErrorKind>,
    error: anyhow::Error,
) -> BootError {
    let (code, is_terminal) = close_code_detail(close.as_ref(), refresh);
    let message = match &close {
        Some(close) => format!("{error:#} (close {} {})", close.code, close.reason),
        None => format!("{error:#}"),
    };
    if is_terminal {
        BootError::new(code, message)
    } else {
        BootError::new("connect_failed", message)
    }
}

/// Installs the reconnect/disconnect observers: `Reconnecting`/`Ready` progress, the host's
/// `onClosed`, the `stopped` detail, and the client-state store's `resync`/`stop`.
pub fn observe(remote: Entity<RemoteClient>, store: Entity<ClientStateStore>, cx: &mut App) {
    // Only transitions are reported: `notify()` fires for every missed heartbeat while the
    // state stays `HeartbeatMissed`, and a heartbeat that recovers goes back to `Connected`
    // without any `RemoteClientEvent`, so the recovery is what re-emits `ready` here.
    let mut previous = remote.read(cx).connection_state();
    cx.observe(&remote, move |remote, cx| {
        let current = remote.read(cx).connection_state();
        if current == previous {
            return;
        }
        #[cfg(feature = "test-hooks")]
        crate::test_hooks::record_connection_transition(previous, current);
        let was_degraded = matches!(
            previous,
            ConnectionState::HeartbeatMissed | ConnectionState::Reconnecting
        );
        previous = current;
        match current {
            ConnectionState::HeartbeatMissed | ConnectionState::Reconnecting => {
                if !was_degraded {
                    bridge::progress(BootStage::Reconnecting, "");
                }
            }
            ConnectionState::Connected => {
                if was_degraded {
                    bridge::ready_once();
                }
            }
            ConnectionState::Connecting | ConnectionState::Disconnected => {}
        }
    })
    .detach();

    cx.subscribe(&remote, move |remote, event, cx| match event {
        RemoteClientEvent::Reconnected => {
            #[cfg(feature = "test-hooks")]
            crate::test_hooks::record_connection_event("reconnected", None);
            bridge::ready_once();
            store.update(cx, |store, cx| store.resync(cx)).detach();
        }
        // Both terminal states (D2): `ServerNotRunning` and `ReconnectExhausted`.
        RemoteClientEvent::Disconnected { .. } => {
            let close = match remote.read(cx).connection_options() {
                RemoteConnectionOptions::WebSocket(options) => options.last_close(),
                _ => None,
            };
            #[cfg(feature = "test-hooks")]
            crate::test_hooks::record_connection_event("disconnected", close.as_ref());
            if let Some(close) = &close {
                bridge::on_closed(close.code, &close.reason);
            }
            let (detail, _) = close_code_detail(close.as_ref(), bridge::last_refresh_error());
            bridge::progress(BootStage::Stopped, detail);
            // Nothing to flush against a session that is no longer ours (b4 §3.7).
            store.update(cx, |store, _| store.stop());
        }
    })
    .detach();
}

/// The `stopped` detail (also the boot-error code) for a terminal close, per D23's close
/// codes as b1/b2 emit them; the second value says whether the outcome is terminal (as
/// opposed to a failed first dial that a reload may fix). The shell and test hooks
/// use these same detail codes.
pub(crate) fn close_code_detail(
    close: Option<&CloseInfo>,
    refresh: Option<RefreshErrorKind>,
) -> (&'static str, bool) {
    match (refresh, close.map(|close| close.code)) {
        // D2: `RefreshError::Stopped` short-circuited the budget.
        (Some(RefreshErrorKind::Stopped), _) => ("workspace_stopped", true),
        // D2: terminal; the transport synthesizes `CloseInfo { 4003 }`.
        (Some(RefreshErrorKind::Unauthorized), _) => ("unauthorized", true),
        (_, Some(websocket_wire::CLOSE_UNAUTHORIZED)) => ("unauthorized", true),
        // This participant reloaded elsewhere, or its replay epoch expired.
        (_, Some(websocket_wire::CLOSE_TAKEN_OVER))
            if close
                .is_some_and(|close| close.reason == websocket_wire::CLOSE_REASON_STALE_EPOCH) =>
        {
            ("rejoin_required", true)
        }
        (_, Some(websocket_wire::CLOSE_TAKEN_OVER)) => ("connection_replaced", true),
        // D23: build mismatch or a malformed Hello.
        (_, Some(websocket_wire::CLOSE_BUILD_MISMATCH))
        | (_, Some(websocket_wire::CLOSE_BAD_HELLO)) => ("incompatible_server", true),
        // D23: the server is going away (SIGTERM, stop); 4004 is retired.
        (_, Some(websocket_wire::CLOSE_GOING_AWAY)) => ("server_stopping", true),
        // D2: 20 attempts with the 8 s backoff cap spent; the last close is in the message.
        _ => ("reconnect_exhausted", false),
    }
}

/// D16: one window, one `Workspace` with the persisted id, no placeholder project.
pub async fn open_remote_workspace(
    remote: Entity<RemoteClient>,
    config: &BootConfig,
    app_state: Arc<AppState>,
    cx: &mut AsyncApp,
) -> Result<OpenedRemoteProject, BootError> {
    let paths: Vec<PathBuf> = config.workspace.paths.iter().map(PathBuf::from).collect();
    let window_options = cx.update(|cx| (app_state.build_window_options)(None, cx));
    cx.update(|cx| {
        workspace::open_remote_project_in_new_window_with_client(
            remote,
            app_state,
            paths,
            window_options,
            cx,
        )
    })
    .await
    .map_err(BootError::window)
}
