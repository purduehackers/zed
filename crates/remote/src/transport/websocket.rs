//! The WebSocket transport: Zed's length-prefixed `Envelope` protocol over one `yawc`
//! WebSocket to a `zed-remote-server serve` endpoint.
//!
//! The socket is dialed in [`WebSocketRemoteConnection::new`] (the `ConnectionPool` path,
//! which has no timeout), so a reconnect's slow work — refreshing `{url, token, session_id}`
//! through [`WebSocketSessionRefresh`], backing off, TCP/TLS, the HTTP upgrade — happens
//! before `RemoteClient::reconnect`'s 5 s resync window starts. The socket is owned by a
//! *bridge* task that exposes only `Send` channels, so the same pump runs on native (where
//! the socket is `Send`) and in the browser (where it is not). `start_proxy` then performs
//! the in-band `Hello`/`HelloAck` exchange and pumps envelopes until the socket closes.

pub mod wire;

#[cfg(not(target_family = "wasm"))]
mod dial_native;
#[cfg(target_family = "wasm")]
mod dial_web;
#[cfg(all(test, not(target_family = "wasm")))]
mod tests;

use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use collections::HashMap;
use futures::{
    FutureExt as _, Sink, SinkExt as _, Stream, StreamExt as _,
    channel::{
        mpsc::{self, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    pin_mut, select_biased,
};
#[cfg(not(target_family = "wasm"))]
use gpui::AppContext as _;
use gpui::{App, AsyncApp, BackgroundExecutor, Global, Task};
use parking_lot::Mutex;
use rpc::{
    ErrorCode, ErrorCodeExt as _, ErrorExt as _,
    proto::{Envelope, EnvelopedMessage as _},
};
use url::Url;
use util::paths::{PathStyle, RemotePathBuf};
use yawc::frame::{Frame, OpCode};

use crate::{
    CommandTemplate, Interactive, RemoteArch, RemoteClientDelegate, RemoteConnection,
    RemoteConnectionOptions, RemoteOs, RemotePlatform,
    protocol::{decode_envelope_frame, encode_envelope_frame},
    proxy::ProxyLaunchError,
};
use wire::{
    CLOSE_BAD_HELLO, CLOSE_BUILD_MISMATCH, CLOSE_GOING_AWAY, CLOSE_REASON_STALE_EPOCH,
    CLOSE_SESSION_ACTIVE, CLOSE_TAKEN_OVER, CLOSE_UNAUTHORIZED, ClientKind, ControlFrame,
    DEV_BUILD_PREFIX, Hello, HelloAck, MAX_FRAME_BYTES, PROTOCOL_VERSION, ZS_BUILD_ID,
    builds_compatible,
};

/// Reconnect budget for the WebSocket transport (D2): attempts, each preceded by a refresh
/// and a backoff of `min(2^(attempt-1), 8)` seconds.
pub const WS_MAX_RECONNECT_ATTEMPTS: usize = 20;

/// Upper bound of the exponential reconnect backoff (D2).
const MAX_BACKOFF: Duration = Duration::from_secs(8);

/// Budget for TCP/TLS plus the HTTP upgrade.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Budget for the `Hello` → `HelloAck` exchange after the upgrade.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// Frames buffered between the pump and the socket-owning bridge task, per direction.
const BRIDGE_CHANNEL_CAPACITY: usize = 64;

/// Fallback shell reported before the server's `HelloAck` has been seen.
const DEFAULT_SHELL: &str = "/bin/sh";

const CLIENT_KIND: ClientKind = if cfg!(target_family = "wasm") {
    ClientKind::Web
} else {
    ClientKind::Desktop
};

static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Connection options for a `zed-remote-server serve` endpoint
/// (`{ url, workspace_id, session_id, token, takeover }` plus two runtime-only fields, D1).
///
/// Identity (`Hash`/`Eq`, workspace persistence, the `ConnectionPool` key) is `workspace_id`
/// only: `url`, `token` and `session_id` change across resumes and reconnects.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct WebSocketConnectionOptions {
    /// Full endpoint, e.g. `wss://<session>.vercel.run/rpc`. Refreshed through `refresh`.
    /// Empty for a restored row until the refresh callback fills it.
    pub url: String,
    /// Stable identity of the workspace (the control-plane workspace id, D1). Persisted as
    /// `remote_connections.name`; the JWT `ws` claim.
    pub workspace_id: String,
    /// Minted by the control plane on every `/connect`; informational (telemetry, logs, the
    /// server's session bookkeeping). Rotated by `refresh`; never part of identity or
    /// persistence (D1).
    #[serde(skip)]
    pub session_id: String,
    /// ES256 session JWT. Never serialized, never printed.
    #[serde(skip)]
    pub token: String,
    /// Ask the server to close any other attached client with 4001 and attach us. Per-attempt,
    /// never persisted.
    #[serde(skip)]
    pub takeover: bool,
    /// Supplies a fresh `{url, token, session_id}` before each reconnect dial. `None` falls
    /// back to the process-wide [`session_refresh_provider`], else the current values are
    /// reused.
    #[serde(skip)]
    pub refresh: Option<Arc<dyn WebSocketSessionRefresh>>,
    /// Shared across the connection objects the pool creates for one session.
    #[serde(skip)]
    pub(crate) state: Option<Arc<WebSocketSessionState>>,
}

impl WebSocketConnectionOptions {
    /// Options for one `/connect` result (D26: `wsUrl`, `workspaceId`, `sessionId`, `token`).
    pub fn new(
        url: impl Into<String>,
        workspace_id: impl Into<String>,
        session_id: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        let url = url.into();
        let session_id = session_id.into();
        let token = token.into();
        let state = WebSocketSessionState::new(WebSocketSession {
            url: url.clone(),
            token: token.clone(),
            session_id: session_id.clone(),
        });
        Self {
            url,
            workspace_id: workspace_id.into(),
            session_id,
            token,
            takeover: false,
            refresh: None,
            state: Some(Arc::new(state)),
        }
    }

    /// Sets the per-attempt takeover flag.
    pub fn with_takeover(mut self, takeover: bool) -> Self {
        self.takeover = takeover;
        self
    }

    /// Sets the refresh callback consulted before every reconnect dial.
    pub fn with_refresh(mut self, refresh: Arc<dyn WebSocketSessionRefresh>) -> Self {
        self.refresh = Some(refresh);
        self
    }

    /// The host of `url`, falling back to `workspace_id` when there is no usable URL.
    pub fn display_name(&self) -> String {
        Url::parse(&self.url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .unwrap_or_else(|| self.workspace_id.clone())
    }

    /// Whether a dial can be attempted: there is a token, a refresh callback, or a
    /// process-wide refresh provider.
    pub fn can_dial(&self, cx: &App) -> bool {
        !self.token.is_empty() || self.refresh.is_some() || session_refresh_provider(cx).is_some()
    }

    /// The server's last close frame, or the close synthesized after a terminal refresh error
    /// (4003 for `Unauthorized`, 1001 for `Stopped`).
    ///
    /// Readable after `RemoteClient` reached `ServerNotRunning`/`ReconnectExhausted`, when the
    /// connection object itself is no longer reachable, through
    /// `RemoteClient::connection_options()` — that snapshot shares this session state.
    pub fn last_close(&self) -> Option<CloseInfo> {
        self.state.as_ref().and_then(|state| state.last_close())
    }

    /// What the server reported in its last `HelloAck`, if any.
    pub fn server_info(&self) -> Option<WebSocketServerInfo> {
        self.state.as_ref().and_then(|state| state.server_info())
    }

    /// Replaces the exponential reconnect backoff with a fixed delay.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_backoff_for_tests(&self, delay: Duration) {
        if let Some(state) = &self.state {
            *state.backoff_override.lock() = Some(delay);
        }
    }

    /// The shared session state, created from the current fields when this options value
    /// came from persistence (where the state is not serialized).
    pub(crate) fn session_state(&mut self) -> Arc<WebSocketSessionState> {
        self.state
            .get_or_insert_with(|| {
                Arc::new(WebSocketSessionState::new(WebSocketSession {
                    url: self.url.clone(),
                    token: self.token.clone(),
                    session_id: self.session_id.clone(),
                }))
            })
            .clone()
    }

    fn apply_session(&mut self, session: &WebSocketSession) {
        self.url = session.url.clone();
        self.token = session.token.clone();
        self.session_id = session.session_id.clone();
    }
}

impl fmt::Debug for WebSocketConnectionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketConnectionOptions")
            .field("url", &self.url)
            .field("workspace_id", &self.workspace_id)
            .field("session_id", &self.session_id)
            .field("token", &"<redacted>")
            .field("takeover", &self.takeover)
            .field("refresh", &self.refresh.is_some())
            .field(
                "instance",
                &self.state.as_ref().map(|state| state.instance.as_str()),
            )
            .finish()
    }
}

impl PartialEq for WebSocketConnectionOptions {
    fn eq(&self, other: &Self) -> bool {
        self.workspace_id == other.workspace_id
    }
}

impl Eq for WebSocketConnectionOptions {}

impl std::hash::Hash for WebSocketConnectionOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.workspace_id.hash(state);
    }
}

/// One `/connect` result (D1): the endpoint, the JWT and the per-connect session id that the
/// JWT's `sid` claim names.
#[derive(Clone, PartialEq, Eq)]
pub struct WebSocketSession {
    /// The `wss://…/rpc` endpoint.
    pub url: String,
    /// The session JWT. Never printed: `Debug` redacts it.
    pub token: String,
    /// The per-connect session id (`sid`).
    pub session_id: String,
}

impl WebSocketSession {
    fn is_dialable(&self) -> bool {
        !self.url.is_empty() && !self.token.is_empty()
    }
}

impl fmt::Debug for WebSocketSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketSession")
            .field("url", &self.url)
            .field("token", &"<redacted>")
            .field("session_id", &self.session_id)
            .finish()
    }
}

/// A close frame received from the server, or one synthesized client-side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseInfo {
    /// The WebSocket close code (`wire::CLOSE_*`).
    pub code: u16,
    /// The close reason text.
    pub reason: String,
}

/// Why [`WebSocketSessionRefresh::refresh`] is being called.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshReason {
    /// A restored workspace (a persisted row without a URL or token) has no session yet;
    /// this is the first dial and no backoff precedes it. A provider that must not resume a
    /// stopped workspace as a side effect of restoring it can answer `RefreshError::Stopped`
    /// here and let the user resume explicitly.
    Initial,
    /// A reconnect after a dropped session. `attempt` starts at 1 for the first reconnect
    /// after a drop; `last_close` is the server's close frame if one was received.
    Reconnect {
        attempt: usize,
        last_close: Option<CloseInfo>,
    },
}

/// `Unauthorized` (the user's control-plane session is gone) and `Stopped` (the control
/// plane answered `409 workspace_stopped`: a reconnect must not resume the workspace) are
/// terminal (D2): the transport stops redialing at once and records a synthesized
/// `CloseInfo { 4003 }` / `CloseInfo { 1001, "workspace stopped" }` so the shell can prompt for
/// sign-in or show "stopped". `Other` is retried with backoff.
#[derive(Debug)]
pub enum RefreshError {
    /// The control-plane session is gone; terminal.
    Unauthorized,
    /// The workspace is stopped or stopping; terminal.
    Stopped,
    /// A transient failure; the next reconnect attempt retries.
    Other(anyhow::Error),
}

impl fmt::Display for RefreshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("session refresh unauthorized"),
            Self::Stopped => f.write_str("workspace stopped"),
            Self::Other(error) => write!(f, "session refresh failed: {error:#}"),
        }
    }
}

impl std::error::Error for RefreshError {}

/// Implemented by the embedder (the browser shell over the host's `refreshConnectInfo()`,
/// desktop against `POST /api/workspaces/{id}/connect`). Must not block: return a gpui `Task`
/// (a foreground `cx.spawn` is fine in the browser). The control plane's `/connect` waits for
/// the sandbox to be healthy, so this may legitimately take minutes — it runs in the pool's
/// connect path, outside `RemoteClient::reconnect`'s 5 s window. The returned `session_id`
/// is the new per-connect id (D1) and must match the new token's `sid`.
pub trait WebSocketSessionRefresh: Send + Sync + 'static {
    /// Fetches a fresh session for `workspace_id`.
    fn refresh(
        &self,
        workspace_id: &str,
        reason: RefreshReason,
        cx: &mut AsyncApp,
    ) -> Task<Result<WebSocketSession, RefreshError>>;
}

/// State shared by every connection object the pool builds for one session, and by the
/// options snapshots that reference it.
#[derive(Default)]
pub struct WebSocketSessionState {
    pub(crate) current: Mutex<Option<WebSocketSession>>,
    /// Per-boot client-instance nonce sent as `Hello.instance` (D25). Stable for the life of
    /// this state (all reconnects of one session), distinct across tabs and across the
    /// shell's from-scratch reconnect.
    pub(crate) instance: String,
    /// Sockets established with this state; `> 0` means the next `new()` is a redial. A dial
    /// that never produced a socket does not count, so retrying a failed first connect is
    /// still a first connect (no backoff, no `RefreshReason::Reconnect`).
    pub(crate) dials: AtomicUsize,
    pub(crate) reconnect_attempts: AtomicUsize,
    /// From the last `HelloAck`; sent back on reconnect.
    pub(crate) epoch: Mutex<Option<u64>>,
    pub(crate) last_close: Mutex<Option<CloseInfo>>,
    pub(crate) server_info: Mutex<Option<WebSocketServerInfo>>,
    /// Set on `RefreshError::Unauthorized | Stopped`; makes `max_reconnect_attempts() == 0`.
    pub(crate) terminal: AtomicBool,
    pub(crate) backoff_override: Mutex<Option<Duration>>,
}

impl WebSocketSessionState {
    fn new(session: WebSocketSession) -> Self {
        Self {
            current: Mutex::new(Some(session)),
            instance: new_instance_nonce(),
            ..Default::default()
        }
    }

    fn current(&self) -> Option<WebSocketSession> {
        self.current.lock().clone()
    }

    fn set_current(&self, session: WebSocketSession) {
        *self.current.lock() = Some(session);
    }

    fn epoch(&self) -> Option<u64> {
        *self.epoch.lock()
    }

    fn last_close(&self) -> Option<CloseInfo> {
        self.last_close.lock().clone()
    }

    fn set_last_close(&self, close: Option<CloseInfo>) {
        *self.last_close.lock() = close;
    }

    fn server_info(&self) -> Option<WebSocketServerInfo> {
        self.server_info.lock().clone()
    }

    fn backoff_delay(&self, attempt: usize) -> Duration {
        if let Some(delay) = *self.backoff_override.lock() {
            return delay;
        }
        let exponent = attempt.saturating_sub(1).min(8) as u32;
        Duration::from_secs(1u64 << exponent).min(MAX_BACKOFF)
    }
}

/// A fresh `Hello.instance` (D25). The server keys same-instance reconnects on it — a
/// matching `instance` with a matching epoch attaches warm and inherits the replay buffer
/// without `takeover` — so it must not be guessable: 128 bits from the platform CSPRNG
/// (`getrandom`, `crypto.getRandomValues` in the browser), plus a process-local counter that
/// keeps two nonces distinct even if the entropy source were to repeat.
fn new_instance_nonce() -> String {
    format!(
        "{:032x}-{:x}",
        rand::random::<u128>(),
        INSTANCE_COUNTER.fetch_add(1, SeqCst)
    )
}

/// What the server reported in its `HelloAck`.
///
/// No `PartialEq`: `RemotePlatform` only derives `Copy, Clone, Debug`.
#[derive(Clone, Debug)]
pub struct WebSocketServerInfo {
    /// The server build id.
    pub build: String,
    /// Parsed from `HelloAck.os` / `HelloAck.arch`.
    pub platform: RemotePlatform,
    /// `HelloAck.os_version`.
    pub os_version: Option<String>,
    /// `HelloAck.shell`.
    pub shell: String,
    /// Whether the attach was a warm reconnect.
    pub resumed: bool,
    /// The server's session epoch.
    pub epoch: u64,
}

impl WebSocketServerInfo {
    fn from_hello_ack(ack: &HelloAck) -> Self {
        Self {
            build: ack.build.clone(),
            platform: parse_platform(&ack.os, &ack.arch),
            os_version: ack.os_version.clone(),
            shell: if ack.shell.is_empty() {
                DEFAULT_SHELL.to_owned()
            } else {
                ack.shell.clone()
            },
            resumed: ack.resumed,
            epoch: ack.epoch,
        }
    }
}

fn parse_platform(os: &str, arch: &str) -> RemotePlatform {
    let parsed_os = match os.trim().to_ascii_lowercase().as_str() {
        "linux" => Some(RemoteOs::Linux),
        "macos" | "darwin" => Some(RemoteOs::MacOs),
        "windows" => Some(RemoteOs::Windows),
        _ => None,
    };
    let arch_lower = arch.trim().to_ascii_lowercase();
    let parsed_arch = if arch_lower.starts_with("aarch64") || arch_lower.starts_with("arm64") {
        Some(RemoteArch::Aarch64)
    } else if arch_lower.starts_with("x86") || arch_lower == "amd64" {
        Some(RemoteArch::X86_64)
    } else {
        None
    };
    if parsed_os.is_none() || parsed_arch.is_none() {
        log::warn!("unrecognised server platform {os:?}/{arch:?}, assuming linux/x86_64");
    }
    RemotePlatform {
        os: parsed_os.unwrap_or(RemoteOs::Linux),
        arch: parsed_arch.unwrap_or(RemoteArch::X86_64),
    }
}

fn default_platform() -> RemotePlatform {
    RemotePlatform {
        os: RemoteOs::Linux,
        arch: RemoteArch::X86_64,
    }
}

/// Whether `url` points at this machine (`localhost` or a loopback address), the only hosts a
/// plaintext `ws://` endpoint is accepted for.
fn is_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

struct GlobalSessionRefreshProvider(Arc<dyn WebSocketSessionRefresh>);

impl Global for GlobalSessionRefreshProvider {}

/// Registers the process-wide fallback refresh provider, consulted when
/// `WebSocketConnectionOptions::refresh` is `None` — the desktop path for restored workspaces.
pub fn set_session_refresh_provider(cx: &mut App, provider: Arc<dyn WebSocketSessionRefresh>) {
    cx.set_global(GlobalSessionRefreshProvider(provider));
}

/// The process-wide refresh provider, if one was registered.
pub fn session_refresh_provider(cx: &App) -> Option<Arc<dyn WebSocketSessionRefresh>> {
    cx.try_global::<GlobalSessionRefreshProvider>()
        .map(|provider| provider.0.clone())
}

#[cfg(any(test, feature = "test-support"))]
struct GlobalClientBuildOverride(String);

#[cfg(any(test, feature = "test-support"))]
impl Global for GlobalClientBuildOverride {}

/// Makes [`client_build_id`] report `build` instead of the compiled-in id, so tests can
/// exercise the build-mismatch path with a release-style client id.
#[cfg(any(test, feature = "test-support"))]
pub fn set_client_build_id_for_tests(cx: &mut App, build: impl Into<String>) {
    cx.set_global(GlobalClientBuildOverride(build.into()));
}

/// The build id this client presents in `Hello.build`: `ZS_BUILD_ID` when baked in, else a
/// `dev-<app version>` id that `wire::builds_compatible` accepts against any server.
pub fn client_build_id(cx: &App) -> String {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(build) = cx.try_global::<GlobalClientBuildOverride>() {
        return build.0.clone();
    }
    match ZS_BUILD_ID {
        Some(build_id) => build_id.to_owned(),
        None => format!(
            "{DEV_BUILD_PREFIX}-{}",
            release_channel::AppVersion::global(cx)
        ),
    }
}

fn ws_err(error: yawc::WebSocketError) -> anyhow::Error {
    anyhow!("{error}")
}

/// Send-safe handle to the task that owns the dialed socket.
struct SocketBridge {
    /// Pump → socket.
    frames_tx: mpsc::Sender<Frame>,
    /// Socket → pump; taken exactly once by `start_proxy`.
    frames_rx: Option<mpsc::Receiver<Result<Frame, String>>>,
    /// Owns the yawc socket; dropping it closes the socket.
    _task: Task<()>,
}

/// The channel pairs connecting a pump to a bridge: `(pump → socket, socket → pump)`, each as
/// `(sender, receiver)`.
type BridgeChannels = (
    (mpsc::Sender<Frame>, mpsc::Receiver<Frame>),
    (
        mpsc::Sender<Result<Frame, String>>,
        mpsc::Receiver<Result<Frame, String>>,
    ),
);

fn bridge_channels() -> BridgeChannels {
    (
        mpsc::channel(BRIDGE_CHANNEL_CAPACITY),
        mpsc::channel(BRIDGE_CHANNEL_CAPACITY),
    )
}

/// Forwards frames between the socket halves and the pump's channels until either side goes
/// away. A send failure is reported to the pump as `Err(message)`; end-of-stream simply drops
/// `inbound`, which the pump sees as `None`. The dial modules spawn this on the executor that
/// can hold their socket (background on native, foreground in the browser).
///
/// The two directions are independent futures: a forwarder parked on a full channel must not
/// stop the other direction from draining, or the bridge and the pump — each waiting for the
/// other to make room — would deadlock under a burst of traffic both ways.
async fn run_bridge<Si, St>(
    mut sink: Si,
    stream: St,
    mut outbound: mpsc::Receiver<Frame>,
    inbound: mpsc::Sender<Result<Frame, String>>,
) where
    Si: Sink<Frame> + Unpin,
    Si::Error: fmt::Display,
    St: Stream<Item = Result<Frame, String>> + Unpin,
{
    // A cloned bounded sender owns one guaranteed slot, so the failure notice below never
    // has to wait for the pump.
    let mut send_errors = inbound.clone();
    let outbound_forwarder = async move {
        while let Some(frame) = outbound.next().await {
            if let Err(error) = sink.send(frame).await {
                send_errors
                    .try_send(Err(format!("websocket send failed: {error}")))
                    .ok();
                break;
            }
        }
    }
    .fuse();
    let inbound_forwarder = async move {
        let mut stream = stream.fuse();
        let mut inbound = inbound;
        while let Some(frame) = stream.next().await {
            if inbound.send(frame).await.is_err() {
                break;
            }
        }
    }
    .fuse();
    pin_mut!(outbound_forwarder, inbound_forwarder);
    // Whichever direction ends first ends the bridge: dropping the other forwarder drops its
    // socket half (closing the socket) and its `inbound` sender (ending the pump's stream).
    select_biased! {
        _ = outbound_forwarder => {}
        _ = inbound_forwarder => {}
    }
}

/// A `RemoteConnection` over one WebSocket.
pub struct WebSocketRemoteConnection {
    options: Mutex<WebSocketConnectionOptions>,
    state: Arc<WebSocketSessionState>,
    killed: Arc<AtomicBool>,
    /// The dialed socket, owned by a task that lives as long as this field. `start_proxy`
    /// takes the channel ends exactly once; `kill()`, the end of the pump, or dropping the
    /// connection drops the task, which closes the socket.
    bridge: Arc<Mutex<Option<SocketBridge>>>,
    server_info: Arc<Mutex<Option<WebSocketServerInfo>>>,
}

impl WebSocketRemoteConnection {
    /// Called from `ConnectionPool::connect` (a foreground task with no timeout). Does
    /// everything slow: the session refresh (on a redial or a restored row), the backoff, the
    /// TCP/TLS dial and the HTTP upgrade. The in-band `Hello`/`HelloAck` exchange happens in
    /// `start_proxy`, where the `reconnect` flag is known.
    pub(crate) async fn new(
        mut options: WebSocketConnectionOptions,
        delegate: Arc<dyn RemoteClientDelegate>,
        cx: &mut AsyncApp,
    ) -> Result<Self> {
        anyhow::ensure!(
            !options.workspace_id.is_empty(),
            "WebSocket connection options need a workspace_id"
        );
        let state = options.session_state();
        if state.terminal.load(SeqCst) {
            let reason = state
                .last_close()
                .map(|close| close.reason)
                .unwrap_or_else(|| "session ended".to_owned());
            anyhow::bail!("workspace session is no longer usable: {reason}");
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let has_tokio = cx.update(|cx| gpui_tokio::Tokio::try_handle(cx).is_some());
            anyhow::ensure!(
                has_tokio,
                "gpui_tokio not initialised: call gpui_tokio::init before connecting over WebSocket"
            );
        }

        let refresh = options
            .refresh
            .clone()
            .or_else(|| cx.update(|cx| session_refresh_provider(cx)));
        let redial = state.dials.load(SeqCst) > 0;
        let needs_session = !state.current().is_some_and(|session| session.is_dialable());
        let mut attempt = 0;
        if redial {
            attempt = state.reconnect_attempts.fetch_add(1, SeqCst) + 1;
            // D2: every reconnect attempt is preceded by the backoff, whatever fails afterwards
            // (the refresh, the dial or the handshake), so the 20 attempts span minutes rather
            // than burning through in a second while the network is still down.
            let delay = state.backoff_delay(attempt);
            if !delay.is_zero() {
                delegate.set_status(Some(&format!("Reconnecting (attempt {attempt})")), cx);
                cx.background_executor().timer(delay).await;
            }
        }

        if (redial || needs_session)
            && let Some(refresh) = refresh
        {
            delegate.set_status(Some("Refreshing session"), cx);
            let reason = if redial {
                RefreshReason::Reconnect {
                    attempt,
                    last_close: state.last_close(),
                }
            } else {
                RefreshReason::Initial
            };
            match refresh.refresh(&options.workspace_id, reason, cx).await {
                Ok(session) => {
                    state.set_current(session.clone());
                    options.apply_session(&session);
                }
                Err(RefreshError::Unauthorized) => {
                    state.set_last_close(Some(CloseInfo {
                        code: CLOSE_UNAUTHORIZED,
                        reason: "session expired".to_owned(),
                    }));
                    state.terminal.store(true, SeqCst);
                    return Err(RefreshError::Unauthorized.into());
                }
                Err(RefreshError::Stopped) => {
                    state.set_last_close(Some(CloseInfo {
                        code: CLOSE_GOING_AWAY,
                        reason: "workspace stopped".to_owned(),
                    }));
                    state.terminal.store(true, SeqCst);
                    return Err(RefreshError::Stopped.into());
                }
                Err(RefreshError::Other(error)) => {
                    return Err(error.context("failed to refresh the workspace session"));
                }
            }
        }

        let session = state
            .current()
            .context("no session available for the workspace")?;
        let url = Url::parse(&session.url)
            .with_context(|| format!("invalid workspace URL {:?}", session.url))?;
        match url.scheme() {
            "wss" => {}
            // The token travels in the upgrade request, so plaintext is only acceptable where
            // it never leaves the machine (local development, tests).
            "ws" => anyhow::ensure!(
                is_loopback_host(&url),
                "workspace URL {:?} must use the wss scheme (the plaintext ws scheme is only allowed for loopback hosts)",
                session.url
            ),
            _ => anyhow::bail!(
                "workspace URL {:?} must use the wss scheme (or ws for a loopback host)",
                session.url
            ),
        }
        anyhow::ensure!(
            !session.token.is_empty(),
            "no session token for workspace {}: register a session refresh provider",
            options.workspace_id
        );

        delegate.set_status(Some("Connecting to workspace"), cx);
        #[cfg(not(target_family = "wasm"))]
        let bridge = dial_native::dial(url, &session.token, cx).await?;
        #[cfg(target_family = "wasm")]
        let bridge = dial_web::dial(url, &session.token, cx).await?;
        state.dials.fetch_add(1, SeqCst);

        Ok(Self {
            options: Mutex::new(options),
            state,
            killed: Arc::new(AtomicBool::new(false)),
            bridge: Arc::new(Mutex::new(Some(bridge))),
            server_info: Arc::new(Mutex::new(None)),
        })
    }

    /// What the server reported in its `HelloAck`; `None` before the handshake.
    pub fn server_info(&self) -> Option<WebSocketServerInfo> {
        self.server_info.lock().clone()
    }

    /// The server's last close frame (or the synthesized one after a terminal refresh error).
    pub fn last_close(&self) -> Option<CloseInfo> {
        self.state.last_close()
    }

    /// Injects a `HelloAck` as if the handshake had completed.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_server_info_for_tests(&self, ack: &HelloAck) {
        let info = WebSocketServerInfo::from_hello_ack(ack);
        *self.server_info.lock() = Some(info.clone());
        *self.state.server_info.lock() = Some(info);
        *self.state.epoch.lock() = Some(ack.epoch);
    }

    /// The `Hello` this connection would send for `start_proxy(unique_identifier, reconnect)`.
    fn compose_hello(
        state: &WebSocketSessionState,
        workspace_id: &str,
        unique_identifier: String,
        reconnect: bool,
        takeover: bool,
        client_build: String,
    ) -> Result<Hello> {
        let session = state
            .current()
            .context("no session available for the workspace")?;
        Ok(Hello {
            protocol: PROTOCOL_VERSION,
            build: client_build,
            workspace_id: workspace_id.to_owned(),
            session_id: session.session_id,
            identifier: unique_identifier,
            instance: state.instance.clone(),
            reconnect,
            takeover,
            client: CLIENT_KIND,
            epoch: if reconnect { state.epoch() } else { None },
        })
    }

    async fn attach_and_pump(
        mut frames_tx: mpsc::Sender<Frame>,
        mut frames_rx: mpsc::Receiver<Result<Frame, String>>,
        incoming_tx: UnboundedSender<Envelope>,
        outgoing_rx: UnboundedReceiver<Envelope>,
        connection_activity_tx: Sender<()>,
        delegate: Arc<dyn RemoteClientDelegate>,
        state: Arc<WebSocketSessionState>,
        server_info: Arc<Mutex<Option<WebSocketServerInfo>>>,
        hello: Hello,
        executor: BackgroundExecutor,
        cx: &mut AsyncApp,
    ) -> Result<i32> {
        delegate.set_status(Some("Attaching to workspace"), cx);
        // From here on `last_close` describes only how *this* attachment ends: the close that
        // ended the previous socket has already been reported to the refresh callback, and
        // a later `ReconnectExhausted` or exit 90 must not be attributed to it.
        state.set_last_close(None);
        let reconnect = hello.reconnect;
        let client_build = hello.build.clone();
        let hello_json = serde_json::to_string(&ControlFrame::Hello(hello))?;
        frames_tx
            .send(Frame::text(hello_json))
            .await
            .map_err(|_| anyhow!("websocket closed before Hello could be sent"))?;

        let deadline = executor.timer(HANDSHAKE_TIMEOUT).fuse();
        pin_mut!(deadline);
        let ack = loop {
            let frame = {
                let next_frame = frames_rx.next().fuse();
                pin_mut!(next_frame);
                select_biased! {
                    frame = next_frame => frame,
                    _ = deadline => anyhow::bail!("timed out waiting for the server's HelloAck"),
                }
            };
            let frame = match frame {
                None => anyhow::bail!("websocket closed before HelloAck"),
                Some(Err(message)) => anyhow::bail!(message),
                Some(Ok(frame)) => frame,
            };
            match frame.opcode() {
                OpCode::Text => match serde_json::from_slice::<ControlFrame>(frame.payload())
                    .context("malformed control frame in place of HelloAck")?
                {
                    ControlFrame::HelloAck(ack) => break ack,
                    // Liveness and log frames carry no handshake meaning; a server or an
                    // intermediary may emit them before the answer to `Hello` arrives.
                    ControlFrame::Heartbeat => {}
                    ControlFrame::Log(record) => record.log(log::logger()),
                    ControlFrame::Hello(_) => {
                        anyhow::bail!("unexpected Hello from the server, expected HelloAck")
                    }
                },
                OpCode::Close => {
                    let close = close_info(&frame);
                    state.set_last_close(Some(close.clone()));
                    return exit_code_for_close(&close);
                }
                // Protocol-level keepalives are answered by the socket layer (or, in the
                // browser, never surfaced at all); yawc reassembles continuations itself.
                OpCode::Ping | OpCode::Pong | OpCode::Continuation => {}
                opcode => anyhow::bail!("unexpected first frame {opcode:?}, expected HelloAck"),
            }
        };

        let info = WebSocketServerInfo::from_hello_ack(&ack);
        *server_info.lock() = Some(info.clone());
        *state.server_info.lock() = Some(info);
        if let Some(exit_code) = exit_code_for_hello_ack(&client_build, reconnect, &ack) {
            if exit_code == ProxyLaunchError::IncompatibleServer.to_exit_code() {
                state.set_last_close(Some(CloseInfo {
                    code: CLOSE_BUILD_MISMATCH,
                    reason: format!(
                        "server build {:?} (protocol {}) is incompatible with client build {client_build:?} (protocol {PROTOCOL_VERSION})",
                        ack.build, ack.protocol
                    ),
                }));
            } else {
                log::warn!(
                    "server did not resume the session (epoch {}); a fresh session is required",
                    ack.epoch
                );
            }
            return Ok(exit_code);
        }
        *state.epoch.lock() = Some(ack.epoch);
        state.reconnect_attempts.store(0, SeqCst);
        delegate.set_status(None, cx);

        #[cfg(not(target_family = "wasm"))]
        {
            cx.background_spawn(run_pump(
                frames_tx,
                frames_rx,
                incoming_tx,
                outgoing_rx,
                connection_activity_tx,
                state,
                executor,
            ))
            .await
        }
        #[cfg(target_family = "wasm")]
        {
            run_pump(
                frames_tx,
                frames_rx,
                incoming_tx,
                outgoing_rx,
                connection_activity_tx,
                state,
                executor,
            )
            .await
        }
    }
}

/// Maps a server close frame to the io task's outcome: terminal exit codes for the codes
/// `RemoteClient::monitor` must not retry, `Err` for everything the reconnect path handles.
fn exit_code_for_close(close: &CloseInfo) -> Result<i32> {
    match close.code {
        CLOSE_GOING_AWAY => Ok(ProxyLaunchError::ServerNotRunning.to_exit_code()),
        // D23 folds the stale-epoch refusal into 4001, but nobody took the session over: the
        // server simply cannot resume this epoch, and the remedy is a fresh session (exit 90,
        // as for `reconnect && !resumed`), not the take-back flow.
        CLOSE_TAKEN_OVER if close.reason == CLOSE_REASON_STALE_EPOCH => {
            Ok(ProxyLaunchError::ServerNotRunning.to_exit_code())
        }
        CLOSE_TAKEN_OVER | CLOSE_SESSION_ACTIVE => {
            Ok(ProxyLaunchError::SessionTakenOver.to_exit_code())
        }
        CLOSE_BUILD_MISMATCH | CLOSE_BAD_HELLO => {
            Ok(ProxyLaunchError::IncompatibleServer.to_exit_code())
        }
        code => Err(anyhow!(
            "websocket closed by the server with code {code}: {}",
            close.reason
        )),
    }
}

/// The terminal exit code a `HelloAck` implies, if any: an incompatible protocol or build is
/// exit 92; a `reconnect` the server answered with `resumed: false` (server restarted, stale
/// epoch, or another instance attached in between) is exit 90, because replaying unacked
/// envelopes against a reset project would be wrong.
fn exit_code_for_hello_ack(client_build: &str, reconnect: bool, ack: &HelloAck) -> Option<i32> {
    if ack.protocol != PROTOCOL_VERSION || !builds_compatible(client_build, &ack.build) {
        Some(ProxyLaunchError::IncompatibleServer.to_exit_code())
    } else if reconnect && !ack.resumed {
        Some(ProxyLaunchError::ServerNotRunning.to_exit_code())
    } else {
        None
    }
}

fn close_info(frame: &Frame) -> CloseInfo {
    CloseInfo {
        code: frame.close_code().map(u16::from).unwrap_or(1005),
        reason: frame
            .close_reason()
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_owned(),
    }
}

#[cfg(target_family = "wasm")]
async fn decode_inbound(
    payload: impl AsRef<[u8]> + Send + 'static,
    executor: &BackgroundExecutor,
) -> Result<Envelope> {
    /// Frames at least this large are decoded off the main thread.
    const DECODE_OFFLOAD_THRESHOLD: usize = 64 * 1024;
    if payload.as_ref().len() >= DECODE_OFFLOAD_THRESHOLD {
        executor
            .spawn(async move { decode_envelope_frame(payload.as_ref(), MAX_FRAME_BYTES) })
            .await
    } else {
        decode_envelope_frame(payload.as_ref(), MAX_FRAME_BYTES)
    }
}

#[cfg(not(target_family = "wasm"))]
async fn decode_inbound(
    payload: impl AsRef<[u8]> + Send + 'static,
    _executor: &BackgroundExecutor,
) -> Result<Envelope> {
    decode_envelope_frame(payload.as_ref(), MAX_FRAME_BYTES)
}

/// Pumps envelopes between `RemoteClient`'s channels and the bridge after the handshake.
/// Returns the io task's outcome (see [`exit_code_for_close`]).
///
/// Like [`run_bridge`], the two directions are independent futures so that a full bridge
/// channel in one direction never stops the other from draining.
async fn run_pump(
    mut frames_tx: mpsc::Sender<Frame>,
    mut frames_rx: mpsc::Receiver<Result<Frame, String>>,
    incoming_tx: UnboundedSender<Envelope>,
    mut outgoing_rx: UnboundedReceiver<Envelope>,
    mut connection_activity_tx: Sender<()>,
    state: Arc<WebSocketSessionState>,
    executor: BackgroundExecutor,
) -> Result<i32> {
    let outbound = {
        let incoming_tx = incoming_tx.clone();
        async move {
            while let Some(envelope) = outgoing_rx.next().await {
                let bytes = encode_envelope_frame(&envelope);
                if bytes.len() > MAX_FRAME_BYTES {
                    let message = format!(
                        "message too large for the WebSocket transport ({} bytes, limit {MAX_FRAME_BYTES})",
                        bytes.len()
                    );
                    log::error!("{message}; dropping envelope {}", envelope.id);
                    if envelope.responding_to.is_none() {
                        // A locally synthesized response: its id is not a server id, so
                        // `ChannelClient` must not treat it as an ack watermark (it keeps
                        // the watermark monotonic for exactly this reason).
                        let error = ErrorCode::Internal
                            .message(message)
                            .to_proto()
                            .into_envelope(0, Some(envelope.id), None);
                        incoming_tx.unbounded_send(error).ok();
                    }
                    continue;
                }
                frames_tx
                    .send(Frame::binary(bytes))
                    .await
                    .map_err(|_| anyhow!("websocket closed"))?;
            }
            anyhow::Ok(0)
        }
    }
    .fuse();
    let inbound = async move {
        loop {
            let frame = match frames_rx.next().await {
                None => anyhow::bail!("websocket closed"),
                Some(Err(message)) => anyhow::bail!(message),
                Some(Ok(frame)) => frame,
            };
            match frame.opcode() {
                OpCode::Binary => {
                    let envelope = decode_inbound(frame.payload().clone(), &executor).await?;
                    incoming_tx.unbounded_send(envelope).ok();
                    connection_activity_tx.try_send(()).ok();
                }
                OpCode::Text => {
                    match serde_json::from_slice::<ControlFrame>(frame.payload()) {
                        Ok(ControlFrame::Heartbeat) => {}
                        Ok(ControlFrame::Log(record)) => record.log(log::logger()),
                        Ok(other) => {
                            log::warn!(
                                "ignoring unexpected control frame after the handshake: {other:?}"
                            );
                        }
                        Err(error) => {
                            log::warn!("ignoring malformed control frame: {error}");
                        }
                    }
                    connection_activity_tx.try_send(()).ok();
                }
                OpCode::Ping | OpCode::Pong => {
                    connection_activity_tx.try_send(()).ok();
                }
                OpCode::Close => {
                    let close = close_info(&frame);
                    state.set_last_close(Some(close.clone()));
                    return exit_code_for_close(&close);
                }
                OpCode::Continuation => {}
            }
        }
    }
    .fuse();
    pin_mut!(outbound, inbound);
    select_biased! {
        result = outbound => result,
        result = inbound => result,
    }
}

#[async_trait(?Send)]
impl RemoteConnection for WebSocketRemoteConnection {
    fn start_proxy(
        &self,
        unique_identifier: String,
        reconnect: bool,
        incoming_tx: UnboundedSender<Envelope>,
        outgoing_rx: UnboundedReceiver<Envelope>,
        connection_activity_tx: Sender<()>,
        delegate: Arc<dyn RemoteClientDelegate>,
        cx: &mut AsyncApp,
    ) -> Task<Result<i32>> {
        let channels = self.bridge.lock().as_mut().and_then(|bridge| {
            bridge
                .frames_rx
                .take()
                .map(|frames_rx| (bridge.frames_tx.clone(), frames_rx))
        });
        let Some((frames_tx, frames_rx)) = channels else {
            return Task::ready(Err(anyhow!("websocket session already attached or killed")));
        };
        let (workspace_id, takeover) = {
            let options = self.options.lock();
            (options.workspace_id.clone(), options.takeover)
        };
        let client_build = cx.update(|cx| client_build_id(cx));
        let hello = match Self::compose_hello(
            &self.state,
            &workspace_id,
            unique_identifier,
            reconnect,
            takeover,
            client_build,
        ) {
            Ok(hello) => hello,
            Err(error) => return Task::ready(Err(error)),
        };
        let state = self.state.clone();
        let killed = self.killed.clone();
        let bridge = self.bridge.clone();
        let server_info = self.server_info.clone();
        let executor = cx.background_executor().clone();
        cx.spawn(async move |cx| {
            let result = Self::attach_and_pump(
                frames_tx,
                frames_rx,
                incoming_tx,
                outgoing_rx,
                connection_activity_tx,
                delegate,
                state,
                server_info,
                hello,
                executor,
                cx,
            )
            .await;
            // The connection is single-use: the pool must never hand it out again, and the
            // socket has no further purpose once the pump is gone.
            killed.store(true, SeqCst);
            bridge.lock().take();
            result
        })
    }

    fn upload_directory(
        &self,
        _src_path: PathBuf,
        _dest_path: RemotePathBuf,
        _cx: &App,
    ) -> Task<Result<()>> {
        Task::ready(Err(anyhow!(
            "uploading directories is not supported over the WebSocket transport"
        )))
    }

    async fn kill(&self) -> Result<()> {
        self.killed.store(true, SeqCst);
        self.bridge.lock().take();
        Ok(())
    }

    fn has_been_killed(&self) -> bool {
        self.killed.load(SeqCst)
    }

    fn build_command(
        &self,
        _program: Option<String>,
        _args: &[String],
        _env: &HashMap<String, String>,
        _working_dir: Option<String>,
        _port_forward: Option<(u16, String, u16)>,
        _interactive: Interactive,
    ) -> Result<CommandTemplate> {
        Err(anyhow!(
            "local commands are not supported over the WebSocket transport"
        ))
    }

    fn build_forward_ports_command(
        &self,
        _forwards: Vec<(u16, String, u16)>,
    ) -> Result<CommandTemplate> {
        Err(anyhow!(
            "port forwarding commands are not supported over the WebSocket transport"
        ))
    }

    fn connection_options(&self) -> RemoteConnectionOptions {
        RemoteConnectionOptions::WebSocket(self.options.lock().clone())
    }

    fn path_style(&self) -> PathStyle {
        PathStyle::Unix
    }

    fn remote_platform(&self) -> RemotePlatform {
        self.server_info
            .lock()
            .as_ref()
            .map(|info| info.platform)
            .unwrap_or_else(default_platform)
    }

    fn remote_os_version(&self) -> Option<String> {
        self.server_info
            .lock()
            .as_ref()
            .and_then(|info| info.os_version.clone())
    }

    fn shell(&self) -> String {
        self.server_info
            .lock()
            .as_ref()
            .map(|info| info.shell.clone())
            .unwrap_or_else(|| DEFAULT_SHELL.to_owned())
    }

    fn default_system_shell(&self) -> String {
        self.shell()
    }

    fn has_wsl_interop(&self) -> bool {
        false
    }

    fn max_reconnect_attempts(&self) -> usize {
        if self.state.terminal.load(SeqCst) {
            0
        } else {
            WS_MAX_RECONNECT_ATTEMPTS
        }
    }

    fn supports_remote_pty(&self) -> bool {
        true
    }

    fn supports_extension_upload(&self) -> bool {
        false
    }
}

/// Headless delegate for callers that drive their own UI (the browser shell's overlay,
/// tests). The callback receives the `AsyncApp` so it can reach a gpui entity through a
/// `WeakEntity`; DOM handles are `!Send` and cannot be captured in the browser.
pub struct WebSocketClientDelegate {
    on_status: Arc<dyn Fn(Option<&str>, &mut AsyncApp) + Send + Sync>,
}

impl WebSocketClientDelegate {
    /// A delegate forwarding every status change to `on_status`.
    pub fn new(on_status: impl Fn(Option<&str>, &mut AsyncApp) + Send + Sync + 'static) -> Self {
        Self {
            on_status: Arc::new(on_status),
        }
    }

    /// A delegate that ignores status changes.
    pub fn silent() -> Self {
        Self::new(|_, _| {})
    }
}

impl RemoteClientDelegate for WebSocketClientDelegate {
    fn ask_password(
        &self,
        prompt: String,
        tx: oneshot::Sender<askpass::EncryptedPassword>,
        _cancellation: oneshot::Receiver<()>,
        _cx: &mut AsyncApp,
    ) {
        log::warn!("password prompt is not supported over the WebSocket transport: {prompt}");
        drop(tx);
    }

    fn get_download_url(
        &self,
        _platform: RemotePlatform,
        _release_channel: release_channel::ReleaseChannel,
        _version: Option<semver::Version>,
        _cx: &mut AsyncApp,
    ) -> Task<Result<Option<String>>> {
        Task::ready(Err(anyhow!(
            "server binaries are not downloaded over the WebSocket transport"
        )))
    }

    fn download_server_binary_locally(
        &self,
        _platform: RemotePlatform,
        _release_channel: release_channel::ReleaseChannel,
        _version: Option<semver::Version>,
        _cx: &mut AsyncApp,
    ) -> Task<Result<PathBuf>> {
        Task::ready(Err(anyhow!(
            "server binaries are not downloaded over the WebSocket transport"
        )))
    }

    fn set_status(&self, status: Option<&str>, cx: &mut AsyncApp) {
        (self.on_status)(status, cx)
    }
}
