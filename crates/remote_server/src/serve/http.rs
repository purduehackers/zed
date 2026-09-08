//! The two HTTP/1.1 listeners of `zed-remote-server serve`: the public one (`/health`,
//! `/rpc`, `/files`, `/extensions/*`) and the loopback control listener (`/control/*`, D5).

use std::{
    collections::HashMap,
    convert::Infallible,
    io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::SeqCst},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::{
    Method, Request, Response, StatusCode,
    body::Incoming,
    header::{self, HeaderValue},
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use remote::{
    json_log::LogRecord,
    websocket_wire::{ControlFrame, LogFrame, MAX_FRAME_BYTES, SUBPROTOCOL},
};
use tokio::net::TcpListener;
use yawc::frame::Frame;

use crate::serve::{
    GpuiCommand,
    auth::{
        AuthConfig, AuthError, Claims, bearer_token, extract_token, redact_query,
        token_shape_is_plausible,
    },
    files,
    session::{BrokerCommand, LOG_FRAME_MAX_LEVEL, SessionMeta, run_connection},
};

/// Response body type shared by every route.
pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, io::Error>;

/// How many ES256 verifications may run at once. This bounds the CPU an attacker can burn;
/// it never queues a valid token behind a penalty, because penalties are served after the
/// permit is released.
pub const VERIFY_CONCURRENCY: usize = 4;
/// Base delay a peer waits after a failed verification, applied per peer address (a flood
/// from one address never delays another address's valid token).
pub const AUTH_FAILURE_DELAY: Duration = Duration::from_millis(250);
/// Consecutive failures from one peer multiply [`AUTH_FAILURE_DELAY`] up to this factor.
pub const AUTH_PENALTY_MAX_STEPS: u32 = 8;
/// A peer's failure streak is forgotten after this long without a failure.
pub const AUTH_PENALTY_RESET: Duration = Duration::from_secs(60);
/// Bound on the number of peers with a remembered failure streak.
pub const AUTH_PENALTY_PEERS: usize = 1024;
/// hyper `header_read_timeout`.
pub const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// hyper `max_buf_size`.
pub const MAX_HEADER_BUF: usize = 64 * 1024;
/// Largest accepted `/control/*` request body.
pub const MAX_CONTROL_BODY_BYTES: usize = 1024 * 1024;

/// `POST` path of lifecycle notices on the control listener.
pub const CONTROL_LIFECYCLE_PATH: &str = "/control/lifecycle";
/// `POST` path of port updates on the control listener.
pub const CONTROL_PORTS_PATH: &str = "/control/ports";
/// `POST` path of extension install requests on the control listener.
pub const CONTROL_EXTENSIONS_PATH: &str = "/control/extensions";

/// One `/control/*` request, HTTP-framework-agnostic (mirrors b4's `ControlRequest`).
#[derive(Debug, Clone)]
pub struct ControlRequest<'a> {
    /// The HTTP method, upper-case.
    pub method: &'a str,
    /// The request path without the query.
    pub path: &'a str,
    /// The `Authorization: Bearer` value, if any.
    pub bearer: Option<&'a str>,
    /// Whether the TCP peer is a loopback address.
    pub peer_is_loopback: bool,
    /// Whether a client session is attached right now.
    pub session_attached: bool,
    /// The request body (at most [`MAX_CONTROL_BODY_BYTES`]).
    pub body: &'a [u8],
}

/// The outcome of a `/control/*` request (mirrors b4's `ControlResponse`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlResponse {
    /// 204.
    NoContent,
    /// 400 with `{"error":"bad_request","message":..}`.
    BadRequest(String),
    /// 401.
    Unauthorized,
    /// 404.
    NotFound,
}

/// The seam between the loopback control listener and the control channel that owns the
/// bodies (`ControlChannel::handle` in b4's `control.rs`); tests stub it.
pub trait ControlRoutes: Send + Sync + 'static {
    /// Whether a request carrying `bearer` from a loopback peer (or not) may proceed. The
    /// router asks before reading the body, so an unauthenticated peer cannot make it buffer
    /// up to [`MAX_CONTROL_BODY_BYTES`]; `handle` checks again on the request it receives.
    fn authorized(&self, bearer: Option<&str>, peer_is_loopback: bool) -> bool {
        let _ = (bearer, peer_is_loopback);
        true
    }

    /// Handles one authenticated-or-not control request.
    fn handle<'a>(&'a self, req: ControlRequest<'a>) -> BoxFuture<'a, ControlResponse>;
}

/// Static configuration of a [`ServeState`].
pub struct ServeConfig {
    /// `ZS_BUILD_ID` else `VERSION`; reported in `/health` and `HelloAck.build`.
    pub build: String,
    /// `VERSION`.
    pub version: String,
    /// The expected `ws` claim.
    pub workspace_id: String,
    /// Canonical directory bounding `/files`.
    pub workspace_root: PathBuf,
    /// Token verification rules.
    pub auth: AuthConfig,
    /// Exact `Origin` values allowed cross-origin; empty disables CORS and the origin check.
    pub allowed_origins: Vec<String>,
    /// When set, a `Hello.build` that is not compatible closes with 4002.
    pub client_build: Option<String>,
}

/// Everything the routes share: configuration, the auth throttle, and the mutable view of
/// the session that `/health` reports.
pub struct ServeState {
    /// When the process started.
    pub started_at: Instant,
    /// See [`ServeConfig::build`].
    pub build: String,
    /// See [`ServeConfig::version`].
    pub version: String,
    /// See [`ServeConfig::workspace_id`].
    pub workspace_id: String,
    /// See [`ServeConfig::workspace_root`].
    pub workspace_root: PathBuf,
    /// See [`ServeConfig::auth`].
    pub auth: AuthConfig,
    /// See [`ServeConfig::allowed_origins`].
    pub allowed_origins: Vec<String>,
    /// See [`ServeConfig::client_build`].
    pub client_build: Option<String>,
    /// Commands to the session broker.
    pub broker_tx: tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
    /// Commands to the gpui side.
    pub gpui_tx: futures::channel::mpsc::UnboundedSender<GpuiCommand>,
    verify_permits: tokio::sync::Semaphore,
    extension_upload_permit: Arc<tokio::sync::Semaphore>,
    auth_failures: AtomicU64,
    auth_penalties: Mutex<HashMap<IpAddr, PeerPenalty>>,
    sessions: Mutex<HashMap<String, SessionMeta>>,
    last_input_at_ms: AtomicU64,
    worktrees: Mutex<Vec<String>>,
    dirty_buffers: AtomicU32,
    log_frame_sinks: Mutex<HashMap<String, tokio::sync::mpsc::Sender<Frame>>>,
    pending_connections: AtomicUsize,
}

/// One peer's streak of failed verifications.
struct PeerPenalty {
    consecutive_failures: u32,
    last_failure: Instant,
}

/// Keeps [`ServeState::pending_connections`] accurate for the life of one `/rpc` connection
/// task (from the 101 until the socket reaches the broker or is dropped).
pub struct ConnectionGuard {
    state: Arc<ServeState>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.state.pending_connections.fetch_sub(1, SeqCst);
    }
}

/// The `GET /health` body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HealthResponse {
    /// Server build id.
    pub build: String,
    /// Server version.
    pub version: String,
    /// Seconds since the process started.
    pub uptime_secs: u64,
    /// The workspace this server serves.
    pub workspace_id: String,
    /// Whether a client is attached.
    pub session_active: bool,
    /// The attached session (full body only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionMeta>,
    /// Unix milliseconds of the last input envelope or upload (full body only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_input_at: Option<u64>,
    /// Absolute worktree roots (full body only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktrees: Option<Vec<String>>,
    /// Open buffers with unsaved edits (full body only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_buffers: Option<u32>,
    /// Failed token verifications since start (full body only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_failures: Option<u64>,
}

/// Outcome of matching a request's `Origin` against the allowed list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsDecision {
    /// No allowed origins configured: no CORS headers, no check.
    Disabled,
    /// The request carries no `Origin`.
    NoOrigin,
    /// The `Origin` is allowed; echo it.
    Allowed(String),
    /// The `Origin` is present and not allowed.
    Denied,
}

/// Unix milliseconds now.
pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

impl ServeState {
    /// Builds the state from its configuration and the two command channels.
    pub fn new(
        config: ServeConfig,
        broker_tx: tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
        gpui_tx: futures::channel::mpsc::UnboundedSender<GpuiCommand>,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            build: config.build,
            version: config.version,
            workspace_id: config.workspace_id,
            workspace_root: config.workspace_root,
            auth: config.auth,
            allowed_origins: config.allowed_origins,
            client_build: config.client_build,
            broker_tx,
            gpui_tx,
            verify_permits: tokio::sync::Semaphore::new(VERIFY_CONCURRENCY),
            extension_upload_permit: Arc::new(tokio::sync::Semaphore::new(1)),
            auth_failures: AtomicU64::new(0),
            auth_penalties: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            last_input_at_ms: AtomicU64::new(0),
            worktrees: Mutex::new(Vec::new()),
            dirty_buffers: AtomicU32::new(0),
            log_frame_sinks: Mutex::default(),
            pending_connections: AtomicUsize::new(0),
        }
    }

    /// Marks user activity now.
    pub fn touch_input(&self) {
        self.last_input_at_ms.store(unix_ms(), SeqCst);
    }

    /// Unix milliseconds of the last input, if any.
    pub fn last_input_at(&self) -> Option<u64> {
        match self.last_input_at_ms.load(SeqCst) {
            0 => None,
            value => Some(value),
        }
    }

    /// Records the attached session (or its absence) for `/health` and the log context.
    pub fn set_session(&self, meta: Option<SessionMeta>) {
        let mut sessions = self.sessions.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(meta) = meta {
            sessions.insert(meta.session_id.clone(), meta);
        } else {
            sessions.clear();
        }
        // A process-wide log record cannot be attributed to one of several participants.
        crate::serve::set_current_session((sessions.len() == 1).then(|| {
            let meta = sessions.values().next().unwrap();
            (meta.session_id.clone(), meta.epoch)
        }));
    }

    pub fn remove_session(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().unwrap_or_else(|p| p.into_inner());
        sessions.remove(session_id);
        crate::serve::set_current_session((sessions.len() == 1).then(|| {
            let meta = sessions.values().next().unwrap();
            (meta.session_id.clone(), meta.epoch)
        }));
    }

    /// The attached session, if any.
    pub fn session(&self) -> Option<SessionMeta> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .next()
            .cloned()
    }

    /// Replaces the worktree roots reported by `/health`.
    pub fn set_worktrees(&self, roots: Vec<String>) {
        *self
            .worktrees
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = roots;
    }

    /// Replaces the dirty-buffer count reported by `/health`.
    pub fn set_dirty_buffers(&self, count: u32) {
        self.dirty_buffers.store(count, SeqCst);
    }

    /// Failed verifications so far.
    pub fn auth_failures(&self) -> u64 {
        self.auth_failures.load(SeqCst)
    }

    /// The `/health` body; `full` adds the fields reserved for loopback or authenticated callers.
    pub fn health(&self, full: bool) -> HealthResponse {
        let session = self.session();
        HealthResponse {
            build: self.build.clone(),
            version: self.version.clone(),
            uptime_secs: self.started_at.elapsed().as_secs(),
            workspace_id: self.workspace_id.clone(),
            session_active: session.is_some(),
            session: if full { session } else { None },
            last_input_at: if full { self.last_input_at() } else { None },
            worktrees: full.then(|| {
                self.worktrees
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
            }),
            dirty_buffers: full.then(|| self.dirty_buffers.load(SeqCst)),
            auth_failures: full.then(|| self.auth_failures()),
        }
    }

    /// Verifies `token` for a request from `peer`. Tokens that cannot possibly be a JWT are
    /// refused before any permit is taken; otherwise a [`VERIFY_CONCURRENCY`] permit is held
    /// only for the ES256 work. A failure bumps the counter and then sleeps a per-peer
    /// penalty *after* the permit is released, so a flood of garbage from one address never
    /// queues another address's valid token, and a valid token is never refused.
    pub async fn verify(&self, token: &str, peer: IpAddr) -> Result<Claims, AuthError> {
        let result = if token_shape_is_plausible(token) {
            // The semaphore is never closed, so `acquire` only fails if it were.
            let permit = self.verify_permits.acquire().await;
            let result = self.auth.verify(token);
            drop(permit);
            result
        } else {
            Err(AuthError::Malformed)
        };
        match &result {
            Ok(_) => self.clear_penalty(peer),
            Err(_) => {
                self.auth_failures.fetch_add(1, SeqCst);
                let delay = self.record_failure(peer);
                tokio::time::sleep(delay).await;
            }
        }
        result
    }

    fn clear_penalty(&self, peer: IpAddr) {
        self.auth_penalties
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&peer);
    }

    /// Records a failure for `peer` and returns the delay it must wait.
    fn record_failure(&self, peer: IpAddr) -> Duration {
        let now = Instant::now();
        let mut penalties = self
            .auth_penalties
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if penalties.len() >= AUTH_PENALTY_PEERS && !penalties.contains_key(&peer) {
            penalties
                .retain(|_, penalty| now.duration_since(penalty.last_failure) < AUTH_PENALTY_RESET);
            if penalties.len() >= AUTH_PENALTY_PEERS {
                // Still full of live streaks: forget the oldest one rather than grow.
                if let Some(oldest) = penalties
                    .iter()
                    .min_by_key(|(_, penalty)| penalty.last_failure)
                    .map(|(address, _)| *address)
                {
                    penalties.remove(&oldest);
                }
            }
        }
        let penalty = penalties.entry(peer).or_insert(PeerPenalty {
            consecutive_failures: 0,
            last_failure: now,
        });
        if now.duration_since(penalty.last_failure) >= AUTH_PENALTY_RESET {
            penalty.consecutive_failures = 0;
        }
        penalty.consecutive_failures = penalty.consecutive_failures.saturating_add(1);
        penalty.last_failure = now;
        AUTH_FAILURE_DELAY * penalty.consecutive_failures.min(AUTH_PENALTY_MAX_STEPS)
    }

    /// Installs (or clears) the attached session's frame queue as the target of mirrored log
    /// records. Nothing in this path logs.
    pub fn set_log_frame_sink(
        &self,
        session_id: &str,
        sink: Option<tokio::sync::mpsc::Sender<Frame>>,
    ) {
        let mut sinks = self
            .log_frame_sinks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(sink) = sink {
            sinks.insert(session_id.to_owned(), sink);
        } else {
            sinks.remove(session_id);
        }
    }

    /// Mirrors one log record to the attached session as `ControlFrame::Log` with `try_send`:
    /// a full queue or no session drops the frame. Never logs (no recursion).
    pub fn mirror_log_record(
        &self,
        level: usize,
        module_path: Option<&str>,
        file: Option<&str>,
        line: Option<u32>,
        message: &str,
    ) {
        let frame = ControlFrame::Log(LogFrame {
            level,
            module_path: module_path.map(str::to_owned),
            file: file.map(str::to_owned),
            line,
            message: message.to_owned(),
        });
        let Ok(json) = serde_json::to_string(&frame) else {
            return;
        };
        let guard = self
            .log_frame_sinks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for sink in guard.values() {
            sink.try_send(Frame::text(json.clone())).ok();
        }
    }

    /// Mirrors `record` when its level is [`LOG_FRAME_MAX_LEVEL`] or more severe; the entry
    /// point the log formatter uses.
    pub fn mirror_record(&self, record: &log::Record) {
        if record.level() > LOG_FRAME_MAX_LEVEL {
            return;
        }
        let log_record = LogRecord::new(record);
        self.mirror_log_record(
            log_record.level,
            log_record.module_path.as_deref(),
            log_record.file.as_deref(),
            log_record.line,
            &log_record.message,
        );
    }

    /// Counts one `/rpc` connection task until the returned guard drops.
    pub fn pending_connection(self: Arc<Self>) -> ConnectionGuard {
        self.pending_connections.fetch_add(1, SeqCst);
        ConnectionGuard { state: self }
    }

    /// `/rpc` sockets past the 101 that have not yet reached the broker or ended.
    pub fn pending_connections(&self) -> usize {
        self.pending_connections.load(SeqCst)
    }

    /// Classifies a request's `Origin` header.
    pub fn cors_decision(&self, origin: Option<&str>) -> CorsDecision {
        if self.allowed_origins.is_empty() {
            return CorsDecision::Disabled;
        }
        match origin {
            None => CorsDecision::NoOrigin,
            Some(origin) if self.allowed_origins.iter().any(|allowed| allowed == origin) => {
                CorsDecision::Allowed(origin.to_owned())
            }
            Some(_) => CorsDecision::Denied,
        }
    }
}

/// A complete in-memory body.
pub fn full_body(bytes: impl Into<Bytes>) -> BoxBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed()
}

/// An empty body.
pub fn empty_body() -> BoxBody {
    full_body(Bytes::new())
}

/// A JSON response with the given status.
pub fn json_response<T: serde::Serialize>(status: StatusCode, value: &T) -> Response<BoxBody> {
    match serde_json::to_vec(value) {
        Ok(bytes) => Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(bytes))
            .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal")),
        Err(error) => {
            log::error!("serializing a JSON response failed: {error}");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal")
        }
    }
}

/// `{"error":"<code>"}` with the given status.
pub fn error_response(status: StatusCode, code: &str) -> Response<BoxBody> {
    let body = format!("{{\"error\":\"{code}\"}}");
    let mut response = Response::new(full_body(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn status_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(empty_body());
    *response.status_mut() = status;
    response
}

/// Accept loop over an already-bound public listener.
pub async fn serve_http(listener: TcpListener, state: Arc<ServeState>) -> anyhow::Result<()> {
    if let Ok(addr) = listener.local_addr() {
        log::info!("listening on {addr}");
    }
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                log::warn!("accept failed on the public listener: {error}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            let service = service_fn({
                let state = state.clone();
                move |req| route(state.clone(), peer, req)
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .max_buf_size(MAX_HEADER_BUF)
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades();
            if let Err(error) = connection.await {
                log::debug!("http connection from {peer} ended: {error}");
            }
        });
    }
}

fn is_cors_route(path: &str) -> bool {
    path == "/files" || path.starts_with("/extensions/") || path == "/health"
}

fn is_origin_checked_route(path: &str) -> bool {
    path == "/files" || path.starts_with("/extensions/") || path == "/rpc"
}

fn apply_cors_headers(response: &mut Response<BoxBody>, origin: &str) {
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("content-disposition, content-length"),
    );
    headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("cross-origin"),
    );
}

fn preflight_response(cors: &CorsDecision) -> Response<BoxBody> {
    let mut response = status_response(StatusCode::NO_CONTENT);
    if let CorsDecision::Allowed(origin) = cors {
        apply_cors_headers(&mut response, origin);
        let headers = response.headers_mut();
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST"),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("authorization, content-type"),
        );
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("600"),
        );
    }
    response
}

/// Routes one request on the public listener.
pub async fn route(
    state: Arc<ServeState>,
    peer: SocketAddr,
    req: Request<Incoming>,
) -> Result<Response<BoxBody>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let logged_target = redact_query(
        req.uri()
            .path_and_query()
            .map_or("/", |path_and_query| path_and_query.as_str()),
    );
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let cors = state.cors_decision(origin.as_deref());

    let mut response = if method == Method::OPTIONS {
        if !is_cors_route(&path) {
            error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed")
        } else if cors == CorsDecision::Denied {
            error_response(StatusCode::FORBIDDEN, "origin_not_allowed")
        } else {
            preflight_response(&cors)
        }
    } else if cors == CorsDecision::Denied && is_origin_checked_route(&path) {
        error_response(StatusCode::FORBIDDEN, "origin_not_allowed")
    } else {
        match (&method, path.as_str()) {
            (&Method::GET, "/health") => health_route(&state, peer, &req).await,
            (_, "/health") => error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
            (&Method::GET, "/rpc") => rpc_route(&state, peer, req).await,
            (_, "/rpc") => error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
            (&Method::POST, "/files") => match authenticate_request(&state, peer, &req).await {
                Ok(claims) => files::handle_upload(&state, &claims, req)
                    .await
                    .unwrap_or_else(files::FilesError::into_response),
                Err(response) => response,
            },
            (&Method::GET, "/files") => match authenticate_request(&state, peer, &req).await {
                Ok(_) => files::handle_download(&state, req)
                    .await
                    .unwrap_or_else(files::FilesError::into_response),
                Err(response) => response,
            },
            (_, "/files") => error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
            (&Method::POST, "/extensions/dev") => {
                match authenticate_request(&state, peer, &req).await {
                    Ok(_) => install_dev_extension(&state, req).await,
                    Err(response) => response,
                }
            }
            (&Method::GET, asset_path) if asset_path.starts_with("/extensions/") => {
                match authenticate_request(&state, peer, &req).await {
                    Ok(_) => match parse_extension_asset_path(asset_path) {
                        Some((id, rel)) => files::handle_extension_asset(&state, id, rel)
                            .await
                            .unwrap_or_else(files::FilesError::into_response),
                        None => error_response(StatusCode::NOT_FOUND, "not_found"),
                    },
                    Err(response) => response,
                }
            }
            (_, asset_path) if asset_path.starts_with("/extensions/") => {
                error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed")
            }
            _ => error_response(StatusCode::NOT_FOUND, "not_found"),
        }
    };

    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if is_cors_route(&path)
        && let CorsDecision::Allowed(origin) = &cors
    {
        apply_cors_headers(&mut response, origin);
    }
    log::debug!(
        "{method} {logged_target} -> {} (peer {peer})",
        response.status().as_u16()
    );
    Ok(response)
}

async fn install_dev_extension(state: &ServeState, req: Request<Incoming>) -> Response<BoxBody> {
    let Ok(permit) = state.extension_upload_permit.clone().try_acquire_owned() else {
        return error_response(StatusCode::CONFLICT, "extension_build_in_progress");
    };
    if req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some("application/x-tar")
    {
        return error_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, "expected_tar_archive");
    }
    let body = match Limited::new(req.into_body(), 64 * 1024 * 1024)
        .collect()
        .await
    {
        Ok(body) => body.to_bytes().to_vec(),
        Err(_) => {
            return error_response(StatusCode::PAYLOAD_TOO_LARGE, "extension_upload_too_large");
        }
    };
    let (reply, result) = futures::channel::oneshot::channel();
    if state
        .gpui_tx
        .unbounded_send(crate::serve::GpuiCommand::InstallDevExtension {
            archive: body,
            permit,
            reply,
        })
        .is_err()
    {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "extension_host_unavailable",
        );
    }
    match result.await {
        Ok(Ok(())) => json_response(StatusCode::OK, &serde_json::json!({"installed": true})),
        Ok(Err(error)) => json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": format!("{error:#}")}),
        ),
        Err(_) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "extension_host_unavailable",
        ),
    }
}

/// `/extensions/{id}/assets/{rel}` → `(id, rel)`, both percent-decoded.
fn parse_extension_asset_path(path: &str) -> Option<(String, String)> {
    let rest = path.strip_prefix("/extensions/")?;
    let (id, rel) = rest.split_once("/assets/")?;
    if id.is_empty() {
        return None;
    }
    let id = percent_encoding::percent_decode_str(id)
        .decode_utf8()
        .ok()?
        .into_owned();
    let rel = percent_encoding::percent_decode_str(rel)
        .decode_utf8()
        .ok()?
        .into_owned();
    Some((id, if rel.is_empty() { ".".to_owned() } else { rel }))
}

/// Verifies the `Authorization: Bearer` or `?zs_token=` token of a `/files` or
/// `/extensions/*` request.
async fn authenticate_request(
    state: &ServeState,
    peer: SocketAddr,
    req: &Request<Incoming>,
) -> Result<Claims, Response<BoxBody>> {
    let (token, _) =
        extract_token(req).map_err(|error| error_response(error.status(), error.code()))?;
    state
        .verify(&token, peer.ip())
        .await
        .map_err(|error| error_response(error.status(), error.code()))
}

async fn health_route(
    state: &ServeState,
    peer: SocketAddr,
    req: &Request<Incoming>,
) -> Response<BoxBody> {
    let mut full = peer.ip().is_loopback();
    if !full && let Some(token) = bearer_token(req) {
        full = state.verify(&token, peer.ip()).await.is_ok();
    }
    json_response(StatusCode::OK, &state.health(full))
}

fn is_websocket_upgrade(req: &Request<Incoming>) -> bool {
    let upgrade = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    let connection = req
        .headers()
        .get(header::CONNECTION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case("upgrade"))
        });
    upgrade && connection
}

fn subprotocol_offered_in_header(req: &Request<Incoming>) -> bool {
    req.headers()
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|value| value.split(',').any(|item| item.trim() == SUBPROTOCOL))
}

async fn rpc_route(
    state: &Arc<ServeState>,
    peer: SocketAddr,
    mut req: Request<Incoming>,
) -> Response<BoxBody> {
    let (token, offered) = match extract_token(&req) {
        Ok(extracted) => extracted,
        Err(error) => return error_response(error.status(), error.code()),
    };
    let claims = match state.verify(&token, peer.ip()).await {
        Ok(claims) => claims,
        Err(error) => return error_response(error.status(), error.code()),
    };
    if !offered {
        let error = AuthError::MissingSubprotocol;
        return error_response(error.status(), error.code());
    }
    if !is_websocket_upgrade(&req) {
        return error_response(StatusCode::UPGRADE_REQUIRED, "upgrade_required");
    }
    let echo_subprotocol = subprotocol_offered_in_header(&req);
    let options = yawc::Options::default()
        .with_max_payload_read(MAX_FRAME_BYTES)
        .with_max_read_buffer(2 * MAX_FRAME_BYTES)
        .without_compression()
        .with_utf8()
        .with_backpressure_boundary(256 * 1024);
    let (response, upgrade) = match yawc::WebSocket::upgrade_with_options(&mut req, options) {
        Ok(upgraded) => upgraded,
        Err(error) => {
            log::debug!("websocket upgrade from {peer} rejected: {error}");
            return error_response(StatusCode::BAD_REQUEST, "bad_upgrade");
        }
    };
    let mut response = response.map(|body| body.map_err(|never| match never {}).boxed());
    if echo_subprotocol {
        response.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(SUBPROTOCOL),
        );
    }
    tokio::spawn(run_connection(upgrade, claims, state.clone(), peer));
    response
}

/// Accept loop for the loopback control listener. Same hyper settings as [`serve_http`]
/// minus upgrades; every connection whose peer is not loopback is answered 401 regardless of
/// the bearer.
pub async fn serve_control(
    listener: TcpListener,
    state: Arc<ServeState>,
    control: Arc<dyn ControlRoutes>,
) -> anyhow::Result<()> {
    if let Ok(addr) = listener.local_addr() {
        log::info!("control listener on {addr}");
    }
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                log::warn!("accept failed on the control listener: {error}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let state = state.clone();
        let control = control.clone();
        tokio::spawn(async move {
            let service = service_fn({
                let state = state.clone();
                let control = control.clone();
                move |req| route_control(state.clone(), control.clone(), peer, req)
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .max_buf_size(MAX_HEADER_BUF)
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), service);
            if let Err(error) = connection.await {
                log::debug!("control connection from {peer} ended: {error}");
            }
        });
    }
}

/// Routes one request on the control listener.
pub async fn route_control(
    state: Arc<ServeState>,
    control: Arc<dyn ControlRoutes>,
    peer: SocketAddr,
    req: Request<Incoming>,
) -> Result<Response<BoxBody>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let mut response = route_control_inner(&state, control.as_ref(), peer, req).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    log::debug!(
        "control {method} {path} -> {} (peer {peer})",
        response.status().as_u16()
    );
    Ok(response)
}

async fn route_control_inner(
    state: &ServeState,
    control: &dyn ControlRoutes,
    peer: SocketAddr,
    req: Request<Incoming>,
) -> Response<BoxBody> {
    let peer_is_loopback = peer.ip().is_loopback();
    if !peer_is_loopback {
        return error_response(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let path = req.uri().path().to_owned();
    if !matches!(
        path.as_str(),
        CONTROL_LIFECYCLE_PATH | CONTROL_PORTS_PATH | CONTROL_EXTENSIONS_PATH
    ) {
        return error_response(StatusCode::NOT_FOUND, "not_found");
    }
    if req.method() != Method::POST {
        return error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    }
    let method = req.method().as_str().to_owned();
    let bearer = bearer_token(&req);
    if !control.authorized(bearer.as_deref(), peer_is_loopback) {
        return error_response(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let session_attached = state.session().is_some();
    let body = match Limited::new(req.into_body(), MAX_CONTROL_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return error_response(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
    };
    let outcome = control
        .handle(ControlRequest {
            method: &method,
            path: &path,
            bearer: bearer.as_deref(),
            peer_is_loopback,
            session_attached,
            body: &body,
        })
        .await;
    match outcome {
        ControlResponse::NoContent => status_response(StatusCode::NO_CONTENT),
        ControlResponse::BadRequest(message) => {
            #[derive(serde::Serialize)]
            struct BadRequestBody<'a> {
                error: &'static str,
                message: &'a str,
            }
            json_response(
                StatusCode::BAD_REQUEST,
                &BadRequestBody {
                    error: "bad_request",
                    message: &message,
                },
            )
        }
        ControlResponse::Unauthorized => error_response(StatusCode::UNAUTHORIZED, "unauthorized"),
        ControlResponse::NotFound => error_response(StatusCode::NOT_FOUND, "not_found"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::{
        auth::test_support as auth_test,
        test_support::{CONTROL_SECRET, TestClient, TestServer, get, http_request, post_json},
    };

    fn with_origin(mut request: Request<Full<Bytes>>, origin: &str) -> Request<Full<Bytes>> {
        request.headers_mut().insert(
            header::ORIGIN,
            HeaderValue::from_str(origin).expect("origin"),
        );
        request
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn health_minimal_vs_full() {
        let server = TestServer::start().await;
        server.state.set_worktrees(vec!["/workspaces/app".into()]);
        server.state.set_dirty_buffers(2);
        server.state.touch_input();

        let loopback = http_request(server.addr, get("/health")).await;
        assert_eq!(loopback.status, StatusCode::OK);
        let full: HealthResponse = loopback.json();
        assert_eq!(full.build, "test-build");
        assert_eq!(full.version, "test-version");
        assert_eq!(full.workspace_id, auth_test::WORKSPACE);
        assert!(!full.session_active);
        assert_eq!(
            full.worktrees.as_deref(),
            Some(&["/workspaces/app".to_owned()][..])
        );
        assert_eq!(full.dirty_buffers, Some(2));
        assert!(full.last_input_at.is_some());
        assert_eq!(full.auth_failures, Some(0));
        assert_eq!(
            loopback.header("cache-control").as_deref(),
            Some("no-store")
        );

        // A non-loopback peer is simulated by calling the router directly.
        let minimal = server.state.health(false);
        assert!(minimal.worktrees.is_none());
        assert!(minimal.last_input_at.is_none());
        assert!(minimal.dirty_buffers.is_none());
        assert!(minimal.auth_failures.is_none());
        assert!(minimal.session.is_none());

        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("sid_1", "inst_1", false, None).await;
        let ack = client.hello_ack().await.expect("hello ack");
        let attached: HealthResponse = http_request(server.addr, get("/health")).await.json();
        assert!(attached.session_active);
        let session = attached.session.expect("session");
        assert_eq!(session.session_id, "sid_1");
        assert_eq!(session.epoch, ack.epoch);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_without_token_is_401_after_the_failure_delay() {
        let server = TestServer::start().await;
        let started = std::time::Instant::now();
        let outcome = http_request(
            server.addr,
            Request::builder()
                .method("GET")
                .uri("/rpc")
                .header(header::HOST, "127.0.0.1")
                .header(header::UPGRADE, "websocket")
                .header(header::CONNECTION, "Upgrade")
                .header(header::SEC_WEBSOCKET_VERSION, "13")
                .header(header::SEC_WEBSOCKET_PROTOCOL, "zs.v1, not-a-token")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::UNAUTHORIZED);
        assert!(started.elapsed() >= AUTH_FAILURE_DELAY);
        assert_eq!(server.state.auth_failures(), 1);

        let missing = http_request(server.addr, get("/rpc")).await;
        assert_eq!(missing.status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            missing.json::<serde_json::Value>()["error"],
            "missing_token"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn valid_token_not_blocked_after_failures() {
        let server = TestServer::start().await;
        // Half of the flood is syntactic garbage (refused before any permit), half is a
        // well-formed JWT under the wrong key (takes a permit, fails the signature).
        let wrong_key =
            auth_test::sign_with(&auth_test::claims("sid_x"), auth_test::OTHER_PRIVATE_PEM);
        let mut failures = Vec::new();
        for index in 0..64 {
            let addr = server.addr;
            let bearer = if index % 2 == 0 {
                "Bearer garbage".to_owned()
            } else {
                format!("Bearer {wrong_key}")
            };
            failures.push(tokio::spawn(async move {
                http_request(
                    addr,
                    Request::builder()
                        .method("GET")
                        .uri("/files?path=x")
                        .header(header::HOST, "127.0.0.1")
                        .header(header::AUTHORIZATION, bearer)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .await
            }));
        }
        // Let the flood reach the server before the valid upgrade is sent.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = std::time::Instant::now();
        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("sid_1", "inst_1", false, None).await;
        assert!(client.hello_ack().await.is_some());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "valid token waited {:?}",
            started.elapsed()
        );
        for failure in failures {
            assert_eq!(failure.await.unwrap().status, StatusCode::UNAUTHORIZED);
        }
        assert_eq!(server.state.auth_failures(), 64);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failure_penalty_is_per_peer_and_capped() {
        let server = TestServer::start().await;
        let peer: IpAddr = "10.0.0.7".parse().unwrap();
        let other: IpAddr = "10.0.0.8".parse().unwrap();
        for step in 1..=AUTH_PENALTY_MAX_STEPS + 2 {
            let started = std::time::Instant::now();
            assert!(server.state.verify("garbage", peer).await.is_err());
            let expected = AUTH_FAILURE_DELAY * step.min(AUTH_PENALTY_MAX_STEPS);
            assert!(
                started.elapsed() >= expected,
                "step {step}: waited {:?}, expected at least {expected:?}",
                started.elapsed()
            );
        }
        let started = std::time::Instant::now();
        assert!(server.state.verify("garbage", other).await.is_err());
        assert!(
            started.elapsed() < AUTH_FAILURE_DELAY * 2,
            "another peer starts at the base delay"
        );
        let started = std::time::Instant::now();
        assert!(
            server
                .state
                .verify(&server.token("sid_1"), peer)
                .await
                .is_ok()
        );
        assert!(
            started.elapsed() < AUTH_FAILURE_DELAY,
            "a valid token is never delayed"
        );
        let started = std::time::Instant::now();
        assert!(server.state.verify("garbage", peer).await.is_err());
        assert!(
            started.elapsed() < AUTH_FAILURE_DELAY * 2,
            "success resets the streak"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_without_subprotocol_is_426() {
        let server = TestServer::start().await;
        let token = server.token("sid_1");
        let outcome = http_request(
            server.addr,
            Request::builder()
                .method("GET")
                .uri("/rpc")
                .header(header::HOST, "127.0.0.1")
                .header(header::UPGRADE, "websocket")
                .header(header::CONNECTION, "Upgrade")
                .header(header::SEC_WEBSOCKET_VERSION, "13")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(
            outcome.json::<serde_json::Value>()["error"],
            "subprotocol_not_offered"
        );

        let outcome = http_request(
            server.addr,
            Request::builder()
                .method("GET")
                .uri(format!("/rpc?zs_proto=zs.v1&zs_token={token}"))
                .header(header::HOST, "127.0.0.1")
                .header(header::UPGRADE, "websocket")
                .header(header::CONNECTION, "Upgrade")
                .header(header::SEC_WEBSOCKET_VERSION, "13")
                .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::SWITCHING_PROTOCOLS);
        assert!(outcome.header("sec-websocket-protocol").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_echoes_the_subprotocol_when_offered_in_the_header() {
        let server = TestServer::start().await;
        let token = server.token("sid_1");
        let outcome = http_request(
            server.addr,
            Request::builder()
                .method("GET")
                .uri("/rpc")
                .header(header::HOST, "127.0.0.1")
                .header(header::UPGRADE, "websocket")
                .header(header::CONNECTION, "Upgrade")
                .header(header::SEC_WEBSOCKET_VERSION, "13")
                .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
                .header(header::SEC_WEBSOCKET_PROTOCOL, format!("zs.v1, {token}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            outcome.header("sec-websocket-protocol").as_deref(),
            Some("zs.v1")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_disallowed_origin_is_403() {
        let server = TestServer::start_with(vec!["https://app.test".into()], None).await;
        let token = server.token("sid_1");
        let outcome = http_request(
            server.addr,
            with_origin(
                Request::builder()
                    .method("GET")
                    .uri("/rpc")
                    .header(header::HOST, "127.0.0.1")
                    .header(header::UPGRADE, "websocket")
                    .header(header::CONNECTION, "Upgrade")
                    .header(header::SEC_WEBSOCKET_VERSION, "13")
                    .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
                    .header(header::SEC_WEBSOCKET_PROTOCOL, format!("zs.v1, {token}"))
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
                "https://evil.test",
            ),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::FORBIDDEN);
        assert_eq!(
            outcome.json::<serde_json::Value>()["error"],
            "origin_not_allowed"
        );

        let mut client = TestClient::connect(server.addr, &token, &[])
            .await
            .expect("no Origin header is unaffected");
        client.hello("sid_1", "inst_1", false, None).await;
        assert!(client.hello_ack().await.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn files_preflight_and_cors_headers() {
        let server = TestServer::start_with(vec!["https://app.test".into()], None).await;
        for path in ["/files", "/extensions/x/assets/y", "/health"] {
            let outcome = http_request(
                server.addr,
                with_origin(
                    Request::builder()
                        .method("OPTIONS")
                        .uri(path)
                        .header(header::HOST, "127.0.0.1")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                    "https://app.test",
                ),
            )
            .await;
            assert_eq!(outcome.status, StatusCode::NO_CONTENT, "{path}");
            assert_eq!(
                outcome.header("access-control-allow-origin").as_deref(),
                Some("https://app.test")
            );
            assert_eq!(outcome.header("vary").as_deref(), Some("Origin"));
            assert_eq!(
                outcome.header("access-control-allow-methods").as_deref(),
                Some("GET, POST")
            );
            assert_eq!(
                outcome.header("access-control-allow-headers").as_deref(),
                Some("authorization, content-type")
            );
            assert_eq!(
                outcome.header("access-control-max-age").as_deref(),
                Some("600")
            );
        }

        let outcome =
            http_request(server.addr, with_origin(get("/health"), "https://app.test")).await;
        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(
            outcome.header("access-control-allow-origin").as_deref(),
            Some("https://app.test")
        );
        assert_eq!(
            outcome.header("access-control-expose-headers").as_deref(),
            Some("content-disposition, content-length")
        );
        assert_eq!(
            outcome.header("cross-origin-resource-policy").as_deref(),
            Some("cross-origin")
        );

        let outcome = http_request(
            server.addr,
            with_origin(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/rpc")
                    .header(header::HOST, "127.0.0.1")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
                "https://app.test",
            ),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::METHOD_NOT_ALLOWED);

        let denied = http_request(
            server.addr,
            with_origin(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/files")
                    .header(header::HOST, "127.0.0.1")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
                "https://evil.test",
            ),
        )
        .await;
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        assert_eq!(
            denied.json::<serde_json::Value>()["error"],
            "origin_not_allowed"
        );
        assert!(denied.header("access-control-allow-origin").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_routes_and_methods() {
        let server = TestServer::start().await;
        let outcome = http_request(server.addr, get("/nope")).await;
        assert_eq!(outcome.status, StatusCode::NOT_FOUND);
        let outcome = http_request(server.addr, post_json("/health", "{}")).await;
        assert_eq!(outcome.status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn control_routes_only_on_the_control_listener() {
        let server = TestServer::start().await;
        let body = "{\"kind\":\"idle_stop_in\",\"seconds\":300}";

        let public = http_request(server.addr, {
            let mut request = post_json(CONTROL_LIFECYCLE_PATH, body);
            request.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {CONTROL_SECRET}")).unwrap(),
            );
            request
        })
        .await;
        assert_eq!(public.status, StatusCode::NOT_FOUND);
        assert!(server.control.seen().is_empty());

        let authorized = http_request(server.control_addr, {
            let mut request = post_json(CONTROL_LIFECYCLE_PATH, body);
            request.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {CONTROL_SECRET}")).unwrap(),
            );
            request
        })
        .await;
        assert_eq!(authorized.status, StatusCode::NO_CONTENT);
        assert_eq!(
            authorized.header("cache-control").as_deref(),
            Some("no-store")
        );
        let seen = server.control.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "POST");
        assert_eq!(seen[0].1, CONTROL_LIFECYCLE_PATH);
        assert!(seen[0].2, "peer_is_loopback");
        assert!(!seen[0].3, "session_attached");
        assert_eq!(seen[0].4, body.len());

        let wrong_bearer = http_request(server.control_addr, {
            let mut request = post_json(CONTROL_PORTS_PATH, "{\"ports\":[]}");
            request.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer nope"),
            );
            request
        })
        .await;
        assert_eq!(wrong_bearer.status, StatusCode::UNAUTHORIZED);

        let missing_bearer = http_request(
            server.control_addr,
            post_json(CONTROL_EXTENSIONS_PATH, "{}"),
        )
        .await;
        assert_eq!(missing_bearer.status, StatusCode::UNAUTHORIZED);

        let wrong_method = http_request(server.control_addr, get(CONTROL_PORTS_PATH)).await;
        assert_eq!(wrong_method.status, StatusCode::METHOD_NOT_ALLOWED);

        let unknown = http_request(server.control_addr, post_json("/control/other", "{}")).await;
        assert_eq!(unknown.status, StatusCode::NOT_FOUND);

        let oversize = http_request(server.control_addr, {
            let mut request =
                post_json(CONTROL_PORTS_PATH, &"x".repeat(MAX_CONTROL_BODY_BYTES + 1));
            request.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {CONTROL_SECRET}")).unwrap(),
            );
            request
        })
        .await;
        assert_eq!(oversize.status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn control_reports_session_attached() {
        let server = TestServer::start().await;
        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("sid_1", "inst_1", false, None).await;
        client.hello_ack().await.expect("hello ack");

        let outcome = http_request(server.control_addr, {
            let mut request = post_json(CONTROL_LIFECYCLE_PATH, "{\"kind\":\"stopping\"}");
            request.headers_mut().insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {CONTROL_SECRET}")).unwrap(),
            );
            request
        })
        .await;
        assert_eq!(outcome.status, StatusCode::NO_CONTENT);
        let seen = server.control.seen();
        assert!(seen.last().expect("a request").3, "session_attached");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn files_requests_require_a_valid_token() {
        let server = TestServer::start().await;
        let outcome = http_request(server.addr, get("/files?path=x")).await;
        assert_eq!(outcome.status, StatusCode::UNAUTHORIZED);

        let mut claims = auth_test::claims("sid_1");
        claims.ws = "ws_other".into();
        let wrong_workspace = auth_test::sign(&claims);
        let outcome = http_request(
            server.addr,
            Request::builder()
                .method("GET")
                .uri("/files?path=x")
                .header(header::HOST, "127.0.0.1")
                .header(header::AUTHORIZATION, format!("Bearer {wrong_workspace}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await;
        assert_eq!(outcome.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_asset_route_reaches_the_gpui_side() {
        let server = TestServer::start().await;
        let token = server.token("sid_1");
        let request = Request::builder()
            .method("GET")
            .uri("/extensions/theme-x/assets/themes/x.json")
            .header(header::HOST, "127.0.0.1")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let stub = async {
            match server.next_gpui_command().await {
                Some(crate::serve::GpuiCommand::ResolveExtensionAsset { id, rel, reply }) => {
                    assert_eq!(id, "theme-x");
                    assert_eq!(rel, "themes/x.json");
                    reply.send(None).ok();
                }
                other => panic!("unexpected command {other:?}"),
            }
        };
        let (outcome, ()) = tokio::join!(http_request(server.addr, request), stub);
        assert_eq!(outcome.status, StatusCode::NOT_FOUND);

        let traversal = Request::builder()
            .method("GET")
            .uri("/extensions/theme-x/assets/..%2Fx.json")
            .header(header::HOST, "127.0.0.1")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let outcome = http_request(server.addr, traversal).await;
        assert_eq!(outcome.status, StatusCode::BAD_REQUEST);

        // `.` as the id would resolve `rel` against the whole extensions tree.
        let dot_id = Request::builder()
            .method("GET")
            .uri("/extensions/./assets/other-extension/extension.toml")
            .header(header::HOST, "127.0.0.1")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let outcome = http_request(server.addr, dot_id).await;
        assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
        assert!(
            server.gpui_rx.lock().await.try_recv().is_err(),
            "a bad id never reaches the gpui side"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_asset_path_parsing() {
        assert_eq!(
            parse_extension_asset_path("/extensions/theme-x/assets/themes/x.json"),
            Some(("theme-x".to_owned(), "themes/x.json".to_owned()))
        );
        assert_eq!(
            parse_extension_asset_path("/extensions/theme%2Dx/assets/a%2Fb.json"),
            Some(("theme-x".to_owned(), "a/b.json".to_owned()))
        );
        assert_eq!(
            parse_extension_asset_path("/extensions/theme-x/assets/"),
            Some(("theme-x".to_owned(), ".".to_owned()))
        );
        assert_eq!(parse_extension_asset_path("/extensions/theme-x"), None);
    }
}
