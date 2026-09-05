//! `zed-remote-server serve`: the public HTTP/WebSocket listener, the loopback control
//! listener, the session broker and their bridge into the gpui-side `HeadlessProject`
//! (BUILD-SPEC §4.2, DECISIONS D21-D25).

pub mod auth;
pub mod files;
pub mod http;
pub mod session;
#[cfg(test)]
pub(crate) mod test_support;

use std::{
    io::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock, Weak},
    time::{Instant, SystemTime},
};

use anyhow::{Context as _, Result};
use futures::{FutureExt as _, StreamExt as _, channel::mpsc, future::BoxFuture};
use gpui::{App, Entity, TaskExt as _};
use gpui_tokio::Tokio;
use http_client::{HttpClient, HttpClientWithUrl};
use language::Buffer;
use project::{buffer_store::BufferStoreEvent, worktree_store::WorktreeStoreEvent};
use remote::{RemoteClient, ServerChannel, json_log::LogRecord};
use reqwest_client::ReqwestClient;
use rpc::AnyProtoClient;
use util::ResultExt as _;

use crate::{
    HeadlessProject, SandboxConfig, VERSION, build_headless_project, extensions::RegistryConfig,
    handle_crash_files_requests, init_crash_handler, init_paths, init_rayon_pool,
    init_telemetry_forwarding,
};
use auth::AuthConfig;
use http::{
    CONTROL_EXTENSIONS_PATH, CONTROL_LIFECYCLE_PATH, CONTROL_PORTS_PATH, ControlRequest,
    ControlResponse, ControlRoutes, ServeConfig, ServeState,
};
use session::{
    BrokerCommand, LOG_FRAME_MAX_LEVEL, ServeHooks, SessionBroker, SessionKind, SessionMeta,
};

pub use crate::ports::DEFAULT_SUPERVISOR_URL;
/// Default bind address of the loopback control listener (D21).
pub const DEFAULT_CONTROL_LISTEN: &str = "127.0.0.1:8451";
/// Largest accepted `--control-secret-file`.
pub const MAX_CONTROL_SECRET_BYTES: usize = 4096;

/// Arguments of the `serve` subcommand.
#[derive(clap::Args, Debug, Clone)]
pub struct ServeArgs {
    /// Address to bind, e.g. 0.0.0.0:8443; port 0 picks an ephemeral port (tests).
    #[arg(long)]
    pub listen: SocketAddr,
    /// PEM file with one or more ES256 public keys; repeatable (two keys during rotation).
    #[arg(long = "jwt-public-key", required = true)]
    pub jwt_public_keys: Vec<PathBuf>,
    /// Expected `ws` claim; also stamped on every log line.
    #[arg(long, visible_alias = "workspace")]
    pub workspace_id: String,
    /// Expected `aud` claim (`manifest.jwt.audience`; defaults to the sandbox name, rotatable
    /// by the control plane).
    #[arg(long)]
    pub audience: String,
    /// Expected `iss` claim.
    #[arg(long, default_value = "zs")]
    pub issuer: String,
    /// Directory that bounds `/files`; must exist.
    #[arg(long)]
    pub workspace_root: PathBuf,
    /// If set, a Hello whose build is not compatible with this id is closed with 4002.
    #[arg(long, visible_alias = "allow-build")]
    pub client_build: Option<String>,
    /// Browser origins allowed to call /files, /extensions/* and /health cross-origin (the
    /// shell page origin; the supervisor passes `manifest.allowedOrigins`, one flag per
    /// origin); repeatable, or comma-separated in ZS_ALLOWED_ORIGINS. Empty disables CORS.
    #[arg(
        long = "allowed-origin",
        env = "ZS_ALLOWED_ORIGINS",
        value_delimiter = ','
    )]
    pub allowed_origins: Vec<String>,
    /// File holding the shared secret the supervisor presents as `Authorization: Bearer` on
    /// `/control/*`. Read once, before anything is spawned; trailing newline stripped; must be
    /// non-empty and at most 4 KiB.
    #[arg(long)]
    pub control_secret_file: PathBuf,
    /// Loopback address of the control listener. Must be a loopback IP; port 0 allowed (tests).
    #[arg(long, default_value = DEFAULT_CONTROL_LISTEN)]
    pub control_listen: SocketAddr,
    /// Base URL of the supervisor's API for port forwards and extension installs.
    #[arg(long, env = "ZS_SUPERVISOR_URL", default_value = DEFAULT_SUPERVISOR_URL)]
    pub supervisor_url: String,
    /// After binding, write "<ip>:<port>\n" of the public listener here (supervisor/tests).
    #[arg(long)]
    pub port_file: Option<PathBuf>,
    /// Also append JSON logs to this rotating file (same format as `run`).
    #[arg(long)]
    pub log_file: Option<PathBuf>,
    /// Override the data and configuration directory (`paths::set_custom_data_dir`). The
    /// supervisor never passes it; tests and sandboxes with a read-only home do.
    #[arg(long)]
    pub user_data_dir: Option<PathBuf>,
}

/// Commands the tokio side sends to the gpui foreground task.
#[derive(Debug)]
pub enum GpuiCommand {
    /// Broker, fresh attach: reset the project and hand the channel client a new pair.
    ResetForFreshSession {
        /// Receives the broker's new channel ends.
        done: futures::channel::oneshot::Sender<remote::ChannelEnds>,
    },
    /// Broker, after every attach.
    SessionAttached {
        /// How the session attached.
        kind: SessionKind,
    },
    /// Broker, after every session-task exit.
    SessionDetached,
    /// `/files` upload, after its renames: absolute paths under the workspace root.
    FilesUploaded(Vec<PathBuf>),
    /// `/extensions/{id}/assets/{rel}`: resolve an installed extension's asset path.
    ResolveExtensionAsset {
        /// Extension id.
        id: String,
        /// Asset path relative to the extension directory.
        rel: String,
        /// `None` for unknown ids, uninstalled extensions and any traversal.
        reply: futures::channel::oneshot::Sender<Option<PathBuf>>,
    },
    /// SIGTERM/SIGINT after the session was closed: kill terminals and quit the app.
    Quit,
}

/// Server-level PTY manager hooks (b3's `PtyManager`, constructed outside `HeadlessProject`
/// per D24). `detach_all` on every session detach; `kill_all` only on process quit.
pub trait PtyHooks: 'static {
    /// Detach every terminal's output stream; the processes keep running into their rings.
    fn detach_all(&self);
    /// Kill every terminal process group.
    fn kill_all(&self);
}

/// The `PtyHooks` used until a PTY manager is installed.
pub struct NoPtys;

impl PtyHooks for NoPtys {
    fn detach_all(&self) {
        log::debug!("no PTY manager installed; nothing to detach");
    }

    fn kill_all(&self) {
        log::debug!("no PTY manager installed; nothing to kill");
    }
}

impl PtyHooks for crate::pty::PtyManager {
    fn detach_all(&self) {
        crate::pty::PtyManager::detach_all(self);
    }

    fn kill_all(&self) {
        crate::pty::PtyManager::kill_all(self);
    }
}

/// The three gpui-side calls the command loop makes on the project (b4's
/// `HeadlessProject::{on_session_attached, notify_files_uploaded}` and
/// `HeadlessExtensionStore::asset_path`); the default implementation covers the pieces that
/// exist without the sandbox extensions.
pub trait ProjectHooks: 'static {
    /// After a fresh attach: replay whatever server-side state the new client needs.
    fn on_session_attached(
        &self,
        project: &Entity<HeadlessProject>,
        kind: SessionKind,
        cx: &mut App,
    );
    /// After an upload landed: rescan the written paths.
    fn files_uploaded(&self, project: &Entity<HeadlessProject>, paths: Vec<PathBuf>, cx: &mut App);
    /// Resolve an installed extension's asset, confined to the extensions directory.
    fn extension_asset_path(
        &self,
        project: &Entity<HeadlessProject>,
        id: &str,
        rel: &str,
        cx: &mut App,
    ) -> Option<PathBuf>;
}

/// `ProjectHooks` without b4's sandbox extensions: rescans uploaded paths in their worktrees
/// and resolves assets directly under `paths::remote_extensions_dir()`.
pub struct DefaultProjectHooks;

impl ProjectHooks for DefaultProjectHooks {
    fn on_session_attached(
        &self,
        _project: &Entity<HeadlessProject>,
        kind: SessionKind,
        _cx: &mut App,
    ) {
        log::debug!("session attached ({kind:?}); no sandbox state to replay");
    }

    fn files_uploaded(&self, project: &Entity<HeadlessProject>, paths: Vec<PathBuf>, cx: &mut App) {
        let worktree_store = project.read(cx).worktree_store.clone();
        let worktrees: Vec<_> = worktree_store.read(cx).worktrees().collect();
        for worktree in worktrees {
            let root = worktree.read(cx).abs_path();
            let relative: Vec<Arc<util::rel_path::RelPath>> = paths
                .iter()
                .filter_map(|path| path.strip_prefix(&root).ok())
                .filter_map(|relative| {
                    util::rel_path::RelPath::new(relative, util::paths::PathStyle::local())
                        .ok()
                        .map(|relative| relative.into_arc())
                })
                .collect();
            if relative.is_empty() {
                continue;
            }
            worktree.update(cx, |worktree, _| {
                if let Some(local) = worktree.as_local() {
                    drop(local.refresh_entries_for_paths(relative));
                }
            });
        }
    }

    fn extension_asset_path(
        &self,
        project: &Entity<HeadlessProject>,
        id: &str,
        rel: &str,
        cx: &mut App,
    ) -> Option<PathBuf> {
        // With the sandbox runtime the installed set is authoritative: a directory left
        // behind by a failed uninstall (or planted next to the extensions) is not served.
        let installed = project
            .read(cx)
            .sandbox
            .as_ref()
            .map(|sandbox| sandbox.extensions.read(cx).is_installed(id));
        if installed == Some(false) {
            return None;
        }
        resolve_extension_asset(paths::remote_extensions_dir(), id, rel)
    }
}

/// `extensions_dir/<id>/<rel>` when `id` is a valid extension id, `<id>` is a directory
/// directly under `extensions_dir` after symlink resolution, and `rel` stays inside it;
/// `None` for anything else, traversal included.
pub fn resolve_extension_asset(extensions_dir: &Path, id: &str, rel: &str) -> Option<PathBuf> {
    if !files::is_valid_extension_id(id) {
        return None;
    }
    let extensions_dir = extensions_dir.canonicalize().ok()?;
    let extension_dir = extensions_dir.join(id).canonicalize().ok()?;
    // The id grammar already excludes `.`/`..`; this keeps a symlinked extension
    // directory from resolving assets anywhere but under the extensions tree.
    if extension_dir.parent() != Some(extensions_dir.as_path()) || !extension_dir.is_dir() {
        return None;
    }
    files::resolve_decoded(&extension_dir, rel).ok()
}

/// `ProjectHooks` for the sandbox runtime: replays server-side state after every attach,
/// notifies the client of uploads, and resolves assets like [`DefaultProjectHooks`].
pub struct SandboxProjectHooks;

impl ProjectHooks for SandboxProjectHooks {
    fn on_session_attached(
        &self,
        project: &Entity<HeadlessProject>,
        kind: SessionKind,
        cx: &mut App,
    ) {
        log::debug!("session attached ({kind:?}); replaying sandbox state");
        project.update(cx, |project, cx| project.on_session_attached(cx));
    }

    fn files_uploaded(&self, project: &Entity<HeadlessProject>, paths: Vec<PathBuf>, cx: &mut App) {
        let mut async_cx = cx.to_async();
        HeadlessProject::notify_files_uploaded(project.downgrade(), paths, &mut async_cx)
            .detach_and_log_err(cx);
    }

    fn extension_asset_path(
        &self,
        project: &Entity<HeadlessProject>,
        id: &str,
        rel: &str,
        cx: &mut App,
    ) -> Option<PathBuf> {
        DefaultProjectHooks.extension_asset_path(project, id, rel, cx)
    }
}

/// Control routes that authenticate the bearer in constant time, validate the bodies of
/// the three routes and acknowledge them without forwarding anything; kept for `serve`
/// tests that run without a `HeadlessProject`. Production mounts
/// `control::ControlChannel` (installed by `HeadlessProject::enable_sandbox`).
pub struct PendingControlRoutes {
    secret: Vec<u8>,
}

impl PendingControlRoutes {
    /// Routes gated by `secret`.
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret }
    }

    fn authorized(&self, req: &ControlRequest<'_>) -> bool {
        ControlRoutes::authorized(self, req.bearer, req.peer_is_loopback)
    }
}

/// `POST /control/lifecycle` body.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LifecycleBody {
    /// The workspace stops in `seconds` unless activity resumes.
    IdleStopIn {
        /// Countdown.
        seconds: u32,
    },
    /// The session cap is reached in `seconds`.
    SessionCapIn {
        /// Countdown.
        seconds: u32,
    },
    /// The workspace is stopping now.
    Stopping,
    /// The workspace resumed.
    Resumed,
}

impl ControlRoutes for PendingControlRoutes {
    fn authorized(&self, bearer: Option<&str>, peer_is_loopback: bool) -> bool {
        use subtle::ConstantTimeEq as _;
        peer_is_loopback
            && bearer.is_some_and(|bearer| bearer.as_bytes().ct_eq(&self.secret).unwrap_u8() == 1)
    }

    fn handle<'a>(&'a self, req: ControlRequest<'a>) -> BoxFuture<'a, ControlResponse> {
        async move {
            if !self.authorized(&req) {
                return ControlResponse::Unauthorized;
            }
            if req.method != "POST" {
                return ControlResponse::NotFound;
            }
            match req.path {
                CONTROL_LIFECYCLE_PATH => match serde_json::from_slice::<LifecycleBody>(req.body) {
                    Ok(body) => {
                        log::info!(
                            "lifecycle notice {body:?} (session attached: {})",
                            req.session_attached
                        );
                        ControlResponse::NoContent
                    }
                    Err(error) => ControlResponse::BadRequest(error.to_string()),
                },
                CONTROL_PORTS_PATH => match serde_json::from_slice::<serde_json::Value>(req.body) {
                    Ok(value) if value.is_object() => {
                        log::info!("ports update received ({} bytes)", req.body.len());
                        ControlResponse::NoContent
                    }
                    Ok(_) => ControlResponse::BadRequest("expected a JSON object".to_owned()),
                    Err(error) => ControlResponse::BadRequest(error.to_string()),
                },
                CONTROL_EXTENSIONS_PATH => {
                    #[derive(serde::Deserialize)]
                    struct Install {
                        install: Vec<String>,
                    }
                    match serde_json::from_slice::<Install>(req.body) {
                        Ok(install) => {
                            if let Some(invalid) = install
                                .install
                                .iter()
                                .find(|id| !files::is_valid_extension_id(id))
                            {
                                return ControlResponse::BadRequest(format!(
                                    "invalid extension id {invalid:?}"
                                ));
                            }
                            log::info!("extension install request for {:?}", install.install);
                            ControlResponse::NoContent
                        }
                        Err(error) => ControlResponse::BadRequest(error.to_string()),
                    }
                }
                _ => ControlResponse::NotFound,
            }
        }
        .boxed()
    }
}

/// The build id reported in `/health` and `HelloAck.build`: `ZS_BUILD_ID` else `VERSION`.
pub fn server_build() -> String {
    remote::websocket_wire::ZS_BUILD_ID
        .map(str::to_owned)
        .unwrap_or_else(|| VERSION.clone())
}

static CURRENT_SESSION: RwLock<Option<(String, u64)>> = RwLock::new(None);

/// The one `ServeState` of this process, for the log formatter's `Log` frame mirror. Set
/// once by `execute_serve` after the state exists (the logger is initialised before it);
/// tests never set it and mirror through their own state instead.
static LOG_STATE: OnceLock<Weak<ServeState>> = OnceLock::new();

/// Registers `state` as the target of mirrored warn/error records. Only the first call in a
/// process takes effect.
pub fn install_log_state(state: &Arc<ServeState>) {
    if LOG_STATE.set(Arc::downgrade(state)).is_err() {
        log::debug!("log state was already installed");
    }
}

/// Validates the arguments that must be right before anything is bound or spawned.
pub fn validate_serve_args(args: &ServeArgs) -> Result<()> {
    anyhow::ensure!(
        args.control_listen.ip().is_loopback(),
        "--control-listen must be a loopback address, got {}",
        args.control_listen
    );
    validate_supervisor_url(&args.supervisor_url)?;
    Ok(())
}

/// The supervisor API is loopback-only (D21) and every call to it carries the control
/// secret as a bearer, so the URL must be plain `http` on a loopback host; anything else
/// (an `https` or remote host smuggled in through `ZS_SUPERVISOR_URL`) is refused before
/// any thread is spawned.
pub fn validate_supervisor_url(url: &str) -> Result<()> {
    let parsed = http_client::Url::parse(url)
        .with_context(|| format!("--supervisor-url {url:?} is not a valid URL"))?;
    anyhow::ensure!(
        parsed.scheme() == "http",
        "--supervisor-url must use the http scheme, got {url:?}"
    );
    let loopback = match parsed.host() {
        Some(http_client::Host::Domain(domain)) => domain == "localhost",
        Some(http_client::Host::Ipv4(address)) => address.is_loopback(),
        Some(http_client::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    anyhow::ensure!(
        loopback,
        "--supervisor-url must point at a loopback host, got {url:?}"
    );
    anyhow::ensure!(
        parsed.username().is_empty() && parsed.password().is_none(),
        "--supervisor-url must not carry credentials"
    );
    Ok(())
}

/// Records the attached session's id and epoch for the log formatter.
pub fn set_current_session(session: Option<(String, u64)>) {
    *CURRENT_SESSION
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = session;
}

fn current_session() -> Option<(String, u64)> {
    CURRENT_SESSION
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// One stderr log line in serve mode.
#[derive(serde::Serialize)]
pub struct ServeLogRecord<'a> {
    /// Unix milliseconds.
    pub ts_ms: u64,
    /// The record proper.
    #[serde(flatten)]
    pub record: LogRecord<'a>,
    /// The workspace id.
    pub ws: &'a str,
    /// JWT `sid` of the attached session, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Epoch of the attached session, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// Always `"serve"`.
    pub mode: &'static str,
}

/// Reads `--control-secret-file`: whole file, one trailing `\r?\n` stripped, non-empty and at
/// most [`MAX_CONTROL_SECRET_BYTES`]. A leftover `ZS_CONTROL_SECRET` in the environment is
/// scrubbed (it would be inherited by every language server, task and PTY) and never used; the
/// returned flag says whether one was found, so the caller can warn once the logger is up.
fn read_control_secret(path: &Path) -> Result<(Vec<u8>, bool)> {
    let mut secret =
        std::fs::read(path).with_context(|| format!("reading control secret file {path:?}"))?;
    if secret.last() == Some(&b'\n') {
        secret.pop();
        if secret.last() == Some(&b'\r') {
            secret.pop();
        }
    }
    anyhow::ensure!(!secret.is_empty(), "control secret file {path:?} is empty");
    anyhow::ensure!(
        secret.len() <= MAX_CONTROL_SECRET_BYTES,
        "control secret file {path:?} is larger than {MAX_CONTROL_SECRET_BYTES} bytes"
    );
    let scrubbed = std::env::var_os("ZS_CONTROL_SECRET").is_some();
    if scrubbed {
        // Safe: this runs before any other thread exists (the crash handler, rayon pool and
        // login-shell probe are spawned later in `execute_serve`).
        unsafe { std::env::remove_var("ZS_CONTROL_SECRET") };
    }
    Ok((secret, scrubbed))
}

/// env_logger to stderr (plus an optional rotating file tee), one [`ServeLogRecord`] per line,
/// the panic hook of `run`, and the warn/error mirror into the attached session's `Log` frames.
fn init_logging_serve(args: &ServeArgs) -> Result<()> {
    struct Target {
        file: Option<crate::RotatingLogFile>,
    }

    impl std::io::Write for Target {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if let Some(file) = self.file.as_mut() {
                file.write_all(buf)?;
            }
            std::io::stderr().write_all(buf)?;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if let Some(file) = self.file.as_mut() {
                file.flush()?;
            }
            std::io::stderr().flush()
        }
    }

    let file = match &args.log_file {
        Some(path) => Some(
            crate::RotatingLogFile::open(path)
                .with_context(|| format!("opening rotating log file {path:?}"))?,
        ),
        None => None,
    };
    let workspace_id: &'static str = Box::leak(args.workspace_id.clone().into_boxed_str());

    let old_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        let message = info.payload_as_str().unwrap_or("Box<Any>").to_owned();
        let location = info
            .location()
            .map_or_else(|| "<unknown>".to_owned(), |location| location.to_string());
        let current_thread = std::thread::current();
        let thread_name = current_thread.name().unwrap_or("<unnamed>");
        log::error!("thread '{thread_name}' panicked at {location}:\n{message}\n{backtrace}");
        old_hook(info);
    }));

    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .target(env_logger::Target::Pipe(Box::new(Target { file })))
        .format(move |buf, record| {
            let log_record = LogRecord::new(record);
            if record.level() <= LOG_FRAME_MAX_LEVEL
                && let Some(state) = LOG_STATE.get().and_then(Weak::upgrade)
            {
                state.mirror_record(record);
            }
            let (session_id, epoch) = match current_session() {
                Some((session_id, epoch)) => (Some(session_id), Some(epoch)),
                None => (None, None),
            };
            let line = ServeLogRecord {
                ts_ms: http::unix_ms(),
                record: log_record,
                ws: workspace_id,
                session_id,
                epoch,
                mode: "serve",
            };
            serde_json::to_writer(&mut *buf, &line)?;
            buf.write_all(b"\n")?;
            Ok(())
        })
        .try_init()
        .context("initializing the serve logger")?;
    Ok(())
}

/// `ServeHooks` over the gpui command channel. The replay buffer is edited directly through
/// the `ServerChannel` handle (its locks are plain mutexes), so no gpui round trip is needed.
struct GpuiHooks {
    tx: mpsc::UnboundedSender<GpuiCommand>,
    channel: ServerChannel,
}

impl ServeHooks for GpuiHooks {
    fn begin_fresh_session(&self) -> BoxFuture<'static, Result<remote::ChannelEnds>> {
        let (done_tx, done_rx) = futures::channel::oneshot::channel();
        let sent = self
            .tx
            .unbounded_send(GpuiCommand::ResetForFreshSession { done: done_tx })
            .is_ok();
        async move {
            anyhow::ensure!(
                sent,
                "gpui command loop is gone; a fresh session cannot reset the project"
            );
            done_rx
                .await
                .context("gpui command loop dropped the fresh-session reply")
        }
        .boxed()
    }

    fn session_attached(&self, meta: &SessionMeta) {
        self.tx
            .unbounded_send(GpuiCommand::SessionAttached { kind: meta.kind })
            .ok();
    }

    fn session_detached(&self, _meta: &SessionMeta) {
        self.tx.unbounded_send(GpuiCommand::SessionDetached).ok();
    }

    fn replace_buffered(&self, id: u32, replacement: Option<rpc::proto::Envelope>) {
        self.channel.replace_buffered(id, replacement);
    }

    fn request_quit(&self) {
        self.tx.unbounded_send(GpuiCommand::Quit).ok();
    }
}

/// Asks the broker to flush and close, then quit; if the broker is gone the quit goes to the
/// gpui loop directly, so SIGTERM never turns into a no-op that only the supervisor's
/// SIGKILL resolves.
async fn shutdown_via_broker(
    broker_tx: &tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
    gpui_tx: &mpsc::UnboundedSender<GpuiCommand>,
) {
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let handled = broker_tx
        .send(BrokerCommand::Shutdown { done: done_tx })
        .is_ok()
        && done_rx.await.is_ok();
    if !handled {
        log::error!("session broker is gone; quitting the app directly");
        if gpui_tx.unbounded_send(GpuiCommand::Quit).is_err() {
            log::error!("gpui command loop is gone too; exiting the process");
            std::process::exit(1);
        }
    }
}

/// gpui-side loop over [`GpuiCommand`]s. Holds an `AsyncApp` so it can call
/// `ServerChannel::begin_fresh_session`.
fn spawn_gpui_command_loop(
    project: Entity<HeadlessProject>,
    server_channel: ServerChannel,
    pty: Arc<dyn PtyHooks>,
    project_hooks: Box<dyn ProjectHooks>,
    mut rx: mpsc::UnboundedReceiver<GpuiCommand>,
    cx: &mut App,
) {
    cx.spawn(async move |cx| {
        while let Some(command) = rx.next().await {
            match command {
                GpuiCommand::ResetForFreshSession { done } => {
                    let discarded =
                        project.update(cx, |project, cx| project.reset_for_new_client(cx));
                    log::info!(
                        "project reset for a fresh session ({discarded} dirty buffers discarded)"
                    );
                    let ends = server_channel.begin_fresh_session(cx);
                    done.send(ends).ok();
                }
                GpuiCommand::SessionAttached { kind } => {
                    cx.update(|cx| project_hooks.on_session_attached(&project, kind, cx));
                }
                GpuiCommand::SessionDetached => pty.detach_all(),
                GpuiCommand::FilesUploaded(paths) => {
                    cx.update(|cx| project_hooks.files_uploaded(&project, paths, cx));
                }
                GpuiCommand::ResolveExtensionAsset { id, rel, reply } => {
                    let path =
                        cx.update(|cx| project_hooks.extension_asset_path(&project, &id, &rel, cx));
                    reply.send(path).ok();
                }
                GpuiCommand::Quit => {
                    pty.kill_all();
                    cx.update(|cx| {
                        // Same hack as `run`: in a headless app `quit` does not run `shutdown`.
                        cx.shutdown();
                        cx.quit();
                    });
                    break;
                }
            }
        }
    })
    .detach();
}

#[cfg(unix)]
async fn signal_task(
    broker_tx: tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
    gpui_tx: mpsc::UnboundedSender<GpuiCommand>,
) {
    use tokio::signal::unix::{SignalKind, signal};
    let (mut terminate, mut interrupt) = match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(terminate), Ok(interrupt)) => (terminate, interrupt),
        (terminate, interrupt) => {
            log::error!(
                "installing signal handlers failed: {:?} {:?}",
                terminate.err(),
                interrupt.err()
            );
            return;
        }
    };
    let name = tokio::select! {
        _ = terminate.recv() => "SIGTERM",
        _ = interrupt.recv() => "SIGINT",
    };
    log::info!("{name} received; closing the session and quitting");
    shutdown_via_broker(&broker_tx, &gpui_tx).await;
}

#[cfg(windows)]
async fn signal_task(
    broker_tx: tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
    gpui_tx: mpsc::UnboundedSender<GpuiCommand>,
) {
    if let Err(error) = tokio::signal::ctrl_c().await {
        log::error!("installing the Ctrl-C handler failed: {error}");
        return;
    }
    log::info!("Ctrl-C received; closing the session and quitting");
    shutdown_via_broker(&broker_tx, &gpui_tx).await;
}

fn bind_listener(addr: SocketAddr, what: &str) -> Result<std::net::TcpListener> {
    let listener =
        std::net::TcpListener::bind(addr).with_context(|| format!("binding {what} on {addr}"))?;
    listener
        .set_nonblocking(true)
        .with_context(|| format!("configuring {what} listener"))?;
    Ok(listener)
}

/// Serve analogue of `start_server`: creates the envelope channels and the `ServerChannel`,
/// binds both listeners, writes `--port-file`, prints `ZS_LISTENING=` and
/// `ZS_CONTROL_LISTENING=` on stdout, and spawns the public accept loop, the broker and the
/// signal task on the gpui_tokio runtime. Returns the bound control listener for
/// [`http::serve_control`]. No idle timeout.
fn start_serve(
    args: &ServeArgs,
    state: Arc<ServeState>,
    broker_rx: tokio::sync::mpsc::UnboundedReceiver<BrokerCommand>,
    gpui_tx: mpsc::UnboundedSender<GpuiCommand>,
    cx: &mut App,
) -> Result<(AnyProtoClient, ServerChannel, tokio::net::TcpListener)> {
    let (incoming_tx, incoming_rx) = mpsc::unbounded();
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
    // `has_wsl_interop` is false by construction: serve only ever runs inside the Linux
    // sandbox, never under WSL, so `run`'s interop probe is not repeated here.
    let server_channel =
        RemoteClient::server_channel_from_channels(incoming_rx, outgoing_tx, cx, "server", false);
    let session = server_channel.proto_client();

    let public = bind_listener(args.listen, "the public listener")?;
    let control = bind_listener(args.control_listen, "the control listener")?;
    let public_addr = public.local_addr().context("reading the public address")?;
    let control_addr = control
        .local_addr()
        .context("reading the control address")?;
    log::info!("listening on {public_addr}");

    let handle = Tokio::handle(cx);
    let (public, control) = {
        let _guard = handle.enter();
        (
            tokio::net::TcpListener::from_std(public).context("registering the public listener")?,
            tokio::net::TcpListener::from_std(control)
                .context("registering the control listener")?,
        )
    };

    if let Some(port_file) = &args.port_file {
        std::fs::write(port_file, format!("{public_addr}\n"))
            .with_context(|| format!("writing port file {port_file:?}"))?;
    }
    {
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(
                format!("ZS_LISTENING={public_addr}\nZS_CONTROL_LISTENING={control_addr}\n")
                    .as_bytes(),
            )
            .context("announcing the listeners on stdout")?;
        stdout.flush().context("flushing stdout")?;
    }

    let broker = SessionBroker::new(
        incoming_tx,
        outgoing_rx,
        broker_rx,
        state.clone(),
        Arc::new(GpuiHooks {
            tx: gpui_tx.clone(),
            channel: server_channel.clone(),
        }),
    );
    Tokio::spawn(
        cx,
        http::serve_http(public, state.clone()).map(|result| result.log_err()),
    )
    .detach();
    Tokio::spawn(cx, broker.run()).detach();
    Tokio::spawn(cx, signal_task(state.broker_tx.clone(), gpui_tx)).detach();
    Ok((session, server_channel, control))
}

fn count_dirty_buffers(buffer_store: &Entity<project::buffer_store::BufferStore>, cx: &App) -> u32 {
    buffer_store
        .read(cx)
        .buffers()
        .filter(|buffer: &Entity<Buffer>| buffer.read(cx).is_dirty())
        .count() as u32
}

fn worktree_roots(project: &Entity<HeadlessProject>, cx: &App) -> Vec<String> {
    project
        .read(cx)
        .worktree_store
        .read(cx)
        .worktrees()
        .map(|worktree| worktree.read(cx).abs_path().to_string_lossy().into_owned())
        .collect()
}

/// Keeps `/health`'s `worktrees` and `dirty_buffers` current.
fn observe_project_for_health(
    project: &Entity<HeadlessProject>,
    state: Arc<ServeState>,
    cx: &mut App,
) {
    let worktree_store = project.read(cx).worktree_store.clone();
    cx.subscribe(&worktree_store, {
        let project = project.clone();
        let state = state.clone();
        move |_, event: &WorktreeStoreEvent, cx| {
            if matches!(
                event,
                WorktreeStoreEvent::WorktreeAdded(_)
                    | WorktreeStoreEvent::WorktreeRemoved(..)
                    | WorktreeStoreEvent::WorktreeReleased(..)
            ) {
                state.set_worktrees(worktree_roots(&project, cx));
            }
        }
    })
    .detach();

    let buffer_store = project.read(cx).buffer_store.clone();
    cx.subscribe(&buffer_store, {
        move |buffer_store, event: &BufferStoreEvent, cx| match event {
            BufferStoreEvent::BufferAdded(buffer) => {
                state.set_dirty_buffers(count_dirty_buffers(&buffer_store, cx));
                cx.observe(buffer, {
                    let state = state.clone();
                    move |_, cx| state.set_dirty_buffers(count_dirty_buffers(&buffer_store, cx))
                })
                .detach();
            }
            BufferStoreEvent::BufferDropped(_) => {
                state.set_dirty_buffers(count_dirty_buffers(&buffer_store, cx));
            }
            _ => {}
        }
    })
    .detach();
}

/// Runs the `serve` subcommand until SIGTERM/SIGINT quits the gpui app.
pub fn execute_serve(args: ServeArgs) -> Result<()> {
    let (control_secret, scrubbed_env_secret) = read_control_secret(&args.control_secret_file)?;
    validate_serve_args(&args)?;
    crate::set_user_data_dir(args.user_data_dir.as_deref())?;
    init_paths()?;
    init_logging_serve(&args)?;
    if scrubbed_env_secret {
        log::warn!(
            "ZS_CONTROL_SECRET was set in the environment; scrubbed it (use --control-secret-file)"
        );
    }
    let auth = AuthConfig::load(
        &args.jwt_public_keys,
        &args.issuer,
        &args.audience,
        &args.workspace_id,
    )?;
    let workspace_root = args
        .workspace_root
        .canonicalize()
        .with_context(|| format!("canonicalizing --workspace-root {:?}", args.workspace_root))?;
    anyhow::ensure!(
        workspace_root.is_dir(),
        "--workspace-root {workspace_root:?} is not a directory"
    );

    let startup_time = Instant::now();
    let app = gpui_platform::headless();
    let crash_handler = init_crash_handler(&app, "zed-remote-server-serve");
    // Debug so that `listening on <addr>` (logged right after the bind) is the first info
    // line on stderr, as CONTRACTS §2 promises.
    log::debug!(
        "starting serve with PID {} for workspace {} (root {workspace_root:?})",
        std::process::id(),
        args.workspace_id
    );
    init_rayon_pool();

    #[cfg(unix)]
    let shell_env_loaded_rx = {
        let (shell_env_loaded_tx, shell_env_loaded_rx) = futures::channel::oneshot::channel();
        app.background_executor()
            .spawn(async {
                util::load_login_shell_environment().await.log_err();
                shell_env_loaded_tx.send(()).ok();
            })
            .detach();
        Some(shell_env_loaded_rx)
    };
    #[cfg(windows)]
    let shell_env_loaded_rx: Option<futures::channel::oneshot::Receiver<()>> = None;

    let git_hosting_provider_registry = Arc::new(git::GitHostingProviderRegistry::new());
    let run = move |cx: &mut App| -> Result<()> {
        if let Some(crash_handler) = crash_handler {
            cx.spawn(async move |_cx| {
                let _crash_handler = crash_handler.await;
            })
            .detach();
        }
        settings::init(cx);
        let app_commit_sha = option_env!("ZED_COMMIT_SHA")
            .map(|sha| release_channel::AppCommitSha::new(sha.to_owned()));
        let app_version = release_channel::AppVersion::load(
            env!("ZED_PKG_VERSION"),
            option_env!("ZED_BUILD_ID"),
            app_commit_sha,
        );
        release_channel::init(app_version, cx);
        gpui_tokio::init(cx);
        HeadlessProject::init(cx);

        let (broker_tx, broker_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gpui_tx, gpui_rx) = mpsc::unbounded();
        let state = Arc::new(ServeState::new(
            ServeConfig {
                build: server_build(),
                version: VERSION.clone(),
                workspace_id: args.workspace_id.clone(),
                workspace_root: workspace_root.clone(),
                auth,
                allowed_origins: args.allowed_origins.clone(),
                client_build: args.client_build.clone(),
            },
            broker_tx.clone(),
            gpui_tx.clone(),
        ));
        install_log_state(&state);
        log::debug!("gpui app started, initializing serve");
        let listeners_bound_at = SystemTime::now();
        let (session, server_channel, control_listener) =
            start_serve(&args, state.clone(), broker_rx, gpui_tx, cx)?;
        init_telemetry_forwarding(session.clone(), cx);
        project::trusted_worktrees::init(collections::HashMap::default(), cx);

        git::GitHostingProviderRegistry::set_global(git_hosting_provider_registry, cx);
        git_hosting_providers::init(cx);
        dap_adapters::init(cx);
        extension::init(cx);
        json_schema_store::init(cx);

        let project =
            build_headless_project(session.clone(), shell_env_loaded_rx, startup_time, cx);
        let on_shutdown: Arc<dyn Fn() + Send + Sync> = Arc::new({
            move || {
                broker_tx
                    .send(BrokerCommand::CloseSession {
                        code: 1000,
                        reason: "client requested shutdown",
                    })
                    .ok();
            }
        });
        project.update(cx, |project, _| {
            project.set_shutdown_request_handler(on_shutdown)
        });

        // The process-level PtyManager (D24): installed by the first HeadlessProject and
        // shared by every session; `detach_all` on detach, `kill_all` only on quit.
        let pty: Arc<dyn PtyHooks> = project.read(cx).pty_manager.clone();
        // Loopback traffic must never go through a proxy: the supervisor calls carry the
        // control secret as a bearer, and the sandbox environment (repo `remoteEnv`,
        // manifest env) may set `HTTP_PROXY`/`ALL_PROXY`, so the supervisor and registry
        // client is built with proxies disabled (unlike the session's client).
        let supervisor_http: Arc<dyn HttpClient> = {
            let _guard = Tokio::handle(cx).enter();
            Arc::new(
                ReqwestClient::user_agent_without_proxy(&format!(
                    "Zed-Server/{} ({}; {})",
                    env!("CARGO_PKG_VERSION"),
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ))
                .context("building the supervisor HTTP client")?,
            )
        };
        let registry = RegistryConfig {
            http: Arc::new(HttpClientWithUrl::new(
                supervisor_http.clone(),
                std::env::var("ZED_SERVER_URL").unwrap_or_else(|_| "https://zed.dev".into()),
                None,
            )),
            release_channel: *release_channel::RELEASE_CHANNEL,
        };
        let control_routes: Arc<dyn ControlRoutes> = project.update(cx, |project, cx| {
            project.enable_sandbox(
                SandboxConfig {
                    control_secret,
                    supervisor_url: args.supervisor_url.clone(),
                    supervisor_http,
                    registry,
                    client_state_dir: paths::remote_server_state_dir().join("client_state"),
                },
                cx,
            )
        });
        Tokio::spawn(
            cx,
            http::serve_control(control_listener, state.clone(), control_routes)
                .map(|result| result.log_err()),
        )
        .detach();

        handle_crash_files_requests(&project, &session);
        observe_project_for_health(&project, state, cx);
        spawn_gpui_command_loop(
            project.clone(),
            server_channel,
            pty,
            Box::new(SandboxProjectHooks),
            gpui_rx,
            cx,
        );
        Tokio::spawn(cx, {
            let root = workspace_root.clone();
            async move { files::sweep_temp_files(&root, listeners_bound_at).await }
        })
        .detach();

        std::mem::forget(project);
        Ok(())
    };

    let startup_result: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
    let app = std::panic::AssertUnwindSafe(app);
    let run = std::panic::AssertUnwindSafe({
        let startup_result = startup_result.clone();
        move |cx: &mut App| {
            let result = run(cx);
            if let Err(error) = &result {
                log::error!("serve startup failed: {error:#}");
                cx.quit();
            }
            *startup_result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
        }
    });
    let outcome = std::panic::catch_unwind(move || { app }.0.run({ run }.0));
    if outcome.is_err() {
        log::error!("app panicked. quitting.");
        anyhow::bail!("panicked");
    }
    match startup_result
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        Some(Err(error)) => Err(error),
        _ => {
            log::info!("gpui app is shut down. quitting.");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(control_listen: &str) -> ServeArgs {
        ServeArgs {
            listen: "127.0.0.1:0".parse().unwrap(),
            jwt_public_keys: vec![PathBuf::from("key.pem")],
            workspace_id: "ws_test".into(),
            audience: "sb_test".into(),
            issuer: "zs".into(),
            workspace_root: PathBuf::from("."),
            client_build: None,
            allowed_origins: Vec::new(),
            control_secret_file: PathBuf::from("secret"),
            control_listen: control_listen.parse().unwrap(),
            supervisor_url: DEFAULT_SUPERVISOR_URL.into(),
            port_file: None,
            log_file: None,
            user_data_dir: None,
        }
    }

    #[test]
    fn control_listen_must_be_loopback() {
        assert!(validate_serve_args(&args("0.0.0.0:0")).is_err());
        assert!(validate_serve_args(&args("10.0.0.1:8451")).is_err());
        assert!(validate_serve_args(&args("127.0.0.1:0")).is_ok());
        assert!(validate_serve_args(&args("[::1]:8451")).is_ok());
        assert!(validate_serve_args(&args(DEFAULT_CONTROL_LISTEN)).is_ok());
    }

    #[test]
    fn supervisor_url_must_be_loopback_http() {
        for url in [
            DEFAULT_SUPERVISOR_URL,
            "http://127.0.0.1:8450/",
            "http://localhost:8450",
            "http://[::1]:8450",
        ] {
            assert!(validate_supervisor_url(url).is_ok(), "{url}");
            let mut args = args(DEFAULT_CONTROL_LISTEN);
            args.supervisor_url = url.into();
            assert!(validate_serve_args(&args).is_ok(), "{url}");
        }
        for url in [
            "https://127.0.0.1:8450",
            "http://attacker.example:8450",
            "http://10.0.0.1:8450",
            "http://user:pass@127.0.0.1:8450",
            "127.0.0.1:8450",
            "",
        ] {
            let error = validate_supervisor_url(url).unwrap_err();
            assert!(
                error.to_string().contains("--supervisor-url"),
                "{url}: {error:#}"
            );
            let mut args = args(DEFAULT_CONTROL_LISTEN);
            args.supervisor_url = url.into();
            assert!(validate_serve_args(&args).is_err(), "{url}");
        }
    }

    #[test]
    fn asset_path_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let extensions_dir = dir.path().join("remote_extensions");
        std::fs::create_dir_all(extensions_dir.join("theme-x/themes")).unwrap();
        std::fs::write(extensions_dir.join("theme-x/themes/x.json"), "{}").unwrap();
        std::fs::write(dir.path().join("secret.txt"), "s").unwrap();
        std::fs::write(extensions_dir.join("loose.json"), "{}").unwrap();

        let resolved = resolve_extension_asset(&extensions_dir, "theme-x", "themes/x.json")
            .expect("an installed asset resolves");
        assert!(resolved.ends_with("theme-x/themes/x.json"), "{resolved:?}");
        assert_eq!(
            resolve_extension_asset(&extensions_dir, "theme-x", "themes/../../loose.json"),
            None
        );
        assert_eq!(
            resolve_extension_asset(&extensions_dir, "theme-x", "../../secret.txt"),
            None
        );
        assert_eq!(
            resolve_extension_asset(&extensions_dir, "theme-x", "/etc/passwd"),
            None
        );
        assert_eq!(resolve_extension_asset(&extensions_dir, "..", "secret.txt"), None);
        assert_eq!(
            resolve_extension_asset(&extensions_dir, "theme-x/..", "loose.json"),
            None
        );
        assert_eq!(resolve_extension_asset(&extensions_dir, "missing", "x"), None);

        #[cfg(unix)]
        {
            // A symlinked extension directory must not resolve outside the tree.
            std::os::unix::fs::symlink(dir.path(), extensions_dir.join("linked")).unwrap();
            assert_eq!(
                resolve_extension_asset(&extensions_dir, "linked", "secret.txt"),
                None
            );
        }
    }

    #[test]
    fn execute_serve_refuses_a_non_loopback_control_listener_before_binding() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, b"s3cret\n").unwrap();
        let mut args = args("0.0.0.0:0");
        args.control_secret_file = secret;
        args.workspace_root = dir.path().to_path_buf();
        let error = execute_serve(args).unwrap_err();
        assert!(error.to_string().contains("--control-listen"), "{error:#}");
    }
}
