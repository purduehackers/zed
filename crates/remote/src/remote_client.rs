#[cfg(any(test, feature = "test-support"))]
use crate::transport::mock::ConnectGuard;
use crate::{
    SshConnectionOptions,
    protocol::MessageId,
    proxy::ProxyLaunchError,
    transport::{
        docker::{DockerConnectionOptions, DockerExecConnection},
        ssh::SshRemoteConnection,
        websocket::{WebSocketConnectionOptions, WebSocketRemoteConnection},
        wsl::{WslConnectionOptions, WslRemoteConnection},
    },
};
use anyhow::{Context as _, Result, anyhow};
use askpass::EncryptedPassword;
use async_trait::async_trait;
use collections::HashMap;
use futures::{
    Future, FutureExt as _, StreamExt as _,
    channel::{
        mpsc::{self, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    future::{BoxFuture, Shared, WeakShared},
    select, select_biased,
    stream::BoxStream,
};
use gpui::{
    App, AppContext as _, AsyncApp, BackgroundExecutor, BorrowAppContext, Context, Entity,
    EventEmitter, FutureExt, Global, Task, TaskExt, WeakEntity,
};
use parking_lot::Mutex;

use release_channel::ReleaseChannel;
use rpc::{
    AnyProtoClient, ErrorExt, ProtoClient, ProtoMessageHandlerSet, RpcError,
    proto::{self, Envelope, EnvelopedMessage, PeerId, RequestMessage, build_typed_envelope},
};
use semver::Version;
use std::{
    collections::VecDeque,
    fmt,
    ops::ControlFlow,
    path::PathBuf,
    sync::{
        Arc, Weak,
        atomic::{AtomicU32, AtomicU64, Ordering::SeqCst},
    },
    time::Duration,
};
use util::{
    ResultExt,
    paths::{PathStyle, RemotePathBuf},
};
// `std::time::Instant::now()` panics on wasm; `web_time` re-exports `std` on native.
use web_time::Instant;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RemoteOs {
    Linux,
    MacOs,
    Windows,
}

impl RemoteOs {
    pub fn as_str(&self) -> &'static str {
        match self {
            RemoteOs::Linux => "linux",
            RemoteOs::MacOs => "macos",
            RemoteOs::Windows => "windows",
        }
    }

    pub fn is_windows(&self) -> bool {
        matches!(self, RemoteOs::Windows)
    }

    /// A human-readable OS name for telemetry. Matches `client::telemetry::os_name`
    /// ignoring the compositor (as we run headless on remotes).
    pub fn display_name(&self) -> &'static str {
        match self {
            RemoteOs::Linux => "Linux",
            RemoteOs::MacOs => "macOS",
            RemoteOs::Windows => "Windows",
        }
    }
}

impl std::fmt::Display for RemoteOs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RemoteArch {
    X86_64,
    Aarch64,
}

impl RemoteArch {
    pub fn as_str(&self) -> &'static str {
        match self {
            RemoteArch::X86_64 => "x86_64",
            RemoteArch::Aarch64 => "aarch64",
        }
    }
}

impl std::fmt::Display for RemoteArch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RemotePlatform {
    pub os: RemoteOs,
    pub arch: RemoteArch,
}

#[derive(Clone, Debug)]
pub struct CommandTemplate {
    pub program: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

/// Whether a command should be run with TTY allocation for interactive use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interactive {
    /// Allocate a pseudo-TTY for interactive terminal use.
    Yes,
    /// Do not allocate a TTY - for commands that communicate via piped stdio.
    No,
}

pub trait RemoteClientDelegate: Send + Sync {
    fn ask_password(
        &self,
        prompt: String,
        tx: oneshot::Sender<EncryptedPassword>,
        cancellation: oneshot::Receiver<()>,
        cx: &mut AsyncApp,
    );
    fn get_download_url(
        &self,
        platform: RemotePlatform,
        release_channel: ReleaseChannel,
        version: Option<Version>,
        cx: &mut AsyncApp,
    ) -> Task<Result<Option<String>>>;
    fn download_server_binary_locally(
        &self,
        platform: RemotePlatform,
        release_channel: ReleaseChannel,
        version: Option<Version>,
        cx: &mut AsyncApp,
    ) -> Task<Result<PathBuf>>;
    fn set_status(&self, status: Option<&str>, cx: &mut AsyncApp);
}

const MAX_MISSED_HEARTBEATS: usize = 5;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_CONNECTION_TIMEOUT: Duration =
    Duration::from_secs(if cfg!(debug_assertions) { 5 } else { 60 });

pub const MAX_RECONNECT_ATTEMPTS: usize = 3;

enum State {
    Connecting,
    Connected {
        remote_connection: Arc<dyn RemoteConnection>,
        delegate: Arc<dyn RemoteClientDelegate>,

        multiplex_task: Task<Result<()>>,
        heartbeat_task: Task<Result<()>>,
    },
    HeartbeatMissed {
        missed_heartbeats: usize,

        remote_connection: Arc<dyn RemoteConnection>,
        delegate: Arc<dyn RemoteClientDelegate>,

        multiplex_task: Task<Result<()>>,
        heartbeat_task: Task<Result<()>>,
    },
    Reconnecting,
    ReconnectFailed {
        remote_connection: Arc<dyn RemoteConnection>,
        delegate: Arc<dyn RemoteClientDelegate>,

        error: anyhow::Error,
        attempts: usize,
    },
    ReconnectExhausted,
    ServerNotRunning,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connecting => write!(f, "connecting"),
            Self::Connected { .. } => write!(f, "connected"),
            Self::Reconnecting => write!(f, "reconnecting"),
            Self::ReconnectFailed { .. } => write!(f, "reconnect failed"),
            Self::ReconnectExhausted => write!(f, "reconnect exhausted"),
            Self::HeartbeatMissed { .. } => write!(f, "heartbeat missed"),
            Self::ServerNotRunning { .. } => write!(f, "server not running"),
        }
    }
}

impl State {
    fn remote_connection(&self) -> Option<Arc<dyn RemoteConnection>> {
        match self {
            Self::Connected {
                remote_connection, ..
            } => Some(remote_connection.clone()),
            Self::HeartbeatMissed {
                remote_connection, ..
            } => Some(remote_connection.clone()),
            Self::ReconnectFailed {
                remote_connection, ..
            } => Some(remote_connection.clone()),
            _ => None,
        }
    }

    fn can_reconnect(&self) -> bool {
        match self {
            Self::Connected { .. }
            | Self::HeartbeatMissed { .. }
            | Self::ReconnectFailed { .. } => true,
            State::Connecting
            | State::Reconnecting
            | State::ReconnectExhausted
            | State::ServerNotRunning => false,
        }
    }

    fn is_reconnect_failed(&self) -> bool {
        matches!(self, Self::ReconnectFailed { .. })
    }

    fn is_reconnect_exhausted(&self) -> bool {
        matches!(self, Self::ReconnectExhausted { .. })
    }

    fn is_server_not_running(&self) -> bool {
        matches!(self, Self::ServerNotRunning)
    }

    fn is_reconnecting(&self) -> bool {
        matches!(self, Self::Reconnecting { .. })
    }

    fn heartbeat_recovered(self) -> Self {
        match self {
            Self::HeartbeatMissed {
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
                ..
            } => Self::Connected {
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
            },
            _ => self,
        }
    }

    fn heartbeat_missed(self) -> Self {
        match self {
            Self::Connected {
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
            } => Self::HeartbeatMissed {
                missed_heartbeats: 1,
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
            },
            Self::HeartbeatMissed {
                missed_heartbeats,
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
            } => Self::HeartbeatMissed {
                missed_heartbeats: missed_heartbeats + 1,
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
            },
            _ => self,
        }
    }
}

/// The state of the ssh connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    HeartbeatMissed,
    Reconnecting,
    Disconnected,
}

impl From<&State> for ConnectionState {
    fn from(value: &State) -> Self {
        match value {
            State::Connecting => Self::Connecting,
            State::Connected { .. } => Self::Connected,
            State::Reconnecting | State::ReconnectFailed { .. } => Self::Reconnecting,
            State::HeartbeatMissed { .. } => Self::HeartbeatMissed,
            State::ReconnectExhausted => Self::Disconnected,
            State::ServerNotRunning => Self::Disconnected,
        }
    }
}

pub struct RemoteClient {
    client: Arc<ChannelClient>,
    unique_identifier: String,
    connection_options: RemoteConnectionOptions,
    path_style: PathStyle,
    platform: RemotePlatform,
    os_version: Option<String>,
    state: Option<State>,
}

#[derive(Debug)]
pub enum RemoteClientEvent {
    Disconnected { server_not_running: bool },
    Reconnected,
}

impl EventEmitter<RemoteClientEvent> for RemoteClient {}

/// Identifies the socket on the remote server so that reconnects
/// can re-join the same project.
pub enum ConnectionIdentifier {
    Setup(u64),
    Workspace(i64),
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl ConnectionIdentifier {
    pub fn setup() -> Self {
        Self::Setup(NEXT_ID.fetch_add(1, SeqCst))
    }

    // This string gets used in a socket name, and so must be relatively short.
    // The total length of:
    //   /home/{username}/.local/share/zed/server_state/{name}/stdout.sock
    // Must be less than about 100 characters
    //   https://unix.stackexchange.com/questions/367008/why-is-socket-path-length-limited-to-a-hundred-chars
    // So our strings should be at most 20 characters or so.
    fn to_string(&self, cx: &App) -> String {
        let identifier_prefix = match ReleaseChannel::global(cx) {
            ReleaseChannel::Stable => "".to_string(),
            release_channel => format!("{}-", release_channel.dev_name()),
        };
        match self {
            Self::Setup(setup_id) => format!("{identifier_prefix}setup-{setup_id}"),
            Self::Workspace(workspace_id) => {
                format!("{identifier_prefix}workspace-{workspace_id}",)
            }
        }
    }
}

pub async fn connect(
    connection_options: RemoteConnectionOptions,
    delegate: Arc<dyn RemoteClientDelegate>,
    cx: &mut AsyncApp,
) -> Result<Arc<dyn RemoteConnection>> {
    cx.update(|cx| {
        cx.update_default_global(|pool: &mut ConnectionPool, cx| {
            pool.connect(connection_options.clone(), delegate.clone(), cx)
        })
    })
    .await
    .map_err(|e| e.cloned())
}

/// Returns `true` if the global [`ConnectionPool`] already has a live
/// connection for the given options. Callers can use this to decide
/// whether to show interactive UI (e.g., a password modal) before
/// connecting.
pub fn has_active_connection(opts: &RemoteConnectionOptions, cx: &App) -> bool {
    cx.try_global::<ConnectionPool>().is_some_and(|pool| {
        matches!(
            pool.connections.get(opts),
            Some(ConnectionPoolEntry::Connected(remote))
                if remote.upgrade().is_some_and(|r| !r.has_been_killed())
        )
    })
}

impl RemoteClient {
    pub fn new(
        unique_identifier: ConnectionIdentifier,
        remote_connection: Arc<dyn RemoteConnection>,
        cancellation: oneshot::Receiver<()>,
        delegate: Arc<dyn RemoteClientDelegate>,
        cx: &mut App,
    ) -> Task<Result<Option<Entity<Self>>>> {
        let unique_identifier = unique_identifier.to_string(cx);
        cx.spawn(async move |cx| {
            let success = Box::pin(async move {
                let (outgoing_tx, outgoing_rx) = mpsc::unbounded::<Envelope>();
                let (incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
                let (connection_activity_tx, connection_activity_rx) = mpsc::channel::<()>(1);

                let client = cx.update(|cx| {
                    ChannelClient::new(
                        incoming_rx,
                        outgoing_tx,
                        cx,
                        "client",
                        remote_connection.has_wsl_interop(),
                    )
                });

                let path_style = remote_connection.path_style();
                let platform = remote_connection.remote_platform();
                let os_version = remote_connection.remote_os_version();
                let connection_options = remote_connection.connection_options();
                let connection_type = connection_options.connection_type();
                let this = cx.new(|_| Self {
                    client: client.clone(),
                    unique_identifier: unique_identifier.clone(),
                    connection_options,
                    path_style,
                    platform,
                    os_version: os_version.clone(),
                    state: Some(State::Connecting),
                });

                let io_task = remote_connection.start_proxy(
                    unique_identifier,
                    false,
                    incoming_tx,
                    outgoing_rx,
                    connection_activity_tx,
                    delegate.clone(),
                    cx,
                );

                // Both the signal and the timeout complete on the background executor: a
                // platform whose background workers never start hangs here without a word.
                log::debug!("remote client: waiting for the server's RemoteStarted");
                let ready = client
                    .wait_for_remote_started()
                    .with_timeout(INITIAL_CONNECTION_TIMEOUT, cx.background_executor())
                    .await;
                match ready {
                    Ok(Some(_)) => {
                        log::debug!("remote client: RemoteStarted received, pinging");
                    }
                    Ok(None) => {
                        let mut error = "remote client exited before becoming ready".to_owned();
                        if let Some(status) = io_task.now_or_never() {
                            match status {
                                Ok(exit_code) => {
                                    error.push_str(&format!(", exit_code={exit_code:?}"))
                                }
                                Err(e) => error.push_str(&format!(", error={e:?}")),
                            }
                        }
                        let error = anyhow::anyhow!("{error}");
                        log::error!("failed to establish connection: {}", error);
                        return Err(error);
                    }
                    Err(_) => {
                        let mut error = String::new();
                        if let Some(status) = io_task.now_or_never() {
                            error.push_str("Client exited with ");
                            match status {
                                Ok(exit_code) => {
                                    error.push_str(&format!("exit_code {exit_code:?}"))
                                }
                                Err(e) => error.push_str(&format!("error {e:?}")),
                            }
                        } else {
                            error.push_str("client did not become ready within the timeout");
                        }
                        let error = anyhow::anyhow!("{error}");
                        log::error!("failed to establish connection: {error}");
                        return Err(error);
                    }
                }
                let multiplex_task = Self::monitor(this.downgrade(), io_task, cx);
                if let Err(error) = client.ping(HEARTBEAT_TIMEOUT).await {
                    log::error!("failed to establish connection: {}", error);
                    return Err(error);
                }
                log::debug!("remote client: initial ping answered, connection established");

                let heartbeat_task = Self::heartbeat(this.downgrade(), connection_activity_rx, cx);

                this.update(cx, |this, _| {
                    this.state = Some(State::Connected {
                        remote_connection,
                        delegate,
                        multiplex_task,
                        heartbeat_task,
                    });
                });

                // Use the same `remote_*` property schema as the forwarded
                // remote events (see `client::telemetry::report_remote_event`)
                // so all remote-origin telemetry can be queried uniformly.
                telemetry::event!(
                    "Remote Connection Established",
                    remote = true,
                    remote_connection_type = connection_type,
                    remote_os_name = platform.os.display_name(),
                    remote_os_version = os_version,
                    remote_architecture = platform.arch.as_str(),
                );

                Ok(Some(this))
            });

            select! {
                _ = cancellation.fuse() => {
                    Ok(None)
                }
                result = success.fuse() =>  result
            }
        })
    }

    pub fn proto_client_from_channels(
        incoming_rx: mpsc::UnboundedReceiver<Envelope>,
        outgoing_tx: mpsc::UnboundedSender<Envelope>,
        cx: &App,
        name: &'static str,
        has_wsl_interop: bool,
    ) -> AnyProtoClient {
        ChannelClient::new(incoming_rx, outgoing_tx, cx, name, has_wsl_interop).into()
    }

    /// Like [`Self::proto_client_from_channels`], but keeps the concrete channel client so a
    /// transport that outlives its client sessions (`zed-remote-server serve`) can reset it.
    pub fn server_channel_from_channels(
        incoming_rx: mpsc::UnboundedReceiver<Envelope>,
        outgoing_tx: mpsc::UnboundedSender<Envelope>,
        cx: &App,
        name: &'static str,
        has_wsl_interop: bool,
    ) -> ServerChannel {
        ServerChannel {
            client: ChannelClient::new(incoming_rx, outgoing_tx, cx, name, has_wsl_interop),
        }
    }

    pub fn shutdown_processes<T: RequestMessage>(
        &mut self,
        shutdown_request: Option<T>,
        executor: BackgroundExecutor,
    ) -> Option<impl Future<Output = ()> + use<T>> {
        let state = self.state.take()?;
        log::info!("shutting down remote processes");

        let State::Connected {
            multiplex_task,
            heartbeat_task,
            remote_connection,
            delegate,
        } = state
        else {
            return None;
        };

        let client = self.client.clone();

        Some(async move {
            if let Some(shutdown_request) = shutdown_request {
                client.send(shutdown_request).log_err();
                // We wait 50ms instead of waiting for a response, because
                // waiting for a response would require us to wait on the main thread
                // which we want to avoid in an `on_app_quit` callback.
                executor.timer(Duration::from_millis(50)).await;
            }

            // Drop `multiplex_task` because it owns our remote_connection_proxy_process, which is a
            // child of master_process.
            drop(multiplex_task);
            // Now drop the rest of state, which kills master process.
            drop(heartbeat_task);
            drop(remote_connection);
            drop(delegate);
        })
    }

    fn reconnect(&mut self, cx: &mut Context<Self>) -> Result<()> {
        let can_reconnect = self
            .state
            .as_ref()
            .map(|state| state.can_reconnect())
            .unwrap_or(false);
        if !can_reconnect {
            let state = if let Some(state) = self.state.as_ref() {
                state.to_string()
            } else {
                "no state set".to_string()
            };
            log::info!(
                "aborting reconnect, because not in state that allows reconnecting: {state}"
            );
            anyhow::bail!(
                "aborting reconnect, because not in state that allows reconnecting: {state}"
            );
        }

        let state = self.state.take().unwrap();
        let (attempts, remote_connection, delegate) = match state {
            State::Connected {
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
            }
            | State::HeartbeatMissed {
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task,
                ..
            } => {
                drop(multiplex_task);
                drop(heartbeat_task);
                (0, remote_connection, delegate)
            }
            State::ReconnectFailed {
                attempts,
                remote_connection,
                delegate,
                ..
            } => (attempts, remote_connection, delegate),
            State::Connecting
            | State::Reconnecting
            | State::ReconnectExhausted
            | State::ServerNotRunning => unreachable!(),
        };

        let attempts = attempts + 1;
        let max_reconnect_attempts = remote_connection.max_reconnect_attempts();
        if attempts > max_reconnect_attempts {
            log::error!(
                "Failed to reconnect to after {} attempts, giving up",
                max_reconnect_attempts
            );
            self.set_state(State::ReconnectExhausted, cx);
            return Ok(());
        }

        self.set_state(State::Reconnecting, cx);

        log::info!(
            "Trying to reconnect to remote server... Attempt {}",
            attempts
        );

        let unique_identifier = self.unique_identifier.clone();
        let client = self.client.clone();
        let reconnect_task = cx.spawn(async move |this, cx| {
            macro_rules! failed {
                ($error:expr, $attempts:expr, $remote_connection:expr, $delegate:expr) => {
                    delegate.set_status(Some(&format!("{error:#}", error = $error)), cx);
                    return State::ReconnectFailed {
                        error: anyhow!($error),
                        attempts: $attempts,
                        remote_connection: $remote_connection,
                        delegate: $delegate,
                    };
                };
            }

            if let Err(error) = remote_connection
                .kill()
                .await
                .context("Failed to kill remote_connection process")
            {
                failed!(error, attempts, remote_connection, delegate);
            };

            let connection_options = remote_connection.connection_options();

            let (outgoing_tx, outgoing_rx) = mpsc::unbounded::<Envelope>();
            let (incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
            let (connection_activity_tx, connection_activity_rx) = mpsc::channel::<()>(1);

            let (remote_connection, io_task) = match async {
                let remote_connection = cx
                    .update_global(|pool: &mut ConnectionPool, cx| {
                        pool.connect(connection_options, delegate.clone(), cx)
                    })
                    .await
                    .map_err(|error| error.cloned())?;

                let io_task = remote_connection.start_proxy(
                    unique_identifier,
                    true,
                    incoming_tx,
                    outgoing_rx,
                    connection_activity_tx,
                    delegate.clone(),
                    cx,
                );
                anyhow::Ok((remote_connection, io_task))
            }
            .await
            {
                Ok((remote_connection, io_task)) => (remote_connection, io_task),
                Err(error) => {
                    failed!(error, attempts, remote_connection, delegate);
                }
            };

            let multiplex_task = Self::monitor(this.clone(), io_task, cx);
            client.reconnect(incoming_rx, outgoing_tx, cx);

            if let Err(error) = client.resync(HEARTBEAT_TIMEOUT).await {
                failed!(error, attempts, remote_connection, delegate);
            };

            State::Connected {
                remote_connection,
                delegate,
                multiplex_task,
                heartbeat_task: Self::heartbeat(this.clone(), connection_activity_rx, cx),
            }
        });

        cx.spawn(async move |this, cx| {
            let new_state = reconnect_task.await;
            this.update(cx, |this, cx| {
                let reconnected = this.state_is(State::is_reconnecting)
                    && matches!(&new_state, State::Connected { .. });
                this.try_set_state(cx, |old_state| {
                    if old_state.is_reconnecting() {
                        match &new_state {
                            State::Connecting
                            | State::Reconnecting
                            | State::HeartbeatMissed { .. }
                            | State::ServerNotRunning => {}
                            State::Connected { .. } => {
                                log::info!("Successfully reconnected");
                            }
                            State::ReconnectFailed {
                                error, attempts, ..
                            } => {
                                log::error!(
                                    "Reconnect attempt {} failed: {:?}. Starting new attempt...",
                                    attempts,
                                    error
                                );
                            }
                            State::ReconnectExhausted => {
                                log::error!("Reconnect attempt failed and all attempts exhausted");
                            }
                        }
                        Some(new_state)
                    } else {
                        None
                    }
                });

                if reconnected {
                    cx.emit(RemoteClientEvent::Reconnected);
                }

                if this.state_is(State::is_reconnect_failed) {
                    this.reconnect(cx)
                } else if this.state_is(State::is_reconnect_exhausted) {
                    Ok(())
                } else {
                    log::debug!("State has transition from Reconnecting into new state while attempting reconnect.");
                    Ok(())
                }
            })
        })
        .detach_and_log_err(cx);

        Ok(())
    }

    fn heartbeat(
        this: WeakEntity<Self>,
        mut connection_activity_rx: mpsc::Receiver<()>,
        cx: &mut AsyncApp,
    ) -> Task<Result<()>> {
        let Ok(client) = this.read_with(cx, |this, _| this.client.clone()) else {
            return Task::ready(Err(anyhow!("remote_connectionRemoteClient lost")));
        };

        cx.spawn(async move |cx| {
            let mut missed_heartbeats = 0;

            let keepalive_timer = cx.background_executor().timer(HEARTBEAT_INTERVAL).fuse();
            futures::pin_mut!(keepalive_timer);

            loop {
                select_biased! {
                    result = connection_activity_rx.next().fuse() => {
                        if result.is_none() {
                            log::warn!("remote heartbeat: connection activity channel has been dropped. stopping.");
                            return Ok(());
                        }

                        if missed_heartbeats != 0 {
                            missed_heartbeats = 0;
                            let _ =this.update(cx, |this, cx| {
                                this.handle_heartbeat_result(missed_heartbeats, cx)
                            })?;
                        }
                    }
                    _ = keepalive_timer => {
                        log::debug!("Sending heartbeat to server...");

                        let result = select_biased! {
                            _ = connection_activity_rx.next().fuse() => {
                                Ok(())
                            }
                            ping_result = client.ping(HEARTBEAT_TIMEOUT).fuse() => {
                                ping_result
                            }
                        };

                        if result.is_err() {
                            missed_heartbeats += 1;
                            log::warn!(
                                "No heartbeat from server after {:?}. Missed heartbeat {} out of {}.",
                                HEARTBEAT_TIMEOUT,
                                missed_heartbeats,
                                MAX_MISSED_HEARTBEATS
                            );
                        } else if missed_heartbeats != 0 {
                            missed_heartbeats = 0;
                        } else {
                            continue;
                        }

                        let result = this.update(cx, |this, cx| {
                            this.handle_heartbeat_result(missed_heartbeats, cx)
                        })?;
                        if result.is_break() {
                            return Ok(());
                        }
                    }
                }

                keepalive_timer.set(cx.background_executor().timer(HEARTBEAT_INTERVAL).fuse());
            }
        })
    }

    fn handle_heartbeat_result(
        &mut self,
        missed_heartbeats: usize,
        cx: &mut Context<Self>,
    ) -> ControlFlow<()> {
        let state = self.state.take().unwrap();
        let next_state = if missed_heartbeats > 0 {
            state.heartbeat_missed()
        } else {
            state.heartbeat_recovered()
        };

        self.set_state(next_state, cx);

        if missed_heartbeats >= MAX_MISSED_HEARTBEATS {
            log::error!(
                "Missed last {} heartbeats. Reconnecting...",
                missed_heartbeats
            );

            self.reconnect(cx)
                .context("failed to start reconnect process after missing heartbeats")
                .log_err();
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn monitor(
        this: WeakEntity<Self>,
        io_task: Task<Result<i32>>,
        cx: &AsyncApp,
    ) -> Task<Result<()>> {
        cx.spawn(async move |cx| {
            let result = io_task.await;

            match result {
                Ok(exit_code) => {
                    if let Some(error) = ProxyLaunchError::from_exit_code(exit_code) {
                        match error {
                            ProxyLaunchError::ServerNotRunning => {
                                log::error!("failed to reconnect because server is not running");
                            }
                            ProxyLaunchError::SessionTakenOver => {
                                log::error!(
                                    "remote session ended because another client is attached to it"
                                );
                            }
                            ProxyLaunchError::IncompatibleServer => {
                                log::error!(
                                    "remote session ended because the server build is incompatible with this client"
                                );
                            }
                        }
                        this.update(cx, |this, cx| {
                            this.set_state(State::ServerNotRunning, cx);
                        })?;
                    } else {
                        log::error!("proxy process terminated unexpectedly: {exit_code}");
                        this.update(cx, |this, cx| {
                            this.reconnect(cx).ok();
                        })?;
                    }
                }
                Err(error) => {
                    log::warn!(
                        "remote io task died with error: {:?}. reconnecting...",
                        error
                    );
                    this.update(cx, |this, cx| {
                        this.reconnect(cx).ok();
                    })?;
                }
            }

            Ok(())
        })
    }

    fn state_is(&self, check: impl FnOnce(&State) -> bool) -> bool {
        self.state.as_ref().is_some_and(check)
    }

    fn try_set_state(&mut self, cx: &mut Context<Self>, map: impl FnOnce(&State) -> Option<State>) {
        let new_state = self.state.as_ref().and_then(map);
        if let Some(new_state) = new_state {
            self.state.replace(new_state);
            cx.notify();
        }
    }

    fn set_state(&mut self, state: State, cx: &mut Context<Self>) {
        log::info!("setting state to '{state}'");

        let is_reconnect_exhausted = state.is_reconnect_exhausted();
        let is_server_not_running = state.is_server_not_running();
        self.state.replace(state);

        if is_reconnect_exhausted || is_server_not_running {
            cx.emit(RemoteClientEvent::Disconnected {
                server_not_running: is_server_not_running,
            });
        }
        cx.notify();
    }

    pub fn shell(&self) -> Option<String> {
        Some(self.remote_connection()?.shell())
    }

    pub fn default_system_shell(&self) -> Option<String> {
        Some(self.remote_connection()?.default_system_shell())
    }

    /// Whether terminals for this remote run as server-managed PTYs over the protocol
    /// instead of a locally spawned `ssh`-style command (D27).
    pub fn supports_remote_pty(&self) -> bool {
        self.remote_connection()
            .is_some_and(|connection| connection.supports_remote_pty())
    }

    /// Whether the extension store may sync local extensions to this remote with
    /// `upload_directory` (D27).
    pub fn supports_extension_upload(&self) -> bool {
        self.remote_connection()
            .is_some_and(|connection| connection.supports_extension_upload())
    }

    pub fn shares_network_interface(&self) -> bool {
        self.remote_connection()
            .map_or(false, |connection| connection.shares_network_interface())
    }

    pub fn has_wsl_interop(&self) -> bool {
        self.remote_connection()
            .map_or(false, |connection| connection.has_wsl_interop())
    }

    pub fn build_command(
        &self,
        program: Option<String>,
        args: &[String],
        env: &HashMap<String, String>,
        working_dir: Option<String>,
        port_forward: Option<(u16, String, u16)>,
        interactive: Interactive,
    ) -> Result<CommandTemplate> {
        let Some(connection) = self.remote_connection() else {
            return Err(anyhow!("no remote connection"));
        };
        connection.build_command(program, args, env, working_dir, port_forward, interactive)
    }

    pub fn build_forward_ports_command(
        &self,
        forwards: Vec<(u16, String, u16)>,
    ) -> Result<CommandTemplate> {
        let Some(connection) = self.remote_connection() else {
            return Err(anyhow!("no remote connection"));
        };
        connection.build_forward_ports_command(forwards)
    }

    pub fn upload_directory(
        &self,
        src_path: PathBuf,
        dest_path: RemotePathBuf,
        cx: &App,
    ) -> Task<Result<()>> {
        let Some(connection) = self.remote_connection() else {
            return Task::ready(Err(anyhow!("no remote connection")));
        };
        connection.upload_directory(src_path, dest_path, cx)
    }

    pub fn proto_client(&self) -> AnyProtoClient {
        self.client.clone().into()
    }

    /// Browser boot (b7): delivers the messages the server sent before the `Project` (and
    /// its handlers) existed, and stops holding any more. Call once the project is open.
    #[cfg(target_family = "wasm")]
    pub fn replay_unhandled_messages(&self, cx: &App) {
        self.client.replay_unhandled(&cx.to_async());
    }

    pub fn connection_options(&self) -> RemoteConnectionOptions {
        self.connection_options.clone()
    }

    pub fn connection(&self) -> Option<Arc<dyn RemoteConnection>> {
        if let State::Connected {
            remote_connection, ..
        } = self.state.as_ref()?
        {
            Some(remote_connection.clone())
        } else {
            None
        }
    }

    pub fn connection_state(&self) -> ConnectionState {
        self.state
            .as_ref()
            .map(ConnectionState::from)
            .unwrap_or(ConnectionState::Disconnected)
    }

    pub fn is_disconnected(&self) -> bool {
        self.connection_state() == ConnectionState::Disconnected
    }

    pub fn path_style(&self) -> PathStyle {
        self.path_style
    }

    /// The platform (OS and architecture) of the remote host, detected during
    /// connection setup.
    pub fn remote_platform(&self) -> RemotePlatform {
        self.platform
    }

    /// The OS version of the remote host (e.g. `"ubuntu 24.04"`), detected
    /// during connection setup. `None` if it could not be determined.
    pub fn remote_os_version(&self) -> Option<String> {
        self.os_version.clone()
    }

    /// A stable identifier for the kind of remote connection (e.g. `"ssh"`,
    /// `"wsl"`, `"docker"`, `"podman"`).
    pub fn connection_type(&self) -> &'static str {
        self.connection_options.connection_type()
    }

    /// Forcibly disconnects from the remote server by killing the underlying connection.
    /// This will trigger the reconnection logic if reconnection attempts remain.
    /// Useful for testing reconnection behavior in real environments.
    pub fn force_disconnect(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let Some(connection) = self.remote_connection() else {
            return Task::ready(Err(anyhow!("no active remote connection to disconnect")));
        };

        log::info!("force_disconnect: killing remote connection");

        cx.spawn(async move |_, _| {
            connection.kill().await?;
            Ok(())
        })
    }

    /// Simulates a timeout by pausing heartbeat responses.
    /// This will cause heartbeat failures and eventually trigger reconnection
    /// after MAX_MISSED_HEARTBEATS are missed.
    /// Useful for testing timeout behavior in real environments.
    pub fn force_heartbeat_timeout(&mut self, attempts: usize, cx: &mut Context<Self>) {
        log::info!("force_heartbeat_timeout: triggering heartbeat failure state");

        if let Some(State::Connected {
            remote_connection,
            delegate,
            multiplex_task,
            heartbeat_task,
        }) = self.state.take()
        {
            self.set_state(
                if attempts == 0 {
                    State::HeartbeatMissed {
                        missed_heartbeats: MAX_MISSED_HEARTBEATS,
                        remote_connection,
                        delegate,
                        multiplex_task,
                        heartbeat_task,
                    }
                } else {
                    State::ReconnectFailed {
                        remote_connection,
                        delegate,
                        error: anyhow!("forced heartbeat timeout"),
                        attempts,
                    }
                },
                cx,
            );

            self.reconnect(cx)
                .context("failed to start reconnect after forced timeout")
                .log_err();
        } else {
            log::warn!("force_heartbeat_timeout: not in Connected state, ignoring");
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn force_server_not_running(&mut self, cx: &mut Context<Self>) {
        self.set_state(State::ServerNotRunning, cx);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn simulate_disconnect(&self, client_cx: &mut App) -> Task<()> {
        let opts = self.connection_options();
        client_cx.spawn(async move |cx| {
            let connection = cx.update_global(|c: &mut ConnectionPool, _| {
                if let Some(ConnectionPoolEntry::Connected(c)) = c.connections.get(&opts) {
                    if let Some(connection) = c.upgrade() {
                        connection
                    } else {
                        panic!("connection was dropped")
                    }
                } else {
                    panic!("missing test connection")
                }
            });

            connection.simulate_disconnect(cx);
        })
    }

    /// Creates a mock connection pair for testing.
    ///
    /// This is the recommended way to create mock remote connections for tests.
    /// It returns the `MockConnectionOptions` (which can be passed to create a
    /// `HeadlessProject`), an `AnyProtoClient` for the server side and a
    /// `ConnectGuard` for the client side which blocks the connection from
    /// being established until dropped.
    ///
    /// # Example
    /// ```ignore
    /// let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);
    /// // Set up HeadlessProject with server_session...
    /// drop(connect_guard);
    /// let client = RemoteClient::fake_client(opts, cx).await;
    /// ```
    #[cfg(any(test, feature = "test-support"))]
    pub fn fake_server(
        client_cx: &mut gpui::TestAppContext,
        server_cx: &mut gpui::TestAppContext,
    ) -> (RemoteConnectionOptions, AnyProtoClient, ConnectGuard) {
        use crate::transport::mock::MockConnection;
        let (opts, server_client, connect_guard) = MockConnection::new(client_cx, server_cx);
        (opts.into(), server_client, connect_guard)
    }

    /// Registers a new mock server for existing connection options.
    ///
    /// Use this to simulate reconnection: after forcing a disconnect, register
    /// a new server so the next `connect()` call succeeds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn fake_server_with_opts(
        opts: &RemoteConnectionOptions,
        client_cx: &mut gpui::TestAppContext,
        server_cx: &mut gpui::TestAppContext,
    ) -> (AnyProtoClient, ConnectGuard) {
        use crate::transport::mock::MockConnection;
        let mock_opts = match opts {
            RemoteConnectionOptions::Mock(mock_opts) => mock_opts.clone(),
            _ => panic!("fake_server_with_opts requires Mock connection options"),
        };
        MockConnection::new_with_opts(mock_opts, client_cx, server_cx)
    }

    /// Like [`RemoteClient::fake_server`], but the mock connection reports that
    /// it hosts PTYs itself, so terminals go over the remote terminal protocol
    /// (D27) instead of being spawned with `build_command`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn fake_server_with_remote_pty(
        client_cx: &mut gpui::TestAppContext,
        server_cx: &mut gpui::TestAppContext,
    ) -> (RemoteConnectionOptions, AnyProtoClient, ConnectGuard) {
        use crate::transport::mock::{MockConnection, MockConnectionOptions};
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1 << 32);
        let opts = MockConnectionOptions {
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        };
        let (server_client, connect_guard) = MockConnection::new_with_opts_and_remote_pty(
            opts.clone(),
            true,
            client_cx,
            server_cx,
        );
        (opts.into(), server_client, connect_guard)
    }

    /// Registers a new remote-PTY mock server for existing connection options,
    /// to simulate a reconnect.
    #[cfg(any(test, feature = "test-support"))]
    pub fn fake_server_with_opts_and_remote_pty(
        opts: &RemoteConnectionOptions,
        client_cx: &mut gpui::TestAppContext,
        server_cx: &mut gpui::TestAppContext,
    ) -> (AnyProtoClient, ConnectGuard) {
        use crate::transport::mock::MockConnection;
        let mock_opts = match opts {
            RemoteConnectionOptions::Mock(mock_opts) => mock_opts.clone(),
            _ => panic!("fake_server_with_opts_and_remote_pty requires Mock connection options"),
        };
        MockConnection::new_with_opts_and_remote_pty(mock_opts, true, client_cx, server_cx)
    }

    /// Creates a `RemoteClient` connected to a mock server.
    ///
    /// Call `fake_server` first to get the connection options, set up the
    /// `HeadlessProject` with the server session, then call this method
    /// to create the client.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn connect_mock(
        opts: RemoteConnectionOptions,
        client_cx: &mut gpui::TestAppContext,
    ) -> Entity<Self> {
        assert!(matches!(opts, RemoteConnectionOptions::Mock(..)));
        use crate::transport::mock::MockDelegate;
        let (_tx, rx) = oneshot::channel();
        let mut cx = client_cx.to_async();
        let connection = connect(opts, Arc::new(MockDelegate), &mut cx)
            .await
            .unwrap();
        client_cx
            .update(|cx| {
                Self::new(
                    ConnectionIdentifier::setup(),
                    connection,
                    rx,
                    Arc::new(MockDelegate),
                    cx,
                )
            })
            .await
            .unwrap()
            .unwrap()
    }

    pub fn remote_connection(&self) -> Option<Arc<dyn RemoteConnection>> {
        self.state
            .as_ref()
            .and_then(|state| state.remote_connection())
    }
}

enum ConnectionPoolEntry {
    Connecting(WeakShared<Task<Result<Arc<dyn RemoteConnection>, Arc<anyhow::Error>>>>),
    Connected(Weak<dyn RemoteConnection>),
}

#[derive(Default)]
struct ConnectionPool {
    connections: HashMap<RemoteConnectionOptions, ConnectionPoolEntry>,
}

impl Global for ConnectionPool {}

impl ConnectionPool {
    fn connect(
        &mut self,
        opts: RemoteConnectionOptions,
        delegate: Arc<dyn RemoteClientDelegate>,
        cx: &mut App,
    ) -> Shared<Task<Result<Arc<dyn RemoteConnection>, Arc<anyhow::Error>>>> {
        let connection = self.connections.get(&opts);
        match connection {
            Some(ConnectionPoolEntry::Connecting(task)) => {
                if let Some(task) = task.upgrade() {
                    log::debug!("Connecting task is still alive");
                    cx.spawn(async move |cx| {
                        delegate.set_status(Some("Waiting for existing connection attempt"), cx)
                    })
                    .detach();
                    return task;
                }
                log::debug!("Connecting task is dead, removing it and restarting a connection");
                self.connections.remove(&opts);
            }
            Some(ConnectionPoolEntry::Connected(remote)) => {
                if let Some(remote) = remote.upgrade()
                    && !remote.has_been_killed()
                {
                    log::debug!("Connection is still alive");
                    return Task::ready(Ok(remote)).shared();
                }
                log::debug!("Connection is dead, removing it and restarting a connection");
                self.connections.remove(&opts);
            }
            None => {
                log::debug!("No existing connection found, starting a new one");
            }
        }

        let task = cx
            .spawn({
                let opts = opts.clone();
                let delegate = delegate.clone();
                async move |cx| {
                    let connection = match opts.clone() {
                        RemoteConnectionOptions::Ssh(opts) => {
                            SshRemoteConnection::new(opts, delegate, cx)
                                .await
                                .map(|connection| Arc::new(connection) as Arc<dyn RemoteConnection>)
                        }
                        RemoteConnectionOptions::Wsl(opts) => {
                            WslRemoteConnection::new(opts, delegate, cx)
                                .await
                                .map(|connection| Arc::new(connection) as Arc<dyn RemoteConnection>)
                        }
                        RemoteConnectionOptions::Docker(opts) => {
                            DockerExecConnection::new(opts, delegate, cx)
                                .await
                                .map(|connection| Arc::new(connection) as Arc<dyn RemoteConnection>)
                        }
                        RemoteConnectionOptions::WebSocket(opts) => {
                            WebSocketRemoteConnection::new(opts, delegate, cx)
                                .await
                                .map(|connection| Arc::new(connection) as Arc<dyn RemoteConnection>)
                        }
                        #[cfg(any(test, feature = "test-support"))]
                        RemoteConnectionOptions::Mock(opts) => match cx.update(|cx| {
                            cx.default_global::<crate::transport::mock::MockConnectionRegistry>()
                                .take(&opts)
                        }) {
                            Some(connection) => Ok(connection.await as Arc<dyn RemoteConnection>),
                            None => Err(anyhow!(
                                "Mock connection not found. Call MockConnection::new() first."
                            )),
                        },
                    };

                    cx.update_global(|pool: &mut Self, _| {
                        debug_assert!(matches!(
                            pool.connections.get(&opts),
                            Some(ConnectionPoolEntry::Connecting(_))
                        ));
                        match connection {
                            Ok(connection) => {
                                pool.connections.insert(
                                    opts.clone(),
                                    ConnectionPoolEntry::Connected(Arc::downgrade(&connection)),
                                );
                                Ok(connection)
                            }
                            Err(error) => {
                                pool.connections.remove(&opts);
                                Err(Arc::new(error))
                            }
                        }
                    })
                }
            })
            .shared();
        if let Some(task) = task.downgrade() {
            self.connections
                .insert(opts.clone(), ConnectionPoolEntry::Connecting(task));
        }
        task
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RemoteConnectionOptions {
    Ssh(SshConnectionOptions),
    Wsl(WslConnectionOptions),
    Docker(DockerConnectionOptions),
    /// A cloud workspace served by `zed-remote-server serve` over one WebSocket. `Hash`/`Eq`
    /// delegate to the options' `workspace_id`-only implementations, so the pool key survives
    /// URL, token and session-id rotation.
    WebSocket(WebSocketConnectionOptions),
    #[cfg(any(test, feature = "test-support"))]
    Mock(crate::transport::mock::MockConnectionOptions),
}

impl RemoteConnectionOptions {
    pub fn display_name(&self) -> String {
        match self {
            RemoteConnectionOptions::Ssh(opts) => opts
                .nickname
                .clone()
                .unwrap_or_else(|| opts.host.to_string()),
            RemoteConnectionOptions::Wsl(opts) => opts.distro_name.clone(),
            RemoteConnectionOptions::Docker(opts) => {
                if opts.use_podman {
                    format!("[podman] {}", opts.name)
                } else {
                    opts.name.clone()
                }
            }
            RemoteConnectionOptions::WebSocket(opts) => opts.display_name(),
            #[cfg(any(test, feature = "test-support"))]
            RemoteConnectionOptions::Mock(opts) => format!("mock-{}", opts.id),
        }
    }

    /// A stable identifier for the kind of remote connection, suitable for
    /// telemetry (e.g. `"ssh"`, `"wsl"`, `"docker"`, `"podman"`, `"websocket"`).
    pub fn connection_type(&self) -> &'static str {
        match self {
            RemoteConnectionOptions::Ssh(_) => "ssh",
            RemoteConnectionOptions::Wsl(_) => "wsl",
            RemoteConnectionOptions::Docker(opts) => {
                if opts.use_podman {
                    "podman"
                } else {
                    "docker"
                }
            }
            RemoteConnectionOptions::WebSocket(_) => "websocket",
            #[cfg(any(test, feature = "test-support"))]
            RemoteConnectionOptions::Mock(_) => "mock",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;
    use rpc::{ErrorCodeExt, proto::ErrorCode};

    #[test]
    fn test_ssh_display_name_prefers_nickname() {
        let options = RemoteConnectionOptions::Ssh(SshConnectionOptions {
            host: "1.2.3.4".into(),
            nickname: Some("My Cool Project".to_string()),
            ..Default::default()
        });

        assert_eq!(options.display_name(), "My Cool Project");
    }

    #[test]
    fn test_ssh_display_name_falls_back_to_host() {
        let options = RemoteConnectionOptions::Ssh(SshConnectionOptions {
            host: "1.2.3.4".into(),
            ..Default::default()
        });

        assert_eq!(options.display_name(), "1.2.3.4");
    }

    #[test]
    fn test_connection_type() {
        assert_eq!(
            RemoteConnectionOptions::Ssh(SshConnectionOptions::default()).connection_type(),
            "ssh"
        );
        assert_eq!(
            RemoteConnectionOptions::Wsl(WslConnectionOptions {
                distro_name: "Ubuntu".to_string(),
                user: None,
            })
            .connection_type(),
            "wsl"
        );
        assert_eq!(
            RemoteConnectionOptions::Docker(DockerConnectionOptions {
                use_podman: false,
                ..Default::default()
            })
            .connection_type(),
            "docker"
        );
        assert_eq!(
            RemoteConnectionOptions::Docker(DockerConnectionOptions {
                use_podman: true,
                ..Default::default()
            })
            .connection_type(),
            "podman"
        );
    }

    #[gpui::test]
    async fn test_channel_client_request_stream_terminates_on_error(cx: &mut TestAppContext) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded::<Envelope>();

        let client =
            cx.update(|cx| ChannelClient::new(incoming_rx, outgoing_tx, cx, "test-client", false));

        // The client sends RemoteStarted on startup; drain the outgoing channel
        // so it doesn't block.
        let _drain_outgoing = cx
            .executor()
            .spawn(async move { while outgoing_rx.next().await.is_some() {} });

        let mut stream = client
            .request_stream_dynamic(proto::Test { id: 0 }.into_envelope(0, None, None), "Test")
            .await
            .unwrap();

        let request_id = 0;

        incoming_tx
            .unbounded_send(proto::Test { id: 1 }.into_envelope(100, Some(request_id), None))
            .unwrap();

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(
            proto::Test::from_envelope(first).unwrap(),
            proto::Test { id: 1 }
        );

        // Send an Error without a trailing EndStream. The Error alone should
        // terminate the stream.
        incoming_tx
            .unbounded_send(
                ErrorCode::Internal
                    .message("boom".to_string())
                    .to_proto()
                    .into_envelope(101, Some(request_id), None),
            )
            .unwrap();

        let second = stream.next().await.unwrap();
        let error = second.unwrap_err();
        assert!(
            format!("{error}").contains("boom"),
            "expected error to surface server message, got: {error}"
        );

        assert!(stream.next().await.is_none());
        assert_eq!(client.stream_response_channels.lock().len(), 0);
    }

    #[gpui::test]
    async fn test_channel_client_dropping_stream_request_before_response_cleans_up_channel(
        cx: &mut TestAppContext,
    ) {
        let (_incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded::<Envelope>();

        let client =
            cx.update(|cx| ChannelClient::new(incoming_rx, outgoing_tx, cx, "test-client", false));

        let _drain_outgoing = cx
            .executor()
            .spawn(async move { while outgoing_rx.next().await.is_some() {} });

        let stream = client
            .request_stream_dynamic(proto::Test { id: 0 }.into_envelope(0, None, None), "Test")
            .await
            .unwrap();

        assert_eq!(client.stream_response_channels.lock().len(), 1);

        drop(stream);
        cx.run_until_parked();

        assert_eq!(
            client.stream_response_channels.lock().len(),
            0,
            "dropping a stream before any responses arrive should remove response channel bookkeeping"
        );
    }

    #[gpui::test]
    async fn test_channel_client_dropping_stream_request_before_completion(
        cx: &mut TestAppContext,
    ) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded::<Envelope>();

        let client =
            cx.update(|cx| ChannelClient::new(incoming_rx, outgoing_tx, cx, "test-client", false));

        let _drain_outgoing = cx
            .executor()
            .spawn(async move { while outgoing_rx.next().await.is_some() {} });

        let mut stream = client
            .request_stream_dynamic(proto::Test { id: 0 }.into_envelope(0, None, None), "Test")
            .await
            .unwrap();

        let request_id = 0;

        incoming_tx
            .unbounded_send(proto::Test { id: 1 }.into_envelope(100, Some(request_id), None))
            .unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        assert_eq!(client.stream_response_channels.lock().len(), 1);

        drop(stream);

        // Inject an orphaned non-terminal response. The read loop should detect
        // that the consumer has been dropped and clean up its bookkeeping (no
        // EndStream sent here on purpose, otherwise the cleanup would happen
        // via the terminal-response path and mask the bug under test).
        incoming_tx
            .unbounded_send(proto::Test { id: 2 }.into_envelope(101, Some(request_id), None))
            .unwrap();

        cx.run_until_parked();

        assert_eq!(
            client.stream_response_channels.lock().len(),
            0,
            "stream channel should be removed once the consumer has dropped the stream"
        );
    }

    struct Responder;

    #[gpui::test]
    async fn test_server_channel_drops_responses_of_a_replaced_session(cx: &mut TestAppContext) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
        let (outgoing_tx, mut old_outgoing_rx) = mpsc::unbounded::<Envelope>();
        let channel = cx.update(|cx| {
            RemoteClient::server_channel_from_channels(
                incoming_rx,
                outgoing_tx,
                cx,
                "server",
                false,
            )
        });
        let client = channel.proto_client();
        let responder = cx.new(|_| Responder);

        // The first handler invocation blocks on the gate; later ones answer at once.
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let gate = Arc::new(Mutex::new(Some(release_rx)));
        client.add_request_handler(responder.downgrade(), {
            let gate = gate.clone();
            move |_, _: proto::TypedEnvelope<proto::Ping>, _| {
                let pending = gate.lock().take();
                async move {
                    if let Some(pending) = pending {
                        pending.await.ok();
                    }
                    Ok(proto::Ack {})
                }
            }
        });

        incoming_tx
            .unbounded_send(proto::Ping {}.into_envelope(40, None, None))
            .unwrap();
        cx.run_until_parked();

        let mut ends = cx.update(|cx| channel.begin_fresh_session(&cx.to_async()));
        ends.incoming_tx
            .unbounded_send(proto::Ping {}.into_envelope(40, None, None))
            .unwrap();
        cx.run_until_parked();
        release_tx.send(()).unwrap();
        cx.run_until_parked();

        let mut new_responses = Vec::new();
        while let Ok(envelope) = ends.outgoing_rx.try_recv() {
            if envelope.responding_to == Some(40) {
                new_responses.push(envelope);
            }
        }
        assert_eq!(
            new_responses.len(),
            1,
            "the replaced session's late response must not reach the new client"
        );
        while let Ok(envelope) = old_outgoing_rx.try_recv() {
            assert_ne!(
                envelope.responding_to,
                Some(40),
                "the old channel never sees the late response either"
            );
        }
    }

    #[gpui::test]
    async fn test_server_channel_replace_buffered(cx: &mut TestAppContext) {
        let (_incoming_tx, incoming_rx) = mpsc::unbounded::<Envelope>();
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded::<Envelope>();
        let channel = cx.update(|cx| {
            RemoteClient::server_channel_from_channels(
                incoming_rx,
                outgoing_tx,
                cx,
                "server",
                false,
            )
        });
        let client = channel.proto_client();
        client.send(proto::Ping {}).unwrap();
        cx.run_until_parked();

        let mut sent_id = None;
        while let Ok(envelope) = outgoing_rx.try_recv() {
            if matches!(envelope.payload, Some(proto::envelope::Payload::Ping(_))) {
                sent_id = Some(envelope.id);
            }
        }
        let sent_id = sent_id.expect("the ping was sent");

        let replacement = proto::Ack {}.into_envelope(sent_id, None, None);
        assert!(channel.replace_buffered(sent_id, Some(replacement)));
        assert!(matches!(
            channel
                .client
                .buffer
                .lock()
                .front()
                .map(|envelope| &envelope.payload),
            Some(Some(proto::envelope::Payload::Ack(_)))
        ));
        assert!(channel.replace_buffered(sent_id, None));
        assert!(channel.client.buffer.lock().is_empty());
        assert!(!channel.replace_buffered(sent_id, None));
    }
}

impl From<SshConnectionOptions> for RemoteConnectionOptions {
    fn from(opts: SshConnectionOptions) -> Self {
        RemoteConnectionOptions::Ssh(opts)
    }
}

impl From<WslConnectionOptions> for RemoteConnectionOptions {
    fn from(opts: WslConnectionOptions) -> Self {
        RemoteConnectionOptions::Wsl(opts)
    }
}

impl From<WebSocketConnectionOptions> for RemoteConnectionOptions {
    fn from(opts: WebSocketConnectionOptions) -> Self {
        RemoteConnectionOptions::WebSocket(opts)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl From<crate::transport::mock::MockConnectionOptions> for RemoteConnectionOptions {
    fn from(opts: crate::transport::mock::MockConnectionOptions) -> Self {
        RemoteConnectionOptions::Mock(opts)
    }
}

#[cfg(target_os = "windows")]
/// Open a wsl path (\\wsl.localhost\<distro>\path)
#[derive(Debug, Clone, PartialEq, Eq, gpui::Action)]
#[action(namespace = workspace, no_json, no_register)]
pub struct OpenWslPath {
    pub distro: WslConnectionOptions,
    pub paths: Vec<PathBuf>,
}

#[async_trait(?Send)]
pub trait RemoteConnection: Send + Sync {
    fn start_proxy(
        &self,
        unique_identifier: String,
        reconnect: bool,
        incoming_tx: UnboundedSender<Envelope>,
        outgoing_rx: UnboundedReceiver<Envelope>,
        connection_activity_tx: Sender<()>,
        delegate: Arc<dyn RemoteClientDelegate>,
        cx: &mut AsyncApp,
    ) -> Task<Result<i32>>;
    fn upload_directory(
        &self,
        src_path: PathBuf,
        dest_path: RemotePathBuf,
        cx: &App,
    ) -> Task<Result<()>>;
    async fn kill(&self) -> Result<()>;
    fn has_been_killed(&self) -> bool;
    fn shares_network_interface(&self) -> bool {
        false
    }
    fn build_command(
        &self,
        program: Option<String>,
        args: &[String],
        env: &HashMap<String, String>,
        working_dir: Option<String>,
        port_forward: Option<(u16, String, u16)>,
        interactive: Interactive,
    ) -> Result<CommandTemplate>;
    fn build_forward_ports_command(
        &self,
        forwards: Vec<(u16, String, u16)>,
    ) -> Result<CommandTemplate>;
    fn connection_options(&self) -> RemoteConnectionOptions;
    fn path_style(&self) -> PathStyle;
    /// The remote platform (OS and architecture), detected during connection setup.
    fn remote_platform(&self) -> RemotePlatform;
    /// The remote host's OS version (e.g. `"ubuntu 24.04"` or `"15.6.1"`),
    /// detected during connection setup. `None` if it could not be determined.
    fn remote_os_version(&self) -> Option<String>;
    fn shell(&self) -> String;
    fn default_system_shell(&self) -> String;
    fn has_wsl_interop(&self) -> bool;
    /// How many consecutive reconnect attempts `RemoteClient` makes before giving up with
    /// `ReconnectExhausted`. Transports whose redial is cheap and expected (a sandbox
    /// resuming behind a WebSocket) raise it; `0` stops retrying at once.
    fn max_reconnect_attempts(&self) -> usize {
        MAX_RECONNECT_ATTEMPTS
    }
    /// Whether terminals are server-managed PTYs driven over the protocol (D27).
    fn supports_remote_pty(&self) -> bool {
        false
    }
    /// Whether `upload_directory` works, i.e. whether the extension store may sync local
    /// extensions to this remote (D27).
    fn supports_extension_upload(&self) -> bool {
        true
    }

    #[cfg(any(test, feature = "test-support"))]
    fn simulate_disconnect(&self, _: &AsyncApp) {}
}

/// Resolves with whichever future completes first, dropping the other. Replaces
/// `smol::future::or`, which the browser build's `smol` shim does not provide.
async fn first_to_finish<T>(
    left: impl Future<Output = T>,
    right: impl Future<Output = T>,
) -> T {
    let left = std::pin::pin!(left);
    let right = std::pin::pin!(right);
    match futures::future::select(left, right).await {
        futures::future::Either::Left((value, _)) | futures::future::Either::Right((value, _)) => {
            value
        }
    }
}

type ResponseChannels = Mutex<HashMap<MessageId, oneshot::Sender<(Envelope, oneshot::Sender<()>)>>>;
type StreamResponseChannels =
    Arc<Mutex<HashMap<MessageId, UnboundedSender<(Result<Envelope>, oneshot::Sender<()>)>>>>;

struct Signal<T: 'static> {
    tx: Mutex<Option<oneshot::Sender<T>>>,
    rx: Shared<Task<Option<T>>>,
}

impl<T: Send + Clone + 'static> Signal<T> {
    pub fn new(cx: &App) -> Self {
        let (tx, rx) = oneshot::channel();

        let task = cx
            .background_executor()
            .spawn(async move { rx.await.ok() })
            .shared();

        Self {
            tx: Mutex::new(Some(tx)),
            rx: task,
        }
    }

    fn set(&self, value: T) {
        if let Some(tx) = self.tx.lock().take() {
            let _ = tx.send(value);
        }
    }

    fn wait(&self) -> Shared<Task<Option<T>>> {
        self.rx.clone()
    }
}

/// The broker's ends of a channel pair.
pub struct ChannelEnds {
    /// Envelopes from the client go in here.
    pub incoming_tx: mpsc::UnboundedSender<Envelope>,
    /// Envelopes for the client come out of here.
    pub outgoing_rx: mpsc::UnboundedReceiver<Envelope>,
}

/// Server-side handle on the channel client, for transports that keep the server process
/// alive across client sessions (`zed-remote-server serve`).
#[derive(Clone)]
pub struct ServerChannel {
    client: Arc<ChannelClient>,
}

impl ServerChannel {
    /// The client as the `AnyProtoClient` the headless project is built on.
    pub fn proto_client(&self) -> AnyProtoClient {
        self.client.clone().into()
    }

    /// Replaces (or, with `None`, removes) the envelope with `id` in the unacked replay
    /// buffer, so a message the transport refused to send is not replayed on every reconnect.
    /// Returns whether an envelope with that id was buffered.
    pub fn replace_buffered(&self, id: u32, replacement: Option<Envelope>) -> bool {
        let mut buffer = self.client.buffer.lock();
        let Some(index) = buffer.iter().position(|envelope| envelope.id == id) else {
            return false;
        };
        match replacement {
            Some(replacement) => buffer[index] = replacement,
            None => {
                buffer.remove(index);
            }
        }
        true
    }

    /// Forgets everything that belonged to the previous client and re-runs the initial
    /// handshake on a new channel pair: clears the unacked replay buffer, resets the
    /// received-id watermark, drops pending response channels (their futures fail), then
    /// restarts message handling, which sends `RemoteStarted` into the new `outgoing_tx`.
    /// Everything queued on the old `outgoing_rx` dies with it when the caller drops it.
    /// `next_message_id` is deliberately not reset: the client only uses server ids as an
    /// ack watermark, and monotonic ids keep buffer trimming correct.
    pub fn begin_fresh_session(&self, cx: &AsyncApp) -> ChannelEnds {
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
        let client = &self.client;
        // Handlers dispatched for the previous client keep running; bumping the generation
        // makes their late responses drop instead of reaching the new client's requests,
        // whose ids restart at 0 and would otherwise collide.
        client.generation.fetch_add(1, SeqCst);
        client.buffer.lock().clear();
        client.max_received.store(0, SeqCst);
        client.response_channels.lock().clear();
        client.stream_response_channels.lock().clear();
        client.reconnect(incoming_rx, outgoing_tx, cx);
        ChannelEnds {
            incoming_tx,
            outgoing_rx,
        }
    }
}

pub(crate) struct ChannelClient {
    next_message_id: AtomicU32,
    outgoing_tx: Mutex<mpsc::UnboundedSender<Envelope>>,
    buffer: Mutex<VecDeque<Envelope>>,
    response_channels: ResponseChannels,
    stream_response_channels: StreamResponseChannels,
    message_handlers: Mutex<ProtoMessageHandlerSet>,
    max_received: AtomicU32,
    /// Bumped by `ServerChannel::begin_fresh_session`; a handler dispatched under an older
    /// generation answers a client that is gone, so its response is dropped.
    generation: AtomicU64,
    name: &'static str,
    task: Mutex<Task<Result<()>>>,
    remote_started: Signal<()>,
    has_wsl_interop: bool,
    executor: BackgroundExecutor,
    /// Browser boot (b7): the `Project` that registers the handlers for what the server
    /// replays right after `HelloAck` (`PortsChanged`, a pending `LifecycleNotice`) is only
    /// created after the client-state round trip, so messages without a handler are held
    /// here until [`ChannelClient::replay_unhandled`] delivers them; `None` once delivered
    /// (or from the start, natively), after which such messages are answered with an error
    /// as before.
    #[cfg(target_family = "wasm")]
    unhandled_before_handlers: Mutex<Option<Vec<Box<dyn proto::AnyTypedEnvelope>>>>,
}

/// Most a boot holds back before falling through to the error path; the replay after attach
/// is a handful of messages.
#[cfg(target_family = "wasm")]
const MAX_UNHANDLED_BEFORE_HANDLERS: usize = 64;

/// The client a dispatched message handler answers through: `send_response` is dropped once
/// the session that carried the request has been replaced by a fresh one.
struct SessionScopedClient {
    client: Arc<ChannelClient>,
    generation: u64,
}

impl SessionScopedClient {
    fn is_replaced(&self) -> bool {
        self.client.generation.load(SeqCst) != self.generation
    }
}

impl ProtoClient for SessionScopedClient {
    fn request(
        &self,
        envelope: proto::Envelope,
        request_type: &'static str,
    ) -> BoxFuture<'static, Result<proto::Envelope>> {
        self.client
            .request_dynamic(envelope, request_type, true)
            .boxed()
    }

    fn request_stream(
        &self,
        envelope: proto::Envelope,
        request_type: &'static str,
    ) -> BoxFuture<'static, Result<BoxStream<'static, Result<proto::Envelope>>>> {
        self.client
            .request_stream_dynamic(envelope, request_type)
            .boxed()
    }

    // `AnyProtoClient::send_response` arrives here as `send` with `responding_to` set, so
    // both entry points apply the generation check to responses.
    fn send(&self, envelope: proto::Envelope, message_type: &'static str) -> Result<()> {
        if envelope.responding_to.is_some() && self.is_replaced() {
            log::debug!(
                "{}:dropping {message_type} response to request {:?} of a replaced session",
                self.client.name,
                envelope.responding_to
            );
            return Ok(());
        }
        self.client.send_dynamic(envelope)
    }

    fn send_response(&self, envelope: Envelope, message_type: &'static str) -> Result<()> {
        self.send(envelope, message_type)
    }

    fn message_handler_set(&self) -> &Mutex<ProtoMessageHandlerSet> {
        &self.client.message_handlers
    }

    fn is_via_collab(&self) -> bool {
        false
    }

    fn has_wsl_interop(&self) -> bool {
        self.client.has_wsl_interop
    }
}

impl ChannelClient {
    pub(crate) fn new(
        incoming_rx: mpsc::UnboundedReceiver<Envelope>,
        outgoing_tx: mpsc::UnboundedSender<Envelope>,
        cx: &App,
        name: &'static str,
        has_wsl_interop: bool,
    ) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            outgoing_tx: Mutex::new(outgoing_tx),
            next_message_id: AtomicU32::new(0),
            max_received: AtomicU32::new(0),
            generation: AtomicU64::new(0),
            response_channels: ResponseChannels::default(),
            stream_response_channels: StreamResponseChannels::default(),
            message_handlers: Default::default(),
            buffer: Mutex::new(VecDeque::new()),
            name,
            executor: cx.background_executor().clone(),
            task: Mutex::new(Self::start_handling_messages(
                this.clone(),
                incoming_rx,
                &cx.to_async(),
            )),
            remote_started: Signal::new(cx),
            has_wsl_interop,
            #[cfg(target_family = "wasm")]
            unhandled_before_handlers: Mutex::new(Some(Vec::new())),
        })
    }

    fn wait_for_remote_started(&self) -> Shared<Task<Option<()>>> {
        self.remote_started.wait()
    }

    /// Holds `envelope` back while the boot buffer is active and no handler exists for its
    /// type; returns it when it must be dispatched (or refused) right away.
    #[cfg(target_family = "wasm")]
    fn hold_until_handlers_exist(
        &self,
        envelope: Box<dyn proto::AnyTypedEnvelope>,
    ) -> std::result::Result<(), Box<dyn proto::AnyTypedEnvelope>> {
        let mut held = self.unhandled_before_handlers.lock();
        let Some(held) = held.as_mut() else {
            return Err(envelope);
        };
        let has_handler = self
            .message_handlers
            .lock()
            .message_handlers
            .contains_key(&envelope.payload_type_id());
        if has_handler || held.len() >= MAX_UNHANDLED_BEFORE_HANDLERS {
            return Err(envelope);
        }
        log::debug!(
            "{}:holding {} until its handler is registered",
            self.name,
            envelope.payload_type_name()
        );
        held.push(envelope);
        Ok(())
    }

    /// Dispatches the messages held by [`Self::hold_until_handlers_exist`] and stops
    /// holding any more; a message still without a handler is answered with an error, as
    /// the receive loop does.
    #[cfg(target_family = "wasm")]
    pub(crate) fn replay_unhandled(self: &Arc<Self>, cx: &AsyncApp) {
        let held = self.unhandled_before_handlers.lock().take();
        for envelope in held.into_iter().flatten() {
            let type_name = envelope.payload_type_name();
            let message_id = envelope.message_id();
            let scoped_client: AnyProtoClient = Arc::new(SessionScopedClient {
                client: self.clone(),
                generation: self.generation.load(SeqCst),
            })
            .into();
            if let Some(future) = ProtoMessageHandlerSet::handle_message(
                &self.message_handlers,
                envelope,
                scoped_client,
                cx.clone(),
            ) {
                log::debug!("{}:replaying held message. name:{type_name}", self.name);
                let name = self.name;
                cx.foreground_executor()
                    .spawn(async move {
                        if let Err(error) = future.await {
                            log::error!(
                                "{name}:error handling held message. type:{type_name}, error:{error:#}"
                            );
                        }
                    })
                    .detach();
            } else {
                log::error!("{}:unhandled held message name:{type_name}", self.name);
                if let Err(error) = AnyProtoClient::from(self.clone()).send_response(
                    message_id,
                    anyhow::anyhow!("no handler registered for {type_name}").to_proto(),
                ) {
                    log::error!(
                        "{}:error sending error response for {type_name}:{error:#}",
                        self.name
                    );
                }
            }
        }
    }

    fn start_handling_messages(
        this: Weak<Self>,
        mut incoming_rx: mpsc::UnboundedReceiver<Envelope>,
        cx: &AsyncApp,
    ) -> Task<Result<()>> {
        cx.spawn(async move |cx| {
            if let Some(this) = this.upgrade() {
                let envelope = proto::RemoteStarted {}.into_envelope(0, None, None);
                this.outgoing_tx.lock().unbounded_send(envelope).ok();
            };

            let peer_id = PeerId { owner_id: 0, id: 0 };
            while let Some(incoming) = incoming_rx.next().await {
                let Some(this) = this.upgrade() else {
                    return anyhow::Ok(());
                };
                if let Some(ack_id) = incoming.ack_id {
                    let mut buffer = this.buffer.lock();
                    while buffer.front().is_some_and(|msg| msg.id <= ack_id) {
                        buffer.pop_front();
                    }
                }
                if let Some(proto::envelope::Payload::FlushBufferedMessages(_)) = &incoming.payload
                {
                    log::debug!(
                        "{}:remote message received. name:FlushBufferedMessages",
                        this.name
                    );
                    {
                        let buffer = this.buffer.lock();
                        for envelope in buffer.iter() {
                            this.outgoing_tx
                                .lock()
                                .unbounded_send(envelope.clone())
                                .ok();
                        }
                    }
                    let mut envelope = proto::Ack {}.into_envelope(0, Some(incoming.id), None);
                    envelope.id = this.next_message_id.fetch_add(1, SeqCst);
                    this.outgoing_tx.lock().unbounded_send(envelope).ok();
                    continue;
                }

                if let Some(proto::envelope::Payload::RemoteStarted(_)) = &incoming.payload {
                    log::debug!("{}:remote message received. name:RemoteStarted", this.name);
                    this.remote_started.set(());
                    let mut envelope = proto::Ack {}.into_envelope(0, Some(incoming.id), None);
                    envelope.id = this.next_message_id.fetch_add(1, SeqCst);
                    this.outgoing_tx.lock().unbounded_send(envelope).ok();
                    continue;
                }

                // Server ids only ever grow, so the watermark is monotonic; this also keeps a
                // locally synthesized response (id 0, e.g. the WebSocket transport's
                // oversize-envelope error) from rolling the ack we send the server back to 0.
                this.max_received.fetch_max(incoming.id, SeqCst);

                if let Some(request_id) = incoming.responding_to {
                    let request_id = MessageId(request_id);
                    // An incoming response with no payload is malformed; drop
                    // it. The request future and any stream consumers will
                    // remain pending until either a real response arrives or
                    // the connection is torn down.
                    if incoming.payload.is_none() {
                        continue;
                    }
                    let sender = this.response_channels.lock().remove(&request_id);
                    if let Some(sender) = sender {
                        let (tx, rx) = oneshot::channel();
                        sender.send((incoming, tx)).ok();
                        rx.await.ok();
                    } else {
                        let terminal_stream_response = matches!(
                            &incoming.payload,
                            Some(proto::envelope::Payload::Error(_))
                                | Some(proto::envelope::Payload::EndStream(_))
                        );
                        let sender = if terminal_stream_response {
                            this.stream_response_channels.lock().remove(&request_id)
                        } else {
                            this.stream_response_channels
                                .lock()
                                .get(&request_id)
                                .cloned()
                        };
                        if let Some(sender) = sender {
                            let (tx, rx) = oneshot::channel();
                            if sender.unbounded_send((Ok(incoming), tx)).is_err() {
                                this.stream_response_channels.lock().remove(&request_id);
                                continue;
                            }
                            rx.await.ok();
                        }
                    }
                } else if let Some(envelope) =
                    build_typed_envelope(peer_id, Instant::now(), incoming)
                {
                    #[cfg(target_family = "wasm")]
                    let envelope = match this.hold_until_handlers_exist(envelope) {
                        Ok(()) => continue,
                        Err(envelope) => envelope,
                    };
                    let type_name = envelope.payload_type_name();
                    let message_id = envelope.message_id();
                    let scoped_client: AnyProtoClient = Arc::new(SessionScopedClient {
                        client: this.clone(),
                        generation: this.generation.load(SeqCst),
                    })
                    .into();
                    if let Some(future) = ProtoMessageHandlerSet::handle_message(
                        &this.message_handlers,
                        envelope,
                        scoped_client,
                        cx.clone(),
                    ) {
                        log::debug!("{}:remote message received. name:{type_name}", this.name);
                        cx.foreground_executor()
                            .spawn(async move {
                                match future.await {
                                    Ok(_) => {
                                        log::debug!(
                                            "{}:remote message handled. name:{type_name}",
                                            this.name
                                        );
                                    }
                                    Err(error) => {
                                        log::error!(
                                            "{}:error handling message. type:{}, error:{:#}",
                                            this.name,
                                            type_name,
                                            format!("{error:#}").lines().fold(
                                                String::new(),
                                                |mut message, line| {
                                                    if !message.is_empty() {
                                                        message.push(' ');
                                                    }
                                                    message.push_str(line);
                                                    message
                                                }
                                            )
                                        );
                                    }
                                }
                            })
                            .detach()
                    } else {
                        log::error!("{}:unhandled remote message name:{type_name}", this.name);
                        if let Err(e) = AnyProtoClient::from(this.clone()).send_response(
                            message_id,
                            anyhow::anyhow!("no handler registered for {type_name}").to_proto(),
                        ) {
                            log::error!(
                                "{}:error sending error response for {type_name}:{e:#}",
                                this.name
                            );
                        }
                    }
                }
            }
            anyhow::Ok(())
        })
    }

    pub(crate) fn reconnect(
        self: &Arc<Self>,
        incoming_rx: UnboundedReceiver<Envelope>,
        outgoing_tx: UnboundedSender<Envelope>,
        cx: &AsyncApp,
    ) {
        *self.outgoing_tx.lock() = outgoing_tx;
        *self.task.lock() = Self::start_handling_messages(Arc::downgrade(self), incoming_rx, cx);
    }

    fn request<T: RequestMessage>(
        &self,
        payload: T,
    ) -> impl 'static + Future<Output = Result<T::Response>> {
        self.request_internal(payload, true)
    }

    fn request_internal<T: RequestMessage>(
        &self,
        payload: T,
        use_buffer: bool,
    ) -> impl 'static + Future<Output = Result<T::Response>> {
        log::debug!("remote request start. name:{}", T::NAME);
        let response =
            self.request_dynamic(payload.into_envelope(0, None, None), T::NAME, use_buffer);
        async move {
            let response = response.await?;
            log::debug!("remote request finish. name:{}", T::NAME);
            T::Response::from_envelope(response).context("received a response of the wrong type")
        }
    }

    async fn resync(&self, timeout: Duration) -> Result<()> {
        let resync = async {
            self.request_internal(proto::FlushBufferedMessages {}, false)
                .await?;

            for envelope in self.buffer.lock().iter() {
                self.outgoing_tx
                    .lock()
                    .unbounded_send(envelope.clone())
                    .ok();
            }
            Ok(())
        };
        let timeout = async {
            self.executor.timer(timeout).await;
            anyhow::bail!("Timed out resyncing remote client")
        };
        first_to_finish(resync, timeout).await
    }

    async fn ping(&self, timeout: Duration) -> Result<()> {
        let ping = async {
            self.request(proto::Ping {}).await?;
            Ok(())
        };
        let timeout = async {
            self.executor.timer(timeout).await;
            anyhow::bail!("Timed out pinging remote client")
        };
        first_to_finish(ping, timeout).await
    }

    fn send<T: EnvelopedMessage>(&self, payload: T) -> Result<()> {
        log::debug!("remote send name:{}", T::NAME);
        self.send_dynamic(payload.into_envelope(0, None, None))
    }

    fn request_dynamic(
        &self,
        mut envelope: proto::Envelope,
        type_name: &'static str,
        use_buffer: bool,
    ) -> impl 'static + Future<Output = Result<proto::Envelope>> {
        envelope.id = self.next_message_id.fetch_add(1, SeqCst);
        let (tx, rx) = oneshot::channel();
        let mut response_channels_lock = self.response_channels.lock();
        response_channels_lock.insert(MessageId(envelope.id), tx);
        drop(response_channels_lock);

        let result = if use_buffer {
            self.send_buffered(envelope)
        } else {
            self.send_unbuffered(envelope)
        };
        async move {
            if let Err(error) = &result {
                log::error!("failed to send message: {error}");
                anyhow::bail!("failed to send message: {error}");
            }

            let response = rx.await.context("connection lost")?.0;
            if let Some(proto::envelope::Payload::Error(error)) = &response.payload {
                return Err(RpcError::from_proto(error, type_name));
            }
            Ok(response)
        }
    }

    fn request_stream_dynamic(
        &self,
        mut envelope: proto::Envelope,
        type_name: &'static str,
    ) -> impl 'static + Future<Output = Result<BoxStream<'static, Result<proto::Envelope>>>> {
        envelope.id = self.next_message_id.fetch_add(1, SeqCst);
        let message_id = MessageId(envelope.id);
        let (tx, rx) = mpsc::unbounded();
        let stream_response_channels = self.stream_response_channels.clone();
        stream_response_channels.lock().insert(message_id, tx);

        let result = self.send_buffered(envelope);
        async move {
            if let Err(error) = &result {
                log::error!("failed to send message: {error}");
                anyhow::bail!("failed to send message: {error}");
            }

            let cleanup_stream_response_channel = util::defer({
                let stream_response_channels = stream_response_channels.clone();
                move || {
                    stream_response_channels.lock().remove(&message_id);
                }
            });

            Ok(rx
                .filter_map(move |(response, _barrier)| {
                    // Keep the cleanup guard alive until the returned stream is dropped.
                    let _keep_cleanup_guard_alive = &cleanup_stream_response_channel;
                    futures::future::ready(match response {
                        Ok(response) => {
                            if let Some(proto::envelope::Payload::Error(error)) = &response.payload
                            {
                                Some(Err(RpcError::from_proto(error, type_name)))
                            } else if let Some(proto::envelope::Payload::EndStream(_)) =
                                &response.payload
                            {
                                None
                            } else {
                                Some(Ok(response))
                            }
                        }
                        Err(error) => Some(Err(error)),
                    })
                })
                .boxed())
        }
    }

    pub fn send_dynamic(&self, mut envelope: proto::Envelope) -> Result<()> {
        envelope.id = self.next_message_id.fetch_add(1, SeqCst);
        self.send_buffered(envelope)
    }

    fn send_buffered(&self, mut envelope: proto::Envelope) -> Result<()> {
        envelope.ack_id = Some(self.max_received.load(SeqCst));
        self.buffer.lock().push_back(envelope.clone());
        // ignore errors on send (happen while we're reconnecting)
        // assume that the global "disconnected" overlay is sufficient.
        self.outgoing_tx.lock().unbounded_send(envelope).ok();
        Ok(())
    }

    fn send_unbuffered(&self, mut envelope: proto::Envelope) -> Result<()> {
        envelope.ack_id = Some(self.max_received.load(SeqCst));
        self.outgoing_tx.lock().unbounded_send(envelope).ok();
        Ok(())
    }
}

impl ProtoClient for ChannelClient {
    fn request(
        &self,
        envelope: proto::Envelope,
        request_type: &'static str,
    ) -> BoxFuture<'static, Result<proto::Envelope>> {
        self.request_dynamic(envelope, request_type, true).boxed()
    }

    fn request_stream(
        &self,
        envelope: proto::Envelope,
        request_type: &'static str,
    ) -> BoxFuture<'static, Result<BoxStream<'static, Result<proto::Envelope>>>> {
        self.request_stream_dynamic(envelope, request_type).boxed()
    }

    fn send(&self, envelope: proto::Envelope, _message_type: &'static str) -> Result<()> {
        self.send_dynamic(envelope)
    }

    fn send_response(&self, envelope: Envelope, _message_type: &'static str) -> anyhow::Result<()> {
        self.send_dynamic(envelope)
    }

    fn message_handler_set(&self) -> &Mutex<ProtoMessageHandlerSet> {
        &self.message_handlers
    }

    fn is_via_collab(&self) -> bool {
        false
    }

    fn has_wsl_interop(&self) -> bool {
        self.has_wsl_interop
    }
}
