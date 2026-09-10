//! The session broker: one tokio task that owns the envelope channel ends and at most one
//! attached WebSocket session; each attached socket is driven by its own task. The broker
//! never awaits a socket write, so a stalled peer cannot delay a replacement or SIGTERM.
//!
//! Arbitration (D23, D24, D25): resume is keyed on the *epoch* the server hands out in every
//! `HelloAck` (echoed back in `Hello.epoch`) and on the client's per-boot `Hello.instance`
//! nonce, never on the per-connect JWT `sid` (D1) and never on `Hello.identifier` (logs only).

use std::{
    fmt::Write as _,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering::SeqCst},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _, future::BoxFuture};
use remote::{
    ChannelEnds,
    protocol::{decode_envelope_frame, encode_envelope_frame},
    websocket_wire::{
        CLOSE_BAD_HELLO, CLOSE_BUILD_MISMATCH, CLOSE_FRAME_TOO_LARGE, CLOSE_GOING_AWAY,
        CLOSE_POLICY_VIOLATION, CLOSE_REASON_STALE_EPOCH, CLOSE_TAKEN_OVER, ClientKind,
        ControlFrame, HEARTBEAT_INTERVAL_SECS, Hello, HelloAck, MAX_FRAME_BYTES, PROTOCOL_VERSION,
        builds_compatible,
    },
};
use rpc::{
    ErrorCodeExt as _, ErrorExt as _,
    proto::{Envelope, EnvelopedMessage as _, ErrorCode},
};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use yawc::{
    HttpStream, WebSocket, WebSocketError,
    close::CloseCode,
    frame::{Frame, OpCode},
};

use crate::serve::{auth::Claims, http::ServeState};

/// A still-attached socket of the same instance and epoch is replaced by a reconnect (D23:
/// "superseded" is 4001; by construction the receiver is the dead half of a socket the same
/// client already replaced).
pub const CLOSE_SUPERSEDED: u16 = CLOSE_TAKEN_OVER;
/// SIGTERM / stop closes with the standard "going away" (D23; 4004 is retired).
pub const CLOSE_STOPPING: u16 = CLOSE_GOING_AWAY;
/// A socket must deliver its `Hello` within this long after the upgrade.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
/// After the 101 is sent, the upgrade future must resolve within this long.
pub const UPGRADE_TIMEOUT: Duration = Duration::from_secs(10);
/// One frame to a peer that is not reading; longer means "slow consumer" (1008).
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// A session task must exit this soon after a close control message.
pub const DETACH_TIMEOUT: Duration = Duration::from_secs(1);
/// Bound on draining queued envelopes before a close.
pub const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);
/// Wait for stragglers produced in the same gpui tick as a close command (the `Ack` of a
/// `ShutdownRemoteServer` request must go out before the close).
pub const CLOSE_GRACE: Duration = Duration::from_millis(100);
/// Bounded broker → session frame queue.
pub const SESSION_QUEUE_FRAMES: usize = 64;
/// RFC 6455 close reasons are at most 123 bytes; browsers reject longer ones.
pub const MAX_CLOSE_REASON_BYTES: usize = 123;
/// Server → client `ControlFrame::Heartbeat` and protocol-level ping cadence (D22).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(HEARTBEAT_INTERVAL_SECS);
/// A socket with no inbound frames of any kind for this long is dead (D22).
pub const DEAD_AFTER: Duration = Duration::from_secs(90);
/// Records at this level or more severe are mirrored to the attached session as
/// `ControlFrame::Log`.
pub const LOG_FRAME_MAX_LEVEL: log::Level = log::Level::Warn;
/// Epoch layout: `(process start unix ms << EPOCH_SEQ_BITS) + attach counter`.
pub const EPOCH_SEQ_BITS: u32 = 16;
/// Longest accepted `Hello.identifier` and `Hello.instance` (they are stored for the life of
/// the session and repeated in `/health` and every log line about it).
pub const MAX_HELLO_NAME_BYTES: usize = 256;
/// Longest accepted `Hello.build`.
pub const MAX_HELLO_BUILD_BYTES: usize = 128;
/// Best-effort budget for writing a close frame to a socket that is being refused or replaced.
const CLOSE_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// Longest payload variant name kept when describing an envelope in a log line.
const MAX_PAYLOAD_NAME_BYTES: usize = 64;

const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<WebSocket<HttpStream>>();
};

/// Whether an attach is a warm reconnect or a fresh session (D24).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    /// `HeadlessProject` reset, new channel pair, new epoch.
    Fresh,
    /// State kept, replay through `FlushBufferedMessages`, epoch unchanged.
    Reconnect,
}

/// What `/health` and the logs report about the attached session.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionMeta {
    /// JWT `sid` (D1: per-connect, informational). A differing `Hello.session_id` is logged.
    pub session_id: String,
    /// JWT `sub`.
    pub sub: String,
    /// JWT `jti`.
    pub jti: String,
    /// `Hello.identifier` (logs only).
    pub identifier: String,
    /// `Hello.instance`, the client's per-boot nonce (D25).
    pub instance: String,
    /// How this session attached.
    pub kind: SessionKind,
    /// The epoch this attach runs under.
    pub epoch: u64,
    /// `Hello.build`.
    pub client_build: String,
    /// `Hello.client`.
    pub client: ClientKind,
    /// Unix milliseconds of the attach.
    pub attached_at_ms: u64,
}

/// Commands to the broker.
pub enum BrokerCommand {
    /// Sent by the per-connection task after the upgrade and the `Hello` exchange succeeded.
    Attach {
        /// The upgraded socket.
        ws: WebSocket<HttpStream>,
        /// The client's `Hello`.
        hello: Hello,
        /// The verified token claims.
        claims: Claims,
    },
    /// From `ShutdownRemoteServer` or a supervisor request: flush, close the current session,
    /// stay alive.
    CloseSession {
        /// Close code.
        code: u16,
        /// Close reason.
        reason: &'static str,
    },
    /// Reclaim an idle multiplayer replay broker without interrupting a live socket.
    RetireIfDetached {
        done: tokio::sync::oneshot::Sender<bool>,
    },
    /// From SIGTERM/SIGINT: flush, close 1001, then `ServeHooks::request_quit`.
    Shutdown {
        /// Completed once the session is closed and quit was requested.
        done: tokio::sync::oneshot::Sender<()>,
    },
}

/// Bridge to the gpui side; the production implementation talks over channels, tests stub it.
pub trait ServeHooks: Send + Sync + 'static {
    fn replay_healthy(&self) -> bool {
        true
    }
    /// Replica assigned to this participant; collaborative replicas start at 8.
    fn replica_id(&self) -> u16;
    /// Resets only this participant's snapshot ledger, then hands its channel a new
    /// pair of ends with `RemoteStarted` queued. Shared buffers and PTYs remain alive.
    /// Returns the broker's new ends;
    /// an error means the gpui side is gone, and the broker refuses the attach instead of
    /// waiting forever.
    fn begin_fresh_session(&self) -> BoxFuture<'static, anyhow::Result<ChannelEnds>>;
    /// After every attach.
    fn session_attached(&self, meta: &SessionMeta);
    /// After every session-task exit (`PtyManager::detach_all` on the gpui side, D24).
    fn session_detached(&self, meta: &SessionMeta);
    /// Replaces (or with `None` removes) envelope `id` in the `ChannelClient` replay buffer:
    /// the broker refused to send it, so a reconnect must not replay it either.
    fn replace_buffered(&self, id: u32, replacement: Option<Envelope>);
    /// Quit the process after SIGTERM/SIGINT.
    fn request_quit(&self);
}

/// Broker → session task control.
enum SessionCtl {
    Close { code: u16, reason: String },
}

/// Why a session task exited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionExit {
    /// The peer sent a close frame (or the TCP stream ended).
    PeerClosed {
        /// The peer's close code, if a close frame was seen.
        code: Option<u16>,
    },
    /// Reading failed.
    ReadError(String),
    /// Writing failed.
    WriteError(String),
    /// A write did not complete within [`WRITE_TIMEOUT`].
    SlowConsumer,
    /// The peer sent a frame over [`MAX_FRAME_BYTES`].
    FrameTooLarge,
    /// No inbound frame for [`DEAD_AFTER`].
    Dead,
    /// The broker closed the session.
    Closed {
        /// The close code sent.
        code: u16,
    },
}

impl SessionExit {
    /// Short label for logs.
    pub fn reason(&self) -> String {
        match self {
            SessionExit::PeerClosed { code: Some(code) } => format!("peer_closed({code})"),
            SessionExit::PeerClosed { code: None } => "peer_closed".to_owned(),
            SessionExit::ReadError(error) => format!("read_error: {error}"),
            SessionExit::WriteError(error) => format!("write_error: {error}"),
            SessionExit::SlowConsumer => "slow_consumer".to_owned(),
            SessionExit::FrameTooLarge => "frame_too_large".to_owned(),
            SessionExit::Dead => "dead".to_owned(),
            SessionExit::Closed { code } => format!("closed({code})"),
        }
    }
}

struct ActiveSession {
    meta: SessionMeta,
    frame_tx: mpsc::Sender<Frame>,
    ctl_tx: mpsc::UnboundedSender<SessionCtl>,
    done: JoinHandle<SessionExit>,
    incoming_watermark: Arc<AtomicU32>,
}

/// Owns the envelope channel ends and the attached session.
pub struct SessionBroker {
    incoming_tx: futures::channel::mpsc::UnboundedSender<Envelope>,
    outgoing_rx: futures::channel::mpsc::UnboundedReceiver<Envelope>,
    outgoing_closed: bool,
    commands: mpsc::UnboundedReceiver<BrokerCommand>,
    state: Arc<ServeState>,
    hooks: Arc<dyn ServeHooks>,
    current: Option<ActiveSession>,
    epoch_base: u64,
    attach_seq: u64,
    current_epoch: Option<u64>,
    current_instance: Option<String>,
    last_watermark: u32,
    last_sub: Option<String>,
    stashed: Option<BrokerCommand>,
    /// An envelope taken off `outgoing_rx` whose push into the session queue was preempted
    /// by a command (or whose session died under it). It is delivered before anything else
    /// once a session of the same epoch is attached, and discarded by a fresh session.
    pending_outgoing: Option<Envelope>,
}

enum Classification {
    Fresh,
    Reconnect(u64),
    StaleEpoch,
}

enum BrokerEvent {
    ReplayCheck,
    Command(Option<BrokerCommand>),
    SessionExited(SessionExit),
    Outgoing(Option<Envelope>),
}

/// Unix milliseconds now.
fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// The payload variant name of an envelope (`"TerminalOutput"`), for log lines. Formatting
/// stops after a few bytes so a multi-megabyte payload is never rendered.
fn payload_variant_name(envelope: &Envelope) -> String {
    struct Bounded {
        text: String,
    }

    impl std::fmt::Write for Bounded {
        fn write_str(&mut self, part: &str) -> std::fmt::Result {
            for character in part.chars() {
                if matches!(character, '(' | '{' | ' ') || self.text.len() >= MAX_PAYLOAD_NAME_BYTES
                {
                    return Err(std::fmt::Error);
                }
                self.text.push(character);
            }
            Ok(())
        }
    }

    let Some(payload) = &envelope.payload else {
        return "<none>".to_owned();
    };
    let mut bounded = Bounded {
        text: String::new(),
    };
    write!(bounded, "{payload:?}").ok();
    bounded.text
}

async fn join_session(session: &mut ActiveSession) -> SessionExit {
    match timeout(DETACH_TIMEOUT, &mut session.done).await {
        Ok(Ok(exit)) => exit,
        Ok(Err(error)) => SessionExit::ReadError(format!("session task failed: {error}")),
        Err(_) => {
            session.done.abort();
            SessionExit::Closed { code: 0 }
        }
    }
}

impl SessionBroker {
    /// Creates a broker over the initial channel ends. Epochs never collide across restarts
    /// because the base carries the process start time.
    pub fn new(
        incoming_tx: futures::channel::mpsc::UnboundedSender<Envelope>,
        outgoing_rx: futures::channel::mpsc::UnboundedReceiver<Envelope>,
        commands: mpsc::UnboundedReceiver<BrokerCommand>,
        state: Arc<ServeState>,
        hooks: Arc<dyn ServeHooks>,
    ) -> Self {
        Self {
            incoming_tx,
            outgoing_rx,
            outgoing_closed: false,
            commands,
            state,
            hooks,
            current: None,
            epoch_base: unix_ms() << EPOCH_SEQ_BITS,
            attach_seq: 0,
            current_epoch: None,
            current_instance: None,
            last_watermark: 0,
            last_sub: None,
            stashed: None,
            pending_outgoing: None,
        }
    }

    /// Runs until `Shutdown` is processed (or every command sender is gone).
    pub async fn run(mut self) {
        loop {
            if self.current.is_some() && !self.hooks.replay_healthy() {
                self.close_current(CLOSE_TAKEN_OVER, CLOSE_REASON_STALE_EPOCH)
                    .await;
            }
            let event = if let Some(command) = self.stashed.take() {
                BrokerEvent::Command(Some(command))
            } else if self.current.is_some() && self.pending_outgoing.is_some() {
                BrokerEvent::Outgoing(self.pending_outgoing.take())
            } else {
                let Self {
                    commands,
                    current,
                    outgoing_rx,
                    outgoing_closed,
                    ..
                } = &mut self;
                match current.as_mut() {
                    Some(current) => {
                        let attached = !*outgoing_closed;
                        tokio::select! {
                            biased;
                            command = commands.recv() => BrokerEvent::Command(command),
                            exit = &mut current.done => BrokerEvent::SessionExited(
                                exit.unwrap_or_else(|error| SessionExit::ReadError(format!("session task failed: {error}")))
                            ),
                            _ = tokio::time::sleep(Duration::from_secs(1)) => BrokerEvent::ReplayCheck,
                            envelope = outgoing_rx.next(), if attached => BrokerEvent::Outgoing(envelope),
                        }
                    }
                    None => BrokerEvent::Command(commands.recv().await),
                }
            };
            match event {
                BrokerEvent::ReplayCheck => {}
                BrokerEvent::Command(None) => {
                    log::info!("broker command channel closed; closing the session");
                    self.close_session(CLOSE_STOPPING, "server going away")
                        .await;
                    return;
                }
                BrokerEvent::Command(Some(BrokerCommand::Attach { ws, hello, claims })) => {
                    self.attach(ws, hello, claims).await;
                }
                BrokerEvent::Command(Some(BrokerCommand::CloseSession { code, reason })) => {
                    self.close_session(code, reason).await;
                }
                BrokerEvent::Command(Some(BrokerCommand::RetireIfDetached { done })) => {
                    self.reap_finished_session().await;
                    let retired = self.current.is_none();
                    done.send(retired).ok();
                    if retired {
                        return;
                    }
                }
                BrokerEvent::Command(Some(BrokerCommand::Shutdown { done })) => {
                    self.close_session(CLOSE_STOPPING, "server shutting down")
                        .await;
                    self.hooks.request_quit();
                    done.send(()).ok();
                    return;
                }
                BrokerEvent::SessionExited(exit) => {
                    if let Some(session) = self.current.take() {
                        self.finish_detach(session, &exit);
                    }
                }
                BrokerEvent::Outgoing(None) => {
                    log::error!(
                        "outgoing envelope channel closed; the server channel client is gone"
                    );
                    self.outgoing_closed = true;
                }
                BrokerEvent::Outgoing(Some(envelope)) => {
                    self.pump(envelope).await;
                }
            }
        }
    }

    fn classify(&self, hello: &Hello) -> Classification {
        if hello.reconnect
            && let Some(epoch) = hello.epoch
            && let Some(current_epoch) = self.current_epoch
        {
            if epoch != current_epoch {
                Classification::StaleEpoch
            } else if self.current_instance.as_deref() == Some(hello.instance.as_str()) {
                Classification::Reconnect(epoch)
            } else {
                Classification::Fresh
            }
        } else {
            Classification::Fresh
        }
    }

    /// A session whose task already exited (its socket died) must not count as attached when
    /// the next command is arbitrated; the exit branch of `run` may simply not have been
    /// polled yet because commands are polled first.
    async fn reap_finished_session(&mut self) {
        if !self
            .current
            .as_ref()
            .is_some_and(|session| session.done.is_finished())
        {
            return;
        }
        if let Some(mut session) = self.current.take() {
            let exit = join_session(&mut session).await;
            self.finish_detach(session, &exit);
        }
    }

    async fn attach(&mut self, ws: WebSocket<HttpStream>, hello: Hello, claims: Claims) {
        self.reap_finished_session().await;
        if hello.reconnect && !self.hooks.replay_healthy() {
            refuse_socket(ws, CLOSE_TAKEN_OVER, CLOSE_REASON_STALE_EPOCH);
            return;
        }
        let (kind, epoch) = match self.classify(&hello) {
            Classification::StaleEpoch => {
                log::warn!(
                    "refusing reconnect of instance {} with stale epoch {:?} (current {:?})",
                    hello.instance,
                    hello.epoch,
                    self.current_epoch
                );
                // The reason string is part of the contract: the client maps 4001 with this
                // exact reason to exit 90 (fresh session) rather than 91 (taken over).
                refuse_socket(ws, CLOSE_TAKEN_OVER, CLOSE_REASON_STALE_EPOCH);
                return;
            }
            Classification::Reconnect(epoch) => (SessionKind::Reconnect, Some(epoch)),
            Classification::Fresh => (SessionKind::Fresh, None),
        };

        if self.current.is_some() {
            match kind {
                SessionKind::Reconnect => {
                    let exit = self
                        .close_current(CLOSE_SUPERSEDED, "superseded by reconnect")
                        .await;
                    log::info!(
                        "session_superseded: instance {} reconnected over its half-open socket ({:?})",
                        hello.instance,
                        exit.map(|exit| exit.reason())
                    );
                }
                SessionKind::Fresh => {
                    let exit = self
                        .close_current(CLOSE_TAKEN_OVER, "replaced by participant reload")
                        .await;
                    log::info!(
                        "session_replaced: instance {} reloaded ({:?})",
                        hello.instance,
                        exit.map(|exit| exit.reason())
                    );
                }
            }
        }

        let epoch = match (kind, epoch) {
            (SessionKind::Reconnect, Some(epoch)) => epoch,
            _ => {
                let ends = match self.hooks.begin_fresh_session().await {
                    Ok(ends) => ends,
                    Err(error) => {
                        log::error!("fresh session could not be started: {error:#}");
                        refuse_socket(ws, 1011, "server not ready");
                        return;
                    }
                };
                self.incoming_tx = ends.incoming_tx;
                self.outgoing_rx = ends.outgoing_rx;
                self.outgoing_closed = false;
                self.pending_outgoing = None;
                self.attach_seq += 1;
                let epoch = self.epoch_base.wrapping_add(self.attach_seq);
                self.current_epoch = Some(epoch);
                self.current_instance = Some(hello.instance.clone());
                self.last_watermark = 0;
                epoch
            }
        };

        if let Some(previous_sub) = self.last_sub.replace(claims.sub.clone())
            && previous_sub != claims.sub
        {
            log::warn!(
                "session user changed from {previous_sub} to {} across sessions",
                claims.sub
            );
        }

        let meta = SessionMeta {
            session_id: claims.sid.clone(),
            sub: claims.sub.clone(),
            jti: claims.jti.clone(),
            identifier: hello.identifier.clone(),
            instance: hello.instance.clone(),
            kind,
            epoch,
            client_build: hello.build.clone(),
            client: hello.client,
            attached_at_ms: unix_ms(),
        };
        let ack = HelloAck {
            replica_id: self.hooks.replica_id(),
            protocol: PROTOCOL_VERSION,
            build: self.state.build.clone(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            os_version: server_os_version(),
            shell: server_shell(),
            resumed: kind == SessionKind::Reconnect,
            session_id: claims.sid.clone(),
            epoch,
        };
        let first_frame = match serde_json::to_string(&ControlFrame::HelloAck(ack)) {
            Ok(json) => Frame::text(json),
            Err(error) => {
                log::error!("serializing HelloAck failed: {error}");
                refuse_socket(ws, 1011, "internal error");
                return;
            }
        };

        let (frame_tx, frame_rx) = mpsc::channel(SESSION_QUEUE_FRAMES);
        let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
        let incoming_watermark = Arc::new(AtomicU32::new(self.last_watermark));
        let task = SessionTask {
            ws,
            first_frame,
            frame_rx,
            ctl_rx,
            incoming_tx: self.incoming_tx.clone(),
            incoming_watermark: incoming_watermark.clone(),
            watermark_at_attach: self.last_watermark,
            state: self.state.clone(),
        };
        let done = tokio::spawn(task.run());
        self.current = Some(ActiveSession {
            meta: meta.clone(),
            frame_tx: frame_tx.clone(),
            ctl_tx,
            done,
            incoming_watermark,
        });
        self.state.set_session(Some(meta.clone()));
        self.state
            .set_log_frame_sink(&meta.session_id, Some(frame_tx));
        self.hooks.session_attached(&meta);
        log::info!(
            "session_attached: session_id={} identifier={} instance={} kind={:?} epoch={} sub={}",
            meta.session_id,
            meta.identifier,
            meta.instance,
            meta.kind,
            meta.epoch,
            meta.sub
        );
    }

    /// The frame to send for `envelope`, or `None` when it must not go out: a response to a
    /// request this epoch never sent (a handler still running for a previous client), or an
    /// envelope over [`MAX_FRAME_BYTES`]. An oversize envelope is removed from the replay
    /// buffer so no reconnect replays it; when it answered a request, a `proto::Error`
    /// response with the same id takes its place both in the buffer and on the wire, so the
    /// client's request fails instead of hanging.
    fn prepare_frame(&self, envelope: &Envelope) -> Option<Frame> {
        let current = self.current.as_ref()?;
        if let Some(responding_to) = envelope.responding_to
            && responding_to >= current.incoming_watermark.load(SeqCst)
        {
            log::debug!(
                "dropping stale response to request {responding_to} from a previous session"
            );
            return None;
        }
        let bytes = encode_envelope_frame(envelope);
        if bytes.len() > MAX_FRAME_BYTES {
            let message = format!(
                "message too large for the WebSocket transport ({} bytes, limit {MAX_FRAME_BYTES})",
                bytes.len()
            );
            log::error!(
                "{message}; dropping outgoing {} envelope {}",
                payload_variant_name(envelope),
                envelope.id
            );
            let replacement = envelope.responding_to.map(|responding_to| {
                let mut reply = ErrorCode::Internal
                    .message(message.clone())
                    .to_proto()
                    .into_envelope(envelope.id, Some(responding_to), None);
                reply.ack_id = envelope.ack_id;
                reply
            });
            self.hooks
                .replace_buffered(envelope.id, replacement.clone());
            return replacement.map(|reply| Frame::binary(encode_envelope_frame(&reply)));
        }
        Some(Frame::binary(bytes))
    }

    /// Pushes one envelope into the attached session's queue. A command arriving while the
    /// queue is full wins the race, and the envelope is kept in `pending_outgoing` instead of
    /// being lost: `reserve` is cancel-safe, so nothing is consumed by the preemption.
    async fn pump(&mut self, envelope: Envelope) {
        if self.current.is_none() {
            self.pending_outgoing = Some(envelope);
            return;
        }
        let Some(frame) = self.prepare_frame(&envelope) else {
            return;
        };
        let Some(current) = self.current.as_ref() else {
            self.pending_outgoing = Some(envelope);
            return;
        };
        let frame_tx = current.frame_tx.clone();
        let reserved = tokio::select! {
            biased;
            command = self.commands.recv() => {
                self.stashed = command;
                self.pending_outgoing = Some(envelope);
                return;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                // Recheck an overflowing replay buffer even while the socket is stalled.
                self.pending_outgoing = Some(envelope);
                return;
            }
            permit = frame_tx.reserve() => permit,
        };
        match reserved {
            Ok(permit) => permit.send(frame),
            Err(_) => {
                // The session task dropped its queue: it has exited, so its real exit reason
                // is available at once.
                if let Some(mut session) = self.current.take() {
                    let exit = join_session(&mut session).await;
                    self.finish_detach(session, &exit);
                }
                self.pending_outgoing = Some(envelope);
            }
        }
    }

    /// Forwards queued envelopes until the queue is empty, waits [`CLOSE_GRACE`] for
    /// stragglers, forwards again, then closes the session; bounded by
    /// [`SHUTDOWN_FLUSH_TIMEOUT`]. A no-op without a session.
    async fn close_session(&mut self, code: u16, reason: &str) {
        self.reap_finished_session().await;
        if self.current.is_none() {
            return;
        }
        let deadline = Instant::now() + SHUTDOWN_FLUSH_TIMEOUT;
        self.drain_outgoing(deadline).await;
        tokio::time::sleep(CLOSE_GRACE).await;
        self.drain_outgoing(deadline).await;
        self.close_current(code, reason).await;
    }

    /// Sends `envelope` to the session before `deadline`; `false` when the session is gone or
    /// the deadline passed (the envelope is then kept as pending).
    async fn send_before(&mut self, envelope: Envelope, deadline: Instant) -> bool {
        if self.current.is_none() {
            self.pending_outgoing = Some(envelope);
            return false;
        }
        let Some(frame) = self.prepare_frame(&envelope) else {
            return true;
        };
        let Some(current) = self.current.as_ref() else {
            self.pending_outgoing = Some(envelope);
            return false;
        };
        let frame_tx = current.frame_tx.clone();
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, frame_tx.send(frame)).await {
            Ok(Ok(())) => true,
            _ => {
                self.pending_outgoing = Some(envelope);
                false
            }
        }
    }

    async fn drain_outgoing(&mut self, deadline: Instant) {
        if let Some(pending) = self.pending_outgoing.take()
            && !self.send_before(pending, deadline).await
        {
            return;
        }
        if self.outgoing_closed {
            return;
        }
        loop {
            if Instant::now() >= deadline {
                return;
            }
            let envelope = match self.outgoing_rx.try_recv() {
                Ok(envelope) => envelope,
                Err(futures::channel::mpsc::TryRecvError::Closed) => {
                    self.outgoing_closed = true;
                    return;
                }
                Err(_) => return,
            };
            if !self.send_before(envelope, deadline).await {
                return;
            }
        }
    }

    async fn close_current(&mut self, code: u16, reason: &str) -> Option<SessionExit> {
        let mut session = self.current.take()?;
        session
            .ctl_tx
            .send(SessionCtl::Close {
                code,
                reason: reason.to_owned(),
            })
            .ok();
        let exit = match timeout(DETACH_TIMEOUT, &mut session.done).await {
            Ok(Ok(exit)) => exit,
            Ok(Err(error)) => SessionExit::ReadError(format!("session task failed: {error}")),
            Err(_) => {
                session.done.abort();
                SessionExit::Closed { code }
            }
        };
        self.finish_detach(session, &exit);
        Some(exit)
    }

    fn finish_detach(&mut self, session: ActiveSession, exit: &SessionExit) {
        self.last_watermark = session.incoming_watermark.load(SeqCst);
        self.state
            .set_log_frame_sink(&session.meta.session_id, None);
        self.state.remove_session(&session.meta.session_id);
        self.hooks.session_detached(&session.meta);
        log::info!(
            "session_detached: session_id={} epoch={} reason={}",
            session.meta.session_id,
            session.meta.epoch,
            exit.reason()
        );
    }
}

/// Closes a socket the broker refused, off the broker task.
pub(super) fn refuse_socket(mut ws: WebSocket<HttpStream>, code: u16, reason: &'static str) {
    tokio::spawn(async move {
        send_close(&mut ws, code, reason).await;
    });
}

fn truncate_reason(reason: &str) -> &str {
    if reason.len() <= MAX_CLOSE_REASON_BYTES {
        return reason;
    }
    let mut end = MAX_CLOSE_REASON_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

async fn send_close(ws: &mut WebSocket<HttpStream>, code: u16, reason: &str) {
    let frame = Frame::close(CloseCode::from(code), truncate_reason(reason).as_bytes());
    timeout(CLOSE_WRITE_TIMEOUT, ws.send(frame)).await.ok();
    timeout(CLOSE_WRITE_TIMEOUT, ws.close()).await.ok();
}

/// `$SHELL` of the server process, else `/bin/sh` (`%COMSPEC%` on Windows).
pub fn server_shell() -> String {
    if cfg!(windows) {
        std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_owned())
    } else {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned())
    }
}

/// `/etc/os-release` derived version, if readable.
pub fn server_os_version() -> Option<String> {
    let content = std::fs::read_to_string("/etc/os-release").ok()?;
    util::parse_os_release(&content)
}

struct SessionTask {
    ws: WebSocket<HttpStream>,
    first_frame: Frame,
    frame_rx: mpsc::Receiver<Frame>,
    ctl_rx: mpsc::UnboundedReceiver<SessionCtl>,
    incoming_tx: futures::channel::mpsc::UnboundedSender<Envelope>,
    incoming_watermark: Arc<AtomicU32>,
    watermark_at_attach: u32,
    state: Arc<ServeState>,
}

/// Writes one frame with [`WRITE_TIMEOUT`]; a close control message preempts the write.
async fn write_frame(
    ws: &mut WebSocket<HttpStream>,
    ctl_rx: &mut mpsc::UnboundedReceiver<SessionCtl>,
    frame: Frame,
) -> Result<(), SessionExit> {
    let preempted = tokio::select! {
        biased;
        ctl = ctl_rx.recv() => Err(ctl),
        result = timeout(WRITE_TIMEOUT, ws.send(frame)) => Ok(result),
    };
    match preempted {
        Err(ctl) => {
            let (code, reason) = close_request(ctl);
            send_close(ws, code, &reason).await;
            Err(SessionExit::Closed { code })
        }
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(SessionExit::WriteError(error.to_string())),
        Ok(Err(_)) => {
            send_close(ws, CLOSE_POLICY_VIOLATION, "slow consumer").await;
            Err(SessionExit::SlowConsumer)
        }
    }
}

fn close_request(ctl: Option<SessionCtl>) -> (u16, String) {
    match ctl {
        Some(SessionCtl::Close { code, reason }) => (code, reason),
        None => (CLOSE_GOING_AWAY, "server going away".to_owned()),
    }
}

impl SessionTask {
    async fn run(self) -> SessionExit {
        let SessionTask {
            mut ws,
            first_frame,
            mut frame_rx,
            mut ctl_rx,
            incoming_tx,
            incoming_watermark,
            watermark_at_attach,
            state,
        } = self;
        if let Err(exit) = write_frame(&mut ws, &mut ctl_rx, first_frame).await {
            return exit;
        }
        let heartbeat_json = match serde_json::to_string(&ControlFrame::Heartbeat) {
            Ok(json) => json,
            Err(error) => return SessionExit::WriteError(error.to_string()),
        };
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
            HEARTBEAT_INTERVAL,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_inbound = Instant::now();

        loop {
            let step = tokio::select! {
                biased;
                ctl = ctl_rx.recv() => Step::Close(ctl),
                result = ws.next_frame() => Step::Inbound(result),
                frame = frame_rx.recv() => Step::Outbound(frame),
                _ = heartbeat.tick() => Step::Heartbeat,
            };
            match step {
                Step::Close(ctl) => {
                    let (code, reason) = close_request(ctl);
                    send_close(&mut ws, code, &reason).await;
                    return SessionExit::Closed { code };
                }
                Step::Inbound(Ok(frame)) => {
                    last_inbound = Instant::now();
                    match frame.opcode() {
                        OpCode::Binary => {
                            match decode_envelope_frame(frame.payload(), MAX_FRAME_BYTES) {
                                Ok(envelope) => {
                                    incoming_watermark
                                        .fetch_max(envelope.id.saturating_add(1), SeqCst);
                                    if envelope.id >= watermark_at_attach
                                        && is_input_envelope(&envelope)
                                    {
                                        state.touch_input();
                                    }
                                    if incoming_tx.unbounded_send(envelope).is_err() {
                                        send_close(&mut ws, 1011, "server channel closed").await;
                                        return SessionExit::ReadError(
                                            "incoming channel closed".to_owned(),
                                        );
                                    }
                                }
                                Err(error) => {
                                    if frame.payload().len() > MAX_FRAME_BYTES {
                                        send_close(
                                            &mut ws,
                                            CLOSE_FRAME_TOO_LARGE,
                                            "frame too large",
                                        )
                                        .await;
                                        return SessionExit::FrameTooLarge;
                                    }
                                    send_close(
                                        &mut ws,
                                        CLOSE_POLICY_VIOLATION,
                                        "malformed envelope",
                                    )
                                    .await;
                                    return SessionExit::ReadError(format!(
                                        "malformed envelope: {error:#}"
                                    ));
                                }
                            }
                        }
                        OpCode::Text => {
                            match serde_json::from_slice::<ControlFrame>(frame.payload()) {
                                Ok(ControlFrame::Heartbeat) => {}
                                Ok(other) => log::warn!(
                                    "ignoring unexpected {} control frame",
                                    control_frame_name(&other)
                                ),
                                Err(_) => {
                                    log::warn!(
                                        "ignoring unparseable {} byte text frame",
                                        frame.payload().len()
                                    )
                                }
                            }
                        }
                        OpCode::Close => {
                            return SessionExit::PeerClosed {
                                code: frame.close_code().map(u16::from),
                            };
                        }
                        OpCode::Ping | OpCode::Pong | OpCode::Continuation => {}
                    }
                }
                Step::Inbound(Err(WebSocketError::FrameTooLarge)) => {
                    timeout(CLOSE_WRITE_TIMEOUT, ws.close()).await.ok();
                    return SessionExit::FrameTooLarge;
                }
                Step::Inbound(Err(WebSocketError::ConnectionClosed)) => {
                    return SessionExit::PeerClosed { code: None };
                }
                Step::Inbound(Err(error)) => {
                    return SessionExit::ReadError(error.to_string());
                }
                Step::Outbound(Some(frame)) => {
                    if let Err(exit) = write_frame(&mut ws, &mut ctl_rx, frame).await {
                        return exit;
                    }
                }
                Step::Outbound(None) => {
                    send_close(&mut ws, CLOSE_GOING_AWAY, "server going away").await;
                    return SessionExit::Closed {
                        code: CLOSE_GOING_AWAY,
                    };
                }
                Step::Heartbeat => {
                    if last_inbound.elapsed() >= DEAD_AFTER {
                        send_close(&mut ws, CLOSE_POLICY_VIOLATION, "no frames for 90 s").await;
                        return SessionExit::Dead;
                    }
                    if let Err(exit) =
                        write_frame(&mut ws, &mut ctl_rx, Frame::text(heartbeat_json.clone())).await
                    {
                        return exit;
                    }
                    if let Err(exit) =
                        write_frame(&mut ws, &mut ctl_rx, Frame::ping(Bytes::new())).await
                    {
                        return exit;
                    }
                }
            }
        }
    }
}

enum Step {
    Close(Option<SessionCtl>),
    Inbound(yawc::Result<Frame>),
    Outbound(Option<Frame>),
    Heartbeat,
}

/// The variant name of a control frame, for log lines that must not echo peer content.
fn control_frame_name(frame: &ControlFrame) -> &'static str {
    match frame {
        ControlFrame::Hello(_) => "hello",
        ControlFrame::HelloAck(_) => "hello_ack",
        ControlFrame::Log(_) => "log",
        ControlFrame::Heartbeat => "heartbeat",
    }
}

/// Checks the parts of a `Hello` that do not need the broker: protocol version, the length of
/// the strings the server keeps for the life of the session, and the workspace identity (D25:
/// arbitration keys on `(workspace_id, instance)`, and this server serves exactly one
/// workspace, the one the token was verified against).
fn validate_hello(hello: &Hello, claims: &Claims) -> Result<(), &'static str> {
    if hello.protocol != PROTOCOL_VERSION {
        return Err("unsupported protocol version");
    }
    if hello.identifier.len() > MAX_HELLO_NAME_BYTES {
        return Err("identifier too long");
    }
    if hello.instance.is_empty() || hello.instance.len() > MAX_HELLO_NAME_BYTES {
        return Err("instance must be 1 to 256 bytes");
    }
    if hello.build.len() > MAX_HELLO_BUILD_BYTES {
        return Err("build too long");
    }
    if hello.session_id.len() > MAX_HELLO_NAME_BYTES
        || hello.workspace_id.len() > MAX_HELLO_NAME_BYTES
    {
        return Err("session_id or workspace_id too long");
    }
    if hello.workspace_id != claims.ws {
        return Err("workspace_id does not match the token");
    }
    Ok(())
}

/// Per-connection task spawned by the router right after the 101: waits for the upgrade,
/// reads the `Hello`, runs the protocol/build checks and hands the socket to the broker.
pub async fn run_connection(
    upgrade: yawc::UpgradeFut,
    claims: Claims,
    state: Arc<ServeState>,
    peer: SocketAddr,
) {
    let _pending = state.clone().pending_connection();
    let mut ws = match timeout(UPGRADE_TIMEOUT, upgrade).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(error)) => {
            log::debug!("websocket upgrade from {peer} failed: {error}");
            return;
        }
        Err(_) => {
            log::debug!(
                "websocket upgrade from {peer} did not complete within {UPGRADE_TIMEOUT:?}"
            );
            return;
        }
    };
    let hello = match read_hello(&mut ws).await {
        Ok(hello) => hello,
        Err(reason) => {
            log::warn!("closing socket from {peer}: {reason}");
            send_close(&mut ws, CLOSE_BAD_HELLO, &reason).await;
            return;
        }
    };
    if let Err(reason) = validate_hello(&hello, &claims) {
        log::warn!("closing socket from {peer}: {reason}");
        send_close(&mut ws, CLOSE_BAD_HELLO, reason).await;
        return;
    }
    if let Some(expected) = &state.client_build
        && !builds_compatible(&hello.build, expected)
    {
        log::warn!(
            "closing socket from {peer}: client build {} is not compatible with {expected}",
            hello.build
        );
        send_close(
            &mut ws,
            CLOSE_BUILD_MISMATCH,
            &format!("expected build {expected}"),
        )
        .await;
        return;
    }
    if hello.session_id != claims.sid {
        log::warn!(
            "Hello.session_id {} differs from the token sid {} (informational, D1)",
            hello.session_id,
            claims.sid
        );
    }
    if state
        .broker_tx
        .send(BrokerCommand::Attach { ws, hello, claims })
        .is_err()
    {
        log::error!("session broker is gone; dropping the socket from {peer}");
    }
}

async fn read_hello(ws: &mut WebSocket<HttpStream>) -> Result<Hello, String> {
    let deadline = tokio::time::Instant::now() + HELLO_TIMEOUT;
    loop {
        let frame = match timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            ws.next_frame(),
        )
        .await
        {
            Ok(Ok(frame)) => frame,
            Ok(Err(error)) => return Err(format!("socket error before Hello: {error}")),
            Err(_) => return Err(format!("no Hello within {HELLO_TIMEOUT:?}")),
        };
        match frame.opcode() {
            OpCode::Text => {
                return match serde_json::from_slice::<ControlFrame>(frame.payload()) {
                    Ok(ControlFrame::Hello(hello)) => Ok(hello),
                    Ok(other) => Err(format!(
                        "expected Hello, got a {} frame",
                        control_frame_name(&other)
                    )),
                    Err(_) => Err(format!(
                        "malformed Hello ({} byte text frame)",
                        frame.payload().len()
                    )),
                };
            }
            OpCode::Ping | OpCode::Pong | OpCode::Continuation => continue,
            OpCode::Close => return Err("peer closed before Hello".to_owned()),
            OpCode::Binary => {
                return Err("expected a Hello text frame, got a binary frame".to_owned());
            }
        }
    }
}

/// True for envelopes that count as user input for the idle clock.
///
/// Excludes heartbeat/handshake traffic, responses to server-initiated requests, terminal
/// flow-control / reattach traffic (D20): a `tail -f` or a long build would otherwise keep the
/// workspace active through the client's acks, and the client-state saver and extension
/// listing (a dirty but idle tab's 15 s save must not keep `last_input_at` fresh forever).
/// Input-bearing terminal messages (`SpawnTerminal`, `TerminalInput`, `CloseTerminal`) stay
/// input.
pub fn is_input_envelope(envelope: &Envelope) -> bool {
    use rpc::proto::envelope::Payload;
    if envelope.responding_to.is_some() {
        return false;
    }
    !matches!(
        envelope.payload,
        None | Some(Payload::Ping(_))
            | Some(Payload::Ack(_))
            | Some(Payload::RemoteStarted(_))
            | Some(Payload::FlushBufferedMessages(_))
            | Some(Payload::AckTerminalOutput(_))
            | Some(Payload::ResizeTerminal(_))
            | Some(Payload::ListTerminals(_))
            | Some(Payload::AttachTerminal(_))
            | Some(Payload::SaveClientState(_))
            | Some(Payload::LoadClientState(_))
            | Some(Payload::ListExtensions(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::test_support::{HookEvent, TestClient, TestServer};
    use remote::protocol::encode_envelope_frame;
    use rpc::proto;

    fn envelope(id: u32, payload: proto::envelope::Payload) -> Envelope {
        Envelope {
            id,
            responding_to: None,
            original_sender_id: None,
            ack_id: None,
            payload: Some(payload),
        }
    }

    fn big_envelope(id: u32, bytes: usize) -> Envelope {
        proto::TerminalOutput {
            project_id: 0,
            terminal_id: 1,
            offset: 0,
            data: vec![0u8; bytes],
            reset: false,
        }
        .into_envelope(id, None, None)
    }

    fn mirror(server: &TestServer, level: log::Level, message: &str) {
        server.state.mirror_record(
            &log::Record::builder()
                .level(level)
                .module_path(Some("test"))
                .args(format_args!("{message}"))
                .build(),
        );
    }

    fn detach_count(server: &TestServer, epoch: u64) -> usize {
        server
            .hooks
            .events()
            .iter()
            .filter(|event| matches!(event, HookEvent::Detached(meta) if meta.epoch == epoch))
            .count()
    }

    #[test]
    fn is_input_envelope_rules() {
        use proto::envelope::Payload;
        let not_input = [
            Payload::Ping(proto::Ping {}),
            Payload::Ack(proto::Ack {}),
            Payload::RemoteStarted(proto::RemoteStarted {}),
            Payload::FlushBufferedMessages(proto::FlushBufferedMessages {}),
            Payload::AckTerminalOutput(proto::AckTerminalOutput {
                project_id: 0,
                terminal_id: 1,
                offset: 0,
            }),
            Payload::ResizeTerminal(proto::ResizeTerminal {
                project_id: 0,
                terminal_id: 1,
                cols: 80,
                rows: 24,
            }),
            Payload::ListTerminals(proto::ListTerminals { project_id: 0 }),
            Payload::AttachTerminal(proto::AttachTerminal {
                project_id: 0,
                terminal_id: 1,
                from_offset: 0,
                cols: 80,
                rows: 24,
            }),
            Payload::SaveClientState(proto::SaveClientState {
                project_id: 0,
                sqlite: vec![1],
                version: 1,
                gzip: false,
                client_build: None,
                stopping: false,
            }),
            Payload::LoadClientState(proto::LoadClientState {
                project_id: 0,
                metadata_only: true,
            }),
            Payload::ListExtensions(proto::ListExtensions {
                project_id: 0,
                search: None,
                include_available: false,
                ..Default::default()
            }),
        ];
        for payload in not_input {
            assert!(
                !is_input_envelope(&envelope(1, payload.clone())),
                "{payload:?} must not be input"
            );
        }

        let input = [
            Payload::AddWorktree(proto::AddWorktree {
                project_id: 0,
                path: "/tmp".into(),
                visible: true,
            }),
            Payload::TerminalInput(proto::TerminalInput {
                project_id: 0,
                terminal_id: 1,
                data: vec![7],
            }),
            Payload::CloseTerminal(proto::CloseTerminal {
                project_id: 0,
                terminal_id: 1,
            }),
            Payload::ForwardPort(proto::ForwardPort {
                project_id: 0,
                port: 3000,
                visibility: 1,
                label: None,
            }),
            Payload::InstallRegistryExtension(proto::InstallRegistryExtension {
                project_id: 0,
                id: "toml".into(),
                version: None,
                ..Default::default()
            }),
        ];
        for payload in input {
            assert!(
                is_input_envelope(&envelope(1, payload.clone())),
                "{payload:?} must be input"
            );
        }

        let mut response = envelope(2, proto::envelope::Payload::Ack(proto::Ack {}));
        response.responding_to = Some(1);
        assert!(!is_input_envelope(&response));
        let mut worktree_response = envelope(
            3,
            proto::envelope::Payload::AddWorktree(proto::AddWorktree {
                project_id: 0,
                path: "/tmp".into(),
                visible: true,
            }),
        );
        worktree_response.responding_to = Some(1);
        assert!(!is_input_envelope(&worktree_response));
    }

    #[test]
    fn close_reasons_are_truncated_to_the_rfc_limit() {
        let long = "x".repeat(MAX_CLOSE_REASON_BYTES + 20);
        assert_eq!(truncate_reason(&long).len(), MAX_CLOSE_REASON_BYTES);
        assert_eq!(truncate_reason("short"), "short");
        let multibyte = "é".repeat(MAX_CLOSE_REASON_BYTES);
        assert!(truncate_reason(&multibyte).len() <= MAX_CLOSE_REASON_BYTES);
    }

    #[test]
    fn payload_variant_name_is_bounded() {
        assert_eq!(
            payload_variant_name(&big_envelope(1, 4 * 1024 * 1024)),
            "TerminalOutput"
        );
        assert_eq!(
            payload_variant_name(&envelope(1, proto::envelope::Payload::Ping(proto::Ping {}))),
            "Ping"
        );
        let mut none = envelope(1, proto::envelope::Payload::Ping(proto::Ping {}));
        none.payload = None;
        assert_eq!(payload_variant_name(&none), "<none>");
    }

    /// Waits until the broker has processed a session exit, so envelopes queued afterwards are
    /// held for the next attach instead of being pushed into the dying socket.
    async fn wait_for_detach(server: &TestServer) {
        for _ in 0..100 {
            if !server.state.health(false).session_active {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the session never detached");
    }

    async fn attach_fresh(
        server: &TestServer,
        sid: &str,
        instance: &str,
    ) -> (TestClient, HelloAck) {
        let mut client = server.connect(&server.token(sid)).await;
        client.hello(sid, instance, false, None).await;
        let ack = client.hello_ack().await.expect("hello ack");
        (client, ack)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retirement_never_evicts_a_live_session() {
        let server = TestServer::start().await;
        let (client, _) = attach_fresh(&server, "sid_1", "inst_1").await;
        let (done, reply) = tokio::sync::oneshot::channel();
        server
            .broker_tx
            .send(BrokerCommand::RetireIfDetached { done })
            .unwrap();
        assert!(!reply.await.unwrap());
        drop(client);
        wait_for_detach(&server).await;
        let (done, reply) = tokio::sync::oneshot::channel();
        server
            .broker_tx
            .send(BrokerCommand::RetireIfDetached { done })
            .unwrap();
        assert!(reply.await.unwrap());
        server.broker_tx.closed().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_session_swaps_channels() {
        let server = TestServer::start().await;
        server
            .hooks
            .send_to_client(envelope(7, proto::envelope::Payload::Ping(proto::Ping {})))
            .await;
        server.hooks.emit_on_next_reset(vec![envelope(
            8,
            proto::envelope::Payload::Ping(proto::Ping {}),
        )]);

        let (mut client, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        assert!(!ack.resumed);
        assert_eq!(ack.session_id, "sid_1");
        assert_eq!(ack.build, "test-build");

        let first = client.next_envelope().await.expect("first envelope");
        assert_eq!(first.id, 0);
        assert!(matches!(
            first.payload,
            Some(proto::envelope::Payload::RemoteStarted(_))
        ));
        assert_eq!(server.hooks.fresh_calls(), 1);
        let events = server.hooks.events();
        let fresh_index = events
            .iter()
            .position(|event| *event == HookEvent::FreshSession)
            .expect("fresh hook");
        let attached_index = events
            .iter()
            .position(|event| matches!(event, HookEvent::Attached(meta) if meta.kind == SessionKind::Fresh))
            .expect("attached hook");
        assert!(fresh_index < attached_index);

        // Nothing queued on the old pair (7) or emitted during the reset (8) may ever arrive:
        // the marker pushed onto the new pair is the very next envelope.
        server
            .hooks
            .send_to_client(envelope(9, proto::envelope::Payload::Ping(proto::Ping {})))
            .await;
        assert_eq!(client.next_envelope().await.expect("marker").id, 9);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_session_failure_refuses_with_1011() {
        let server = TestServer::start().await;
        server.hooks.fail_next_fresh();
        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("sid_1", "inst_1", false, None).await;
        let (code, reason) = client.wait_for_close().await.expect("close");
        assert_eq!(code, 1011);
        assert_eq!(reason, "server not ready");
        assert!(!server.state.health(false).session_active);

        let (mut client, ack) = attach_fresh(&server, "sid_2", "inst_2").await;
        assert!(!ack.resumed);
        assert_eq!(client.next_envelope().await.expect("RemoteStarted").id, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dead_session_never_refuses_the_next_client() {
        let server = TestServer::start().await;
        for round in 0..5 {
            let sid = format!("sid_{round}");
            let (mut first, _) = attach_fresh(&server, &sid, &format!("inst_{round}")).await;
            first.next_envelope().await.expect("RemoteStarted");
            drop(first);
            // No wait: the Attach below may reach the broker before it polled the exit.
            let mut second = server.connect(&server.token("sid_next")).await;
            second
                .hello("sid_next", &format!("next_{round}"), false, None)
                .await;
            let ack = second
                .hello_ack()
                .await
                .expect("a dead session must not be arbitrated as active");
            assert!(!ack.resumed);
            second.next_envelope().await.expect("RemoteStarted");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_consumer_does_not_block_broker() {
        let server = TestServer::start().await;
        let (mut first, first_ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        first.next_envelope().await.expect("RemoteStarted");

        for id in 1..=256u32 {
            server
                .hooks
                .send_to_client(big_envelope(id, 256 * 1024))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let started = std::time::Instant::now();
        let mut second = server.connect(&server.token("sid_2")).await;
        second.hello("sid_2", "inst_2", false, None).await;
        let stalled = tokio::spawn(async move { first.wait_for_close().await });
        let ack = second.hello_ack().await.expect("replacement hello ack");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "replacement took {:?}",
            started.elapsed()
        );
        assert!(!ack.resumed);
        assert_eq!(second.next_envelope().await.expect("RemoteStarted").id, 0);

        let (code, _) = stalled.await.expect("reader").expect("close");
        assert!(
            code == CLOSE_TAKEN_OVER || code == CLOSE_POLICY_VIOLATION,
            "unexpected close code {code}"
        );
        assert_eq!(detach_count(&server, first_ack.epoch), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_supersedes_half_open_socket() {
        let server = TestServer::start().await;
        let (mut first, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        first.next_envelope().await.expect("RemoteStarted");

        let mut second = server.connect(&server.token("sid_2")).await;
        second.hello("sid_2", "inst_1", true, Some(ack.epoch)).await;
        let resumed = second.hello_ack().await.expect("hello ack");
        assert!(resumed.resumed);
        assert_eq!(resumed.epoch, ack.epoch);
        assert_eq!(resumed.session_id, "sid_2");

        let (code, reason) = first.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_SUPERSEDED);
        assert_eq!(reason, "superseded by reconnect");
        assert_eq!(server.hooks.fresh_calls(), 1);
        assert_eq!(detach_count(&server, ack.epoch), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replacement_closes_old_with_4001_and_stale_epoch_is_refused() {
        let server = TestServer::start().await;
        let (mut first, first_ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        first.next_envelope().await.expect("RemoteStarted");

        let mut second = server.connect(&server.token("sid_2")).await;
        second.hello("sid_2", "inst_2", false, None).await;
        let second_ack = second.hello_ack().await.expect("hello ack");
        assert!(!second_ack.resumed);
        assert!(second_ack.epoch > first_ack.epoch);
        second.next_envelope().await.expect("RemoteStarted");

        let (code, _) = first.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_TAKEN_OVER);
        assert_eq!(detach_count(&server, first_ack.epoch), 1);

        let hooks_before = server.hooks.events().len();
        let mut third = server.connect(&server.token("sid_3")).await;
        third
            .hello("sid_3", "inst_1", true, Some(first_ack.epoch))
            .await;
        let (code, reason) = third.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_TAKEN_OVER);
        assert_eq!(reason, "stale epoch");
        assert_eq!(server.hooks.events().len(), hooks_before, "no hook ran");

        server
            .hooks
            .send_to_client(envelope(11, proto::envelope::Payload::Ping(proto::Ping {})))
            .await;
        let still_alive = second.next_envelope().await.expect("second still attached");
        assert_eq!(still_alive.id, 11);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn epoch_is_unique_and_monotonic() {
        let server = TestServer::start().await;
        let mut epochs = Vec::new();
        for index in 0..3 {
            let sid = format!("sid_{index}");
            let mut client = server.connect(&server.token(&sid)).await;
            client
                .hello(&sid, &format!("inst_{index}"), false, None)
                .await;
            let ack = client.hello_ack().await.expect("hello ack");
            epochs.push(ack.epoch);
            client.next_envelope().await.expect("RemoteStarted");
        }
        assert!(
            epochs.windows(2).all(|pair| pair[1] > pair[0]),
            "{epochs:?}"
        );
        assert!(epochs[0] > (1u64 << EPOCH_SEQ_BITS));

        // Two brokers constructed 2 ms apart never produce equal epochs.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let later = TestServer::start().await;
        let (_client, later_ack) = attach_fresh(&later, "sid_x", "inst_x").await;
        assert!(
            later_ack.epoch > epochs[2],
            "{} vs {epochs:?}",
            later_ack.epoch
        );
        assert!(!epochs.contains(&later_ack.epoch));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_session_keeps_queue() {
        let server = TestServer::start().await;
        let (mut client, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");
        drop(client);
        wait_for_detach(&server).await;

        for id in 1..=3 {
            server
                .hooks
                .send_to_client(envelope(id, proto::envelope::Payload::Ping(proto::Ping {})))
                .await;
        }

        let mut client = server.connect(&server.token("sid_9")).await;
        client.hello("sid_9", "inst_1", true, Some(ack.epoch)).await;
        let resumed = client.hello_ack().await.expect("hello ack");
        assert!(resumed.resumed);
        assert_eq!(resumed.epoch, ack.epoch);
        for id in 1..=3 {
            assert_eq!(client.next_envelope().await.expect("queued").id, id);
        }
        assert_eq!(server.hooks.fresh_calls(), 1);
        assert!(matches!(
            server.hooks.events().last(),
            Some(HookEvent::Attached(meta)) if meta.kind == SessionKind::Reconnect
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_to_fresh_process_is_not_resumed() {
        let server = TestServer::start().await;
        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("sid_1", "inst_1", true, Some(123)).await;
        let ack = client.hello_ack().await.expect("hello ack");
        assert!(!ack.resumed);
        assert_eq!(client.next_envelope().await.expect("RemoteStarted").id, 0);

        let server = TestServer::start().await;
        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("sid_1", "inst_1", true, None).await;
        let ack = client.hello_ack().await.expect("hello ack");
        assert!(!ack.resumed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_response_dropped_after_fresh() {
        let server = TestServer::start().await;
        let (mut client, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");

        let mut stale = envelope(50, proto::envelope::Payload::Ack(proto::Ack {}));
        stale.responding_to = Some(41);
        server.hooks.send_to_client(stale).await;

        client
            .send_envelope(&proto::Ping {}.into_envelope(41, None, None))
            .await;
        assert_eq!(server.hooks.next_from_client().await.expect("ping").id, 41);

        let mut answer = envelope(51, proto::envelope::Payload::Ack(proto::Ack {}));
        answer.responding_to = Some(41);
        server.hooks.send_to_client(answer).await;

        let delivered = client.next_envelope().await.expect("answer");
        assert_eq!(delivered.id, 51);
        assert_eq!(delivered.responding_to, Some(41));

        // After a resumed reconnect the watermark carries over: a response to id 41 (received
        // before the blip) is delivered, one to id 60 (never received) is not.
        drop(client);
        wait_for_detach(&server).await;
        let mut client = server.connect(&server.token("sid_2")).await;
        client.hello("sid_2", "inst_1", true, Some(ack.epoch)).await;
        assert!(client.hello_ack().await.expect("hello ack").resumed);
        let mut never_sent = envelope(52, proto::envelope::Payload::Ack(proto::Ack {}));
        never_sent.responding_to = Some(60);
        server.hooks.send_to_client(never_sent).await;
        let mut carried = envelope(53, proto::envelope::Payload::Ack(proto::Ack {}));
        carried.responding_to = Some(41);
        server.hooks.send_to_client(carried).await;
        let delivered = client.next_envelope().await.expect("carried answer");
        assert_eq!(delivered.id, 53);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hello_timeout_and_bad_protocol_close_4006() {
        let server = TestServer::start().await;
        let mut client = server.connect(&server.token("sid_1")).await;
        let started = std::time::Instant::now();
        let (code, _) = tokio::time::timeout(HELLO_TIMEOUT * 3, client.wait_for_close())
            .await
            .expect("close within the hello timeout")
            .expect("close frame");
        assert_eq!(code, CLOSE_BAD_HELLO);
        assert!(started.elapsed() >= HELLO_TIMEOUT);

        let mut client = server.connect(&server.token("sid_1")).await;
        client
            .send_hello(Hello {
                protocol: 2,
                build: "test-build".into(),
                workspace_id: crate::serve::auth::test_support::WORKSPACE.into(),
                session_id: "sid_1".into(),
                identifier: "setup-1".into(),
                instance: "inst_1".into(),
                reconnect: false,
                client: ClientKind::Web,
                epoch: None,
            })
            .await;
        let (code, _) = client.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_BAD_HELLO);

        let mut client = server.connect(&server.token("sid_1")).await;
        client.send_text("{\"type\":\"bogus\"}").await;
        let (code, _) = client.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_BAD_HELLO);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversize_or_foreign_hello_fields_close_4006() {
        let server = TestServer::start().await;
        let base = Hello {
            protocol: PROTOCOL_VERSION,
            build: "test-build".into(),
            workspace_id: crate::serve::auth::test_support::WORKSPACE.into(),
            session_id: "sid_1".into(),
            identifier: "setup-1".into(),
            instance: "inst_1".into(),
            reconnect: false,
            client: ClientKind::Web,
            epoch: None,
        };
        let cases = [
            Hello {
                identifier: "i".repeat(MAX_HELLO_NAME_BYTES + 1),
                ..base.clone()
            },
            Hello {
                instance: "n".repeat(MAX_HELLO_NAME_BYTES + 1),
                ..base.clone()
            },
            Hello {
                instance: String::new(),
                ..base.clone()
            },
            Hello {
                build: "b".repeat(MAX_HELLO_BUILD_BYTES + 1),
                ..base.clone()
            },
            Hello {
                workspace_id: "ws_other".into(),
                ..base.clone()
            },
        ];
        for hello in cases {
            let mut client = server.connect(&server.token("sid_1")).await;
            client.send_hello(hello).await;
            let (code, _) = client.wait_for_close().await.expect("close");
            assert_eq!(code, CLOSE_BAD_HELLO);
        }
        assert!(!server.state.health(false).session_active);

        let mut client = server.connect(&server.token("sid_1")).await;
        client
            .send_hello(Hello {
                identifier: "i".repeat(MAX_HELLO_NAME_BYTES),
                ..base
            })
            .await;
        assert!(client.hello_ack().await.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_mismatch_closes_4002() {
        let server = TestServer::start_with(Vec::new(), Some("b1".into())).await;
        let mut client = server.connect(&server.token("sid_1")).await;
        client
            .send_hello(Hello {
                protocol: PROTOCOL_VERSION,
                build: "b2".into(),
                workspace_id: crate::serve::auth::test_support::WORKSPACE.into(),
                session_id: "sid_1".into(),
                identifier: "setup-1".into(),
                instance: "inst_1".into(),
                reconnect: false,
                client: ClientKind::Web,
                epoch: None,
            })
            .await;
        let (code, reason) = client.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_BUILD_MISMATCH);
        assert_eq!(reason, "expected build b1");

        let mut client = server.connect(&server.token("sid_1")).await;
        client
            .send_hello(Hello {
                protocol: PROTOCOL_VERSION,
                build: "dev-local".into(),
                workspace_id: crate::serve::auth::test_support::WORKSPACE.into(),
                session_id: "sid_1".into(),
                identifier: "setup-1".into(),
                instance: "inst_1".into(),
                reconnect: false,
                client: ClientKind::Web,
                epoch: None,
            })
            .await;
        assert!(client.hello_ack().await.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hello_session_id_mismatch_is_accepted() {
        let server = TestServer::start().await;
        let mut client = server.connect(&server.token("sid_1")).await;
        client.hello("other", "inst_1", false, None).await;
        let ack = client.hello_ack().await.expect("hello ack");
        assert_eq!(ack.session_id, "sid_1");
        let health = server.state.health(true);
        assert_eq!(
            health.session.expect("session").session_id,
            "sid_1".to_owned()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn activity_excludes_pings_heartbeats_and_replay() {
        let server = TestServer::start().await;
        let (mut client, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");

        client
            .send_envelope(&proto::Ping {}.into_envelope(1, None, None))
            .await;
        server.hooks.next_from_client().await.expect("ping");
        client.send_text("{\"type\":\"heartbeat\"}").await;
        assert!(server.state.last_input_at().is_none());

        client
            .send_envelope(
                &proto::AddWorktree {
                    project_id: 0,
                    path: "/tmp".into(),
                    visible: true,
                }
                .into_envelope(2, None, None),
            )
            .await;
        server.hooks.next_from_client().await.expect("add worktree");
        let after_input = server.state.last_input_at().expect("input recorded");

        drop(client);
        wait_for_detach(&server).await;
        let mut client = server.connect(&server.token("sid_2")).await;
        client.hello("sid_2", "inst_1", true, Some(ack.epoch)).await;
        client.hello_ack().await.expect("hello ack");
        client
            .send_envelope(
                &proto::AddWorktree {
                    project_id: 0,
                    path: "/tmp".into(),
                    visible: true,
                }
                .into_envelope(2, None, None),
            )
            .await;
        server.hooks.next_from_client().await.expect("replayed");
        assert_eq!(server.state.last_input_at(), Some(after_input));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_heartbeat_every_5s() {
        let server = TestServer::start().await;
        let (mut client, _) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");
        let mut arrivals = Vec::new();
        while arrivals.len() < 2 {
            match tokio::time::timeout(HEARTBEAT_INTERVAL * 2, client.next_control(false))
                .await
                .expect("heartbeat within two intervals")
            {
                Some(ControlFrame::Heartbeat) => arrivals.push(std::time::Instant::now()),
                Some(_) => continue,
                None => panic!("socket closed"),
            }
        }
        let gap = arrivals[1].duration_since(arrivals[0]);
        assert!(
            gap >= HEARTBEAT_INTERVAL - Duration::from_millis(500)
                && gap <= HEARTBEAT_INTERVAL + Duration::from_secs(2),
            "heartbeat gap {gap:?}"
        );

        // None after detach: the close is the last frame on the socket.
        server
            .broker_tx
            .send(BrokerCommand::CloseSession {
                code: 1000,
                reason: "done",
            })
            .expect("close command");
        let (code, _) = client.wait_for_close().await.expect("close");
        assert_eq!(code, 1000);
        assert!(client.next_frame().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn log_frames_mirror_warnings() {
        let server = TestServer::start().await;
        // Records logged with no session attached go nowhere and never block.
        mirror(&server, log::Level::Error, "before attach");
        let (mut client, _) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");

        mirror(&server, log::Level::Warn, "careful");
        mirror(&server, log::Level::Info, "chatty");
        mirror(&server, log::Level::Error, "broken");
        match client.next_control(true).await.expect("log frame") {
            ControlFrame::Log(frame) => {
                assert_eq!(frame.level, 2);
                assert_eq!(frame.message, "careful");
                assert_eq!(frame.module_path.as_deref(), Some("test"));
            }
            other => panic!("expected a log frame, got {other:?}"),
        }
        match client.next_control(true).await.expect("second log frame") {
            ControlFrame::Log(frame) => {
                assert_eq!(frame.level, 1);
                assert_eq!(frame.message, "broken", "info records are not mirrored");
            }
            other => panic!("expected a log frame, got {other:?}"),
        }

        // With the queue full (the client stopped reading behind 64 MiB of output) mirroring
        // drops the record without blocking.
        for id in 1..=256u32 {
            server
                .hooks
                .send_to_client(big_envelope(id, 256 * 1024))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        for _ in 0..200 {
            mirror(&server, log::Level::Warn, "dropped");
        }
        assert!(started.elapsed() < Duration::from_millis(500));
        drop(client);
        wait_for_detach(&server).await;
        mirror(&server, log::Level::Warn, "after detach");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn detach_hook_runs_on_peer_close() {
        let server = TestServer::start().await;
        let (mut client, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");
        drop(client);

        for _ in 0..100 {
            if detach_count(&server, ack.epoch) == 1 {
                assert!(!server.state.health(false).session_active);
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "session_detached was never called: {:?}",
            server.hooks.events()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn close_session_delivers_queued_envelopes_first() {
        let server = TestServer::start().await;
        let (mut client, _) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");

        server
            .hooks
            .send_to_client(envelope(5, proto::envelope::Payload::Ack(proto::Ack {})))
            .await;
        server
            .broker_tx
            .send(BrokerCommand::CloseSession {
                code: 1000,
                reason: "client requested shutdown",
            })
            .expect("close command");

        let delivered = client.next_envelope().await.expect("ack before close");
        assert_eq!(delivered.id, 5);
        let (code, _) = client.wait_for_close().await.expect("close");
        assert_eq!(code, 1000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_flushes_then_closes_going_away() {
        let server = TestServer::start().await;
        let (mut client, _) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");
        for id in 20..22 {
            server
                .hooks
                .send_to_client(envelope(id, proto::envelope::Payload::Ping(proto::Ping {})))
                .await;
        }

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        server
            .broker_tx
            .send(BrokerCommand::Shutdown { done: done_tx })
            .expect("shutdown command");
        assert_eq!(client.next_envelope().await.expect("first").id, 20);
        assert_eq!(client.next_envelope().await.expect("second").id, 21);
        let (code, _) = client.wait_for_close().await.expect("close");
        assert_eq!(code, CLOSE_STOPPING);
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("shutdown completes")
            .expect("done");
        assert_eq!(server.hooks.quit_calls(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_without_session_is_immediate() {
        let server = TestServer::start().await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        server
            .broker_tx
            .send(BrokerCommand::Shutdown { done: done_tx })
            .expect("shutdown command");
        tokio::time::timeout(Duration::from_millis(500), done_rx)
            .await
            .expect("shutdown completes promptly")
            .expect("done");
        assert_eq!(server.hooks.quit_calls(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversize_incoming_frame_detaches_only_that_session() {
        let server = TestServer::start().await;
        let (mut client, _) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");
        // The server may reset the connection mid-write, so the send itself can fail; either
        // way the socket ends and the session detaches.
        client
            .send_binary(vec![0u8; MAX_FRAME_BYTES + 1])
            .await
            .ok();
        drop(client);
        let mut detached = false;
        for _ in 0..50 {
            if !server.state.health(false).session_active {
                detached = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(detached, "the oversize frame must detach the session");

        let outcome = crate::serve::test_support::http_request(
            server.addr,
            crate::serve::test_support::get("/health"),
        )
        .await;
        assert_eq!(outcome.status, hyper::StatusCode::OK);

        let (mut client, ack) = attach_fresh(&server, "sid_2", "inst_2").await;
        assert!(!ack.resumed);
        assert_eq!(client.next_envelope().await.expect("RemoteStarted").id, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversize_outgoing_frame_is_dropped_and_replaced() {
        let server = TestServer::start().await;
        let (mut client, ack) = attach_fresh(&server, "sid_1", "inst_1").await;
        client.next_envelope().await.expect("RemoteStarted");

        // A notification over the ceiling is dropped and removed from the replay buffer; the
        // session stays attached and the next envelope arrives normally.
        let huge = big_envelope(99, MAX_FRAME_BYTES + 1);
        assert!(encode_envelope_frame(&huge).len() > MAX_FRAME_BYTES);
        server.hooks.send_to_client(huge).await;
        server
            .hooks
            .send_to_client(envelope(
                100,
                proto::envelope::Payload::Ping(proto::Ping {}),
            ))
            .await;
        assert_eq!(client.next_envelope().await.expect("next").id, 100);
        assert!(server.hooks.events().contains(&HookEvent::Replaced {
            id: 99,
            with_error: false
        }));

        // A response over the ceiling becomes an Error response with the same id, so the
        // client's request fails instead of hanging.
        client
            .send_envelope(&proto::Ping {}.into_envelope(7, None, None))
            .await;
        server.hooks.next_from_client().await.expect("ping");
        let mut huge_response = big_envelope(101, MAX_FRAME_BYTES + 1);
        huge_response.responding_to = Some(7);
        server.hooks.send_to_client(huge_response).await;
        let replaced = client.next_envelope().await.expect("error response");
        assert_eq!(replaced.id, 101);
        assert_eq!(replaced.responding_to, Some(7));
        assert!(matches!(
            replaced.payload,
            Some(proto::envelope::Payload::Error(_))
        ));
        assert!(server.hooks.events().contains(&HookEvent::Replaced {
            id: 101,
            with_error: true
        }));
        assert!(server.state.health(false).session_active);

        // A reconnect resumes: nothing was closed.
        drop(client);
        wait_for_detach(&server).await;
        let mut client = server.connect(&server.token("sid_2")).await;
        client.hello("sid_2", "inst_1", true, Some(ack.epoch)).await;
        assert!(client.hello_ack().await.expect("hello ack").resumed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upgrade_never_completing_is_dropped() {
        let server = TestServer::start().await;
        let token = server.token("sid_1");
        let stream = tokio::net::TcpStream::connect(server.addr)
            .await
            .expect("connect");
        let request = format!(
            "GET /rpc HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: zs.v1, {token}\r\n\r\n"
        );
        let mut stream = stream;
        tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
            .await
            .expect("write request");
        let mut buffer = [0u8; 256];
        let read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer)
            .await
            .expect("read status line");
        assert!(
            String::from_utf8_lossy(&buffer[..read]).starts_with("HTTP/1.1 101"),
            "{}",
            String::from_utf8_lossy(&buffer[..read])
        );
        assert!(!server.state.health(false).session_active);
        assert_eq!(server.state.pending_connections(), 1);

        // The peer never speaks: the per-connection task gives up on its own.
        let deadline = std::time::Instant::now() + UPGRADE_TIMEOUT + Duration::from_secs(2);
        while server.state.pending_connections() != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the connection task must end without a Hello"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!server.state.health(false).session_active);
        drop(stream);
    }
}
