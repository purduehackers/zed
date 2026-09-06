//! In-process harness for the `serve` unit tests: a bound public listener, a bound control
//! listener, a running [`SessionBroker`] with recording hooks, a plain HTTP client and a
//! WebSocket client that speaks the `zs.v1` handshake.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::{
    SinkExt as _, StreamExt as _,
    channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded},
    future::BoxFuture,
};
use http_body_util::{BodyExt as _, Full};
use hyper::{HeaderMap, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use remote::{
    ChannelEnds,
    protocol::{decode_envelope_frame, encode_envelope_frame},
    websocket_wire::{
        ClientKind, ControlFrame, Hello, HelloAck, MAX_FRAME_BYTES, PROTOCOL_VERSION, SUBPROTOCOL,
    },
};
use rpc::proto::{self, Envelope, EnvelopedMessage as _};
use tokio::{net::TcpListener, task::JoinHandle};
use yawc::{
    Options, WebSocket,
    frame::{Frame, OpCode},
};

use crate::serve::{
    GpuiCommand,
    auth::test_support as auth_test,
    http::{
        ControlRequest, ControlResponse, ControlRoutes, ServeConfig, ServeState, serve_control,
        serve_http,
    },
    session::{BrokerCommand, ServeHooks, SessionBroker, SessionMeta},
};

/// What the recording hooks observed, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookEvent {
    /// `begin_fresh_session` was called.
    FreshSession,
    /// `session_attached` was called.
    Attached(SessionMeta),
    /// `session_detached` was called.
    Detached(SessionMeta),
    /// `replace_buffered` was called for envelope `id`; `with_error` says whether an `Error`
    /// response took its place.
    Replaced {
        /// The envelope id the broker refused to send.
        id: u32,
        /// Whether a synthesized error response replaced it.
        with_error: bool,
    },
    /// `request_quit` was called.
    Quit,
}

struct ClientEnds {
    outgoing_tx: UnboundedSender<Envelope>,
    incoming_rx: UnboundedReceiver<Envelope>,
}

/// `ServeHooks` stub: hands out fresh channel pairs with `RemoteStarted` pre-queued and
/// records every call.
pub struct RecordingHooks {
    state: Arc<HooksState>,
}

struct HooksState {
    ends: tokio::sync::Mutex<Option<ClientEnds>>,
    events: Mutex<Vec<HookEvent>>,
    fresh_calls: AtomicUsize,
    quit_calls: AtomicUsize,
    stale_on_reset: Mutex<Vec<Envelope>>,
    fail_next_fresh: AtomicBool,
}

impl RecordingHooks {
    fn new(ends: ClientEnds) -> Self {
        Self {
            state: Arc::new(HooksState {
                ends: tokio::sync::Mutex::new(Some(ends)),
                events: Mutex::new(Vec::new()),
                fresh_calls: AtomicUsize::new(0),
                quit_calls: AtomicUsize::new(0),
                stale_on_reset: Mutex::new(Vec::new()),
                fail_next_fresh: AtomicBool::new(false),
            }),
        }
    }

    /// Envelopes pushed onto the *old* channel just before a fresh reset swaps it out.
    pub fn emit_on_next_reset(&self, envelopes: Vec<Envelope>) {
        *self.state.stale_on_reset.lock().unwrap() = envelopes;
    }

    /// Makes the next `begin_fresh_session` fail, as if the gpui side were gone.
    pub fn fail_next_fresh(&self) {
        self.state.fail_next_fresh.store(true, SeqCst);
    }

    /// Everything the hooks saw so far.
    pub fn events(&self) -> Vec<HookEvent> {
        self.state.events.lock().unwrap().clone()
    }

    /// How many fresh sessions were started.
    pub fn fresh_calls(&self) -> usize {
        self.state.fresh_calls.load(SeqCst)
    }

    /// How many quit requests were made.
    pub fn quit_calls(&self) -> usize {
        self.state.quit_calls.load(SeqCst)
    }

    /// Queues an envelope for the attached client.
    pub async fn send_to_client(&self, envelope: Envelope) {
        let guard = self.state.ends.lock().await;
        let ends = guard.as_ref().expect("channel ends");
        ends.outgoing_tx
            .unbounded_send(envelope)
            .expect("queueing an envelope for the client");
    }

    /// The next envelope the client sent, or `None` if the deadline passes.
    pub async fn next_from_client(&self) -> Option<Envelope> {
        let mut guard = self.state.ends.lock().await;
        let ends = guard.as_mut().expect("channel ends");
        tokio::time::timeout(Duration::from_secs(5), ends.incoming_rx.next())
            .await
            .ok()
            .flatten()
    }
}

impl ServeHooks for RecordingHooks {
    fn replica_id(&self) -> u16 {
        8
    }
    fn begin_fresh_session(&self) -> BoxFuture<'static, anyhow::Result<ChannelEnds>> {
        let state = self.state.clone();
        Box::pin(async move {
            if state.fail_next_fresh.swap(false, SeqCst) {
                anyhow::bail!("gpui side is gone");
            }
            state.fresh_calls.fetch_add(1, SeqCst);
            state.events.lock().unwrap().push(HookEvent::FreshSession);
            let stale = std::mem::take(&mut *state.stale_on_reset.lock().unwrap());
            let mut guard = state.ends.lock().await;
            if let Some(previous) = guard.as_ref() {
                for envelope in stale {
                    previous.outgoing_tx.unbounded_send(envelope).ok();
                }
            }
            let (incoming_tx, incoming_rx) = unbounded();
            let (outgoing_tx, outgoing_rx) = unbounded();
            outgoing_tx
                .unbounded_send(proto::RemoteStarted {}.into_envelope(0, None, None))
                .expect("queueing RemoteStarted");
            *guard = Some(ClientEnds {
                outgoing_tx,
                incoming_rx,
            });
            Ok(ChannelEnds {
                incoming_tx,
                outgoing_rx,
            })
        })
    }

    fn replace_buffered(&self, id: u32, replacement: Option<Envelope>) {
        self.state.events.lock().unwrap().push(HookEvent::Replaced {
            id,
            with_error: replacement.is_some(),
        });
    }

    fn session_attached(&self, meta: &SessionMeta) {
        self.state
            .events
            .lock()
            .unwrap()
            .push(HookEvent::Attached(meta.clone()));
    }

    fn session_detached(&self, meta: &SessionMeta) {
        self.state
            .events
            .lock()
            .unwrap()
            .push(HookEvent::Detached(meta.clone()));
    }

    fn request_quit(&self) {
        self.state.quit_calls.fetch_add(1, SeqCst);
        self.state.events.lock().unwrap().push(HookEvent::Quit);
    }
}

/// Records the control requests it saw and answers a canned response.
pub struct RecordingControl {
    secret: Vec<u8>,
    seen: Mutex<Vec<(String, String, bool, bool, usize)>>,
}

impl RecordingControl {
    /// Routes gated by `secret`.
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.as_bytes().to_vec(),
            seen: Mutex::new(Vec::new()),
        }
    }

    /// `(method, path, peer_is_loopback, session_attached, body_len)` per request.
    pub fn seen(&self) -> Vec<(String, String, bool, bool, usize)> {
        self.seen.lock().unwrap().clone()
    }
}

impl ControlRoutes for RecordingControl {
    fn handle<'a>(&'a self, req: ControlRequest<'a>) -> BoxFuture<'a, ControlResponse> {
        let entry = (
            req.method.to_owned(),
            req.path.to_owned(),
            req.peer_is_loopback,
            req.session_attached,
            req.body.len(),
        );
        let authorized = req
            .bearer
            .is_some_and(|bearer| bearer.as_bytes() == self.secret);
        Box::pin(async move {
            self.seen.lock().unwrap().push(entry);
            if authorized {
                ControlResponse::NoContent
            } else {
                ControlResponse::Unauthorized
            }
        })
    }
}

/// A running serve instance on ephemeral ports.
pub struct TestServer {
    /// Shared state (health, auth throttle, session view).
    pub state: Arc<ServeState>,
    /// Address of the public listener.
    pub addr: SocketAddr,
    /// Address of the control listener.
    pub control_addr: SocketAddr,
    /// The recording hooks the broker calls.
    pub hooks: Arc<RecordingHooks>,
    /// The recording control routes.
    pub control: Arc<RecordingControl>,
    /// Commands the tests send to the broker directly.
    pub broker_tx: tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
    /// Commands the routes send to the (absent) gpui side; kept alive so `/files` and
    /// `/extensions/*` sends do not fail with a closed channel.
    pub gpui_rx: tokio::sync::Mutex<UnboundedReceiver<GpuiCommand>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// The control secret the harness installs.
pub const CONTROL_SECRET: &str = "test-control-secret";

impl TestServer {
    /// Starts a server with no allowed origins and no `--client-build`.
    pub async fn start() -> Self {
        Self::start_with(Vec::new(), None).await
    }

    /// Starts a server with the given allowed origins and expected client build.
    pub async fn start_with(allowed_origins: Vec<String>, client_build: Option<String>) -> Self {
        let workspace_root = std::env::temp_dir();
        let (broker_tx, broker_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gpui_tx, gpui_rx) = unbounded();
        let state = Arc::new(ServeState::new(
            ServeConfig {
                build: "test-build".into(),
                version: "test-version".into(),
                workspace_id: auth_test::WORKSPACE.into(),
                workspace_root: workspace_root.canonicalize().expect("temp dir"),
                auth: auth_test::config(),
                allowed_origins,
                client_build,
            },
            broker_tx.clone(),
            gpui_tx,
        ));

        let (incoming_tx, incoming_rx) = unbounded();
        let (outgoing_tx, outgoing_rx) = unbounded();
        let hooks = Arc::new(RecordingHooks::new(ClientEnds {
            outgoing_tx,
            incoming_rx,
        }));
        let control = Arc::new(RecordingControl::new(CONTROL_SECRET));

        let public = TcpListener::bind("127.0.0.1:0").await.expect("public bind");
        let control_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("control bind");
        let addr = public.local_addr().expect("public addr");
        let control_addr = control_listener.local_addr().expect("control addr");

        let broker = SessionBroker::new(
            incoming_tx,
            outgoing_rx,
            broker_rx,
            state.clone(),
            hooks.clone(),
        );
        let tasks = vec![
            tokio::spawn({
                let state = state.clone();
                async move {
                    serve_http(public, state).await.ok();
                }
            }),
            tokio::spawn({
                let state = state.clone();
                let control = control.clone();
                async move {
                    serve_control(control_listener, state, control).await.ok();
                }
            }),
            tokio::spawn(broker.run()),
        ];

        Self {
            state,
            addr,
            control_addr,
            hooks,
            control,
            broker_tx,
            gpui_rx: tokio::sync::Mutex::new(gpui_rx),
            tasks,
        }
    }

    /// A valid token for this server with the given session id.
    pub fn token(&self, sid: &str) -> String {
        auth_test::token(sid)
    }

    /// The next command the routes sent to the gpui side, or `None` on timeout.
    pub async fn next_gpui_command(&self) -> Option<GpuiCommand> {
        let mut guard = self.gpui_rx.lock().await;
        tokio::time::timeout(Duration::from_secs(5), guard.next())
            .await
            .ok()
            .flatten()
    }

    /// Opens a WebSocket to `/rpc` with `zs.v1, <token>` in `Sec-WebSocket-Protocol`.
    pub async fn connect(&self, token: &str) -> TestClient {
        TestClient::connect(self.addr, token, &[])
            .await
            .expect("upgrade")
    }
}

/// One HTTP response, fully read.
pub struct HttpOutcome {
    /// Response status.
    pub status: StatusCode,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response body.
    pub body: Bytes,
}

impl HttpOutcome {
    /// The body parsed as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body).expect("JSON body")
    }

    /// A header value as a string.
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }
}

/// Sends one HTTP/1.1 request over a fresh connection to `addr`.
pub async fn http_request(addr: SocketAddr, request: Request<Full<Bytes>>) -> HttpOutcome {
    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("handshake");
    let driver = tokio::spawn(async move {
        connection.await.ok();
    });
    let response: Response<hyper::body::Incoming> =
        sender.send_request(request).await.expect("send request");
    let (parts, body) = response.into_parts();
    let body = body.collect().await.expect("read body").to_bytes();
    driver.abort();
    HttpOutcome {
        status: parts.status,
        headers: parts.headers,
        body,
    }
}

/// A `GET` with no body.
pub fn get(target: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("GET")
        .uri(target)
        .header(header::HOST, "127.0.0.1")
        .body(Full::new(Bytes::new()))
        .expect("request")
}

/// A `POST` with a JSON body.
pub fn post_json(target: &str, body: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri(target)
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_owned())))
        .expect("request")
}

/// The `zs.v1` client side of a session.
pub struct TestClient {
    ws: WebSocket<yawc::MaybeTlsStream<tokio::net::TcpStream>>,
}

impl TestClient {
    /// Dials `/rpc` with the token in the subprotocol list plus any extra headers.
    pub async fn connect(
        addr: SocketAddr,
        token: &str,
        extra_headers: &[(&str, &str)],
    ) -> yawc::Result<Self> {
        let url: url::Url = format!("ws://{addr}/rpc").parse().expect("url");
        let mut builder = yawc::HttpRequestBuilder::new()
            .header("sec-websocket-protocol", format!("{SUBPROTOCOL}, {token}"));
        for (name, value) in extra_headers {
            builder = builder.header(*name, *value);
        }
        let ws = WebSocket::connect(url)
            .with_options(Options::default().without_compression())
            .with_request(builder)
            .await?;
        Ok(Self { ws })
    }

    /// Sends a `Hello` with the given knobs.
    pub async fn hello(
        &mut self,
        session_id: &str,
        instance: &str,
        reconnect: bool,
        epoch: Option<u64>,
    ) {
        self.send_hello(Hello {
            protocol: PROTOCOL_VERSION,
            build: "test-build".into(),
            workspace_id: auth_test::WORKSPACE.into(),
            session_id: session_id.into(),
            identifier: format!("setup-{instance}"),
            instance: instance.into(),
            reconnect,
            client: ClientKind::Web,
            epoch,
        })
        .await;
    }

    /// Sends an arbitrary `Hello`.
    pub async fn send_hello(&mut self, hello: Hello) {
        let json = serde_json::to_string(&ControlFrame::Hello(hello)).expect("hello json");
        self.ws.send(Frame::text(json)).await.expect("send hello");
    }

    /// Sends a raw text frame.
    pub async fn send_text(&mut self, text: &str) {
        self.ws
            .send(Frame::text(text.to_owned()))
            .await
            .expect("send text");
    }

    /// Sends one envelope as a binary frame.
    pub async fn send_envelope(&mut self, envelope: &Envelope) {
        self.ws
            .send(Frame::binary(encode_envelope_frame(envelope)))
            .await
            .expect("send envelope");
    }

    /// Sends a raw binary frame; the error is returned because an oversize frame may make the
    /// server reset the connection before the write completes.
    pub async fn send_binary(&mut self, bytes: Vec<u8>) -> yawc::Result<()> {
        self.ws.send(Frame::binary(bytes)).await
    }

    /// The next frame, or `None` on timeout / socket end. The budget is generous because the
    /// server's own timers (`HELLO_TIMEOUT`, `HEARTBEAT_INTERVAL`) are seconds apart.
    pub async fn next_frame(&mut self) -> Option<Frame> {
        tokio::time::timeout(Duration::from_secs(20), self.ws.next_frame())
            .await
            .ok()
            .and_then(Result::ok)
    }

    /// The next frame that is not a ping/pong.
    pub async fn next_interesting_frame(&mut self) -> Option<Frame> {
        loop {
            let frame = self.next_frame().await?;
            if matches!(frame.opcode(), OpCode::Ping | OpCode::Pong) {
                continue;
            }
            return Some(frame);
        }
    }

    /// The next control (text) frame, skipping heartbeats when `skip_heartbeats`.
    pub async fn next_control(&mut self, skip_heartbeats: bool) -> Option<ControlFrame> {
        loop {
            let frame = self.next_interesting_frame().await?;
            match frame.opcode() {
                OpCode::Text => {
                    let control: ControlFrame =
                        serde_json::from_slice(frame.payload()).expect("control frame");
                    if skip_heartbeats && control == ControlFrame::Heartbeat {
                        continue;
                    }
                    return Some(control);
                }
                OpCode::Close => return None,
                _ => continue,
            }
        }
    }

    /// The `HelloAck` the server answers with, or `None` if it closed instead.
    pub async fn hello_ack(&mut self) -> Option<HelloAck> {
        match self.next_control(true).await? {
            ControlFrame::HelloAck(ack) => Some(ack),
            other => panic!("expected HelloAck, got {other:?}"),
        }
    }

    /// The next binary frame decoded as an envelope, skipping control frames.
    pub async fn next_envelope(&mut self) -> Option<Envelope> {
        loop {
            let frame = self.next_interesting_frame().await?;
            match frame.opcode() {
                OpCode::Binary => {
                    return Some(
                        decode_envelope_frame(frame.payload(), MAX_FRAME_BYTES)
                            .expect("decode envelope"),
                    );
                }
                OpCode::Close => return None,
                _ => continue,
            }
        }
    }

    /// Waits for the socket to close and returns the close code and reason.
    pub async fn wait_for_close(&mut self) -> Option<(u16, String)> {
        loop {
            let frame = self.next_frame().await?;
            if frame.opcode() == OpCode::Close {
                let code = frame.close_code().map(u16::from).unwrap_or(0);
                let reason = frame
                    .close_reason()
                    .ok()
                    .flatten()
                    .unwrap_or_default()
                    .to_owned();
                return Some((code, reason));
            }
        }
    }
}
