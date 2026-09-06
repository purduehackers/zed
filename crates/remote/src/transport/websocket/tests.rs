//! Loopback tests for the WebSocket transport against an in-process fake `serve`.
//!
//! The fake server speaks just enough of the `serve` contract (the upgrade with the
//! subprotocol echo, `Hello`/`HelloAck`, `Ack`s for `Ping`/`RemoteStarted`/
//! `FlushBufferedMessages`, close frames) for the client state machine to be driven end to
//! end. Parking is allowed so real sockets work under the deterministic dispatcher; the fake
//! clock advances with real time while the test parks, so every wait here is short.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::anyhow;
use async_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
        http::HeaderValue,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
use futures::{
    FutureExt as _, SinkExt as _, StreamExt as _,
    channel::{mpsc, oneshot},
    select_biased,
};
use gpui::{Entity, Subscription, Task, TestAppContext};
use parking_lot::Mutex;
use rpc::proto::{self, Envelope, EnvelopedMessage as _, envelope::Payload};
use smol::net::{TcpListener, TcpStream};

use super::*;
use crate::{
    ConnectionIdentifier, ConnectionState, RemoteClient, RemoteConnectionOptions, connect,
};

fn init_test(cx: &mut TestAppContext) {
    cx.executor().allow_parking();
    cx.update(|cx| {
        release_channel::init(semver::Version::new(0, 0, 0), cx);
        gpui_tokio::init(cx);
    });
}

fn hello_ack(resumed: bool, epoch: u64) -> HelloAck {
    HelloAck {
        replica_id: 8,
        protocol: PROTOCOL_VERSION,
        build: "dev-fake-server".into(),
        os: "linux".into(),
        arch: "x86_64".into(),
        os_version: Some("ubuntu 24.04".into()),
        shell: "/bin/bash".into(),
        resumed,
        session_id: String::new(),
        epoch,
    }
}

fn binary_message(envelope: &Envelope) -> Message {
    Message::Binary(encode_envelope_frame(envelope).into())
}

fn remote_started() -> Envelope {
    proto::RemoteStarted {}.into_envelope(0, None, None)
}

/// An in-process stand-in for `zed-remote-server serve`.
struct FakeServer {
    url: String,
    connections: mpsc::UnboundedReceiver<FakeConnection>,
    _accept_task: Task<()>,
}

impl FakeServer {
    // The upgrade callback's error type is tungstenite's full HTTP response.
    #[allow(clippy::result_large_err)]
    async fn start(cx: &TestAppContext) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (connections_tx, connections) = mpsc::unbounded();
        let executor = cx.executor();
        let accept_task = executor.spawn({
            let executor = executor.clone();
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let request_info = Arc::new(Mutex::new(None));
                    let websocket = async_tungstenite::accept_hdr_async(stream, {
                        let request_info = request_info.clone();
                        move |request: &Request, mut response: Response| {
                            let subprotocol = request
                                .headers()
                                .get("sec-websocket-protocol")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned);
                            *request_info.lock() = Some((request.uri().to_string(), subprotocol));
                            response.headers_mut().insert(
                                "sec-websocket-protocol",
                                HeaderValue::from_static("zs.v1"),
                            );
                            Ok(response)
                        }
                    })
                    .await;
                    let Ok(websocket) = websocket else {
                        continue;
                    };
                    let Some((uri, subprotocol_header)) = request_info.lock().take() else {
                        continue;
                    };
                    let connection =
                        FakeConnection::new(websocket, uri, subprotocol_header, &executor);
                    if connections_tx.unbounded_send(connection).is_err() {
                        break;
                    }
                }
            }
        });
        Self {
            url: format!("ws://{address}/rpc"),
            connections,
            _accept_task: accept_task,
        }
    }

    async fn next_connection(&mut self) -> FakeConnection {
        self.connections
            .next()
            .await
            .expect("fake server stopped accepting")
    }
}

/// One accepted socket. Dropping it drops the TCP connection without a close frame.
struct FakeConnection {
    uri: String,
    subprotocol_header: Option<String>,
    inbound: mpsc::UnboundedReceiver<Message>,
    outbound: mpsc::UnboundedSender<Message>,
    _task: Task<()>,
}

impl FakeConnection {
    fn new(
        websocket: WebSocketStream<TcpStream>,
        uri: String,
        subprotocol_header: Option<String>,
        executor: &gpui::BackgroundExecutor,
    ) -> Self {
        let (mut sink, stream) = websocket.split();
        let mut stream = stream.fuse();
        let (inbound_tx, inbound) = mpsc::unbounded();
        let (outbound, mut outbound_rx) = mpsc::unbounded::<Message>();
        let task = executor.spawn(async move {
            let mut next_id = 1000;
            loop {
                select_biased! {
                    message = outbound_rx.next() => {
                        let Some(message) = message else { break };
                        if sink.send(message).await.is_err() {
                            break;
                        }
                    }
                    message = stream.next() => {
                        let Some(Ok(message)) = message else { break };
                        if let Some(ack) = auto_ack(&message, &mut next_id)
                            && sink.send(ack).await.is_err()
                        {
                            break;
                        }
                        if inbound_tx.unbounded_send(message).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            uri,
            subprotocol_header,
            inbound,
            outbound,
            _task: task,
        }
    }

    async fn next_message(&mut self) -> Option<Message> {
        self.inbound.next().await
    }

    /// Drains whatever is still in flight and resolves once the client has closed the socket.
    /// Panics (rather than hanging until the scheduler's park timeout) if it stays open.
    async fn wait_until_closed(&mut self, cx: &TestAppContext, label: &str) {
        loop {
            let message = self.inbound.next().fuse();
            let deadline = cx.executor().timer(Duration::from_secs(5)).fuse();
            futures::pin_mut!(message, deadline);
            select_biased! {
                message = message => if message.is_none() {
                    return;
                },
                _ = deadline => panic!("{label}: the client did not close the socket"),
            }
        }
    }

    async fn expect_hello(&mut self) -> Hello {
        match self
            .next_message()
            .await
            .expect("connection closed before Hello")
        {
            Message::Text(text) => match serde_json::from_str::<ControlFrame>(text.as_str()) {
                Ok(ControlFrame::Hello(hello)) => hello,
                other => panic!("expected Hello, got {other:?}"),
            },
            other => panic!("expected a text Hello frame, got {other:?}"),
        }
    }

    async fn next_envelope_where(&mut self, predicate: impl Fn(&Envelope) -> bool) -> Envelope {
        loop {
            let message = self.next_message().await.expect("connection closed");
            if let Message::Binary(bytes) = message
                && let Ok(envelope) = decode_envelope_frame(&bytes, MAX_FRAME_BYTES)
                && predicate(&envelope)
            {
                return envelope;
            }
        }
    }

    fn send(&self, message: Message) {
        self.outbound.unbounded_send(message).ok();
    }

    fn send_control(&self, frame: &ControlFrame) {
        self.send(Message::text(serde_json::to_string(frame).unwrap()));
    }

    fn send_hello_ack(&self, ack: HelloAck) {
        self.send_control(&ControlFrame::HelloAck(ack));
    }

    fn send_envelope(&self, envelope: &Envelope) {
        self.send(binary_message(envelope));
    }

    fn close(&self, code: u16, reason: &str) {
        self.send(Message::Close(Some(CloseFrame {
            code: CloseCode::from(code),
            reason: reason.into(),
        })));
    }
}

fn auto_ack(message: &Message, next_id: &mut u32) -> Option<Message> {
    let Message::Binary(bytes) = message else {
        return None;
    };
    let envelope = decode_envelope_frame(bytes, MAX_FRAME_BYTES).ok()?;
    if envelope.responding_to.is_some() {
        return None;
    }
    match envelope.payload {
        Some(Payload::Ping(_))
        | Some(Payload::RemoteStarted(_))
        | Some(Payload::FlushBufferedMessages(_)) => {
            let mut ack = proto::Ack {}.into_envelope(0, Some(envelope.id), None);
            ack.id = *next_id;
            *next_id += 1;
            Some(binary_message(&ack))
        }
        _ => None,
    }
}

/// A scripted `WebSocketSessionRefresh`.
#[derive(Default)]
struct StubRefresh {
    calls: Mutex<Vec<(String, RefreshReason)>>,
    responses: Mutex<VecDeque<Result<WebSocketSession, RefreshError>>>,
}

impl StubRefresh {
    fn with_responses(
        responses: impl IntoIterator<Item = Result<WebSocketSession, RefreshError>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::default(),
            responses: Mutex::new(responses.into_iter().collect()),
        })
    }

    fn session(url: &str, token: &str, session_id: &str) -> Result<WebSocketSession, RefreshError> {
        Ok(WebSocketSession {
            url: url.to_owned(),
            token: token.to_owned(),
            session_id: session_id.to_owned(),
        })
    }

    fn calls(&self) -> Vec<(String, RefreshReason)> {
        self.calls.lock().clone()
    }
}

impl WebSocketSessionRefresh for StubRefresh {
    fn refresh(
        &self,
        workspace_id: &str,
        reason: RefreshReason,
        _cx: &mut AsyncApp,
    ) -> Task<Result<WebSocketSession, RefreshError>> {
        self.calls.lock().push((workspace_id.to_owned(), reason));
        let response = self
            .responses
            .lock()
            .pop_front()
            .unwrap_or_else(|| Err(RefreshError::Other(anyhow!("no scripted session left"))));
        Task::ready(response)
    }
}

fn recording_delegate() -> (
    Arc<WebSocketClientDelegate>,
    Arc<Mutex<Vec<Option<String>>>>,
) {
    let statuses = Arc::new(Mutex::new(Vec::new()));
    let delegate = Arc::new(WebSocketClientDelegate::new({
        let statuses = statuses.clone();
        move |status, _| statuses.lock().push(status.map(str::to_owned))
    }));
    (delegate, statuses)
}

/// A `RemoteClient` attached to a fake server, with its delegate statuses and events.
struct TestClient {
    client: Entity<RemoteClient>,
    first_connection: Arc<dyn RemoteConnection>,
    statuses: Arc<Mutex<Vec<Option<String>>>>,
    events: Arc<Mutex<Vec<String>>>,
    _subscription: Subscription,
    _cancel_tx: oneshot::Sender<()>,
}

impl TestClient {
    fn state(&self, cx: &TestAppContext) -> ConnectionState {
        cx.update(|cx| self.client.read(cx).connection_state())
    }

    fn websocket_options(&self, cx: &TestAppContext) -> WebSocketConnectionOptions {
        match cx.update(|cx| self.client.read(cx).connection_options()) {
            RemoteConnectionOptions::WebSocket(options) => options,
            other => panic!("unexpected options {other:?}"),
        }
    }

    fn live_connection(&self, cx: &TestAppContext) -> Arc<dyn RemoteConnection> {
        cx.update(|cx| self.client.read(cx).connection())
            .expect("client is not connected")
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().clone()
    }

    async fn wait_for_state(&self, cx: &TestAppContext, expected: ConnectionState) {
        wait_until(cx, || self.state(cx) == expected).await;
    }

    async fn wait_for_event(&self, cx: &TestAppContext, needle: &str) {
        wait_until(cx, || {
            self.events().iter().any(|event| event.contains(needle))
        })
        .await;
    }
}

async fn wait_until(cx: &TestAppContext, mut condition: impl FnMut() -> bool) {
    for _ in 0..400 {
        if condition() {
            return;
        }
        cx.executor().timer(Duration::from_millis(25)).await;
    }
    panic!("condition not reached in time");
}

/// Dials `server` with `options`, then completes the handshake as a fresh session and waits
/// for `RemoteClient::new` to resolve.
async fn open_session(
    cx: &mut TestAppContext,
    server: &mut FakeServer,
    options: WebSocketConnectionOptions,
) -> (TestClient, FakeConnection) {
    let (delegate, statuses) = recording_delegate();
    let mut async_cx = cx.to_async();
    let connection = connect(options.into(), delegate.clone(), &mut async_cx)
        .await
        .expect("connect failed");
    let mut fake_connection = server.next_connection().await;

    let (cancel_tx, cancel_rx) = oneshot::channel();
    let client_task = cx.update(|cx| {
        RemoteClient::new(
            ConnectionIdentifier::setup(),
            connection.clone(),
            cancel_rx,
            delegate,
            cx,
        )
    });
    let hello = fake_connection.expect_hello().await;
    assert!(!hello.reconnect);
    fake_connection.send_hello_ack(hello_ack(false, 1));
    fake_connection.send_envelope(&remote_started());
    let client = client_task
        .await
        .expect("RemoteClient::new failed")
        .expect("RemoteClient::new was cancelled");

    let events = Arc::new(Mutex::new(Vec::new()));
    let subscription = cx.update(|cx| {
        cx.subscribe(&client, {
            let events = events.clone();
            move |_, event, _| events.lock().push(format!("{event:?}"))
        })
    });
    let test_client = TestClient {
        client,
        first_connection: connection,
        statuses,
        events,
        _subscription: subscription,
        _cancel_tx: cancel_tx,
    };
    test_client
        .wait_for_state(cx, ConnectionState::Connected)
        .await;
    (test_client, fake_connection)
}

/// Channels for driving `start_proxy` directly. Held (not read) by the tests that only care
/// about the handshake: dropping them would end the pump early.
#[allow(dead_code)]
struct ProxyChannels {
    incoming_rx: mpsc::UnboundedReceiver<Envelope>,
    outgoing_tx: mpsc::UnboundedSender<Envelope>,
    activity_rx: mpsc::Receiver<()>,
}

fn start_proxy_directly(
    cx: &mut TestAppContext,
    connection: &WebSocketRemoteConnection,
    reconnect: bool,
) -> (Task<anyhow::Result<i32>>, ProxyChannels) {
    let (incoming_tx, incoming_rx) = mpsc::unbounded();
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
    let (activity_tx, activity_rx) = mpsc::channel(1);
    let task = connection.start_proxy(
        "setup-1".into(),
        reconnect,
        incoming_tx,
        outgoing_rx,
        activity_tx,
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    );
    (
        task,
        ProxyChannels {
            incoming_rx,
            outgoing_tx,
            activity_rx,
        },
    )
}

fn options_for(server: &FakeServer, session_id: &str, token: &str) -> WebSocketConnectionOptions {
    let options = WebSocketConnectionOptions::new(server.url.clone(), "ws_1", session_id, token);
    options.set_backoff_for_tests(Duration::ZERO);
    options
}

#[test]
fn options_identity_serialization_and_debug() {
    let a = WebSocketConnectionOptions::new("wss://a.example/rpc", "ws_1", "sess_1", "secret-a");
    let b = WebSocketConnectionOptions::new("wss://b.example/rpc", "ws_1", "sess_2", "secret-b");
    let c = WebSocketConnectionOptions::new("wss://a.example/rpc", "ws_2", "sess_1", "secret-a");
    assert_eq!(a, b);
    assert_ne!(a, c);
    let hash = |options: &WebSocketConnectionOptions| {
        use std::hash::{Hash as _, Hasher as _};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        options.hash(&mut hasher);
        hasher.finish()
    };
    assert_eq!(hash(&a), hash(&b));

    let json = serde_json::to_string(&a).unwrap();
    assert!(json.contains("wss://a.example/rpc"), "{json}");
    assert!(json.contains("ws_1"), "{json}");
    assert!(!json.contains("sess_1"), "{json}");
    assert!(!json.contains("secret-a"), "{json}");
    assert!(!json.contains("refresh"), "{json}");
    let restored: WebSocketConnectionOptions = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.workspace_id, "ws_1");
    assert!(restored.session_id.is_empty());
    assert!(restored.token.is_empty());
    assert!(restored.refresh.is_none());
    assert!(restored.state.is_none());

    let debug = format!("{a:?}");
    assert!(!debug.contains("secret-a"), "{debug}");
    assert!(debug.contains("<redacted>"), "{debug}");

    let session = WebSocketSession {
        url: "wss://a.example/rpc".into(),
        token: "secret-session".into(),
        session_id: "sess_9".into(),
    };
    let debug = format!("{session:?}");
    assert!(!debug.contains("secret-session"), "{debug}");
    assert!(debug.contains("<redacted>"), "{debug}");
    assert!(debug.contains("sess_9"), "{debug}");

    let clone = a.clone();
    let (Some(original_state), Some(cloned_state)) = (&a.state, &clone.state) else {
        panic!("options without state");
    };
    assert!(Arc::ptr_eq(original_state, cloned_state));
    assert_eq!(original_state.instance, cloned_state.instance);
    assert_ne!(
        a.state.as_ref().unwrap().instance,
        b.state.as_ref().unwrap().instance
    );

    assert_eq!(a.display_name(), "a.example");
    assert_eq!(
        WebSocketConnectionOptions::new("", "ws_9", "", "").display_name(),
        "ws_9"
    );
}

#[gpui::test]
async fn test_can_dial_needs_a_token_or_provider(cx: &mut TestAppContext) {
    let restored = WebSocketConnectionOptions::new("", "ws_1", "", "");
    assert!(!cx.update(|cx| restored.can_dial(cx)));
    assert!(cx.update(|cx| {
        WebSocketConnectionOptions::new("wss://a/rpc", "ws_1", "s", "t").can_dial(cx)
    }));
    assert!(cx.update(|cx| {
        restored
            .clone()
            .with_refresh(StubRefresh::with_responses([]))
            .can_dial(cx)
    }));
    cx.update(|cx| set_session_refresh_provider(cx, StubRefresh::with_responses([])));
    assert!(cx.update(|cx| restored.can_dial(cx)));
    assert!(cx.update(|cx| session_refresh_provider(cx).is_some()));
}

#[gpui::test]
async fn test_new_validates_before_dialing(cx: &mut TestAppContext) {
    init_test(cx);
    let delegate: Arc<dyn RemoteClientDelegate> = Arc::new(WebSocketClientDelegate::silent());
    let mut async_cx = cx.to_async();

    let http = WebSocketConnectionOptions::new("http://127.0.0.1:1/rpc", "ws_1", "s", "t");
    let error = WebSocketRemoteConnection::new(http, delegate.clone(), &mut async_cx)
        .await
        .err()
        .expect("http scheme must be rejected");
    assert!(error.to_string().contains("scheme"), "{error:#}");

    let plaintext = WebSocketConnectionOptions::new("ws://example.com/rpc", "ws_1", "s", "t");
    let error = WebSocketRemoteConnection::new(plaintext, delegate.clone(), &mut async_cx)
        .await
        .err()
        .expect("plaintext ws to a non-loopback host must be rejected");
    assert!(error.to_string().contains("wss"), "{error:#}");
    for loopback in [
        "ws://localhost:1/rpc",
        "ws://[::1]:1/rpc",
        "wss://example.com/rpc",
    ] {
        let options = WebSocketConnectionOptions::new(loopback, "ws_1", "s", "t");
        options.set_backoff_for_tests(Duration::ZERO);
        let error = WebSocketRemoteConnection::new(options, delegate.clone(), &mut async_cx)
            .await
            .err()
            .expect("nothing listens there");
        assert!(
            !error.to_string().contains("scheme"),
            "{loopback} must pass scheme validation: {error:#}"
        );
    }

    let no_workspace = WebSocketConnectionOptions::new("ws://127.0.0.1:1/rpc", "", "s", "t");
    let error = WebSocketRemoteConnection::new(no_workspace, delegate.clone(), &mut async_cx)
        .await
        .err()
        .expect("empty workspace_id must be rejected");
    assert!(error.to_string().contains("workspace_id"), "{error:#}");

    let no_token = WebSocketConnectionOptions::new("ws://127.0.0.1:1/rpc", "ws_1", "s", "");
    let error = WebSocketRemoteConnection::new(no_token, delegate, &mut async_cx)
        .await
        .err()
        .expect("empty token without refresh must be rejected");
    assert!(error.to_string().contains("token"), "{error:#}");
}

#[gpui::test]
async fn test_new_without_tokio_fails_cleanly(cx: &mut TestAppContext) {
    cx.executor().allow_parking();
    cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
    let options = WebSocketConnectionOptions::new("ws://127.0.0.1:1/rpc", "ws_1", "s", "t");
    let error = WebSocketRemoteConnection::new(
        options,
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .err()
    .expect("missing tokio runtime must be an error");
    assert!(error.to_string().contains("gpui_tokio"), "{error:#}");
}

#[gpui::test]
async fn test_connection_defaults_and_capabilities(cx: &mut TestAppContext) {
    init_test(cx);
    let server = FakeServer::start(cx).await;
    let options = options_for(&server, "sess_1", "token-1");
    let connection = WebSocketRemoteConnection::new(
        options,
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .unwrap();

    assert!(matches!(connection.remote_platform().os, RemoteOs::Linux));
    assert!(matches!(
        connection.remote_platform().arch,
        RemoteArch::X86_64
    ));
    assert_eq!(connection.path_style(), PathStyle::Unix);
    assert_eq!(connection.shell(), "/bin/sh");
    assert_eq!(connection.default_system_shell(), "/bin/sh");
    assert_eq!(connection.remote_os_version(), None);
    assert!(connection.server_info().is_none());

    let mut ack = hello_ack(true, 7);
    ack.os = "linux".into();
    ack.arch = "aarch64".into();
    ack.shell = "/usr/bin/zsh".into();
    connection.set_server_info_for_tests(&ack);
    assert!(matches!(
        connection.remote_platform().arch,
        RemoteArch::Aarch64
    ));
    assert_eq!(connection.shell(), "/usr/bin/zsh");
    assert_eq!(
        connection.remote_os_version().as_deref(),
        Some("ubuntu 24.04")
    );
    let info = connection.server_info().unwrap();
    assert_eq!(info.epoch, 7);
    assert!(info.resumed);
    assert_eq!(info.build, "dev-fake-server");

    assert!(
        connection
            .build_command(None, &[], &HashMap::default(), None, None, Interactive::No)
            .is_err()
    );
    assert!(connection.build_forward_ports_command(vec![]).is_err());
    let upload = cx.update(|cx| {
        connection.upload_directory(
            PathBuf::from("/tmp/x"),
            RemotePathBuf::new("/tmp/y".into(), PathStyle::Unix),
            cx,
        )
    });
    assert!(upload.await.is_err());
    assert!(!connection.has_wsl_interop());
    assert!(connection.supports_remote_pty());
    assert!(!connection.supports_extension_upload());
    assert!(!connection.shares_network_interface());
    assert_eq!(
        connection.max_reconnect_attempts(),
        WS_MAX_RECONNECT_ATTEMPTS
    );
    assert!(!connection.has_been_killed());
    connection.state.terminal.store(true, SeqCst);
    assert_eq!(connection.max_reconnect_attempts(), 0);
    connection.kill().await.unwrap();
    assert!(connection.has_been_killed());
}

#[test]
fn close_code_mapping() {
    let close = |code: u16| CloseInfo {
        code,
        reason: String::new(),
    };
    assert_eq!(exit_code_for_close(&close(1001)).unwrap(), 90);
    assert_eq!(exit_code_for_close(&close(4001)).unwrap(), 91);
    assert_eq!(exit_code_for_close(&close(4002)).unwrap(), 92);
    assert_eq!(exit_code_for_close(&close(4006)).unwrap(), 92);
    for code in [1000, 1006, 1008, 1009, 4003, 4004, 4005, 4999] {
        assert!(exit_code_for_close(&close(code)).is_err(), "{code}");
    }
    // D23 folds the stale-epoch refusal into 4001; only the reason tells it apart, and it
    // means "open a fresh session", not "taken over".
    let stale_epoch = CloseInfo {
        code: 4001,
        reason: CLOSE_REASON_STALE_EPOCH.into(),
    };
    assert_eq!(exit_code_for_close(&stale_epoch).unwrap(), 90);
    let taken_over = CloseInfo {
        code: 4001,
        reason: "taken over by another session".into(),
    };
    assert_eq!(exit_code_for_close(&taken_over).unwrap(), 91);

    assert_eq!(
        exit_code_for_hello_ack("b", false, &hello_ack(false, 1)),
        None
    );
    assert_eq!(
        exit_code_for_hello_ack("b", true, &hello_ack(true, 1)),
        None
    );
    assert_eq!(
        exit_code_for_hello_ack("b", true, &hello_ack(false, 2)),
        Some(90)
    );
    let mut wrong_protocol = hello_ack(true, 1);
    wrong_protocol.replica_id = 1;
    assert_eq!(
        exit_code_for_hello_ack("b", false, &wrong_protocol),
        Some(92)
    );
    wrong_protocol.replica_id = 8;
    wrong_protocol.protocol = 2;
    assert_eq!(
        exit_code_for_hello_ack("b", false, &wrong_protocol),
        Some(92)
    );
    let mut other_build = hello_ack(false, 1);
    other_build.build = "release-2".into();
    assert_eq!(
        exit_code_for_hello_ack("release-1", false, &other_build),
        Some(92)
    );
    assert_eq!(
        exit_code_for_hello_ack("dev-local", false, &other_build),
        None
    );
}

#[test]
fn hello_composition() {
    let options = WebSocketConnectionOptions::new("wss://a/rpc", "ws_1", "sess_1", "t");
    let state = options.state.unwrap();
    let hello = WebSocketRemoteConnection::compose_hello(
        &state,
        "ws_1",
        "setup-1".into(),
        false,
        "build-1".into(),
    )
    .unwrap();
    assert_eq!(hello.identifier, "setup-1");
    assert_eq!(hello.instance, state.instance);
    assert_eq!(hello.workspace_id, "ws_1");
    assert_eq!(hello.session_id, "sess_1");
    assert!(!hello.reconnect);
    assert_eq!(hello.epoch, None);
    assert_eq!(hello.protocol, PROTOCOL_VERSION);
    assert_eq!(hello.build, "build-1");

    state.set_current(WebSocketSession {
        url: "wss://b/rpc".into(),
        token: "t2".into(),
        session_id: "sess_2".into(),
    });
    *state.epoch.lock() = Some(4);
    let hello = WebSocketRemoteConnection::compose_hello(
        &state,
        "ws_1",
        "setup-1".into(),
        true,
        "build-1".into(),
    )
    .unwrap();
    assert_eq!(hello.session_id, "sess_2");
    assert_eq!(hello.epoch, Some(4));
    assert!(hello.reconnect);

    let other_tab = WebSocketConnectionOptions::new("wss://a/rpc", "ws_1", "sess_3", "t");
    let other_hello = WebSocketRemoteConnection::compose_hello(
        other_tab.state.as_ref().unwrap(),
        "ws_1",
        "setup-1".into(),
        false,
        "build-1".into(),
    )
    .unwrap();
    assert_eq!(other_hello.identifier, hello.identifier);
    assert_ne!(other_hello.instance, hello.instance);
}

#[gpui::test]
async fn test_pump_oversize_outbound_heartbeat_and_close(cx: &mut TestAppContext) {
    let (frames_tx, mut bridge_outbound) = mpsc::channel(BRIDGE_CHANNEL_CAPACITY);
    let (mut bridge_inbound, frames_rx) = mpsc::channel(BRIDGE_CHANNEL_CAPACITY);
    let (incoming_tx, mut incoming_rx) = mpsc::unbounded();
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
    let (activity_tx, mut activity_rx) = mpsc::channel(1);
    let state = Arc::new(WebSocketSessionState::new(WebSocketSession {
        url: String::new(),
        token: String::new(),
        session_id: String::new(),
    }));
    let pump = cx.executor().spawn(run_pump(
        frames_tx,
        frames_rx,
        incoming_tx,
        outgoing_rx,
        activity_tx,
        state.clone(),
        cx.executor(),
    ));

    let huge = proto::Error {
        message: "x".repeat(17 * 1024 * 1024),
        code: 0,
        tags: vec![],
    }
    .into_envelope(41, None, None);
    outgoing_tx.unbounded_send(huge).unwrap();
    let error_response = incoming_rx.next().await.unwrap();
    assert_eq!(error_response.responding_to, Some(41));
    assert!(matches!(error_response.payload, Some(Payload::Error(_))));

    let small = proto::Ping {}.into_envelope(42, None, None);
    outgoing_tx.unbounded_send(small.clone()).unwrap();
    let frame = bridge_outbound.next().await.unwrap();
    assert_eq!(frame.opcode(), OpCode::Binary);
    assert_eq!(
        decode_envelope_frame(frame.payload(), MAX_FRAME_BYTES).unwrap(),
        small
    );

    bridge_inbound
        .send(Ok(Frame::text(r#"{"type":"heartbeat"}"#)))
        .await
        .unwrap();
    activity_rx.next().await.unwrap();

    let inbound_envelope = proto::Ack {}.into_envelope(7, Some(42), None);
    bridge_inbound
        .send(Ok(Frame::binary(encode_envelope_frame(&inbound_envelope))))
        .await
        .unwrap();
    assert_eq!(incoming_rx.next().await.unwrap(), inbound_envelope);
    activity_rx.next().await.unwrap();

    bridge_inbound
        .send(Ok(Frame::close(
            yawc::close::CloseCode::Away,
            "server shutting down",
        )))
        .await
        .unwrap();
    assert_eq!(pump.await.unwrap(), 90);
    assert_eq!(
        state.last_close(),
        Some(CloseInfo {
            code: 1001,
            reason: "server shutting down".into()
        })
    );
}

#[gpui::test]
async fn test_pump_drains_both_directions_under_bursts(cx: &mut TestAppContext) {
    const BURST: u32 = 200;
    let (frames_tx, mut bridge_outbound) = mpsc::channel(BRIDGE_CHANNEL_CAPACITY);
    let (mut bridge_inbound, frames_rx) = mpsc::channel(BRIDGE_CHANNEL_CAPACITY);
    let (incoming_tx, mut incoming_rx) = mpsc::unbounded();
    let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
    let (activity_tx, _activity_rx) = mpsc::channel(1);
    let state = Arc::new(WebSocketSessionState::new(WebSocketSession {
        url: String::new(),
        token: String::new(),
        session_id: String::new(),
    }));
    let _pump = cx.executor().spawn(run_pump(
        frames_tx,
        frames_rx,
        incoming_tx,
        outgoing_rx,
        activity_tx,
        state,
        cx.executor(),
    ));

    // The client queues far more than the bridge channel holds...
    for id in 0..BURST {
        outgoing_tx
            .unbounded_send(proto::Ping {}.into_envelope(id, None, None))
            .unwrap();
    }
    // ...while the socket side pushes its own burst without taking anything off the pump's
    // outbound channel, as a bridge does whenever its inbound forwarder runs ahead. The pump
    // must keep draining inbound frames although its outbound channel is full.
    let push = async {
        for id in 0..BURST {
            let envelope = proto::Ack {}.into_envelope(1000 + id, None, None);
            bridge_inbound
                .send(Ok(Frame::binary(encode_envelope_frame(&envelope))))
                .await
                .unwrap();
        }
    }
    .fuse();
    let deadline = cx.executor().timer(Duration::from_secs(30)).fuse();
    futures::pin_mut!(push, deadline);
    select_biased! {
        _ = push => {}
        _ = deadline => panic!("the pump stopped taking inbound frames while its outbound channel was full"),
    }

    for id in 0..BURST {
        assert_eq!(incoming_rx.next().await.unwrap().id, 1000 + id);
    }
    for id in 0..BURST {
        let frame = bridge_outbound.next().await.unwrap();
        assert_eq!(frame.opcode(), OpCode::Binary);
        assert_eq!(
            decode_envelope_frame(frame.payload(), MAX_FRAME_BYTES)
                .unwrap()
                .id,
            id
        );
    }
}

#[gpui::test]
async fn test_handshake_and_token_placement(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let (delegate, statuses) = recording_delegate();
    let mut async_cx = cx.to_async();
    let connection = connect(
        options_for(&server, "sess_1", "token-1").into(),
        delegate.clone(),
        &mut async_cx,
    )
    .await
    .unwrap();

    let mut fake_connection = server.next_connection().await;
    assert_eq!(fake_connection.uri, "/rpc");
    assert_eq!(
        fake_connection.subprotocol_header.as_deref(),
        Some("zs.v1, token-1")
    );

    let (_cancel_tx, cancel_rx) = oneshot::channel();
    let client_task = cx.update(|cx| {
        RemoteClient::new(
            ConnectionIdentifier::setup(),
            connection.clone(),
            cancel_rx,
            delegate,
            cx,
        )
    });
    let hello = fake_connection.expect_hello().await;
    assert_eq!(hello.protocol, PROTOCOL_VERSION);
    assert!(!hello.reconnect);
    assert_eq!(hello.epoch, None);
    assert_eq!(hello.workspace_id, "ws_1");
    assert_eq!(hello.session_id, "sess_1");
    assert!(hello.identifier.contains("setup-"), "{}", hello.identifier);
    assert!(!hello.instance.is_empty());
    assert_eq!(hello.client, ClientKind::Desktop);
    assert_eq!(hello.build, cx.update(|cx| client_build_id(cx)));

    fake_connection.send_hello_ack(hello_ack(false, 1));
    fake_connection.send_envelope(&remote_started());
    let client = client_task.await.unwrap().expect("not cancelled");
    assert_eq!(
        cx.update(|cx| client.read(cx).connection_state()),
        ConnectionState::Connected
    );

    let statuses = statuses.lock().clone();
    assert_eq!(
        statuses,
        vec![
            Some("Connecting to workspace".to_owned()),
            Some("Attaching to workspace".to_owned()),
            None
        ]
    );
    let options = match cx.update(|cx| client.read(cx).connection_options()) {
        RemoteConnectionOptions::WebSocket(options) => options,
        other => panic!("{other:?}"),
    };
    assert_eq!(options.server_info().unwrap().epoch, 1);
    assert!(!options.server_info().unwrap().resumed);
    assert_eq!(options.last_close(), None);
    assert!(!connection.has_been_killed());
}

#[gpui::test]
async fn test_envelope_pump_and_server_heartbeat(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let options = options_for(&server, "sess_1", "token-1");
    let (client, mut fake_connection) = open_session(cx, &mut server, options).await;

    let proto_client = cx.update(|cx| client.client.read(cx).proto_client());
    proto_client.request(proto::Ping {}).await.unwrap();
    fake_connection
        .next_envelope_where(|envelope| matches!(envelope.payload, Some(Payload::Ping(_))))
        .await;

    let unsolicited = proto::Test { id: 5 }.into_envelope(500, None, None);
    fake_connection.send_envelope(&unsolicited);
    let response = fake_connection
        .next_envelope_where(|envelope| envelope.responding_to == Some(500))
        .await;
    assert!(matches!(response.payload, Some(Payload::Error(_))));

    for _ in 0..3 {
        fake_connection.send_control(&ControlFrame::Heartbeat);
    }
    fake_connection.send_control(&ControlFrame::Log(wire::LogFrame {
        level: 3,
        module_path: None,
        file: None,
        line: None,
        message: "hello from the fake server".into(),
    }));
    proto_client.request(proto::Ping {}).await.unwrap();
    assert_eq!(client.state(cx), ConnectionState::Connected);
    assert!(client.events().is_empty());
}

#[gpui::test]
async fn test_reconnect_with_refresh(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server_one = FakeServer::start(cx).await;
    let mut server_two = FakeServer::start(cx).await;
    let refresh =
        StubRefresh::with_responses([StubRefresh::session(&server_two.url, "token-2", "sess_2")]);
    let options = options_for(&server_one, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server_one, options).await;
    let first_instance = client.websocket_options(cx).state.unwrap().instance.clone();

    drop(fake_connection);

    let mut second_connection = server_two.next_connection().await;
    assert_eq!(
        second_connection.subprotocol_header.as_deref(),
        Some("zs.v1, token-2")
    );
    let hello = second_connection.expect_hello().await;
    assert!(hello.reconnect);
    assert_eq!(hello.epoch, Some(1));
    assert_eq!(hello.session_id, "sess_2");
    assert_eq!(hello.workspace_id, "ws_1");
    assert_eq!(hello.instance, first_instance);
    second_connection.send_hello_ack(hello_ack(true, 1));
    second_connection
        .next_envelope_where(|envelope| {
            matches!(envelope.payload, Some(Payload::FlushBufferedMessages(_)))
        })
        .await;

    client.wait_for_state(cx, ConnectionState::Connected).await;
    client.wait_for_event(cx, "Reconnected").await;

    assert_eq!(
        refresh.calls(),
        vec![(
            "ws_1".to_owned(),
            RefreshReason::Reconnect {
                attempt: 1,
                last_close: None
            }
        )]
    );
    let live = client.live_connection(cx);
    assert!(!Arc::ptr_eq(&live, &client.first_connection));
    assert!(client.first_connection.has_been_killed());
    assert!(!live.has_been_killed());
    let RemoteConnectionOptions::WebSocket(live_options) = live.connection_options() else {
        panic!("not websocket options");
    };
    assert_eq!(live_options.url, server_two.url);
    assert_eq!(live_options.session_id, "sess_2");
    assert_eq!(live_options.token, "token-2");
    assert_eq!(live_options.workspace_id, "ws_1");
    assert_eq!(live_options.state.unwrap().instance, first_instance);
    assert_eq!(client.websocket_options(cx).server_info().unwrap().epoch, 1);
    assert!(client.websocket_options(cx).server_info().unwrap().resumed);
    assert!(
        client
            .statuses
            .lock()
            .contains(&Some("Refreshing session".to_owned()))
    );
}

async fn assert_terminal_close(cx: &mut TestAppContext, code: u16, reason: &str) {
    let mut server = FakeServer::start(cx).await;
    let refresh = StubRefresh::with_responses([]);
    let options = options_for(&server, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server, options).await;

    fake_connection.close(code, reason);
    client
        .wait_for_state(cx, ConnectionState::Disconnected)
        .await;
    client
        .wait_for_event(cx, "Disconnected { server_not_running: true }")
        .await;
    assert!(refresh.calls().is_empty());
    assert_eq!(
        client.websocket_options(cx).last_close(),
        Some(CloseInfo {
            code,
            reason: reason.to_owned()
        })
    );
    assert!(client.first_connection.has_been_killed());

    // The shell's from-scratch reconnect: new options for the same workspace must dial anew
    // with a new instance nonce, never reuse the dead pooled connection.
    let fresh_options = options_for(&server, "sess_3", "token-3");
    let fresh_instance = fresh_options.state.as_ref().unwrap().instance.clone();
    let old_instance = client.websocket_options(cx).state.unwrap().instance.clone();
    assert_ne!(fresh_instance, old_instance);
    let delegate: Arc<dyn RemoteClientDelegate> = Arc::new(WebSocketClientDelegate::silent());
    let fresh_connection = connect(fresh_options.into(), delegate.clone(), &mut cx.to_async())
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&fresh_connection, &client.first_connection));
    assert!(!fresh_connection.has_been_killed());
    let mut fake_connection = server.next_connection().await;
    let (_cancel_tx, cancel_rx) = oneshot::channel();
    let client_task = cx.update(|cx| {
        RemoteClient::new(
            ConnectionIdentifier::setup(),
            fresh_connection,
            cancel_rx,
            delegate,
            cx,
        )
    });
    let hello = fake_connection.expect_hello().await;
    assert!(!hello.reconnect);
    assert_eq!(hello.epoch, None);
    assert_eq!(hello.instance, fresh_instance);
    fake_connection.send_hello_ack(hello_ack(false, 2));
    fake_connection.send_envelope(&remote_started());
    let fresh_client = client_task.await.unwrap().expect("not cancelled");
    assert_eq!(
        cx.update(|cx| fresh_client.read(cx).connection_state()),
        ConnectionState::Connected
    );
}

#[gpui::test]
async fn test_superseded_is_terminal(cx: &mut TestAppContext) {
    init_test(cx);
    assert_terminal_close(cx, 4001, "taken over").await;
}

#[gpui::test]
async fn test_server_going_away_is_terminal(cx: &mut TestAppContext) {
    init_test(cx);
    assert_terminal_close(cx, 1001, "server shutting down").await;
}

#[gpui::test]
async fn test_stale_epoch_close_is_terminal(cx: &mut TestAppContext) {
    init_test(cx);
    assert_terminal_close(cx, 4001, CLOSE_REASON_STALE_EPOCH).await;
}

#[gpui::test]
async fn test_backoff_spaces_every_reconnect_attempt(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    // Every refresh fails with `RefreshError::Other`; the attempts must still be spaced.
    let refresh = StubRefresh::with_responses([]);
    let options = options_for(&server, "sess_1", "token-1").with_refresh(refresh.clone());
    options.set_backoff_for_tests(Duration::from_secs(10));
    let (client, fake_connection) = open_session(cx, &mut server, options).await;

    drop(fake_connection);
    client
        .wait_for_state(cx, ConnectionState::Reconnecting)
        .await;
    cx.executor().timer(Duration::from_millis(200)).await;
    assert!(
        refresh.calls().is_empty(),
        "the first attempt refreshed before its backoff elapsed"
    );
    assert!(
        client
            .statuses
            .lock()
            .contains(&Some("Reconnecting (attempt 1)".to_owned()))
    );

    cx.executor().advance_clock(Duration::from_secs(10));
    wait_until(cx, || refresh.calls().len() == 1).await;
    cx.executor().timer(Duration::from_millis(200)).await;
    assert_eq!(
        refresh.calls().len(),
        1,
        "the second attempt refreshed before its backoff elapsed"
    );
    assert_eq!(client.state(cx), ConnectionState::Reconnecting);

    cx.executor().advance_clock(Duration::from_secs(10));
    wait_until(cx, || refresh.calls().len() == 2).await;
    assert_eq!(
        refresh
            .calls()
            .into_iter()
            .map(|(_, reason)| reason)
            .collect::<Vec<_>>(),
        vec![
            RefreshReason::Reconnect {
                attempt: 1,
                last_close: None
            },
            RefreshReason::Reconnect {
                attempt: 2,
                last_close: None
            },
        ]
    );
}

#[gpui::test]
async fn test_failed_first_dial_is_not_a_redial(cx: &mut TestAppContext) {
    init_test(cx);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let (delegate, statuses) = recording_delegate();
    let options =
        WebSocketConnectionOptions::new(format!("ws://{address}/rpc"), "ws_1", "sess_1", "token-1");
    // A non-zero backoff would make a mistaken redial visible as a 10 s wait.
    options.set_backoff_for_tests(Duration::from_secs(10));
    let state = options.state.clone().unwrap();
    for _ in 0..2 {
        let error =
            WebSocketRemoteConnection::new(options.clone(), delegate.clone(), &mut cx.to_async())
                .await
                .err()
                .expect("nothing listens on the port");
        assert!(error.to_string().contains("connect"), "{error:#}");
    }
    assert_eq!(state.dials.load(SeqCst), 0);
    assert_eq!(state.reconnect_attempts.load(SeqCst), 0);
    let statuses = statuses.lock().clone();
    assert!(
        statuses.iter().all(|status| {
            !status.as_deref().is_some_and(|status| {
                status.starts_with("Reconnecting") || status == "Refreshing session"
            })
        }),
        "{statuses:?}"
    );
}

#[gpui::test]
async fn test_last_close_describes_only_the_current_attachment(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let refresh =
        StubRefresh::with_responses([StubRefresh::session(&server.url, "token-2", "sess_2")]);
    let options = options_for(&server, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server, options).await;

    // 4003 is retried: the close is reported to the refresh callback, then the reconnect
    // attaches warm.
    fake_connection.close(4003, "unauthorized");
    let mut second_connection = server.next_connection().await;
    let hello = second_connection.expect_hello().await;
    assert!(hello.reconnect);
    second_connection.send_hello_ack(hello_ack(true, 1));
    client.wait_for_event(cx, "Reconnected").await;
    assert_eq!(
        refresh.calls(),
        vec![(
            "ws_1".to_owned(),
            RefreshReason::Reconnect {
                attempt: 1,
                last_close: Some(CloseInfo {
                    code: 4003,
                    reason: "unauthorized".into()
                })
            }
        )]
    );
    assert_eq!(
        client.websocket_options(cx).last_close(),
        None,
        "a successful attachment must forget the previous socket's close"
    );

    // The next incident ends without a close frame and exhausts the budget: neither the
    // refresh callback nor the exhausted state may see the stale 4003.
    drop(second_connection);
    client
        .wait_for_state(cx, ConnectionState::Disconnected)
        .await;
    client
        .wait_for_event(cx, "Disconnected { server_not_running: false }")
        .await;
    let calls = refresh.calls();
    assert_eq!(calls.len(), 1 + WS_MAX_RECONNECT_ATTEMPTS);
    assert_eq!(
        calls[1].1,
        RefreshReason::Reconnect {
            attempt: 1,
            last_close: None
        }
    );
    assert_eq!(client.websocket_options(cx).last_close(), None);
}

#[gpui::test]
async fn test_oversize_request_keeps_the_ack_watermark(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let options = options_for(&server, "sess_1", "token-1");
    let (client, mut fake_connection) = open_session(cx, &mut server, options).await;
    // The ping `RemoteClient::new` sent while connecting.
    fake_connection
        .next_envelope_where(|envelope| matches!(envelope.payload, Some(Payload::Ping(_))))
        .await;

    // Raise the client's watermark to a known server id.
    let unsolicited = proto::Test { id: 1 }.into_envelope(5000, None, None);
    fake_connection.send_envelope(&unsolicited);
    fake_connection
        .next_envelope_where(|envelope| envelope.responding_to == Some(5000))
        .await;

    // A request too large for the transport fails locally with a synthesized response...
    let proto_client = cx.update(|cx| client.client.read(cx).proto_client());
    let error = proto_client
        .request(proto::GetPathMetadata {
            project_id: 0,
            path: "x".repeat(17 * 1024 * 1024),
        })
        .await
        .err()
        .expect("an oversize request must fail");
    assert!(error.to_string().contains("too large"), "{error:#}");

    // ...that must not roll the ack the server receives back to 0.
    proto_client.request(proto::Ping {}).await.unwrap();
    let ping = fake_connection
        .next_envelope_where(|envelope| matches!(envelope.payload, Some(Payload::Ping(_))))
        .await;
    assert_eq!(ping.ack_id, Some(5000));
}

#[gpui::test]
async fn test_handshake_tolerates_keepalives_before_hello_ack(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let connection = WebSocketRemoteConnection::new(
        options_for(&server, "sess_1", "token-1"),
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .unwrap();
    let mut fake_connection = server.next_connection().await;
    let (task, mut channels) = start_proxy_directly(cx, &connection, false);
    fake_connection.expect_hello().await;

    // A protocol ping (surfaced by native yawc after it answers it), a server heartbeat and
    // a log frame may all precede the answer to `Hello`.
    fake_connection.send(Message::Ping(vec![1, 2, 3].into()));
    fake_connection.send_control(&ControlFrame::Heartbeat);
    fake_connection.send_control(&ControlFrame::Log(wire::LogFrame {
        level: 2,
        module_path: None,
        file: None,
        line: None,
        message: "starting up".into(),
    }));
    fake_connection.send_hello_ack(hello_ack(false, 3));
    fake_connection.send_envelope(&remote_started());

    assert_eq!(channels.incoming_rx.next().await.unwrap(), remote_started());
    assert_eq!(connection.server_info().unwrap().epoch, 3);
    fake_connection.close(1001, "bye");
    assert_eq!(task.await.unwrap(), 90);
}

#[gpui::test]
async fn test_build_mismatch_on_reconnect_ends_the_client(cx: &mut TestAppContext) {
    init_test(cx);
    cx.update(|cx| set_client_build_id_for_tests(cx, "release-1"));
    let mut server_one = FakeServer::start(cx).await;
    let mut server_two = FakeServer::start(cx).await;
    let refresh =
        StubRefresh::with_responses([StubRefresh::session(&server_two.url, "token-2", "sess_2")]);
    let options = options_for(&server_one, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server_one, options).await;

    drop(fake_connection);
    let mut second_connection = server_two.next_connection().await;
    let hello = second_connection.expect_hello().await;
    assert!(hello.reconnect);
    assert_eq!(hello.build, "release-1");
    let mut ack = hello_ack(true, 1);
    ack.build = "release-2".into();
    second_connection.send_hello_ack(ack);

    // `monitor` maps exit 92 to `ServerNotRunning`: no further reconnect, no refresh.
    client
        .wait_for_state(cx, ConnectionState::Disconnected)
        .await;
    client
        .wait_for_event(cx, "Disconnected { server_not_running: true }")
        .await;
    assert_eq!(refresh.calls().len(), 1);
    assert_eq!(
        client
            .websocket_options(cx)
            .last_close()
            .map(|close| close.code),
        Some(4002)
    );
}

#[gpui::test]
async fn test_two_tabs_present_distinct_instances(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let delegate: Arc<dyn RemoteClientDelegate> = Arc::new(WebSocketClientDelegate::silent());
    let mut hellos = Vec::new();
    let mut sessions = Vec::new();
    for session_id in ["sess_a", "sess_b"] {
        // Two tabs are two processes, each with its own options for the same workspace.
        let connection = WebSocketRemoteConnection::new(
            options_for(&server, session_id, "token"),
            delegate.clone(),
            &mut cx.to_async(),
        )
        .await
        .unwrap();
        let mut fake_connection = server.next_connection().await;
        let (task, channels) = start_proxy_directly(cx, &connection, false);
        hellos.push(fake_connection.expect_hello().await);
        sessions.push((connection, fake_connection, task, channels));
    }
    let [first, second] = hellos.as_slice() else {
        panic!("expected two hellos");
    };
    assert_eq!(first.workspace_id, second.workspace_id);
    assert_eq!(first.identifier, second.identifier);
    assert_ne!(first.session_id, second.session_id);
    assert_ne!(
        first.instance, second.instance,
        "the server keys same-instance reconnects on Hello.instance"
    );
}

#[gpui::test]
async fn test_build_mismatch(cx: &mut TestAppContext) {
    init_test(cx);
    cx.update(|cx| set_client_build_id_for_tests(cx, "release-1"));
    let mut server = FakeServer::start(cx).await;
    let connection = WebSocketRemoteConnection::new(
        options_for(&server, "sess_1", "token-1"),
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .unwrap();
    let mut fake_connection = server.next_connection().await;
    let (task, _channels) = start_proxy_directly(cx, &connection, false);
    let hello = fake_connection.expect_hello().await;
    assert_eq!(hello.build, "release-1");
    let mut ack = hello_ack(false, 1);
    ack.build = "other".into();
    fake_connection.send_hello_ack(ack);
    assert_eq!(task.await.unwrap(), 92);
    assert_eq!(connection.last_close().map(|close| close.code), Some(4002));
    assert!(connection.has_been_killed());
    assert!(matches!(
        ProxyLaunchError::from_exit_code(92),
        Some(ProxyLaunchError::IncompatibleServer)
    ));
}

#[gpui::test]
async fn test_reconnect_not_resumed_is_fresh_session(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server_one = FakeServer::start(cx).await;
    let mut server_two = FakeServer::start(cx).await;
    let refresh =
        StubRefresh::with_responses([StubRefresh::session(&server_two.url, "token-2", "sess_2")]);
    let options = options_for(&server_one, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server_one, options).await;

    drop(fake_connection);
    let mut second_connection = server_two.next_connection().await;
    let hello = second_connection.expect_hello().await;
    assert!(hello.reconnect);
    second_connection.send_hello_ack(hello_ack(false, 2));

    client
        .wait_for_state(cx, ConnectionState::Disconnected)
        .await;
    client
        .wait_for_event(cx, "Disconnected { server_not_running: true }")
        .await;
    assert_eq!(refresh.calls().len(), 1);
    assert_eq!(client.websocket_options(cx).last_close(), None);
}

#[gpui::test]
async fn test_force_disconnect_reconnects_and_drop_closes(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let refresh =
        StubRefresh::with_responses([StubRefresh::session(&server.url, "token-2", "sess_2")]);
    let options = options_for(&server, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, mut fake_connection) = open_session(cx, &mut server, options).await;

    client
        .client
        .update(cx, |client, cx| client.force_disconnect(cx))
        .await
        .unwrap();
    assert!(client.first_connection.has_been_killed());
    fake_connection
        .wait_until_closed(cx, "after force_disconnect")
        .await;

    let mut second_connection = server.next_connection().await;
    let hello = second_connection.expect_hello().await;
    assert!(hello.reconnect);
    second_connection.send_hello_ack(hello_ack(true, 1));
    client.wait_for_state(cx, ConnectionState::Connected).await;
    assert_eq!(refresh.calls().len(), 1);
    let live = client.live_connection(cx);
    assert!(!Arc::ptr_eq(&live, &client.first_connection));

    // The connection owns the socket, so the last `Arc` has to go before the socket can close,
    // and the entity is only released when the app flushes effects.
    drop(live);
    drop(client);
    cx.update(|_| {});
    cx.run_until_parked();
    second_connection
        .wait_until_closed(cx, "after dropping the client")
        .await;
    assert_eq!(refresh.calls().len(), 1);
}

#[gpui::test]
async fn test_oversize_inbound_frame_reconnects_until_exhausted(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let refresh = StubRefresh::with_responses([]);
    let options = options_for(&server, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server, options).await;

    fake_connection.send(Message::Binary(vec![0u8; 17 * 1024 * 1024].into()));
    client
        .wait_for_state(cx, ConnectionState::Disconnected)
        .await;
    client
        .wait_for_event(cx, "Disconnected { server_not_running: false }")
        .await;

    let calls = refresh.calls();
    assert_eq!(calls.len(), WS_MAX_RECONNECT_ATTEMPTS);
    for (index, (workspace_id, reason)) in calls.iter().enumerate() {
        assert_eq!(workspace_id, "ws_1");
        assert_eq!(
            reason,
            &RefreshReason::Reconnect {
                attempt: index + 1,
                last_close: None
            }
        );
    }
    assert_eq!(client.websocket_options(cx).last_close(), None);
}

#[gpui::test]
async fn test_handshake_timeout_and_bad_first_frame(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;

    let connection = WebSocketRemoteConnection::new(
        options_for(&server, "sess_1", "token-1"),
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .unwrap();
    let mut fake_connection = server.next_connection().await;
    let (task, _channels) = start_proxy_directly(cx, &connection, false);
    fake_connection.expect_hello().await;
    // Let the client arm its handshake timer before the clock jumps past it.
    cx.executor().timer(Duration::from_millis(100)).await;
    cx.executor().advance_clock(Duration::from_secs(15));
    let error = task.await.err().expect("handshake must time out");
    assert!(error.to_string().contains("timed out"), "{error:#}");

    let connection = WebSocketRemoteConnection::new(
        options_for(&server, "sess_1", "token-1"),
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .unwrap();
    let mut fake_connection = server.next_connection().await;
    let (task, _channels) = start_proxy_directly(cx, &connection, false);
    fake_connection.expect_hello().await;
    fake_connection.send_envelope(&remote_started());
    let error = task.await.err().expect("binary first frame must fail");
    assert!(
        error.to_string().contains("unexpected first frame"),
        "{error:#}"
    );
}

#[gpui::test]
async fn test_connect_timeout_without_upgrade(cx: &mut TestAppContext) {
    init_test(cx);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _hold_sockets = cx.executor().spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            sockets.push(socket);
        }
    });

    let options =
        WebSocketConnectionOptions::new(format!("ws://{address}/rpc"), "ws_1", "sess_1", "token-1");
    let connect_task = cx.spawn(|mut cx| async move {
        connect(
            options.into(),
            Arc::new(WebSocketClientDelegate::silent()),
            &mut cx,
        )
        .await
    });
    cx.executor().timer(Duration::from_millis(200)).await;
    cx.executor().advance_clock(Duration::from_secs(30));
    let error = connect_task.await.err().expect("dial must time out");
    assert!(error.to_string().contains("timed out"), "{error:#}");
}

async fn assert_terminal_refresh(
    cx: &mut TestAppContext,
    response: RefreshError,
    expected_close: CloseInfo,
) {
    let mut server = FakeServer::start(cx).await;
    let refresh = StubRefresh::with_responses([Err(response)]);
    let options = options_for(&server, "sess_1", "token-1").with_refresh(refresh.clone());
    let (client, fake_connection) = open_session(cx, &mut server, options).await;

    drop(fake_connection);
    client
        .wait_for_state(cx, ConnectionState::Disconnected)
        .await;
    client
        .wait_for_event(cx, "Disconnected { server_not_running: false }")
        .await;
    assert_eq!(refresh.calls().len(), 1);
    assert_eq!(
        client.websocket_options(cx).last_close(),
        Some(expected_close)
    );
    assert_eq!(client.first_connection.max_reconnect_attempts(), 0);

    let error = WebSocketRemoteConnection::new(
        client.websocket_options(cx),
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .err()
    .expect("a terminal session must not dial again");
    assert!(error.to_string().contains("no longer usable"), "{error:#}");
    assert_eq!(refresh.calls().len(), 1);
}

#[gpui::test]
async fn test_refresh_unauthorized_is_terminal(cx: &mut TestAppContext) {
    init_test(cx);
    assert_terminal_refresh(
        cx,
        RefreshError::Unauthorized,
        CloseInfo {
            code: 4003,
            reason: "session expired".into(),
        },
    )
    .await;
}

#[gpui::test]
async fn test_refresh_stopped_is_terminal(cx: &mut TestAppContext) {
    init_test(cx);
    assert_terminal_refresh(
        cx,
        RefreshError::Stopped,
        CloseInfo {
            code: 1001,
            reason: "workspace stopped".into(),
        },
    )
    .await;
}

#[gpui::test]
async fn test_restored_row_dials_through_the_refresh_provider(cx: &mut TestAppContext) {
    init_test(cx);
    let mut server = FakeServer::start(cx).await;
    let refresh =
        StubRefresh::with_responses([StubRefresh::session(&server.url, "token-r", "sess_r")]);
    cx.update(|cx| set_session_refresh_provider(cx, refresh.clone()));

    let restored: WebSocketConnectionOptions = serde_json::from_str(
        &serde_json::to_string(&WebSocketConnectionOptions::new("", "ws_1", "", "")).unwrap(),
    )
    .unwrap();
    assert!(restored.state.is_none());
    let connection = WebSocketRemoteConnection::new(
        restored,
        Arc::new(WebSocketClientDelegate::silent()),
        &mut cx.to_async(),
    )
    .await
    .unwrap();
    assert_eq!(
        refresh.calls(),
        vec![("ws_1".to_owned(), RefreshReason::Initial)]
    );
    let RemoteConnectionOptions::WebSocket(options) = connection.connection_options() else {
        panic!("not websocket options");
    };
    assert_eq!(options.url, server.url);
    assert_eq!(options.session_id, "sess_r");
    assert_eq!(options.token, "token-r");
    let fake_connection = server.next_connection().await;
    assert_eq!(
        fake_connection.subprotocol_header.as_deref(),
        Some("zs.v1, token-r")
    );
}
