//! End-to-end test of `zed-remote-server serve` against the real binary.
//!
//! Skipped unless `ZED_RUN_SERVE_INTEGRATION=1` so the default test run stays fast; CI sets it.

#![cfg(unix)]
#![allow(
    clippy::disallowed_methods,
    reason = "an integration test that drives the real binary synchronously has no async thread to block"
)]

use std::{
    io::{BufRead as _, BufReader, Read as _, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::SinkExt as _;
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use remote::{
    protocol::{decode_envelope_frame, encode_envelope_frame},
    websocket_wire::{
        CLOSE_GOING_AWAY, ClientKind, ControlFrame, Hello, HelloAck, MAX_FRAME_BYTES,
        PROTOCOL_VERSION, SUBPROTOCOL,
    },
};
use rpc::proto::{self, Envelope, EnvelopedMessage as _};
use serde_json::Value;
use yawc::{
    Options, WebSocket,
    frame::{Frame, OpCode},
};

const WORKSPACE_ID: &str = "ws_test";
const AUDIENCE: &str = "sb_test";
const ISSUER: &str = "zs";
const PRIVATE_PEM: &str = include_str!("fixtures/es256_private.pem");
const CONTROL_SECRET: &str = include_str!("fixtures/control_secret");

/// `true` when the integration tests were asked for (`ZED_RUN_SERVE_INTEGRATION=1`). Asking
/// for them on a host where the fixture cannot run is a failure, never a silent `ok`.
fn enabled() -> bool {
    if std::env::var("ZED_RUN_SERVE_INTEGRATION").as_deref() != Ok("1") {
        return false;
    }
    require_writable_paths();
    true
}

/// The test-profile binary links `util` with `test-support`, which hard-codes the home
/// directory (`/Users/zed`, `/home/zed`). On platforms where `paths::logs_dir()` is derived
/// from that home (macOS) the spawned binary cannot create its log directory and the test
/// cannot run; on Linux `--user-data-dir` covers every path.
fn require_writable_paths() {
    if let Err(error) = std::fs::create_dir_all(paths::logs_dir()) {
        panic!(
            "ZED_RUN_SERVE_INTEGRATION=1 but the test build cannot create {:?} ({error}); \
             run the serve integration tests on Linux",
            paths::logs_dir()
        );
    }
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[derive(serde::Serialize)]
struct Claims {
    iss: String,
    sub: String,
    ws: String,
    sid: String,
    pid: String,
    aud: String,
    iat: u64,
    exp: u64,
    jti: String,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

fn mint(sid: &str) -> String {
    let claims = Claims {
        iss: ISSUER.into(),
        sub: "user_1".into(),
        ws: WORKSPACE_ID.into(),
        sid: sid.into(),
        pid: format!("p_{:032x}", sid.bytes().map(u64::from).sum::<u64>()),
        aud: AUDIENCE.into(),
        iat: now(),
        exp: now() + 3600,
        jti: format!("jti_{sid}"),
    };
    encode(
        &Header::new(Algorithm::ES256),
        &claims,
        &EncodingKey::from_ec_pem(PRIVATE_PEM.as_bytes()).expect("private key"),
    )
    .expect("token")
}

fn mint_expired(sid: &str) -> String {
    let claims = Claims {
        iss: ISSUER.into(),
        sub: "user_1".into(),
        ws: WORKSPACE_ID.into(),
        sid: sid.into(),
        pid: format!("p_{:032x}", sid.bytes().map(u64::from).sum::<u64>()),
        aud: AUDIENCE.into(),
        iat: now() - 7200,
        exp: now() - 3600,
        jti: format!("jti_{sid}"),
    };
    encode(
        &Header::new(Algorithm::ES256),
        &claims,
        &EncodingKey::from_ec_pem(PRIVATE_PEM.as_bytes()).expect("private key"),
    )
    .expect("token")
}

struct ServeProcess {
    child: Child,
    public: SocketAddr,
    control: SocketAddr,
    stderr: mpsc::Receiver<String>,
    _workspace: tempfile::TempDir,
    workspace_root: PathBuf,
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

impl ServeProcess {
    fn spawn() -> Self {
        let workspace = tempfile::tempdir().expect("workspace dir");
        let workspace_root = workspace.path().canonicalize().expect("canonical root");
        let port_file = workspace_root.join("server.port");
        let mut child = Command::new(env!("CARGO_BIN_EXE_remote_server"))
            .args([
                "serve",
                "--listen",
                "127.0.0.1:0",
                "--control-listen",
                "127.0.0.1:0",
                "--control-secret-file",
            ])
            .arg(fixture("control_secret"))
            .arg("--port-file")
            .arg(&port_file)
            .arg("--jwt-public-key")
            .arg(fixture("es256_public.pem"))
            .args([
                "--workspace-id",
                WORKSPACE_ID,
                "--audience",
                AUDIENCE,
                "--issuer",
                ISSUER,
                "--workspace-root",
            ])
            .arg(&workspace_root)
            .arg("--user-data-dir")
            .arg(workspace_root.join("zed-data"))
            .env("RUST_LOG", "info")
            .env("ZS_CONTROL_SECRET", "leak")
            .env_remove("ZS_ALLOWED_ORIGINS")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawning serve");

        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        let (log_tx, log_rx) = mpsc::channel();
        let stderr = BufReader::new(child.stderr.take().expect("stderr"));
        std::thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                log_tx.send(line).ok();
            }
        });

        let mut public = None;
        let mut control = None;
        for line in stdout.lines().map_while(Result::ok) {
            if let Some(value) = line.strip_prefix("ZS_LISTENING=") {
                public = Some(value.trim().parse().expect("public address"));
            } else if let Some(value) = line.strip_prefix("ZS_CONTROL_LISTENING=") {
                control = Some(value.trim().parse().expect("control address"));
            }
            if public.is_some() && control.is_some() {
                break;
            }
        }
        let public: SocketAddr = public.expect("ZS_LISTENING line");
        let control: SocketAddr = control.expect("ZS_CONTROL_LISTENING line");

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut port_file_contents = String::new();
        while Instant::now() < deadline {
            if let Ok(contents) = std::fs::read_to_string(&port_file)
                && !contents.trim().is_empty()
            {
                port_file_contents = contents;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(port_file_contents.trim(), public.to_string());

        Self {
            child,
            public,
            control,
            stderr: log_rx,
            _workspace: workspace,
            workspace_root,
        }
    }

    fn logs(&self) -> Vec<String> {
        let mut lines = Vec::new();
        while let Ok(line) = self.stderr.try_recv() {
            lines.push(line);
        }
        lines
    }
}

struct HttpOutcome {
    status: StatusCode,
    body: Bytes,
}

impl HttpOutcome {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("JSON body")
    }
}

async fn request(addr: SocketAddr, request: Request<Full<Bytes>>) -> HttpOutcome {
    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("handshake");
    let driver = tokio::spawn(async move {
        connection.await.ok();
    });
    let response: Response<hyper::body::Incoming> =
        sender.send_request(request).await.expect("send request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    driver.abort();
    HttpOutcome { status, body }
}

fn get(target: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("GET")
        .uri(target)
        .header(header::HOST, "127.0.0.1")
        .body(Full::new(Bytes::new()))
        .expect("request")
}

fn post(target: &str, content_type: &str, body: Bytes) -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri(target)
        .header(header::HOST, "127.0.0.1")
        .header(header::CONTENT_TYPE, content_type)
        .body(Full::new(body))
        .expect("request")
}

fn with_bearer(mut request: Request<Full<Bytes>>, token: &str) -> Request<Full<Bytes>> {
    request.headers_mut().insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_str(&format!("Bearer {token}")).expect("bearer"),
    );
    request
}

struct Client {
    ws: WebSocket<yawc::MaybeTlsStream<tokio::net::TcpStream>>,
}

impl Client {
    async fn connect(addr: SocketAddr, token: &str) -> Self {
        let url: url::Url = format!("ws://{addr}/rpc").parse().expect("url");
        let ws = WebSocket::connect(url)
            .with_options(Options::default().without_compression())
            .with_request(
                yawc::HttpRequestBuilder::new()
                    .header("sec-websocket-protocol", format!("{SUBPROTOCOL}, {token}")),
            )
            .await
            .expect("upgrade");
        Self { ws }
    }

    async fn hello(
        &mut self,
        session_id: &str,
        instance: &str,
        reconnect: bool,
        epoch: Option<u64>,
    ) {
        let hello = Hello {
            protocol: PROTOCOL_VERSION,
            build: "dev-integration".into(),
            workspace_id: WORKSPACE_ID.into(),
            session_id: session_id.into(),
            identifier: format!("setup-{instance}"),
            instance: instance.into(),
            reconnect,
            client: ClientKind::Web,
            epoch,
        };
        let json = serde_json::to_string(&ControlFrame::Hello(hello)).expect("hello json");
        self.ws.send(Frame::text(json)).await.expect("send hello");
    }

    async fn next_frame(&mut self) -> Option<Frame> {
        tokio::time::timeout(Duration::from_secs(10), self.ws.next_frame())
            .await
            .ok()
            .and_then(Result::ok)
    }

    async fn hello_ack(&mut self) -> HelloAck {
        loop {
            let frame = self.next_frame().await.expect("frame");
            match frame.opcode() {
                OpCode::Text => match serde_json::from_slice(frame.payload()).expect("control") {
                    ControlFrame::HelloAck(ack) => return ack,
                    ControlFrame::Heartbeat => continue,
                    other => panic!("unexpected control frame {other:?}"),
                },
                OpCode::Ping | OpCode::Pong => continue,
                other => panic!("unexpected frame {other:?}"),
            }
        }
    }

    async fn next_envelope(&mut self) -> Envelope {
        loop {
            let frame = self.next_frame().await.expect("frame");
            match frame.opcode() {
                OpCode::Binary => {
                    let envelope =
                        decode_envelope_frame(frame.payload(), MAX_FRAME_BYTES).expect("envelope");
                    if envelope.responding_to.is_some()
                        || matches!(
                            envelope.payload,
                            Some(proto::envelope::Payload::RemoteStarted(_))
                        )
                    {
                        return envelope;
                    }
                }
                OpCode::Close => panic!("socket closed while waiting for an envelope"),
                _ => continue,
            }
        }
    }

    async fn wait_for_heartbeat(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline {
            let Some(frame) = self.next_frame().await else {
                panic!("socket ended while waiting for a heartbeat");
            };
            if frame.opcode() == OpCode::Text
                && matches!(
                    serde_json::from_slice::<ControlFrame>(frame.payload()),
                    Ok(ControlFrame::Heartbeat)
                )
            {
                return;
            }
        }
        panic!("no heartbeat within 12 s");
    }

    async fn send_envelope(&mut self, envelope: &Envelope) {
        self.ws
            .send(Frame::binary(encode_envelope_frame(envelope)))
            .await
            .expect("send envelope");
    }

    async fn wait_for_close(&mut self) -> (u16, String) {
        loop {
            let frame = self.next_frame().await.expect("frame before close");
            if frame.opcode() == OpCode::Close {
                return (
                    frame.close_code().map(u16::from).unwrap_or(0),
                    frame
                        .close_reason()
                        .ok()
                        .flatten()
                        .unwrap_or_default()
                        .to_owned(),
                );
            }
        }
    }
}

#[test]
fn serve_cli_rejects_a_missing_control_secret() {
    if !enabled() {
        return;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_remote_server"))
        .args([
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--workspace",
            WORKSPACE_ID,
            "--audience",
            AUDIENCE,
            "--allow-build",
            "x",
            "--workspace-root",
            "/tmp",
        ])
        .arg("--jwt-public-key")
        .arg(fixture("es256_public.pem"))
        .output()
        .expect("running serve");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--control-secret-file"), "{stderr}");
}

#[test]
fn serve_cli_rejects_a_non_loopback_control_listener() {
    if !enabled() {
        return;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_remote_server"))
        .args([
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--control-listen",
            "0.0.0.0:0",
            "--workspace-id",
            WORKSPACE_ID,
            "--audience",
            AUDIENCE,
            "--workspace-root",
            "/tmp",
            "--control-secret-file",
        ])
        .arg(fixture("control_secret"))
        .arg("--jwt-public-key")
        .arg(fixture("es256_public.pem"))
        .output()
        .expect("running serve");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("loopback"), "{stderr}");
}

#[tokio::test(flavor = "multi_thread")]
async fn serve_end_to_end() {
    if !enabled() {
        return;
    }
    let mut server = ServeProcess::spawn();
    let secret = CONTROL_SECRET.trim_end_matches(['\r', '\n']);

    // 2. /health and the control listener.
    let health = request(server.public, get("/health")).await;
    assert_eq!(health.status, StatusCode::OK);
    let health_json = health.json();
    assert_eq!(health_json["session_active"], false);
    assert_eq!(health_json["workspace_id"], WORKSPACE_ID);
    assert!(
        health_json["build"]
            .as_str()
            .is_some_and(|build| !build.is_empty())
    );
    assert_eq!(health_json["worktrees"], serde_json::json!([]));

    let lifecycle = "{\"kind\":\"idle_stop_in\",\"seconds\":300}";
    let public_control = request(
        server.public,
        with_bearer(
            post(
                "/control/lifecycle",
                "application/json",
                Bytes::from_static(lifecycle.as_bytes()),
            ),
            secret,
        ),
    )
    .await;
    assert_eq!(public_control.status, StatusCode::NOT_FOUND);

    let control_ok = request(
        server.control,
        with_bearer(
            post(
                "/control/lifecycle",
                "application/json",
                Bytes::from_static(lifecycle.as_bytes()),
            ),
            secret,
        ),
    )
    .await;
    assert_eq!(control_ok.status, StatusCode::NO_CONTENT);

    let leaked_secret = request(
        server.control,
        with_bearer(
            post(
                "/control/lifecycle",
                "application/json",
                Bytes::from_static(lifecycle.as_bytes()),
            ),
            "leak",
        ),
    )
    .await;
    assert_eq!(leaked_secret.status, StatusCode::UNAUTHORIZED);

    let no_bearer = request(
        server.control,
        post(
            "/control/lifecycle",
            "application/json",
            Bytes::from_static(lifecycle.as_bytes()),
        ),
    )
    .await;
    assert_eq!(no_bearer.status, StatusCode::UNAUTHORIZED);

    // 3-6. Handshake.
    let token_1 = mint("ses_1");
    let mut first = Client::connect(server.public, &token_1).await;
    first.hello("ses_1", "inst_1", false, None).await;
    let ack = first.hello_ack().await;
    assert!(!ack.resumed);
    assert_eq!(ack.session_id, "ses_1");
    assert_eq!(ack.os, std::env::consts::OS);
    let epoch_1 = ack.epoch;

    let started = first.next_envelope().await;
    assert_eq!(started.id, 0);
    assert!(matches!(
        started.payload,
        Some(proto::envelope::Payload::RemoteStarted(_))
    ));

    first
        .send_envelope(&proto::RemoteStarted {}.into_envelope(0, None, None))
        .await;
    let ack_envelope = first.next_envelope().await;
    assert_eq!(ack_envelope.responding_to, Some(0));

    // 7. Ping is not input.
    first
        .send_envelope(&proto::Ping {}.into_envelope(1, None, None))
        .await;
    let ping_ack = first.next_envelope().await;
    assert_eq!(ping_ack.responding_to, Some(1));
    let health_json = request(server.public, get("/health")).await.json();
    assert_eq!(health_json["session_active"], true);
    assert_eq!(health_json["session"]["epoch"].as_u64(), Some(epoch_1));
    assert!(health_json.get("last_input_at").is_none());
    first.wait_for_heartbeat().await;

    // 8. A real request counts as input.
    first
        .send_envelope(
            &proto::ListRemoteDirectory {
                dev_server_id: 0,
                path: server.workspace_root.to_string_lossy().into_owned(),
                config: None,
            }
            .into_envelope(2, None, None),
        )
        .await;
    let listing = first.next_envelope().await;
    assert_eq!(listing.responding_to, Some(2));
    let health_json = request(server.public, get("/health")).await.json();
    assert!(health_json["last_input_at"].as_u64().is_some());

    // 9. /files.
    let upload = request(
        server.public,
        with_bearer(
            post(
                "/files?path=hello.txt",
                "application/octet-stream",
                Bytes::from_static(b"hello world"),
            ),
            &token_1,
        ),
    )
    .await;
    assert_eq!(upload.status, StatusCode::CREATED);
    assert_eq!(
        std::fs::read(server.workspace_root.join("hello.txt")).expect("uploaded file"),
        b"hello world"
    );

    let download = request(
        server.public,
        get(&format!("/files?path=hello.txt&zs_token={token_1}")),
    )
    .await;
    assert_eq!(download.status, StatusCode::OK);
    assert_eq!(download.body, Bytes::from_static(b"hello world"));

    let traversal = request(
        server.public,
        with_bearer(
            post(
                "/files?path=..%2Fx",
                "application/octet-stream",
                Bytes::from_static(b"x"),
            ),
            &token_1,
        ),
    )
    .await;
    assert_eq!(traversal.status, StatusCode::BAD_REQUEST);

    let missing_asset = request(
        server.public,
        with_bearer(get("/extensions/nope/assets/x.json"), &token_1),
    )
    .await;
    assert_eq!(missing_asset.status, StatusCode::NOT_FOUND);

    let expired = request(
        server.public,
        with_bearer(get("/files?path=hello.txt"), &mint_expired("ses_1")),
    )
    .await;
    assert_eq!(expired.status, StatusCode::UNAUTHORIZED);

    first
        .send_envelope(&proto::Ping {}.into_envelope(3, None, None))
        .await;
    assert_eq!(first.next_envelope().await.responding_to, Some(3));

    // A second signed participant gets an independent replica and RPC sequence.
    let mut second = Client::connect(server.public, &mint("ses_2")).await;
    second.hello("ses_2", "inst_2", false, None).await;
    let second_ack = second.hello_ack().await;
    assert!(!second_ack.resumed);
    assert_eq!(second_ack.replica_id, 9);
    assert_eq!(second.next_envelope().await.id, 0);
    second
        .send_envelope(&proto::RemoteStarted {}.into_envelope(0, None, None))
        .await;
    assert_eq!(second.next_envelope().await.responding_to, Some(0));
    first
        .send_envelope(&proto::Ping {}.into_envelope(4, None, None))
        .await;
    second
        .send_envelope(&proto::Ping {}.into_envelope(1, None, None))
        .await;
    assert_eq!(first.next_envelope().await.responding_to, Some(4));
    assert_eq!(second.next_envelope().await.responding_to, Some(1));

    // A browser's shutdown request cannot terminate the shared host.
    second
        .send_envelope(&proto::ShutdownRemoteServer {}.into_envelope(2, None, None))
        .await;
    assert_eq!(second.next_envelope().await.responding_to, Some(2));
    second
        .send_envelope(&proto::Ping {}.into_envelope(3, None, None))
        .await;
    assert_eq!(second.next_envelope().await.responding_to, Some(3));
    assert_eq!(
        request(server.public, get("/health")).await.status,
        StatusCode::OK
    );
    drop(first);

    let pid = server.child.id();
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    let (code, _) = second.wait_for_close().await;
    assert_eq!(code, CLOSE_GOING_AWAY);

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match server.child.try_wait().expect("wait") {
            Some(status) => break status,
            None if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            None => panic!("serve did not exit within 10 s of SIGTERM"),
        }
    };
    assert!(status.success(), "serve exited with {status:?}");

    let logs = server.logs();
    assert!(!logs.is_empty());
    for line in &logs {
        let record: Value = serde_json::from_str(line).unwrap_or_else(|error| {
            panic!("stderr line is not JSON ({error}): {line}");
        });
        assert_eq!(record["mode"], "serve");
        assert_eq!(record["ws"], WORKSPACE_ID);
        assert!(!line.contains(&token_1), "a token leaked into the logs");
        assert!(
            !line.contains(secret),
            "the control secret leaked into the logs"
        );
    }
    assert!(
        logs.iter()
            .any(|line| line.contains("\"session_id\":\"ses_2\"")),
        "no log line carried the participant session id"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_without_a_session_exits_promptly() {
    if !enabled() {
        return;
    }
    let mut server = ServeProcess::spawn();
    assert_eq!(
        request(server.public, get("/health")).await.status,
        StatusCode::OK
    );
    unsafe { libc::kill(server.child.id() as i32, libc::SIGTERM) };
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match server.child.try_wait().expect("wait") {
            Some(status) => {
                assert!(status.success(), "serve exited with {status:?}");
                return;
            }
            None if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            None => panic!("serve did not exit within 5 s of SIGTERM"),
        }
    }
}

#[test]
fn upload_and_download_round_trip_through_the_binary() {
    if !enabled() {
        return;
    }
    // A blocking sanity check that the fixture files are readable and well formed, so a
    // misconfigured checkout fails here rather than deep inside the async test.
    let mut private = String::new();
    std::fs::File::open(fixture("es256_private.pem"))
        .expect("private key fixture")
        .read_to_string(&mut private)
        .expect("reading the private key");
    assert!(private.contains("BEGIN PRIVATE KEY"));
    let mut secret = Vec::new();
    std::fs::File::open(fixture("control_secret"))
        .expect("control secret fixture")
        .read_to_end(&mut secret)
        .expect("reading the control secret");
    assert!(!secret.is_empty());
    let mut sink = std::io::sink();
    sink.write_all(&secret).expect("sink");
}
