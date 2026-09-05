//! Smoke test for `zed-remote-server run` over its three Unix sockets, guarding the
//! `execute_run` extraction that `serve` shares (`init_crash_handler`, `init_rayon_pool`,
//! `build_headless_project`).
//!
//! Skipped unless `ZED_RUN_SERVE_INTEGRATION=1`, like the `serve` integration test.

#![cfg(unix)]
#![allow(
    clippy::disallowed_methods,
    reason = "an integration test that drives the real binary synchronously has no async thread to block"
)]

use std::{
    io::{Read as _, Write as _},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use prost::Message as _;
use rpc::proto::{self, Envelope, EnvelopedMessage as _};

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

struct RunProcess {
    child: Child,
}

impl Drop for RunProcess {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

fn connect(path: &Path) -> UnixStream {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match UnixStream::connect(path) {
            Ok(stream) => return stream,
            Err(error) if Instant::now() < deadline => {
                if error.kind() != std::io::ErrorKind::NotFound
                    && error.kind() != std::io::ErrorKind::ConnectionRefused
                {
                    panic!("connecting to {path:?}: {error}");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("timed out connecting to {path:?}: {error}"),
        }
    }
}

fn write_envelope(stream: &mut UnixStream, envelope: &Envelope) {
    let mut bytes = Vec::with_capacity(envelope.encoded_len() + 4);
    bytes.extend_from_slice(&(envelope.encoded_len() as u32).to_le_bytes());
    envelope.encode(&mut bytes).expect("encoding an envelope");
    stream.write_all(&bytes).expect("writing an envelope");
    stream.flush().expect("flushing");
}

fn read_envelope(stream: &mut UnixStream) -> Envelope {
    let mut length = [0u8; 4];
    stream
        .read_exact(&mut length)
        .expect("reading the length prefix");
    let mut payload = vec![0u8; u32::from_le_bytes(length) as usize];
    stream
        .read_exact(&mut payload)
        .expect("reading the payload");
    Envelope::decode(payload.as_slice()).expect("decoding an envelope")
}

#[test]
fn run_answers_ping_and_shuts_down() {
    if !enabled() {
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let path = |name: &str| -> PathBuf { dir.path().join(name) };
    let child = Command::new(env!("CARGO_BIN_EXE_remote_server"))
        .arg("run")
        .arg("--log-file")
        .arg(path("server.log"))
        .arg("--pid-file")
        .arg(path("server.pid"))
        .arg("--stdin-socket")
        .arg(path("stdin.sock"))
        .arg("--stdout-socket")
        .arg(path("stdout.sock"))
        .arg("--stderr-socket")
        .arg(path("stderr.sock"))
        .arg("--user-data-dir")
        .arg(path("zed-data"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning run");
    let mut server = RunProcess { child };

    let mut stdin = connect(&path("stdin.sock"));
    let mut stdout = connect(&path("stdout.sock"));
    let _stderr = connect(&path("stderr.sock"));

    stdout
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");

    let started = read_envelope(&mut stdout);
    assert_eq!(started.id, 0);
    assert!(matches!(
        started.payload,
        Some(proto::envelope::Payload::RemoteStarted(_))
    ));

    write_envelope(&mut stdin, &proto::Ping {}.into_envelope(1, None, None));
    let ack = loop {
        let envelope = read_envelope(&mut stdout);
        if envelope.responding_to == Some(1) {
            break envelope;
        }
    };
    assert!(matches!(
        ack.payload,
        Some(proto::envelope::Payload::Ack(_))
    ));

    write_envelope(
        &mut stdin,
        &proto::ShutdownRemoteServer {}.into_envelope(2, None, None),
    );

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match server.child.try_wait().expect("wait") {
            Some(status) => {
                assert!(status.success(), "run exited with {status:?}");
                return;
            }
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            None => panic!("run did not exit within 20 s of ShutdownRemoteServer"),
        }
    }
}
