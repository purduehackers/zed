//! Wire definitions shared by the WebSocket transport and `zed-remote-server serve`.
//!
//! Text frames carry JSON [`ControlFrame`]s; binary frames carry one length-prefixed
//! `Envelope` each (`crate::protocol::encode_envelope_frame`). The first frame in each
//! direction after the upgrade is a control frame: [`Hello`] from the client, [`HelloAck`]
//! (or a close) from the server. This module depends on `serde` only so the server can
//! import it as `remote::websocket_wire`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Version of the control-frame protocol carried in [`Hello::protocol`] / [`HelloAck::protocol`].
pub const PROTOCOL_VERSION: u32 = 1;

/// The WebSocket subprotocol name offered first in `Sec-WebSocket-Protocol` and echoed by the
/// server.
pub const SUBPROTOCOL: &str = "zs.v1";

/// Inbound frame ceiling on both sides (D3): 16 MiB, length prefix included.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Cadence of the server → client [`ControlFrame::Heartbeat`] (D22). The client treats each
/// one as connection activity; `remote_client.rs`'s own proto `Ping` cadence is private to
/// that file, hence the separate constant.
pub const HEARTBEAT_INTERVAL_SECS: u64 = 5;

/// Server going away: SIGTERM or lifecycle `STOPPING`. Terminal for the client, which never
/// refreshes on it (a reconnect must not resume a stopped workspace). Also synthesized
/// client-side after `RefreshError::Stopped`.
pub const CLOSE_GOING_AWAY: u16 = 1001;
/// Policy violation (server-side slow consumer).
pub const CLOSE_POLICY_VIOLATION: u16 = 1008;
/// Frame over [`MAX_FRAME_BYTES`].
pub const CLOSE_FRAME_TOO_LARGE: u16 = 1009;
/// Superseded: the participant reloaded, a same-instance redial replaced
/// this socket, or the server rejected a stale epoch (reason [`CLOSE_REASON_STALE_EPOCH`]).
pub const CLOSE_TAKEN_OVER: u16 = 4001;
/// Close reason the server sends with [`CLOSE_TAKEN_OVER`] when it refuses a reconnect whose
/// `Hello.epoch` is stale (D23). Nobody took the session over in that case; the client maps
/// it to "fresh session needed" (exit 90) rather than to "taken over" (exit 91), so a shell
/// that offers a take-back flow for 4001 should check this reason first.
pub const CLOSE_REASON_STALE_EPOCH: &str = "stale epoch";
/// The client build is not compatible with the server build.
pub const CLOSE_BUILD_MISMATCH: u16 = 4002;
/// Unauthorized after the upgrade. Also synthesized client-side after
/// `RefreshError::Unauthorized`; the client refreshes and redials on a server-sent one.
pub const CLOSE_UNAUTHORIZED: u16 = 4003;
/// No or malformed `Hello` within the server's hello timeout, or a protocol mismatch.
pub const CLOSE_BAD_HELLO: u16 = 4006;

/// The build id baked into this binary at compile time (`crates/remote/build.rs` keeps it
/// fresh). `None` for local builds, which report a `dev-` prefixed id and are compatible
/// with everything.
pub const ZS_BUILD_ID: Option<&str> = option_env!("ZS_BUILD_ID");

/// Prefix of build ids that skip the exact-match check in [`builds_compatible`].
pub const DEV_BUILD_PREFIX: &str = "dev";

/// The session token contains characters that cannot appear in a `Sec-WebSocket-Protocol`
/// item (RFC 6455 requires each subprotocol name to be an RFC 2616 token).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidToken;

impl fmt::Display for InvalidToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("session token is not a valid WebSocket subprotocol token")
    }
}

impl std::error::Error for InvalidToken {}

/// Checks that `token` can be offered as a subprotocol name.
///
/// RFC 6455 requires each subprotocol name to be an RFC 2616 token (no `=`, `/`, spaces).
/// The accepted set here is narrower still — the base64url alphabet plus `.`, exactly what a
/// base64url-without-padding JWT is made of — so a standard-base64 or otherwise opaque token
/// is rejected before it can fail inside an HTTP header builder or be silently mangled by a
/// browser.
pub fn validate_subprotocol_token(token: &str) -> Result<(), InvalidToken> {
    if token.is_empty() {
        return Err(InvalidToken);
    }
    let valid = token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid { Ok(()) } else { Err(InvalidToken) }
}

/// The `Sec-WebSocket-Protocol` header value for the native client: `"zs.v1, <token>"`.
pub fn subprotocol_header_value(token: &str) -> Result<String, InvalidToken> {
    validate_subprotocol_token(token)?;
    Ok(format!("{SUBPROTOCOL}, {token}"))
}

/// The subprotocol list for the browser's `new WebSocket(url, protocols)`: `["zs.v1", token]`.
pub fn subprotocols(token: &str) -> Result<[&str; 2], InvalidToken> {
    validate_subprotocol_token(token)?;
    Ok([SUBPROTOCOL, token])
}

/// Server side: splits a `Sec-WebSocket-Protocol` header value into `(SUBPROTOCOL, token)`.
/// Returns `None` when the first item is not [`SUBPROTOCOL`] or no token follows it.
pub fn split_subprotocol_header(value: &str) -> Option<(&str, &str)> {
    let mut items = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty());
    let protocol = items.next()?;
    if protocol != SUBPROTOCOL {
        return None;
    }
    let token = items.next()?;
    validate_subprotocol_token(token).ok()?;
    Some((protocol, token))
}

/// Whether `build` is a local development build (no `ZS_BUILD_ID`, see [`ZS_BUILD_ID`]).
pub fn is_dev_build(build: &str) -> bool {
    build.starts_with(DEV_BUILD_PREFIX)
}

/// Exact match unless either side is a dev build.
pub fn builds_compatible(client: &str, server: &str) -> bool {
    client == server || is_dev_build(client) || is_dev_build(server)
}

/// JSON control message carried in a text frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlFrame {
    /// Client → server, first frame.
    Hello(Hello),
    /// Server → client, first frame.
    HelloAck(HelloAck),
    /// Server → client, any time: a log record mirrored to the client.
    Log(LogFrame),
    /// Server → client every [`HEARTBEAT_INTERVAL_SECS`] (D22); activity only on the client.
    Heartbeat,
}

/// The client's first frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// [`PROTOCOL_VERSION`].
    pub protocol: u32,
    /// The client build id (`client_build_id`).
    pub build: String,
    /// Stable workspace identity (D1); equals the JWT `ws` claim.
    pub workspace_id: String,
    /// Per-connect id from the same `/connect` as the token (D1); equals the JWT `sid` claim.
    pub session_id: String,
    /// The `RemoteClient` unique identifier (`setup-N` / `workspace-N`); informational.
    pub identifier: String,
    /// Per-boot nonce within one signed participant. Reloading replaces only that
    /// participant's connection; other participants have independent brokers.
    pub instance: String,
    /// `start_proxy`'s `reconnect` flag: `true` when resuming an existing session.
    pub reconnect: bool,
    /// Which kind of client this is.
    pub client: ClientKind,
    /// On `reconnect`, the [`HelloAck::epoch`] of the attachment being resumed. The server
    /// attaches warm and replays only when `reconnect` is true, this equals its current epoch
    /// and it still holds the state; otherwise it starts a fresh session (D24).
    pub epoch: Option<u64>,
}

/// The kind of client behind a [`Hello`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    /// Native desktop Zed.
    Desktop,
    /// The browser build.
    Web,
}

/// The server's first frame after a successful [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloAck {
    /// The sandbox-assigned Zed CRDT replica for this participant.
    pub replica_id: u16,
    /// [`PROTOCOL_VERSION`].
    pub protocol: u32,
    /// The server build id.
    pub build: String,
    /// `std::env::consts::OS` of the server, e.g. `"linux"`.
    pub os: String,
    /// `std::env::consts::ARCH` of the server, e.g. `"x86_64"` or `"aarch64"`.
    pub arch: String,
    /// Human-readable OS version (`/etc/os-release` derived), if known.
    pub os_version: Option<String>,
    /// `$SHELL` of the server process, or `/bin/sh`.
    pub shell: String,
    /// `true` iff this is a warm reconnect (state kept, replay follows); `false` for every
    /// fresh session.
    pub resumed: bool,
    /// Echo of [`Hello::session_id`].
    pub session_id: String,
    /// Incremented on every fresh session; unchanged on a resumed reconnect.
    pub epoch: u64,
}

/// A server log record mirrored to the client; same fields as `json_log::LogRecord`, owned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFrame {
    /// Numeric level as serialized by `json_log` (1 = error … 5 = trace).
    pub level: usize,
    /// The originating module path, if known.
    pub module_path: Option<String>,
    /// The originating file, if known.
    pub file: Option<String>,
    /// The originating line, if known.
    pub line: Option<u32>,
    /// The formatted message.
    pub message: String,
}

impl LogFrame {
    /// Emits this record through `logger`, as the stdio transport does for server stderr.
    pub fn log(&self, logger: &dyn log::Log) {
        crate::json_log::LogRecord {
            level: self.level,
            module_path: self.module_path.as_deref().map(std::borrow::Cow::Borrowed),
            file: self.file.as_deref().map(std::borrow::Cow::Borrowed),
            line: self.line,
            message: std::borrow::Cow::Borrowed(&self.message),
        }
        .log(logger)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_helpers() {
        assert_eq!(subprotocol_header_value("t").unwrap(), "zs.v1, t");
        assert_eq!(subprotocols("t").unwrap(), ["zs.v1", "t"]);
        assert_eq!(
            subprotocol_header_value("eyJh.eyJz-_.sig").unwrap(),
            "zs.v1, eyJh.eyJz-_.sig"
        );
        for bad in ["a=b", "a+b", "a b", "", "a/b", "a,b"] {
            assert_eq!(subprotocol_header_value(bad), Err(InvalidToken), "{bad:?}");
            assert_eq!(subprotocols(bad), Err(InvalidToken), "{bad:?}");
        }

        assert_eq!(split_subprotocol_header("zs.v1, t"), Some(("zs.v1", "t")));
        assert_eq!(split_subprotocol_header("zs.v1,t"), Some(("zs.v1", "t")));
        assert_eq!(split_subprotocol_header("zs.v1"), None);
        assert_eq!(split_subprotocol_header("zs.v1, "), None);
        assert_eq!(split_subprotocol_header("other, t"), None);
        assert_eq!(split_subprotocol_header(""), None);
    }

    #[test]
    fn control_frames_round_trip() {
        let hello = ControlFrame::Hello(Hello {
            protocol: PROTOCOL_VERSION,
            build: "abc-1".into(),
            workspace_id: "ws_1".into(),
            session_id: "sess_1".into(),
            identifier: "setup-1".into(),
            instance: "1a2b-1".into(),
            reconnect: false,
            client: ClientKind::Web,
            epoch: None,
        });
        let json = serde_json::to_string(&hello).unwrap();
        assert!(json.starts_with(r#"{"type":"hello","#), "{json}");
        assert!(json.contains(r#""client":"web""#), "{json}");
        assert!(json.contains(r#""epoch":null"#), "{json}");
        assert_eq!(serde_json::from_str::<ControlFrame>(&json).unwrap(), hello);

        let ack = ControlFrame::HelloAck(HelloAck {
            replica_id: 8,
            protocol: PROTOCOL_VERSION,
            build: "abc-1".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            os_version: Some("ubuntu 24.04".into()),
            shell: "/bin/bash".into(),
            resumed: true,
            session_id: "sess_1".into(),
            epoch: 3,
        });
        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.starts_with(r#"{"type":"hello_ack","#), "{json}");
        assert_eq!(serde_json::from_str::<ControlFrame>(&json).unwrap(), ack);

        let log = ControlFrame::Log(LogFrame {
            level: 2,
            module_path: None,
            file: None,
            line: None,
            message: "careful".into(),
        });
        let json = serde_json::to_string(&log).unwrap();
        assert!(json.starts_with(r#"{"type":"log","#), "{json}");
        assert_eq!(serde_json::from_str::<ControlFrame>(&json).unwrap(), log);

        assert_eq!(
            serde_json::to_string(&ControlFrame::Heartbeat).unwrap(),
            r#"{"type":"heartbeat"}"#
        );
        assert_eq!(
            serde_json::from_str::<ControlFrame>(r#"{"type":"heartbeat"}"#).unwrap(),
            ControlFrame::Heartbeat
        );

        assert!(serde_json::from_str::<ControlFrame>(r#"{"type":"bogus"}"#).is_err());

        let without_workspace_id = r#"{"type":"hello","protocol":1,"build":"b","session_id":"s","identifier":"i","instance":"n","reconnect":false,"client":"desktop","epoch":null}"#;
        assert!(serde_json::from_str::<ControlFrame>(without_workspace_id).is_err());

        let with_epoch = r#"{"type":"hello","protocol":1,"build":"b","workspace_id":"w","session_id":"s","identifier":"i","instance":"n","reconnect":true,"client":"desktop","epoch":7}"#;
        match serde_json::from_str::<ControlFrame>(with_epoch).unwrap() {
            ControlFrame::Hello(hello) => assert_eq!(hello.epoch, Some(7)),
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[test]
    fn build_compatibility() {
        assert!(builds_compatible("abc-1", "abc-1"));
        assert!(!builds_compatible("abc-1", "abc-2"));
        assert!(builds_compatible("dev-0.1.0", "abc-2"));
        assert!(builds_compatible("abc-1", "dev"));
    }

    #[test]
    fn constants_are_pinned() {
        assert_eq!(CLOSE_GOING_AWAY, 1001);
        assert_eq!(CLOSE_TAKEN_OVER, 4001);
        assert_eq!(CLOSE_BUILD_MISMATCH, 4002);
        assert_eq!(CLOSE_UNAUTHORIZED, 4003);
        assert_eq!(CLOSE_BAD_HELLO, 4006);
        assert_eq!(HEARTBEAT_INTERVAL_SECS, 5);
        assert_eq!(MAX_FRAME_BYTES, 16 * 1024 * 1024);
        assert_eq!(PROTOCOL_VERSION, 1);
        assert_eq!(SUBPROTOCOL, "zs.v1");
    }
}
