//! The JSON the shell page hands to `start()` (CONTRACTS.md §8.4, b7 §4.1) and the progress
//! and error vocabulary it receives back. Field names are camelCase on the wire.

use serde::{Deserialize, Serialize};

/// The first argument of `start()`: everything the client needs to boot one workspace.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootConfig {
    /// The bundle's build id (`ZS_BUILD_ID`), for the shell's version check.
    pub build_id: String,
    /// The `/connect` result to dial first.
    pub connect: ConnectInfo,
    /// Which workspace to open.
    pub workspace: WorkspaceTarget,
    /// User `settings.json` document; `""` when the user has none.
    #[serde(default)]
    pub settings_json: String,
    /// User `keymap.json` document; `""` when the user has none.
    #[serde(default)]
    pub keymap_json: String,
    /// Rendering backend preference.
    #[serde(default)]
    pub backend: Backend,
    /// `"mac" | "windows" | "linux"`; `None` = detect from `navigator`.
    #[serde(default)]
    pub host_os: Option<String>,
    /// The control-plane origin (`scheme://host[:port]`) the AI proxy (`/api/ai/*`) and the
    /// keys page live at; `None` = `window.location.origin`, which is what the shell relies
    /// on. In a browser any other value cannot work: the `zs_ai` cookie is sent same-origin
    /// only and the editor CSP allows `connect-src 'self'`, so a foreign origin's requests
    /// are refused before they leave the tab. The field exists for native and unit harnesses
    /// that have no `window` (b11 §3.18).
    #[serde(default)]
    pub origin: Option<String>,
}

/// One `/connect` result (D26). `session_id` is minted per `/connect` and informational
/// (telemetry, logs, `Hello.session_id`); the identity is [`WorkspaceTarget::id`] (D1).
/// `Debug` redacts the token so a logged config or error can never carry it.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectInfo {
    /// `wss://<sandbox-host>/rpc`; may differ on every `/connect`.
    pub ws_url: String,
    /// ES256 session JWT; offered in `Sec-WebSocket-Protocol`, never a query parameter.
    pub token: String,
    /// Per-connect session id; never an identity or a persistence key.
    pub session_id: String,
    /// Close any other attached client with 4001 and attach us (D3).
    #[serde(default)]
    pub takeover: bool,
    /// The server build the control plane expects us to meet.
    #[serde(default)]
    pub server_build: Option<String>,
    /// RFC 3339 expiry of `token`; drives the pre-emptive refresh for HTTP calls.
    #[serde(default)]
    pub session_expires_at: Option<String>,
}

impl std::fmt::Debug for ConnectInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectInfo")
            .field("ws_url", &self.ws_url)
            .field("token", &"<redacted>")
            .field("session_id", &self.session_id)
            .field("takeover", &self.takeover)
            .field("server_build", &self.server_build)
            .field("session_expires_at", &self.session_expires_at)
            .finish()
    }
}

/// Which workspace to open and where.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceTarget {
    /// The control-plane workspace id: the stable identity of D1 (the persistence row,
    /// the connection-pool key, the sandbox name, the save-back key).
    pub id: String,
    /// Absolute sandbox paths, e.g. `["/workspaces/repo"]`; may be empty.
    #[serde(default)]
    pub paths: Vec<String>,
}

/// Rendering backend preference; maps onto `gpui_web::WebBackendPreference`.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    /// WebGPU when available, else WebGL2.
    #[default]
    Auto,
    /// Force WebGPU.
    WebGpu,
    /// Force WebGL2.
    WebGl,
}

/// The stages `ZsHost.bootProgress` reports, in the order they occur, plus the three
/// post-boot stages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootStage {
    /// `start()` was called.
    Booting,
    /// The asset pack is being parsed.
    Assets,
    /// Settings and keymap are being seeded.
    Settings,
    /// Dialing the WebSocket; `detail` carries the transport's status text.
    Connecting,
    /// The client-state image is being loaded and restored.
    Database,
    /// Registries, panels and languages are initializing.
    Languages,
    /// The workspace window is opening.
    Window,
    /// The remote project has opened its paths; also emitted after a successful reconnect.
    Ready,
    /// The transport lost the session and is redialing.
    Reconnecting,
    /// The session ended; `detail` is one of the [`BootError`] codes.
    Stopped,
    /// Boot failed; `detail` is one of the [`BootError`] codes.
    Failed,
}

impl BootStage {
    /// Every stage, in order.
    pub const ALL: [BootStage; 11] = [
        Self::Booting,
        Self::Assets,
        Self::Settings,
        Self::Connecting,
        Self::Database,
        Self::Languages,
        Self::Window,
        Self::Ready,
        Self::Reconnecting,
        Self::Stopped,
        Self::Failed,
    ];

    /// The snake_case name sent to the shell.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Booting => "booting",
            Self::Assets => "assets",
            Self::Settings => "settings",
            Self::Connecting => "connecting",
            Self::Database => "database",
            Self::Languages => "languages",
            Self::Window => "window",
            Self::Ready => "ready",
            Self::Reconnecting => "reconnecting",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

/// The rejection payload of `start()`; `Stopped` details use the same code vocabulary.
///
/// Codes: `bad_config`, `bad_host`, `bad_assets`, `runtime_missing`, `ctors_missing`,
/// `settings`, `connect_failed`, `session_busy` (close 4005), `taken_over` (4001),
/// `incompatible_server` (4002 / 4006), `server_stopping` (1001), `unauthorized`
/// (`RefreshError::Unauthorized`), `workspace_stopped` (`RefreshError::Stopped`),
/// `reconnect_exhausted`, `database`, `window`, `boot_timeout`, `cancelled`, `quit`.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct BootError {
    /// The machine-readable code the shell branches on.
    pub code: &'static str,
    /// A human-readable explanation.
    pub message: String,
}

impl BootError {
    /// An error with `code` and `message`.
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// `cancelled`: the connection was cancelled before the client was ready.
    pub fn cancelled() -> Self {
        Self::new("cancelled", "the connection was cancelled")
    }

    /// `window`: the workspace window could not be opened.
    pub fn window(error: anyhow::Error) -> Self {
        Self::new("window", format!("{error:#}"))
    }

    /// `database`: the client-state image could not be loaded.
    pub fn database(error: anyhow::Error) -> Self {
        Self::new("database", format!("{error:#}"))
    }
}

impl std::fmt::Display for BootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for BootError {}

/// Which of the two user documents `saveDocument` carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DocumentKind {
    /// `settings.json`.
    Settings,
    /// `keymap.json`.
    Keymap,
}

impl DocumentKind {
    /// The name sent to the shell.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Settings => "settings",
            Self::Keymap => "keymap",
        }
    }
}

/// Parses the `start()` configuration document.
pub fn parse_boot_config(json: &str) -> anyhow::Result<BootConfig> {
    serde_json::from_str(json).map_err(|error| anyhow::anyhow!("invalid boot config: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal() {
        let config = parse_boot_config(
            r#"{
                "buildId": "abc1234-0",
                "connect": { "wsUrl": "wss://x/rpc", "token": "t", "sessionId": "con_1" },
                "workspace": { "id": "ws_1", "paths": [] }
            }"#,
        )
        .unwrap();
        assert_eq!(config.build_id, "abc1234-0");
        assert_eq!(config.settings_json, "");
        assert_eq!(config.keymap_json, "");
        assert_eq!(config.backend, Backend::Auto);
        assert_eq!(config.host_os, None);
        assert_eq!(config.origin, None);
        assert!(!config.connect.takeover);
        assert!(config.workspace.paths.is_empty());
        assert_eq!(config.workspace.id, "ws_1");
        assert_eq!(config.connect.session_id, "con_1");
    }

    #[test]
    fn parses_full() {
        let config = parse_boot_config(
            r#"{
                "buildId": "abc1234-0",
                "connect": { "wsUrl": "wss://x/rpc", "token": "t", "sessionId": "con_1",
                             "takeover": true, "serverBuild": "abc1234-0",
                             "sessionExpiresAt": "2026-09-03T00:00:00Z" },
                "workspace": { "id": "ws_1", "paths": ["/workspaces/repo"] },
                "settingsJson": "{}", "keymapJson": "[]", "backend": "webgl", "hostOs": "windows",
                "origin": "https://zs.example.com"
            }"#,
        )
        .unwrap();
        assert!(config.connect.takeover);
        assert_eq!(config.backend, Backend::WebGl);
        assert_eq!(config.host_os.as_deref(), Some("windows"));
        assert_eq!(config.origin.as_deref(), Some("https://zs.example.com"));
        assert_eq!(config.workspace.paths, vec!["/workspaces/repo".to_string()]);
    }

    #[test]
    fn debug_redacts_the_token() {
        let config = parse_boot_config(
            r#"{
                "buildId": "abc1234-0",
                "connect": { "wsUrl": "wss://x/rpc", "token": "sekrit-jwt", "sessionId": "con_1" },
                "workspace": { "id": "ws_1" }
            }"#,
        )
        .unwrap();
        let rendered = format!("{:?} {:?}", config, config.connect);
        assert!(!rendered.contains("sekrit-jwt"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(rendered.contains("con_1"), "{rendered}");
    }

    #[test]
    fn rejects_missing_connect() {
        let error = parse_boot_config(r#"{ "buildId": "x", "workspace": { "id": "w" } }"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("connect"), "{error}");
    }

    #[test]
    fn boot_stage_names_are_snake_case() {
        let mut seen = std::collections::HashSet::new();
        for stage in BootStage::ALL {
            let name = stage.as_str();
            assert!(
                name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{name}"
            );
            assert!(seen.insert(name), "duplicate stage name {name}");
        }
        let json = serde_json::to_string(&BootError::new("bad_config", "nope")).unwrap();
        assert_eq!(json, r#"{"code":"bad_config","message":"nope"}"#);
        assert_eq!(DocumentKind::Settings.as_str(), "settings");
        assert_eq!(DocumentKind::Keymap.as_str(), "keymap");
    }
}
