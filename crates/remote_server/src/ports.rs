//! Server side of `ForwardPort` / `UnforwardPort`: a thin client for the supervisor's
//! loopback API (BUILD-SPEC 5.2, D8, D21). The supervisor is what talks to the control
//! plane; the server never holds a control-plane credential.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use futures::AsyncReadExt as _;
use gpui::{AsyncApp, Entity};
use http_client::{AsyncBody, HttpClient, Method, Request};
use rpc::{TypedEnvelope, proto};
use serde::{Deserialize, Serialize};

use crate::HeadlessProject;

/// Default base URL of the supervisor's loopback API (D21); `--supervisor-url` /
/// `ZS_SUPERVISOR_URL` override it.
pub const DEFAULT_SUPERVISOR_URL: &str = "http://127.0.0.1:8450";
/// `POST` to forward, `DELETE {path}/{port}` to unforward.
pub const SUPERVISOR_PORTS_PATH: &str = "/ports";
/// `POST` with the installed-extension set (relayed to the control plane by the supervisor).
pub const SUPERVISOR_EXTENSIONS_PATH: &str = "/extensions";
/// Longest `ForwardPort.label` forwarded to the supervisor (and on to the control plane's
/// `forwards` table); longer or non-printable labels are refused before any HTTP call.
pub const MAX_LABEL_BYTES: usize = 256;

/// Calls the supervisor for the client's port requests.
pub struct PortForwarder {
    /// Proxy-less client: the session client may carry the user's proxy settings, and
    /// loopback traffic must never be routed through a proxy.
    http: Arc<dyn HttpClient>,
    base_url: String,
    secret: Vec<u8>,
}

#[derive(Serialize)]
struct ForwardRequestBody<'a> {
    port: u16,
    visibility: &'a str,
    label: Option<&'a str>,
}

#[derive(Deserialize)]
struct ForwardResponseBody {
    url: Option<String>,
}

#[derive(Serialize)]
struct InstalledExtensionsBody<'a> {
    installed: &'a [&'a str],
}

#[derive(Deserialize)]
struct SupervisorError {
    error: String,
}

impl PortForwarder {
    /// A forwarder over `http` against `base_url`, authenticating with the control `secret`.
    pub fn new(http: Arc<dyn HttpClient>, base_url: String, secret: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            secret,
        })
    }

    /// The supervisor base URL this forwarder targets.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `POST {base}/ports` → the forward's opaque URL (D8); `""` when the supervisor
    /// returned `null`. A refusal (no free private slot, D8) surfaces as `Err` carrying the
    /// supervisor's `error` text.
    pub async fn forward(
        &self,
        port: u16,
        visibility: proto::PortVisibility,
        label: Option<String>,
    ) -> Result<String> {
        let visibility = match visibility {
            proto::PortVisibility::PortPublic => "public",
            proto::PortVisibility::PortPrivate => "private",
        };
        let body = serde_json::to_string(&ForwardRequestBody {
            port,
            visibility,
            label: label.as_deref(),
        })?;
        let response = self
            .send(Method::POST, SUPERVISOR_PORTS_PATH, Some(body))
            .await
            .with_context(|| format!("forwarding port {port}"))?;
        let parsed: ForwardResponseBody = serde_json::from_slice(&response)
            .context("parsing the supervisor's forward response")?;
        Ok(parsed.url.unwrap_or_default())
    }

    /// `DELETE {base}/ports/{port}`.
    pub async fn unforward(&self, port: u16) -> Result<()> {
        self.send(
            Method::DELETE,
            &format!("{SUPERVISOR_PORTS_PATH}/{port}"),
            None,
        )
        .await
        .with_context(|| format!("unforwarding port {port}"))?;
        Ok(())
    }

    /// `POST {base}/extensions {installed}`; the supervisor relays the set to the control
    /// plane so a rebuild reinstalls it (D18, D19).
    pub async fn report_installed_extensions(&self, ids: &[&str]) -> Result<()> {
        let body = serde_json::to_string(&InstalledExtensionsBody { installed: ids })?;
        self.send(Method::POST, SUPERVISOR_EXTENSIONS_PATH, Some(body))
            .await
            .context("reporting installed extensions to the supervisor")?;
        Ok(())
    }

    /// Sends one authenticated JSON request and returns the body of a 2xx response; a
    /// non-2xx becomes `Err` carrying the supervisor's `error` field (or the status).
    async fn send(&self, method: Method, path: &str, body: Option<String>) -> Result<Vec<u8>> {
        let secret = String::from_utf8_lossy(&self.secret).into_owned();
        let mut builder = Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.base_url))
            .header("Authorization", format!("Bearer {secret}"));
        let body = match body {
            Some(body) => {
                builder = builder.header("Content-Type", "application/json");
                AsyncBody::from(body)
            }
            None => AsyncBody::empty(),
        };
        let request = builder.body(body)?;
        let mut response = self.http.send(request).await?;
        let mut bytes = Vec::new();
        response
            .body_mut()
            .read_to_end(&mut bytes)
            .await
            .context("reading the supervisor's response")?;
        let status = response.status();
        if !status.is_success() {
            let detail = serde_json::from_slice::<SupervisorError>(&bytes)
                .map(|error| error.error)
                .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
            anyhow::bail!("supervisor answered {}: {detail}", status.as_u16());
        }
        Ok(bytes)
    }

    /// `ForwardPort` handler; `Err` when this server has no sandbox runtime (SSH `run`
    /// mode) or the port is out of range.
    pub async fn handle_forward_port(
        this: Entity<HeadlessProject>,
        envelope: TypedEnvelope<proto::ForwardPort>,
        cx: AsyncApp,
    ) -> Result<proto::ForwardPortResponse> {
        let ports = forwarder(&this, &cx)?;
        let port = valid_port(envelope.payload.port)?;
        let label = valid_label(envelope.payload.label)?;
        let visibility = proto::PortVisibility::try_from(envelope.payload.visibility)
            .unwrap_or(proto::PortVisibility::PortPrivate);
        let url = ports.forward(port, visibility, label).await?;
        Ok(proto::ForwardPortResponse { url })
    }

    /// `UnforwardPort` handler; same preconditions as `handle_forward_port`.
    pub async fn handle_unforward_port(
        this: Entity<HeadlessProject>,
        envelope: TypedEnvelope<proto::UnforwardPort>,
        cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let ports = forwarder(&this, &cx)?;
        let port = valid_port(envelope.payload.port)?;
        ports.unforward(port).await?;
        Ok(proto::Ack {})
    }
}

fn forwarder(this: &Entity<HeadlessProject>, cx: &AsyncApp) -> Result<Arc<PortForwarder>> {
    this.read_with(cx, |project, _| {
        project
            .sandbox
            .as_ref()
            .map(|sandbox| sandbox.ports.clone())
    })
    .context("port forwarding is not available on this server")
}

fn valid_port(port: u32) -> Result<u16> {
    u16::try_from(port)
        .ok()
        .filter(|port| *port >= 1)
        .with_context(|| format!("port {port} is out of range (1..=65535)"))
}

/// A label is at most [`MAX_LABEL_BYTES`] of printable text; an empty label is `None`.
fn valid_label(label: Option<String>) -> Result<Option<String>> {
    let Some(label) = label else {
        return Ok(None);
    };
    anyhow::ensure!(
        label.len() <= MAX_LABEL_BYTES,
        "label is {} bytes, over the {MAX_LABEL_BYTES} byte limit",
        label.len()
    );
    anyhow::ensure!(
        !label.chars().any(char::is_control),
        "label contains control characters"
    );
    Ok(Some(label).filter(|label| !label.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_editing_tests::{SandboxHarness, init_test};
    use fs::FakeFs;
    use gpui::TestAppContext;
    use http_client::{FakeHttpClient, HttpClientWithUrl, Response};
    use std::{sync::Mutex, time::Instant};

    /// A captured supervisor request.
    #[derive(Debug, Clone)]
    struct Captured {
        method: String,
        path: String,
        bearer: Option<String>,
        content_type: Option<String>,
        body: String,
    }

    fn fake_supervisor(
        status: u16,
        response_body: &'static str,
    ) -> (Arc<HttpClientWithUrl>, Arc<Mutex<Vec<Captured>>>) {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let client = FakeHttpClient::create({
            let captured = captured.clone();
            move |request| {
                let captured = captured.clone();
                async move {
                    let method = request.method().to_string();
                    let path = request.uri().path().to_owned();
                    let bearer = request
                        .headers()
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let content_type = request
                        .headers()
                        .get("content-type")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let mut body = Vec::new();
                    request.into_body().read_to_end(&mut body).await?;
                    captured.lock().unwrap().push(Captured {
                        method,
                        path,
                        bearer,
                        content_type,
                        body: String::from_utf8_lossy(&body).into_owned(),
                    });
                    Ok(Response::builder()
                        .status(status)
                        .body(AsyncBody::from(response_body))?)
                }
            }
        });
        (client, captured)
    }

    fn forwarder_over(client: Arc<HttpClientWithUrl>) -> Arc<PortForwarder> {
        PortForwarder::new(client, "http://127.0.0.1:8450/".into(), b"secret".to_vec())
    }

    #[gpui::test]
    async fn forward_posts_expected_json_and_returns_url() {
        let (client, captured) = fake_supervisor(200, r#"{"url":"https://x.vercel.run"}"#);
        let url = forwarder_over(client)
            .forward(3000, proto::PortVisibility::PortPublic, Some("web".into()))
            .await
            .unwrap();
        assert_eq!(url, "https://x.vercel.run");
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].method, "POST");
        assert_eq!(captured[0].path, "/ports");
        assert_eq!(captured[0].bearer.as_deref(), Some("Bearer secret"));
        assert_eq!(
            captured[0].content_type.as_deref(),
            Some("application/json")
        );
        let body: serde_json::Value = serde_json::from_str(&captured[0].body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"port": 3000, "visibility": "public", "label": "web"})
        );
    }

    #[gpui::test]
    async fn forward_maps_null_url_to_empty() {
        let (client, _) = fake_supervisor(200, r#"{"url":null}"#);
        let url = forwarder_over(client)
            .forward(3000, proto::PortVisibility::PortPrivate, None)
            .await
            .unwrap();
        assert_eq!(url, "");
    }

    #[gpui::test]
    async fn forward_refusal_surfaces_supervisor_error() {
        let (client, _) = fake_supervisor(409, r#"{"error":"no_private_slot"}"#);
        let error = forwarder_over(client)
            .forward(3000, proto::PortVisibility::PortPrivate, None)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("no_private_slot"),
            "{error:#}"
        );
    }

    #[gpui::test]
    async fn unforward_sends_delete() {
        let (client, captured) = fake_supervisor(204, "");
        forwarder_over(client).unforward(3000).await.unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(captured[0].method, "DELETE");
        assert_eq!(captured[0].path, "/ports/3000");
        assert_eq!(captured[0].bearer.as_deref(), Some("Bearer secret"));
    }

    #[gpui::test]
    async fn report_installed_extensions_posts_ids() {
        let (client, captured) = fake_supervisor(204, "");
        forwarder_over(client)
            .report_installed_extensions(&["toml", "html"])
            .await
            .unwrap();
        let captured = captured.lock().unwrap();
        assert_eq!(captured[0].method, "POST");
        assert_eq!(captured[0].path, "/extensions");
        let body: serde_json::Value = serde_json::from_str(&captured[0].body).unwrap();
        assert_eq!(body, serde_json::json!({"installed": ["toml", "html"]}));
    }

    #[gpui::test]
    async fn supervisor_error_propagates() {
        let (client, _) = fake_supervisor(500, "boom");
        let error = forwarder_over(client)
            .report_installed_extensions(&["toml"])
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("500"), "{error:#}");
    }

    fn envelope<T>(payload: T) -> TypedEnvelope<T> {
        TypedEnvelope {
            sender_id: proto::REMOTE_SERVER_PEER_ID,
            original_sender_id: None,
            message_id: 1,
            payload,
            received_at: Instant::now(),
        }
    }

    #[gpui::test]
    async fn forward_without_sandbox_errors(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let fs = FakeFs::new(server_cx.executor());
        let (_project, headless) = init_test(&fs, cx, server_cx).await;
        let error = PortForwarder::handle_forward_port(
            headless,
            envelope(proto::ForwardPort {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                port: 3000,
                visibility: proto::PortVisibility::PortPublic as i32,
                label: None,
            }),
            server_cx.to_async(),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("not available"), "{error:#}");
    }

    #[gpui::test]
    async fn forward_rejects_port_out_of_range(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let fs = FakeFs::new(server_cx.executor());
        let (project, headless) = init_test(&fs, cx, server_cx).await;
        let (supervisor, captured) = fake_supervisor(200, r#"{"url":"https://x"}"#);
        let _harness = SandboxHarness::enable_with_supervisor(
            project,
            headless.clone(),
            "s",
            supervisor,
            server_cx,
        );
        for port in [0u32, 70_000] {
            let error = PortForwarder::handle_forward_port(
                headless.clone(),
                envelope(proto::ForwardPort {
                    project_id: proto::REMOTE_SERVER_PROJECT_ID,
                    port,
                    visibility: proto::PortVisibility::PortPublic as i32,
                    label: None,
                }),
                server_cx.to_async(),
            )
            .await
            .unwrap_err();
            assert!(format!("{error:#}").contains("out of range"), "{error:#}");
        }
        for label in ["x".repeat(MAX_LABEL_BYTES + 1), "line\nbreak".to_owned()] {
            let error = PortForwarder::handle_forward_port(
                headless.clone(),
                envelope(proto::ForwardPort {
                    project_id: proto::REMOTE_SERVER_PROJECT_ID,
                    port: 3000,
                    visibility: proto::PortVisibility::PortPublic as i32,
                    label: Some(label),
                }),
                server_cx.to_async(),
            )
            .await
            .unwrap_err();
            assert!(format!("{error:#}").contains("label"), "{error:#}");
        }
        assert!(captured.lock().unwrap().is_empty(), "no HTTP call was made");

        let response = PortForwarder::handle_forward_port(
            headless,
            envelope(proto::ForwardPort {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                port: 3000,
                visibility: proto::PortVisibility::PortPublic as i32,
                label: Some("x".repeat(MAX_LABEL_BYTES)),
            }),
            server_cx.to_async(),
        )
        .await
        .unwrap();
        assert_eq!(response.url, "https://x");
        assert_eq!(captured.lock().unwrap().len(), 1);
    }
}
