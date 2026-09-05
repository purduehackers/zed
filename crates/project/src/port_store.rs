//! Client side of port forwarding (BUILD-SPEC 5.2, D8): the listening ports the supervisor
//! reports and the current forwards, plus the requests that create and remove forwards.
//! The ports panel consumes this; it is not implemented here.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use gpui::{AsyncApp, Context, Entity, EventEmitter, Task};
use rpc::{AnyProtoClient, TypedEnvelope, proto};

/// A TCP port something in the sandbox listens on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListeningPort {
    /// The port.
    pub port: u16,
    /// The listening process.
    pub pid: u32,
    /// Its name.
    pub process_name: String,
}

/// Who may reach a forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortVisibility {
    /// Cookie-gated through a proxy slot (D8).
    Private,
    /// Anyone with the URL.
    Public,
}

impl PortVisibility {
    /// The wire value.
    pub fn to_proto(self) -> proto::PortVisibility {
        match self {
            Self::Private => proto::PortVisibility::PortPrivate,
            Self::Public => proto::PortVisibility::PortPublic,
        }
    }

    /// Decodes a wire value; unknown values are private, the safer reading.
    pub fn from_proto(visibility: i32) -> Self {
        match proto::PortVisibility::try_from(visibility) {
            Ok(proto::PortVisibility::PortPublic) => Self::Public,
            Ok(proto::PortVisibility::PortPrivate) | Err(_) => Self::Private,
        }
    }
}

/// An active forward.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortForward {
    /// The forwarded port.
    pub port: u16,
    /// Who may reach it.
    pub visibility: PortVisibility,
    /// Optional label.
    pub label: Option<String>,
    /// Opaque URL to open as a top-level navigation: the slot URL for a public forward, the
    /// control plane's `/open` link for a private one (D8); empty when not yet known.
    pub url: String,
}

impl PortForward {
    fn from_proto(forward: proto::PortForward) -> Option<Self> {
        Some(Self {
            port: u16::try_from(forward.port).ok()?,
            visibility: PortVisibility::from_proto(forward.visibility),
            label: forward.label,
            url: sanitize_forward_url(&forward.url),
        })
    }
}

/// Forward URLs are opened as top-level navigations (`cx.open_url`), and the server that
/// sends them runs inside the sandbox, where any process could have produced the string.
/// Only web URLs pass: `https` on any host, `http` on a loopback host (local supervisors and
/// tests). Everything else (`javascript:`, `file:`, `zed:`, custom schemes, unparsable
/// text) becomes `""`, the "not yet known" value, with a warning.
pub fn sanitize_forward_url(url: &str) -> String {
    if url.is_empty() {
        return String::new();
    }
    let parsed = match url::Url::parse(url) {
        Ok(parsed) => parsed,
        Err(error) => {
            log::warn!("dropping unparsable forward url {url:?}: {error}");
            return String::new();
        }
    };
    let allowed = match parsed.scheme() {
        "https" => parsed.host().is_some(),
        "http" => matches!(
            parsed.host(),
            Some(url::Host::Domain("localhost"))
                | Some(url::Host::Ipv4(std::net::Ipv4Addr::LOCALHOST))
                | Some(url::Host::Ipv6(std::net::Ipv6Addr::LOCALHOST))
        ),
        _ => false,
    };
    if allowed {
        url.to_owned()
    } else {
        log::warn!("dropping forward url {url:?}: only https (or loopback http) is opened");
        String::new()
    }
}

/// The client's view of the sandbox's ports.
pub struct PortStore {
    client: AnyProtoClient,
    project_id: u64,
    listening: Vec<ListeningPort>,
    forwards: BTreeMap<u16, PortForward>,
}

/// Events a [`PortStore`] emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortStoreEvent {
    /// Either list changed.
    Changed,
}

impl EventEmitter<PortStoreEvent> for PortStore {}

impl PortStore {
    /// A store over the remote server session.
    pub fn remote(client: AnyProtoClient, project_id: u64) -> Self {
        Self {
            client,
            project_id,
            listening: Vec::new(),
            forwards: BTreeMap::new(),
        }
    }

    /// Registers the server-to-client handler.
    pub fn init(client: &AnyProtoClient) {
        client.add_entity_message_handler(Self::handle_ports_changed);
    }

    /// Ports the supervisor last reported as listening.
    pub fn listening_ports(&self) -> &[ListeningPort] {
        &self.listening
    }

    /// Current forwards, by port.
    pub fn forwards(&self) -> impl Iterator<Item = &PortForward> {
        self.forwards.values()
    }

    /// The forward for `port`, if any.
    pub fn forward(&self, port: u16) -> Option<&PortForward> {
        self.forwards.get(&port)
    }

    /// Asks the server to forward `port`. The result is recorded at once; the supervisor's
    /// next `PortsChanged` carries the authoritative picture.
    pub fn forward_port(
        &self,
        port: u16,
        visibility: PortVisibility,
        label: Option<String>,
        cx: &mut Context<Self>,
    ) -> Task<Result<PortForward>> {
        let request = self.client.request(proto::ForwardPort {
            project_id: self.project_id,
            port: u32::from(port),
            visibility: visibility.to_proto() as i32,
            label: label.clone(),
        });
        cx.spawn(async move |this, cx| {
            let response = request
                .await
                .with_context(|| format!("forwarding port {port}"))?;
            let forward = PortForward {
                port,
                visibility,
                label,
                url: sanitize_forward_url(&response.url),
            };
            this.update(cx, |this, cx| {
                this.forwards.insert(port, forward.clone());
                cx.emit(PortStoreEvent::Changed);
            })?;
            Ok(forward)
        })
    }

    /// Asks the server to remove the forward for `port`.
    pub fn unforward_port(&self, port: u16, cx: &mut Context<Self>) -> Task<Result<()>> {
        let request = self.client.request(proto::UnforwardPort {
            project_id: self.project_id,
            port: u32::from(port),
        });
        cx.spawn(async move |this, cx| {
            request
                .await
                .with_context(|| format!("unforwarding port {port}"))?;
            this.update(cx, |this, cx| {
                if this.forwards.remove(&port).is_some() {
                    cx.emit(PortStoreEvent::Changed);
                }
            })?;
            Ok(())
        })
    }

    async fn handle_ports_changed(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::PortsChanged>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        let listening = envelope
            .payload
            .ports
            .into_iter()
            .filter_map(|port| {
                Some(ListeningPort {
                    port: u16::try_from(port.port).ok()?,
                    pid: port.pid,
                    process_name: port.process_name,
                })
            })
            .collect();
        let forwards = envelope
            .payload
            .forwards
            .into_iter()
            .filter_map(PortForward::from_proto)
            .map(|forward| (forward.port, forward))
            .collect();
        this.update(&mut cx, |this, cx| {
            this.listening = listening;
            this.forwards = forwards;
            cx.emit(PortStoreEvent::Changed);
        });
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Project;
    use client::{Client, UserStore};
    use clock::FakeSystemClock;
    use fs::FakeFs;
    use gpui::{AppContext as _, TestAppContext};
    use http_client::FakeHttpClient;
    use language::LanguageRegistry;
    use node_runtime::NodeRuntime;
    use remote::RemoteClient;
    use settings::SettingsStore;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    /// The server side of a bare mock session (no `HeadlessProject`): the session, plus
    /// the entity test handlers are registered on (it must stay alive with the session).
    pub(crate) struct MockServer {
        pub session: AnyProtoClient,
        pub entity: Entity<()>,
    }

    /// A remote project over a bare mock server that only answers the handshake `Ping`.
    pub(crate) async fn remote_test_project(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) -> (Entity<Project>, MockServer) {
        let fs = FakeFs::new(cx.executor());
        remote_test_project_with_fs(cx, server_cx, fs).await
    }

    /// [`remote_test_project`] over a caller-provided filesystem (for the global config
    /// files the project reads at creation).
    pub(crate) async fn remote_test_project_with_fs(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
        fs: Arc<FakeFs>,
    ) -> (Entity<Project>, MockServer) {
        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
            if !cx.has_global::<SettingsStore>() {
                let settings_store = SettingsStore::test(cx);
                cx.set_global(settings_store);
            }
        });
        server_cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);
        let entity = server_cx.new(|_| ());
        server_session.add_request_handler(entity.downgrade(), {
            |_, _: TypedEnvelope<proto::Ping>, _| async { Ok(proto::Ack {}) }
        });
        drop(connect_guard);
        let remote_client = RemoteClient::connect_mock(opts, cx).await;

        let client = cx.update(|cx| {
            Client::new(
                Arc::new(FakeSystemClock::new()),
                FakeHttpClient::with_404_response(),
                cx,
            )
        });
        let user_store = cx.new(|cx| UserStore::new(client.clone(), cx));
        let languages = Arc::new(LanguageRegistry::test(cx.executor()));
        cx.update(|cx| Project::init(&client, cx));
        let project = cx.update(|cx| {
            Project::remote(
                remote_client,
                client,
                NodeRuntime::unavailable(),
                user_store,
                languages,
                fs,
                false,
                cx,
            )
        });
        (
            project,
            MockServer {
                session: server_session,
                entity,
            },
        )
    }

    #[test]
    fn forward_urls_are_restricted_to_web_schemes() {
        for url in [
            "https://x.vercel.run",
            "https://zs.example.com/open?t=1",
            "http://127.0.0.1:8444/",
            "http://localhost:3000/path",
            "http://[::1]:8080/",
        ] {
            assert_eq!(sanitize_forward_url(url), url, "{url:?}");
        }
        assert_eq!(sanitize_forward_url(""), "");
        for url in [
            "javascript:alert(1)",
            "file:///etc/passwd",
            "zed://settings",
            "ssh://evil.example",
            "x-apple.systempreferences:com.apple.preference",
            "http://evil.example/",
            "https:",
            "not a url",
        ] {
            assert_eq!(sanitize_forward_url(url), "", "{url:?}");
        }
    }

    #[gpui::test]
    async fn ports_changed_updates_store(cx: &mut TestAppContext, server_cx: &mut TestAppContext) {
        let (project, server) = remote_test_project(cx, server_cx).await;
        let server_session = server.session.clone();
        let store = project.read_with(cx, |project, _| project.port_store().cloned().unwrap());
        let changed = Arc::new(AtomicUsize::new(0));
        cx.update({
            let changed = changed.clone();
            let store = store.clone();
            move |cx| {
                cx.subscribe(&store, move |_, event, _| {
                    if matches!(event, PortStoreEvent::Changed) {
                        changed.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .detach();
            }
        });

        server_session
            .send(proto::PortsChanged {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                ports: vec![proto::ListeningPort {
                    port: 3000,
                    pid: 42,
                    process_name: "node".into(),
                }],
                forwards: vec![proto::PortForward {
                    port: 3000,
                    visibility: proto::PortVisibility::PortPublic as i32,
                    label: Some("web".into()),
                    url: "https://x.example".into(),
                }],
            })
            .unwrap();
        cx.run_until_parked();

        store.read_with(cx, |store, _| {
            assert_eq!(
                store.listening_ports(),
                &[ListeningPort {
                    port: 3000,
                    pid: 42,
                    process_name: "node".into()
                }]
            );
            assert_eq!(
                store.forward(3000),
                Some(&PortForward {
                    port: 3000,
                    visibility: PortVisibility::Public,
                    label: Some("web".into()),
                    url: "https://x.example".into()
                })
            );
        });
        assert_eq!(changed.load(Ordering::SeqCst), 1);

        // The next picture replaces both lists.
        server_session
            .send(proto::PortsChanged {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                ports: vec![],
                forwards: vec![],
            })
            .unwrap();
        cx.run_until_parked();
        store.read_with(cx, |store, _| {
            assert!(store.listening_ports().is_empty());
            assert_eq!(store.forwards().count(), 0);
        });
        assert_eq!(changed.load(Ordering::SeqCst), 2);
    }

    #[gpui::test]
    async fn forward_port_records_the_response(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, server) = remote_test_project(cx, server_cx).await;
        let server_session = server.session.clone();
        let store = project.read_with(cx, |project, _| project.port_store().cloned().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler_entity = server.entity.clone();
        server_session.add_request_handler(handler_entity.downgrade(), {
            let seen = seen.clone();
            move |_, envelope: TypedEnvelope<proto::ForwardPort>, _| {
                seen.lock().unwrap().push(envelope.payload.clone());
                async move {
                    Ok(proto::ForwardPortResponse {
                        url: "https://forwarded.example".into(),
                    })
                }
            }
        });
        server_session.add_request_handler(handler_entity.downgrade(), {
            move |_, _: TypedEnvelope<proto::UnforwardPort>, _| async move { Ok(proto::Ack {}) }
        });

        let forward = store
            .update(cx, |store, cx| {
                store.forward_port(3000, PortVisibility::Private, Some("api".into()), cx)
            })
            .await
            .unwrap();
        assert_eq!(forward.url, "https://forwarded.example");
        assert_eq!(forward.visibility, PortVisibility::Private);
        assert_eq!(
            seen.lock().unwrap()[0],
            proto::ForwardPort {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                port: 3000,
                visibility: proto::PortVisibility::PortPrivate as i32,
                label: Some("api".into()),
            }
        );
        store.read_with(cx, |store, _| assert!(store.forward(3000).is_some()));

        store
            .update(cx, |store, cx| store.unforward_port(3000, cx))
            .await
            .unwrap();
        store.read_with(cx, |store, _| assert!(store.forward(3000).is_none()));
        drop(handler_entity);
    }
}
