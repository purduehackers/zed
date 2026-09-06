//! The control channel the supervisor pokes over the loopback listener (D5, D21): lifecycle
//! notices, port updates and extension-install requests. HTTP-framework-agnostic so `serve`
//! can mount it; the request/response shapes mirror `serve::http`'s and the adapter lives at
//! the bottom of this file.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{
    FutureExt as _,
    channel::mpsc,
    future::{BoxFuture, Either, Shared},
};
use gpui::BackgroundExecutor;
use rpc::{
    AnyProtoClient,
    proto::{self, REMOTE_SERVER_PROJECT_ID},
};
use serde::Deserialize;
use util::ResultExt as _;

/// `POST` path of lifecycle notices.
pub const LIFECYCLE_PATH: &str = "/control/lifecycle";
/// `POST` path of port updates.
pub const PORTS_PATH: &str = "/control/ports";
/// `POST` path of extension-install requests.
pub const EXTENSIONS_PATH: &str = "/control/extensions";
/// How long `POST /control/lifecycle {"kind":"stopping"}` waits for the client's flush before
/// answering anyway. The supervisor's request budget exceeds it (D18).
pub const STOPPING_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
/// Most ids one `POST /control/extensions` may carry (after de-duplication); each id can
/// cost a registry download, so the list is bounded well under the 1 MiB body cap.
pub const MAX_INSTALL_REQUEST_IDS: usize = 64;

/// The last port picture the supervisor posted.
type LastPorts = (Vec<proto::ListeningPort>, Vec<proto::PortForward>);

/// One in-progress stopping wait, shared by every `stopping` request that arrives while it
/// runs.
type StoppingWait = Shared<BoxFuture<'static, ()>>;

/// Receives the supervisor's control requests and turns them into messages to the client.
pub struct ControlChannel {
    session: AnyProtoClient,
    secret: Vec<u8>,
    /// Written only by `POST /control/ports`; replayed after every attach.
    last_ports: Mutex<Option<LastPorts>>,
    /// A `resumed` notice that arrived while no session was attached.
    pending_resumed: AtomicBool,
    /// Versions of accepted saves tagged `stopping`, from the server-side
    /// `ClientStateStore`: the only saves that end a stopping wait.
    participant_saves: Mutex<collections::HashMap<proto::PeerId, watch::Receiver<u64>>>,
    /// The stopping wait in progress, if any: later `stopping` requests join it instead of
    /// each pinning a router task for up to [`STOPPING_FLUSH_TIMEOUT`].
    stopping_wait: Mutex<Option<StoppingWait>>,
    events_tx: mpsc::UnboundedSender<ControlEvent>,
    executor: BackgroundExecutor,
}

/// Work the control channel hands to the gpui side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEvent {
    /// Install these extensions from the registry (ids already validated); ids already on
    /// disk are skipped by the consumer.
    InstallExtensions(Vec<String>),
}

/// One control request.
#[derive(Debug, Clone)]
pub struct ControlRequest<'a> {
    /// The HTTP method, upper-case.
    pub method: &'a str,
    /// The request path without the query.
    pub path: &'a str,
    /// The `Authorization: Bearer` value, if any.
    pub bearer: Option<&'a str>,
    /// Whether the TCP peer is a loopback address.
    pub peer_is_loopback: bool,
    /// Whether a client session is attached right now.
    pub session_attached: bool,
    /// The request body.
    pub body: &'a [u8],
}

/// The outcome of a control request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlResponse {
    /// 204.
    NoContent,
    /// 400 with `{"error":"bad_request","message":..}`.
    BadRequest(String),
    /// 401.
    Unauthorized,
    /// 404.
    NotFound,
}

/// `POST /control/lifecycle` body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LifecycleBody {
    /// The workspace stops in `seconds` unless activity resumes.
    IdleStopIn {
        /// Countdown.
        seconds: u32,
    },
    /// The session cap is reached in `seconds`.
    SessionCapIn {
        /// Countdown.
        seconds: u32,
    },
    /// The workspace is stopping now.
    Stopping,
    /// The workspace resumed.
    Resumed,
}

/// `POST /control/ports` body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PortsBody {
    /// Listening TCP ports the supervisor watches.
    pub ports: Vec<ListeningPortBody>,
    /// Current forwards (the full picture every time).
    #[serde(default)]
    pub forwards: Vec<PortForwardBody>,
}

/// One listening port.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ListeningPortBody {
    /// The port.
    pub port: u16,
    /// The listening process.
    pub pid: u32,
    /// Its name.
    pub process_name: String,
}

/// One forward.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PortForwardBody {
    /// The port.
    pub port: u16,
    /// `"public"` or `"private"`.
    pub visibility: String,
    /// Optional label.
    pub label: Option<String>,
    /// Opaque URL (D8); the control plane returned `null` when empty.
    #[serde(default)]
    pub url: Option<String>,
}

/// `POST /control/extensions` body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ExtensionsBody {
    /// Extension ids to install.
    pub install: Vec<String>,
}

impl ControlChannel {
    /// A channel sending to `session`, authenticated with `secret`. Registered
    /// participant saves complete the stopping flush; the receiver feeds the gpui side.
    pub fn new(
        session: AnyProtoClient,
        secret: Vec<u8>,
        executor: BackgroundExecutor,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<ControlEvent>) {
        let (events_tx, events_rx) = mpsc::unbounded();
        (
            Arc::new(Self {
                session,
                secret,
                last_ports: Mutex::new(None),
                pending_resumed: AtomicBool::new(false),
                participant_saves: Mutex::default(),
                stopping_wait: Mutex::new(None),
                events_tx,
                executor,
            }),
            events_rx,
        )
    }

    /// Async entry point for the HTTP router (runs on the router's task; nothing in it
    /// needs gpui).
    pub async fn handle(&self, request: ControlRequest<'_>) -> ControlResponse {
        if !self.is_authorized(request.bearer, request.peer_is_loopback) {
            return ControlResponse::Unauthorized;
        }
        if request.method != "POST" {
            return ControlResponse::NotFound;
        }
        match request.path {
            LIFECYCLE_PATH => match serde_json::from_slice::<LifecycleBody>(request.body) {
                Ok(body) => self.handle_lifecycle(body, request.session_attached).await,
                Err(error) => ControlResponse::BadRequest(error.to_string()),
            },
            PORTS_PATH => match serde_json::from_slice::<PortsBody>(request.body) {
                Ok(body) => self.handle_ports(body),
                Err(error) => ControlResponse::BadRequest(error.to_string()),
            },
            EXTENSIONS_PATH => match serde_json::from_slice::<ExtensionsBody>(request.body) {
                Ok(body) => self.handle_extensions(body),
                Err(error) => ControlResponse::BadRequest(error.to_string()),
            },
            _ => ControlResponse::NotFound,
        }
    }

    pub fn set_participant_saves(
        &self,
        peer: proto::PeerId,
        versions: Option<watch::Receiver<u64>>,
    ) {
        let mut peers = self
            .participant_saves
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(versions) = versions {
            peers.insert(peer, versions);
        } else {
            peers.remove(&peer);
        }
    }

    /// Re-sends what a freshly attached session missed: the last `PortsChanged` and a
    /// pending `Resumed`. Idempotent (every message carries the full picture; a pending
    /// `Resumed` is delivered at most once).
    pub fn replay_after_attach(&self) {
        let last_ports = self
            .last_ports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some((ports, forwards)) = last_ports {
            self.session
                .send(proto::PortsChanged {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    ports,
                    forwards,
                })
                .log_err();
        }
        if self.pending_resumed.swap(false, Ordering::AcqRel) {
            self.send_notice(proto::LifecycleKind::Resumed, 0);
        }
    }

    /// The last port picture posted by the supervisor, if any.
    pub fn last_ports(&self) -> Option<LastPorts> {
        self.last_ports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Whether a request from a loopback peer (or not) carrying `bearer` may use the
    /// channel: constant-time comparison with the control secret. The router checks this
    /// before reading a request body.
    pub fn is_authorized(&self, bearer: Option<&str>, peer_is_loopback: bool) -> bool {
        use subtle::ConstantTimeEq as _;
        peer_is_loopback
            && bearer.is_some_and(|bearer| bearer.as_bytes().ct_eq(&self.secret).unwrap_u8() == 1)
    }

    fn send_notice(&self, kind: proto::LifecycleKind, seconds: u32) {
        send_notice(&self.session, kind, seconds);
    }

    async fn handle_lifecycle(
        &self,
        body: LifecycleBody,
        session_attached: bool,
    ) -> ControlResponse {
        match body {
            LifecycleBody::IdleStopIn { seconds } => {
                self.send_notice(proto::LifecycleKind::IdleStopIn, seconds);
            }
            LifecycleBody::SessionCapIn { seconds } => {
                self.send_notice(proto::LifecycleKind::SessionCapIn, seconds);
            }
            LifecycleBody::Resumed => {
                if session_attached {
                    self.send_notice(proto::LifecycleKind::Resumed, 0);
                } else {
                    self.pending_resumed.store(true, Ordering::Release);
                }
            }
            LifecycleBody::Stopping => {
                if !session_attached {
                    return ControlResponse::NoContent;
                }
                self.wait_for_stopping_flush().await;
            }
        }
        ControlResponse::NoContent
    }

    /// Sends `Stopping` and waits until the client's stopping flush is accepted (the next
    /// `stopping`-tagged save above the version seen before the notice) or
    /// [`STOPPING_FLUSH_TIMEOUT`] elapses. A `stopping` request that arrives while a wait
    /// is in progress joins it rather than sending a second notice.
    async fn wait_for_stopping_flush(&self) {
        let wait = {
            let mut slot = self
                .stopping_wait
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match slot.as_ref() {
                Some(wait) => wait.clone(),
                None => {
                    let saves = self
                        .participant_saves
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .values()
                        .cloned()
                        .collect::<Vec<_>>();
                    let wait = stopping_wait(self.session.clone(), saves, self.executor.clone())
                        .boxed()
                        .shared();
                    *slot = Some(wait.clone());
                    wait
                }
            }
        };
        wait.clone().await;
        let mut slot = self
            .stopping_wait
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.as_ref().is_some_and(|current| current.ptr_eq(&wait)) {
            *slot = None;
        }
    }

    fn handle_ports(&self, body: PortsBody) -> ControlResponse {
        let ports: Vec<proto::ListeningPort> = body
            .ports
            .into_iter()
            .map(|port| proto::ListeningPort {
                port: u32::from(port.port),
                pid: port.pid,
                process_name: port.process_name,
            })
            .collect();
        let mut forwards = Vec::with_capacity(body.forwards.len());
        for forward in body.forwards {
            let visibility = match forward.visibility.as_str() {
                "public" => proto::PortVisibility::PortPublic,
                "private" => proto::PortVisibility::PortPrivate,
                other => {
                    return ControlResponse::BadRequest(format!(
                        "unknown visibility {other:?} for port {}",
                        forward.port
                    ));
                }
            };
            forwards.push(proto::PortForward {
                port: u32::from(forward.port),
                visibility: visibility as i32,
                label: forward.label,
                url: forward.url.unwrap_or_default(),
            });
        }
        *self
            .last_ports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((ports.clone(), forwards.clone()));
        self.session
            .send(proto::PortsChanged {
                project_id: REMOTE_SERVER_PROJECT_ID,
                ports,
                forwards,
            })
            .log_err();
        ControlResponse::NoContent
    }

    fn handle_extensions(&self, body: ExtensionsBody) -> ControlResponse {
        if let Some(invalid) = body
            .install
            .iter()
            .find(|id| !crate::extensions::is_valid_extension_id(id))
        {
            return ControlResponse::BadRequest(format!("invalid extension id {invalid:?}"));
        }
        let mut install: Vec<String> =
            Vec::with_capacity(body.install.len().min(MAX_INSTALL_REQUEST_IDS));
        for id in body.install {
            if !install.contains(&id) {
                install.push(id);
            }
        }
        if install.len() > MAX_INSTALL_REQUEST_IDS {
            return ControlResponse::BadRequest(format!(
                "{} extension ids requested; at most {MAX_INSTALL_REQUEST_IDS} per request",
                install.len()
            ));
        }
        if install.is_empty() {
            return ControlResponse::NoContent;
        }
        if self
            .events_tx
            .unbounded_send(ControlEvent::InstallExtensions(install))
            .is_err()
        {
            log::warn!("extension install request dropped: the gpui side is gone");
        }
        ControlResponse::NoContent
    }
}

fn send_notice(session: &AnyProtoClient, kind: proto::LifecycleKind, seconds: u32) {
    session
        .send(proto::LifecycleNotice {
            project_id: REMOTE_SERVER_PROJECT_ID,
            kind: kind as i32,
            seconds,
        })
        .log_err();
}

/// The body of one stopping wait; `'static` so concurrent requests can share it.
async fn stopping_wait(
    session: AnyProtoClient,
    versions: Vec<watch::Receiver<u64>>,
    executor: BackgroundExecutor,
) {
    let waits = versions
        .into_iter()
        .map(|mut versions| {
            let before = *versions.borrow();
            async move {
                while *versions.borrow() <= before {
                    if versions.changed().await.is_err() {
                        break;
                    }
                }
            }
        })
        .collect::<Vec<_>>();
    send_notice(&session, proto::LifecycleKind::Stopping, 0);
    if matches!(
        futures::future::select(
            Box::pin(futures::future::join_all(waits)),
            Box::pin(executor.timer(STOPPING_FLUSH_TIMEOUT))
        )
        .await,
        Either::Right(_)
    ) {
        log::warn!("not every participant saved client state within {STOPPING_FLUSH_TIMEOUT:?}");
    }
}

#[cfg(feature = "serve")]
impl crate::serve::http::ControlRoutes for ControlChannel {
    fn authorized(&self, bearer: Option<&str>, peer_is_loopback: bool) -> bool {
        self.is_authorized(bearer, peer_is_loopback)
    }

    fn handle<'a>(
        &'a self,
        req: crate::serve::http::ControlRequest<'a>,
    ) -> futures::future::BoxFuture<'a, crate::serve::http::ControlResponse> {
        use crate::serve::http::ControlResponse as HttpResponse;
        async move {
            match ControlChannel::handle(
                self,
                ControlRequest {
                    method: req.method,
                    path: req.path,
                    bearer: req.bearer,
                    peer_is_loopback: req.peer_is_loopback,
                    session_attached: req.session_attached,
                    body: req.body,
                },
            )
            .await
            {
                ControlResponse::NoContent => HttpResponse::NoContent,
                ControlResponse::BadRequest(message) => HttpResponse::BadRequest(message),
                ControlResponse::Unauthorized => HttpResponse::Unauthorized,
                ControlResponse::NotFound => HttpResponse::NotFound,
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_editing_tests::{SandboxHarness, init_test};
    use fs::FakeFs;
    use futures::StreamExt as _;
    use gpui::TestAppContext;
    use project::{
        lifecycle::LifecycleKind,
        port_store::{ListeningPort, PortForward, PortStoreEvent, PortVisibility},
    };
    use std::sync::atomic::AtomicUsize;

    const SECRET: &str = "control-secret";

    fn request<'a>(
        path: &'a str,
        body: &'a str,
        bearer: Option<&'a str>,
        session_attached: bool,
    ) -> ControlRequest<'a> {
        ControlRequest {
            method: "POST",
            path,
            bearer,
            peer_is_loopback: true,
            session_attached,
            body: body.as_bytes(),
        }
    }

    async fn harness(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) -> (SandboxHarness, Arc<ControlChannel>) {
        let fs = FakeFs::new(server_cx.executor());
        let (project, headless) = init_test(&fs, cx, server_cx).await;
        let harness = SandboxHarness::enable(project, headless, SECRET, server_cx);
        let control = harness.control.clone();
        (harness, control)
    }

    #[gpui::test]
    async fn rejects_bad_secret_and_non_loopback(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (harness, control) = harness(cx, server_cx).await;
        let notices = harness.lifecycle_notices(cx);

        let body = r#"{"kind":"idle_stop_in","seconds":30}"#;
        assert_eq!(
            control
                .handle(request(LIFECYCLE_PATH, body, Some("wrong"), true))
                .await,
            ControlResponse::Unauthorized
        );
        assert_eq!(
            control
                .handle(request(LIFECYCLE_PATH, body, None, true))
                .await,
            ControlResponse::Unauthorized
        );
        let mut non_loopback = request(LIFECYCLE_PATH, body, Some(SECRET), true);
        non_loopback.peer_is_loopback = false;
        assert_eq!(
            control.handle(non_loopback).await,
            ControlResponse::Unauthorized
        );

        let mut get = request(LIFECYCLE_PATH, body, Some(SECRET), true);
        get.method = "GET";
        assert_eq!(control.handle(get).await, ControlResponse::NotFound);
        assert_eq!(
            control
                .handle(request("/control/other", body, Some(SECRET), true))
                .await,
            ControlResponse::NotFound
        );

        cx.run_until_parked();
        assert!(
            notices.lock().unwrap().is_empty(),
            "nothing reached the client"
        );
    }

    #[gpui::test]
    async fn lifecycle_body_reaches_client(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (harness, control) = harness(cx, server_cx).await;
        let notices = harness.lifecycle_notices(cx);

        assert_eq!(
            control
                .handle(request(
                    LIFECYCLE_PATH,
                    r#"{"kind":"idle_stop_in","seconds":300}"#,
                    Some(SECRET),
                    true,
                ))
                .await,
            ControlResponse::NoContent
        );
        cx.run_until_parked();
        assert_eq!(
            notices.lock().unwrap().as_slice(),
            &[(LifecycleKind::IdleStopIn, 300)]
        );

        assert!(matches!(
            control
                .handle(request(
                    LIFECYCLE_PATH,
                    r#"{"kind":"nope"}"#,
                    Some(SECRET),
                    true
                ))
                .await,
            ControlResponse::BadRequest(_)
        ));
    }

    #[gpui::test]
    async fn ports_body_reaches_port_store(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (harness, control) = harness(cx, server_cx).await;
        let port_store = harness.port_store(cx);
        let changed = Arc::new(AtomicUsize::new(0));
        cx.update({
            let changed = changed.clone();
            let port_store = port_store.clone();
            move |cx| {
                cx.subscribe(&port_store, move |_, event, _| {
                    if matches!(event, PortStoreEvent::Changed) {
                        changed.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .detach();
            }
        });

        let body = r#"{
            "ports": [{"port": 3000, "pid": 4242, "process_name": "node"}],
            "forwards": [{"port": 3000, "visibility": "public", "label": "web", "url": "https://x.vercel.run"}]
        }"#;
        assert_eq!(
            control
                .handle(request(PORTS_PATH, body, Some(SECRET), true))
                .await,
            ControlResponse::NoContent
        );
        cx.run_until_parked();
        port_store.read_with(cx, |store, _| {
            assert_eq!(
                store.listening_ports(),
                &[ListeningPort {
                    port: 3000,
                    pid: 4242,
                    process_name: "node".into()
                }]
            );
            assert_eq!(
                store.forwards().cloned().collect::<Vec<_>>(),
                vec![PortForward {
                    port: 3000,
                    visibility: PortVisibility::Public,
                    label: Some("web".into()),
                    url: "https://x.vercel.run".into()
                }]
            );
        });
        assert_eq!(changed.load(Ordering::SeqCst), 1);

        let bad = r#"{"ports": [], "forwards": [{"port": 1, "visibility": "shared", "url": ""}]}"#;
        assert!(matches!(
            control
                .handle(request(PORTS_PATH, bad, Some(SECRET), true))
                .await,
            ControlResponse::BadRequest(_)
        ));
    }

    #[gpui::test]
    async fn stopping_without_session_returns_immediately(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (harness, control) = harness(cx, server_cx).await;
        let notices = harness.lifecycle_notices(cx);
        assert_eq!(
            control
                .handle(request(
                    LIFECYCLE_PATH,
                    r#"{"kind":"stopping"}"#,
                    Some(SECRET),
                    false
                ))
                .await,
            ControlResponse::NoContent
        );
        cx.run_until_parked();
        assert!(notices.lock().unwrap().is_empty());
    }

    #[gpui::test]
    async fn stopping_waits_for_every_participant(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (_harness, control) = harness(cx, server_cx).await;
        let (mut a_tx, a_rx) = watch::channel(10);
        let (mut b_tx, b_rx) = watch::channel(3);
        control.set_participant_saves(proto::PeerId { owner_id: 0, id: 8 }, Some(a_rx));
        control.set_participant_saves(proto::PeerId { owner_id: 0, id: 9 }, Some(b_rx));
        let mut stopping = server_cx.background_executor.spawn(async move {
            control
                .handle(request(
                    LIFECYCLE_PATH,
                    r#"{"kind":"stopping"}"#,
                    Some(SECRET),
                    true,
                ))
                .await
        });
        cx.run_until_parked();
        a_tx.send(11).unwrap();
        cx.run_until_parked();
        assert!(
            (&mut stopping).now_or_never().is_none(),
            "one participant cannot finish everybody's flush"
        );
        b_tx.send(4).unwrap();
        assert_eq!(stopping.await, ControlResponse::NoContent);
    }

    #[gpui::test]
    async fn stopping_waits_for_save(cx: &mut TestAppContext, server_cx: &mut TestAppContext) {
        let (harness, control) = harness(cx, server_cx).await;
        let notices = harness.lifecycle_notices(cx);

        // Without a save the wait ends at the timeout.
        let stopping = server_cx.background_executor.spawn({
            let control = control.clone();
            async move {
                control
                    .handle(ControlRequest {
                        method: "POST",
                        path: LIFECYCLE_PATH,
                        bearer: Some(SECRET),
                        peer_is_loopback: true,
                        session_attached: true,
                        body: br#"{"kind":"stopping"}"#,
                    })
                    .await
            }
        });
        cx.run_until_parked();
        assert_eq!(
            notices.lock().unwrap().as_slice(),
            &[(LifecycleKind::Stopping, 0)]
        );
        server_cx
            .background_executor
            .advance_clock(STOPPING_FLUSH_TIMEOUT);
        assert_eq!(stopping.await, ControlResponse::NoContent);

        // A plain save (a ticker save that was already in flight when the notice went out)
        // does not end the wait; the stopping-tagged flush does, without any clock advance.
        // A second stopping request meanwhile joins the wait instead of re-notifying.
        let stopping_request = || {
            server_cx.background_executor.spawn({
                let control = control.clone();
                async move {
                    control
                        .handle(ControlRequest {
                            method: "POST",
                            path: LIFECYCLE_PATH,
                            bearer: Some(SECRET),
                            peer_is_loopback: true,
                            session_attached: true,
                            body: br#"{"kind":"stopping"}"#,
                        })
                        .await
                }
            })
        };
        let mut stopping = stopping_request();
        let mut joined = stopping_request();
        cx.run_until_parked();
        assert_eq!(
            notices.lock().unwrap().as_slice(),
            &[(LifecycleKind::Stopping, 0), (LifecycleKind::Stopping, 0)],
            "one notice per wait, not per request"
        );
        let plain = harness.save_client_state(1, false, cx).await;
        assert!(plain.accepted);
        cx.run_until_parked();
        assert!(
            (&mut stopping).now_or_never().is_none(),
            "a plain save does not end the stopping wait"
        );
        assert!((&mut joined).now_or_never().is_none());
        let flush = harness.save_client_state(2, true, cx).await;
        assert!(flush.accepted);
        assert_eq!(stopping.await, ControlResponse::NoContent);
        assert_eq!(joined.await, ControlResponse::NoContent);

        // The wait slot is cleared: a later stopping request starts a fresh wait.
        let stopping = stopping_request();
        cx.run_until_parked();
        assert_eq!(notices.lock().unwrap().len(), 3);
        harness.save_client_state(3, true, cx).await;
        assert_eq!(stopping.await, ControlResponse::NoContent);
    }

    #[gpui::test]
    async fn resumed_is_replayed_after_attach(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (harness, control) = harness(cx, server_cx).await;
        let notices = harness.lifecycle_notices(cx);
        let port_store = harness.port_store(cx);

        assert_eq!(
            control
                .handle(request(
                    LIFECYCLE_PATH,
                    r#"{"kind":"resumed"}"#,
                    Some(SECRET),
                    false
                ))
                .await,
            ControlResponse::NoContent
        );
        let body = r#"{"ports": [{"port": 8080, "pid": 1, "process_name": "python"}]}"#;
        assert_eq!(
            control
                .handle(request(PORTS_PATH, body, Some(SECRET), false))
                .await,
            ControlResponse::NoContent
        );
        cx.run_until_parked();
        assert!(
            notices.lock().unwrap().is_empty(),
            "resumed is held while detached"
        );

        control.replay_after_attach();
        cx.run_until_parked();
        assert_eq!(
            notices.lock().unwrap().as_slice(),
            &[(LifecycleKind::Resumed, 0)]
        );
        port_store.read_with(cx, |store, _| {
            assert_eq!(store.listening_ports().len(), 1);
            assert_eq!(store.listening_ports()[0].port, 8080);
        });

        control.replay_after_attach();
        cx.run_until_parked();
        assert_eq!(
            notices.lock().unwrap().len(),
            1,
            "resumed is delivered at most once"
        );
    }

    #[gpui::test]
    async fn extensions_body_emits_event(cx: &mut TestAppContext, server_cx: &mut TestAppContext) {
        let fs = FakeFs::new(server_cx.executor());
        let (_project, headless) = init_test(&fs, cx, server_cx).await;
        let session = headless.read_with(server_cx, |headless, _| headless.session.clone());
        let (control, mut events) = ControlChannel::new(
            session,
            SECRET.as_bytes().to_vec(),
            server_cx.background_executor.clone(),
        );

        assert_eq!(
            control
                .handle(request(
                    EXTENSIONS_PATH,
                    r#"{"install":["toml","html","toml"]}"#,
                    Some(SECRET),
                    true
                ))
                .await,
            ControlResponse::NoContent
        );
        assert_eq!(
            events.next().await,
            Some(ControlEvent::InstallExtensions(vec![
                "toml".into(),
                "html".into()
            ])),
            "duplicates are dropped"
        );

        assert!(matches!(
            control
                .handle(request(
                    EXTENSIONS_PATH,
                    r#"{"install":["../../.."]}"#,
                    Some(SECRET),
                    true
                ))
                .await,
            ControlResponse::BadRequest(_)
        ));
        assert!(events.try_recv().is_err(), "an invalid id emits nothing");

        assert_eq!(
            control
                .handle(request(
                    EXTENSIONS_PATH,
                    r#"{"install":[]}"#,
                    Some(SECRET),
                    true
                ))
                .await,
            ControlResponse::NoContent
        );
        assert!(events.try_recv().is_err(), "an empty list emits nothing");

        let too_many = serde_json::json!({
            "install": (0..MAX_INSTALL_REQUEST_IDS + 1)
                .map(|index| format!("ext{index}"))
                .collect::<Vec<_>>()
        })
        .to_string();
        assert!(matches!(
            control
                .handle(request(EXTENSIONS_PATH, &too_many, Some(SECRET), true))
                .await,
            ControlResponse::BadRequest(_)
        ));
        assert!(
            events.try_recv().is_err(),
            "an over-long list emits nothing"
        );
    }
}
