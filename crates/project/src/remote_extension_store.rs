//! Client side of registry-driven extension management on the sandbox (BUILD-SPEC 5.6):
//! the installed set the server reports and the install/uninstall/search requests. The
//! extensions panel consumes this; it is not implemented here.

use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context as _, Result};
use cloud_api_types::{ExtensionApiManifest, ExtensionMetadata, ExtensionProvides};
use gpui::{AsyncApp, Context, Entity, EventEmitter, Task};
use rpc::{AnyProtoClient, TypedEnvelope, proto};

/// An extension installed on the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledExtension {
    /// Registry id.
    pub id: Arc<str>,
    /// Installed version.
    pub version: Arc<str>,
    /// Display name.
    pub name: String,
    /// Description from the manifest.
    pub description: Option<String>,
    /// `ExtensionProvides` kebab-case names.
    pub provides: Vec<String>,
    /// Whether it was uploaded as a dev extension rather than installed from the registry.
    pub dev: bool,
    /// Installed artifact revision, including same-version development rebuilds.
    pub revision: u64,
}

impl InstalledExtension {
    fn from_proto(extension: proto::InstalledExtension) -> Self {
        Self {
            id: extension.id.into(),
            version: extension.version.into(),
            name: extension.name,
            description: extension.description,
            provides: extension.provides,
            dev: extension.dev,
            revision: extension.revision,
        }
    }
}

fn registry_metadata(extension: proto::AvailableExtension) -> Result<ExtensionMetadata> {
    Ok(ExtensionMetadata {
        id: extension.id.into(),
        published_at: extension
            .published_at
            .parse()
            .context("invalid extension publication date")?,
        download_count: extension.download_count,
        manifest: ExtensionApiManifest {
            version: extension.version.into(),
            name: extension.name,
            description: extension.description,
            authors: extension.authors,
            repository: extension.repository,
            provides: extension
                .provides
                .iter()
                .map(|value| value.parse())
                .collect::<Result<_, _>>()?,
            schema_version: extension.schema_version,
            wasm_api_version: extension.wasm_api_version,
        },
    })
}

/// The client's view of the server's extensions.
pub struct RemoteExtensionStore {
    client: AnyProtoClient,
    project_id: u64,
    installed: Vec<InstalledExtension>,
    pending: BTreeSet<Arc<str>>,
}

/// Events a [`RemoteExtensionStore`] emits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteExtensionEvent {
    /// The installed set changed.
    InstalledChanged,
    /// An install or uninstall of `id` finished, successfully or with `error`.
    OperationFinished {
        /// The extension.
        id: Arc<str>,
        /// The failure, if any.
        error: Option<String>,
    },
}

impl EventEmitter<RemoteExtensionEvent> for RemoteExtensionStore {}

impl RemoteExtensionStore {
    /// A revision-pinned presentation bridge for the browser's LSP adapter.
    #[cfg(target_family = "wasm")]
    pub fn language_server_labels(
        &self,
        extension_id: Arc<str>,
        revision: u64,
    ) -> extension::RemoteLanguageServerLabels {
        use futures::FutureExt;
        let client = self.client.clone();
        let project_id = self.project_id;
        Arc::new(move |server, request| {
            let client = client.clone();
            let extension_id = extension_id.to_string();
            async move {
                let response = client
                    .request(proto::GetExtensionLanguageServerLabels {
                        project_id,
                        extension_id,
                        revision,
                        language_server_id: server.to_string(),
                        request_json: serde_json::to_vec(&request)?,
                    })
                    .await?;
                Ok(serde_json::from_slice(&response.labels_json)?)
            }
            .boxed()
        })
    }

    /// A store over the remote server session.
    pub fn remote(client: AnyProtoClient, project_id: u64) -> Self {
        Self {
            client,
            project_id,
            installed: Vec::new(),
            pending: BTreeSet::new(),
        }
    }

    /// Registers the server-to-client handler.
    pub fn init(client: &AnyProtoClient) {
        client.add_entity_message_handler(Self::handle_extensions_changed);
    }

    /// The installed set the server last reported.
    pub fn installed(&self) -> &[InstalledExtension] {
        &self.installed
    }

    /// Whether an install or uninstall of `id` is in flight.
    pub fn is_pending(&self, id: &str) -> bool {
        self.pending.contains(id)
    }

    /// Re-fetches the installed set (`ListExtensions { include_available: false }`).
    pub fn refresh(&self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let request = self.client.request(proto::ListExtensions {
            project_id: self.project_id,
            search: None,
            include_available: false,
            ..Default::default()
        });
        cx.spawn(async move |this, cx| {
            let response = request.await.context("listing extensions")?;
            this.update(cx, |this, cx| this.set_installed(response.installed, cx))?;
            Ok(())
        })
    }

    /// Searches the registry through the server (`ListExtensions { include_available: true }`);
    /// also refreshes the installed set from the same response.
    pub fn search(
        &self,
        search: Option<String>,
        extension_id: Option<String>,
        provides: BTreeSet<ExtensionProvides>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Vec<ExtensionMetadata>>> {
        let request = self.client.request(proto::ListExtensions {
            project_id: self.project_id,
            search,
            include_available: true,
            extension_id,
            provides: provides.iter().map(ToString::to_string).collect(),
        });
        cx.spawn(async move |this, cx| {
            let response = request.await.context("searching extensions")?;
            this.update(cx, |this, cx| this.set_installed(response.installed, cx))?;
            response
                .available
                .into_iter()
                .map(registry_metadata)
                .collect()
        })
    }

    /// Installs `id` (latest compatible version unless `version` is given). The id is
    /// pending until the server answers; `OperationFinished` is emitted either way.
    pub fn install(
        &mut self,
        id: Arc<str>,
        version: Option<Arc<str>>,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        self.install_request(
            proto::InstallRegistryExtension {
                project_id: self.project_id,
                id: id.to_string(),
                version: version.map(|version| version.to_string()),
                ..Default::default()
            },
            cx,
        )
    }

    /// Startup settings/updates must not overwrite a concurrent peer's install.
    pub fn install_automatically(
        &mut self,
        id: Arc<str>,
        version: Option<Arc<str>>,
        expected_revision: Option<u64>,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        self.install_request(
            proto::InstallRegistryExtension {
                project_id: self.project_id,
                id: id.to_string(),
                version: version.map(|version| version.to_string()),
                expected_revision,
                only_if_missing: expected_revision.is_none(),
            },
            cx,
        )
    }

    fn install_request(
        &mut self,
        request: proto::InstallRegistryExtension,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        let id: Arc<str> = request.id.as_str().into();
        let request = self.client.request(request);
        self.pending.insert(id.clone());
        cx.notify();
        cx.spawn(async move |this, cx| {
            let result = request
                .await
                .map(|_ack| ())
                .with_context(|| format!("installing extension {id}"));
            Self::finish(this, id, &result, cx);
            result
        })
    }

    /// Uninstalls `id`; same pending semantics as [`Self::install`].
    pub fn uninstall(&mut self, id: Arc<str>, cx: &mut Context<Self>) -> Task<Result<()>> {
        let request = self.client.request(proto::UninstallExtension {
            project_id: self.project_id,
            id: id.to_string(),
        });
        self.pending.insert(id.clone());
        cx.notify();
        cx.spawn(async move |this, cx| {
            let result = request
                .await
                .map(|_ack| ())
                .with_context(|| format!("uninstalling extension {id}"));
            Self::finish(this, id, &result, cx);
            result
        })
    }

    /// Fails every pending operation with `error: Some("disconnected")` and clears
    /// `pending`; called on every `RemoteClientEvent::Disconnected`.
    pub fn fail_pending(&mut self, cx: &mut Context<Self>) {
        for id in std::mem::take(&mut self.pending) {
            cx.emit(RemoteExtensionEvent::OperationFinished {
                id,
                error: Some("disconnected".to_owned()),
            });
        }
        cx.notify();
    }

    fn finish(this: gpui::WeakEntity<Self>, id: Arc<str>, result: &Result<()>, cx: &mut AsyncApp) {
        let error = result.as_ref().err().map(|error| format!("{error:#}"));
        this.update(cx, |this, cx| {
            // A disconnect may already have failed and removed it; the spinner is then
            // already gone and a second event would be misleading.
            if this.pending.remove(&id) {
                cx.emit(RemoteExtensionEvent::OperationFinished { id, error });
                cx.notify();
            }
        })
        .ok();
    }

    fn set_installed(&mut self, installed: Vec<proto::InstalledExtension>, cx: &mut Context<Self>) {
        let installed: Vec<InstalledExtension> = installed
            .into_iter()
            .map(InstalledExtension::from_proto)
            .collect();
        if installed != self.installed {
            self.installed = installed;
            cx.emit(RemoteExtensionEvent::InstalledChanged);
            cx.notify();
        }
    }

    async fn handle_extensions_changed(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::ExtensionsChanged>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        this.update(&mut cx, |this, cx| {
            this.set_installed(envelope.payload.installed, cx)
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port_store::tests::remote_test_project;
    use futures::channel::oneshot;
    use gpui::TestAppContext;
    use remote::RemoteClientEvent;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    fn installed(id: &str) -> proto::InstalledExtension {
        proto::InstalledExtension {
            id: id.into(),
            version: "1.0.0".into(),
            name: id.to_uppercase(),
            description: None,
            provides: vec!["languages".into()],
            dev: false,
            revision: 1,
        }
    }

    #[gpui::test]
    async fn extensions_changed_updates_store(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, server) = remote_test_project(cx, server_cx).await;
        let server_session = server.session.clone();
        let store = project.read_with(cx, |project, _| {
            project.remote_extension_store().cloned().unwrap()
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        cx.update({
            let events = events.clone();
            let store = store.clone();
            move |cx| {
                cx.subscribe(&store, move |_, event, _| {
                    events.lock().unwrap().push(event.clone())
                })
                .detach();
            }
        });

        server_session
            .send(proto::ExtensionsChanged {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                installed: vec![installed("toml")],
            })
            .unwrap();
        cx.run_until_parked();
        store.read_with(cx, |store, _| {
            assert_eq!(store.installed().len(), 1);
            assert_eq!(store.installed()[0].id.as_ref(), "toml");
            assert_eq!(store.installed()[0].name, "TOML");
        });
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[RemoteExtensionEvent::InstalledChanged]
        );

        // The same picture again emits nothing.
        server_session
            .send(proto::ExtensionsChanged {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                installed: vec![installed("toml")],
            })
            .unwrap();
        cx.run_until_parked();
        assert_eq!(events.lock().unwrap().len(), 1);
    }

    #[gpui::test]
    async fn install_marks_pending_until_response(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, server) = remote_test_project(cx, server_cx).await;
        let server_session = server.session.clone();
        let store = project.read_with(cx, |project, _| {
            project.remote_extension_store().cloned().unwrap()
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        cx.update({
            let events = events.clone();
            let store = store.clone();
            move |cx| {
                cx.subscribe(&store, move |_, event, _| {
                    events.lock().unwrap().push(event.clone())
                })
                .detach();
            }
        });

        let gate: Arc<Mutex<Option<oneshot::Receiver<()>>>> = Arc::default();
        let fail = Arc::new(AtomicBool::new(false));
        let handler_entity = server.entity.clone();
        server_session.add_request_handler(handler_entity.downgrade(), {
            let gate = gate.clone();
            let fail = fail.clone();
            move |_, _: TypedEnvelope<proto::InstallRegistryExtension>, _| {
                let gate = gate.lock().unwrap().take();
                let fail = fail.load(Ordering::SeqCst);
                async move {
                    if let Some(gate) = gate {
                        gate.await.ok();
                    }
                    anyhow::ensure!(!fail, "registry unreachable");
                    Ok(proto::Ack {})
                }
            }
        });

        let (release, receiver) = oneshot::channel();
        *gate.lock().unwrap() = Some(receiver);
        let install = store.update(cx, |store, cx| store.install("foo".into(), None, cx));
        cx.run_until_parked();
        assert!(store.read_with(cx, |store, _| store.is_pending("foo")));
        release.send(()).unwrap();
        install.await.unwrap();
        assert!(!store.read_with(cx, |store, _| store.is_pending("foo")));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[RemoteExtensionEvent::OperationFinished {
                id: "foo".into(),
                error: None
            }]
        );

        fail.store(true, Ordering::SeqCst);
        let error = store
            .update(cx, |store, cx| store.install("bar".into(), None, cx))
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("registry unreachable"),
            "{error:#}"
        );
        assert!(!store.read_with(cx, |store, _| store.is_pending("bar")));
        assert!(matches!(
            events.lock().unwrap().last(),
            Some(RemoteExtensionEvent::OperationFinished { id, error: Some(error) })
                if id.as_ref() == "bar" && error.contains("registry unreachable")
        ));

        // A hung request is failed by `fail_pending`, and its late completion is silent.
        fail.store(false, Ordering::SeqCst);
        let (release, receiver) = oneshot::channel();
        *gate.lock().unwrap() = Some(receiver);
        let install = store.update(cx, |store, cx| store.install("baz".into(), None, cx));
        cx.run_until_parked();
        assert!(store.read_with(cx, |store, _| store.is_pending("baz")));
        store.update(cx, |store, cx| store.fail_pending(cx));
        assert!(!store.read_with(cx, |store, _| store.is_pending("baz")));
        assert_eq!(
            events.lock().unwrap().last(),
            Some(&RemoteExtensionEvent::OperationFinished {
                id: "baz".into(),
                error: Some("disconnected".into())
            })
        );
        let events_before = events.lock().unwrap().len();
        release.send(()).unwrap();
        install.await.unwrap();
        assert_eq!(events.lock().unwrap().len(), events_before);
        drop(handler_entity);
    }

    #[gpui::test]
    async fn pending_fails_on_reconnect_exhausted(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let (project, server) = remote_test_project(cx, server_cx).await;
        let server_session = server.session.clone();
        let store = project.read_with(cx, |project, _| {
            project.remote_extension_store().cloned().unwrap()
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        cx.update({
            let events = events.clone();
            let store = store.clone();
            move |cx| {
                cx.subscribe(&store, move |_, event, _| {
                    events.lock().unwrap().push(event.clone())
                })
                .detach();
            }
        });
        let (_release, receiver) = oneshot::channel::<()>();
        let gate = Arc::new(Mutex::new(Some(receiver)));
        let handler_entity = server.entity.clone();
        server_session.add_request_handler(handler_entity.downgrade(), {
            let gate = gate.clone();
            move |_, _: TypedEnvelope<proto::UninstallExtension>, _| {
                let gate = gate.lock().unwrap().take();
                async move {
                    if let Some(gate) = gate {
                        gate.await.ok();
                    }
                    Ok(proto::Ack {})
                }
            }
        });

        let _uninstall = store.update(cx, |store, cx| store.uninstall("foo".into(), cx));
        cx.run_until_parked();
        assert!(store.read_with(cx, |store, _| store.is_pending("foo")));

        // The `ReconnectExhausted` shape of the terminal disconnect (D2).
        let remote = project.read_with(cx, |project, _| project.remote_client().unwrap());
        remote.update(cx, |_, cx| {
            cx.emit(RemoteClientEvent::Disconnected {
                server_not_running: false,
            })
        });
        cx.run_until_parked();
        assert!(!store.read_with(cx, |store, _| store.is_pending("foo")));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[RemoteExtensionEvent::OperationFinished {
                id: "foo".into(),
                error: Some("disconnected".into())
            }]
        );
        drop(handler_entity);
    }
}
