use anyhow::{Context as _, Result, anyhow};
use client::ProjectId;
use collections::HashMap;
use collections::HashSet;
use gpui::TasksIncluded;
use language::File;
use lsp::LanguageServerId;

use extension::{ExtensionEvents, ExtensionHostProxy};
use extension_host::headless_host::HeadlessExtensionStore;
use fs::Fs;
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, PromptLevel, Subscription, Task, TaskExt,
    WeakEntity,
};
use http_client::HttpClient;
use language::{Buffer, BufferEvent, LanguageRegistry, proto::serialize_operation};
use node_runtime::NodeRuntime;
use project::{
    AgentRegistryStore, LspStore, LspStoreEvent, ManifestTree, PrettierStore, ProjectEnvironment,
    ProjectPath, ToolchainStore, WorktreeId,
    agent_server_store::AgentServerStore,
    buffer_store::{BufferStore, BufferStoreEvent},
    context_server_store::ContextServerStore,
    debugger::{breakpoint_store::BreakpointStore, dap_store::DapStore},
    git_store::GitStore,
    image_store::ImageId,
    lsp_store::log_store::{self, GlobalLogStore, LanguageServerKind, LogKind},
    project_settings::SettingsObserver,
    search::SearchQuery,
    task_store::TaskStore,
    trusted_worktrees::{PathTrust, RemoteHostLocation, TrustedWorktrees},
    worktree_store::{WorktreeIdCounter, WorktreeStore},
};
use rpc::{
    AnyProtoClient, TypedEnvelope,
    proto::{self, REMOTE_SERVER_PROJECT_ID},
};
use smol::process::Child;

use crate::{
    client_state::ClientStateStore,
    control::{ControlChannel, ControlEvent},
    extensions::{InstalledExtensionRecord, RegistryConfig, SandboxExtensions},
    ports::PortForwarder,
    pty,
};

use settings::initial_server_settings_content;
use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};
use sysinfo::{ProcessRefreshKind, RefreshKind, System, UpdateKind};
use util::{ResultExt, paths::PathStyle, rel_path::RelPath};
use worktree::Worktree;

mod recovery;

pub struct HeadlessProject {
    pub hub: Option<Arc<remote::ServerHub>>,
    participants: HashSet<proto::PeerId>,
    retained_buffers: Vec<Entity<Buffer>>,
    edited_buffers: HashSet<u64>,
    worktree_add_lock: Arc<futures::lock::Mutex<()>>,
    pub fs: Arc<dyn Fs>,
    pub session: AnyProtoClient,
    pub worktree_store: Entity<WorktreeStore>,
    pub buffer_store: Entity<BufferStore>,
    pub lsp_store: Entity<LspStore>,
    pub task_store: Entity<TaskStore>,
    pub dap_store: Entity<DapStore>,
    pub breakpoint_store: Entity<BreakpointStore>,
    pub agent_server_store: Entity<AgentServerStore>,
    pub context_server_store: Entity<ContextServerStore>,
    pub settings_observer: Entity<SettingsObserver>,
    pub next_entry_id: Arc<AtomicUsize>,
    pub languages: Arc<LanguageRegistry>,
    pub extensions: Entity<HeadlessExtensionStore>,
    pub git_store: Entity<GitStore>,
    pub environment: Entity<ProjectEnvironment>,
    pub profiling_collector: gpui::ProfilingCollector,
    // Used mostly to keep alive the toolchain store for RPC handlers.
    // Local variant is used within LSP store, but that's a separate entity.
    pub _toolchain_store: Entity<ToolchainStore>,
    pub kernels: HashMap<String, Child>,
    /// Handle to the process-level PTY manager (D3, D24). The manager is an
    /// `App` global that outlives this project and every session; this is not
    /// an owner.
    pub pty_manager: Arc<pty::PtyManager>,
    /// Serve mode: called instead of quitting the process when a client sends
    /// `ShutdownRemoteServer` (the session is closed, the process stays alive).
    pub shutdown_request_handler: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Serve mode: everything sandbox-specific (client state, control channel, port
    /// forwarding, registry extensions); `None` in SSH `run` mode. Installed once per
    /// process by `enable_sandbox`.
    pub sandbox: Option<SandboxRuntime>,
}

/// What `HeadlessProject::enable_sandbox` needs from `serve`.
pub struct SandboxConfig {
    /// The `--control-secret-file` contents: bearer of the control listener and of the
    /// server's calls to the supervisor.
    pub control_secret: Vec<u8>,
    /// Base URL of the supervisor's loopback API (`crate::ports::DEFAULT_SUPERVISOR_URL`
    /// unless overridden).
    pub supervisor_url: String,
    /// Proxy-less HTTP client for the supervisor (loopback traffic must bypass any proxy).
    pub supervisor_http: Arc<dyn HttpClient>,
    /// The extension registry.
    pub registry: RegistryConfig,
    /// Where client-state images are stored
    /// (`paths::remote_server_state_dir().join("client_state")`).
    pub client_state_dir: PathBuf,
}

/// The sandbox pieces `enable_sandbox` installed; they outlive every session.
pub struct SandboxRuntime {
    /// Server-side client-state blob store.
    pub client_state: Entity<ClientStateStore>,
    /// The control channel `serve` mounts on the loopback listener.
    pub control: Arc<ControlChannel>,
    /// Supervisor client for port forwards and the installed-extension report.
    pub ports: Arc<PortForwarder>,
    /// Registry-driven extension management.
    pub extensions: Entity<SandboxExtensions>,
    _install_drain: Task<()>,
    _extension_events: Option<Subscription>,
}

pub struct HeadlessAppState {
    pub session: AnyProtoClient,
    pub fs: Arc<dyn Fs>,
    pub http_client: Arc<dyn HttpClient>,
    pub node_runtime: NodeRuntime,
    pub languages: Arc<LanguageRegistry>,
    pub extension_host_proxy: Arc<ExtensionHostProxy>,
    pub startup_time: Instant,
}

impl HeadlessProject {
    pub fn init(cx: &mut App) {
        settings::init(cx);
        log_store::init(true, cx);
    }

    pub fn new(
        HeadlessAppState {
            session,
            fs,
            http_client,
            node_runtime,
            languages,
            extension_host_proxy: proxy,
            startup_time,
        }: HeadlessAppState,
        init_worktree_trust: bool,
        cx: &mut Context<Self>,
    ) -> Self {
        debug_adapter_extension::init(proxy.clone(), cx);
        languages::init(languages.clone(), fs.clone(), node_runtime.clone(), cx);

        let worktree_store = cx.new(|cx| {
            let mut store = WorktreeStore::local(true, fs.clone(), WorktreeIdCounter::get(cx));
            store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            store
        });

        if init_worktree_trust {
            project::trusted_worktrees::track_worktree_trust(
                worktree_store.clone(),
                None::<RemoteHostLocation>,
                Some((session.clone(), ProjectId(REMOTE_SERVER_PROJECT_ID))),
                None,
                cx,
            );
        }

        let environment =
            cx.new(|cx| ProjectEnvironment::new(None, worktree_store.downgrade(), None, true, cx));
        let manifest_tree = ManifestTree::new(worktree_store.clone(), cx);
        let toolchain_store = cx.new(|cx| {
            ToolchainStore::local(
                languages.clone(),
                worktree_store.clone(),
                environment.clone(),
                manifest_tree.clone(),
                cx,
            )
        });

        let buffer_store = cx.new(|cx| {
            let mut buffer_store = BufferStore::local(worktree_store.clone(), cx);
            buffer_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            buffer_store
        });

        let breakpoint_store = cx.new(|_| {
            let mut breakpoint_store =
                BreakpointStore::local(worktree_store.clone(), buffer_store.clone());
            breakpoint_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone());

            breakpoint_store
        });

        let dap_store = cx.new(|cx| {
            let mut dap_store = DapStore::new_local(
                http_client.clone(),
                node_runtime.clone(),
                fs.clone(),
                environment.clone(),
                toolchain_store.read(cx).as_language_toolchain_store(),
                worktree_store.clone(),
                breakpoint_store.clone(),
                true,
                cx,
            );
            dap_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            dap_store
        });

        let git_store = cx.new(|cx| {
            let mut store = GitStore::local(
                &worktree_store,
                buffer_store.clone(),
                environment.clone(),
                fs.clone(),
                cx,
            );
            store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            store
        });

        let prettier_store = cx.new(|cx| {
            PrettierStore::new(
                node_runtime.clone(),
                fs.clone(),
                languages.clone(),
                worktree_store.clone(),
                cx,
            )
        });

        let task_store = cx.new(|cx| {
            let mut task_store = TaskStore::local(
                buffer_store.downgrade(),
                worktree_store.clone(),
                toolchain_store.read(cx).as_language_toolchain_store(),
                environment.clone(),
                git_store.clone(),
                cx,
            );
            task_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            task_store
        });
        let settings_observer = cx.new(|cx| {
            let mut observer = SettingsObserver::new_local(
                fs.clone(),
                worktree_store.clone(),
                task_store.clone(),
                true,
                cx,
            );
            observer.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            observer
        });

        let lsp_store = cx.new(|cx| {
            let mut lsp_store = LspStore::new_local(
                buffer_store.clone(),
                worktree_store.clone(),
                prettier_store.clone(),
                toolchain_store
                    .read(cx)
                    .as_local_store()
                    .expect("Toolchain store to be local")
                    .clone(),
                environment.clone(),
                manifest_tree,
                languages.clone(),
                http_client.clone(),
                fs.clone(),
                cx,
            );
            lsp_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            lsp_store
        });

        AgentRegistryStore::init_global(cx, fs.clone(), http_client.clone());

        let agent_server_store = cx.new(|cx| {
            let mut agent_server_store = AgentServerStore::local(
                node_runtime.clone(),
                fs.clone(),
                environment.clone(),
                http_client.clone(),
                cx,
            );
            agent_server_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone(), cx);
            agent_server_store
        });

        let context_server_store = cx.new(|cx| {
            let mut context_server_store =
                ContextServerStore::local(worktree_store.clone(), None, true, cx);
            context_server_store.shared(REMOTE_SERVER_PROJECT_ID, session.clone());
            context_server_store
        });

        cx.subscribe(&lsp_store, Self::on_lsp_store_event).detach();
        language_extension::init(
            language_extension::LspAccess::ViaLspStore(lsp_store.downgrade()),
            proxy.clone(),
            languages.clone(),
        );

        cx.subscribe(&buffer_store, |this, _buffer_store, event, cx| {
            if let BufferStoreEvent::BufferAdded(buffer) = event {
                if this.hub.is_some() {
                    this.retained_buffers.push(buffer.clone());
                }
                cx.subscribe(buffer, Self::on_buffer_event).detach();
            }
        })
        .detach();

        let extensions = HeadlessExtensionStore::new(
            fs.clone(),
            http_client.clone(),
            paths::remote_extensions_dir().to_path_buf(),
            proxy,
            node_runtime,
            cx,
        );

        // local_machine -> ssh handlers
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &worktree_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &buffer_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &cx.entity());
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &lsp_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &task_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &toolchain_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &dap_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &breakpoint_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &settings_observer);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &git_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &agent_server_store);
        session.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &context_server_store);

        // Process-level (D3, D24): the first project in this process installs the
        // manager; a fresh session's replacement project reuses it, so PTYs
        // survive. `serve` may install it explicitly before building the project.
        let pty_manager = pty::PtyManager::global(cx).unwrap_or_else(|| {
            pty::PtyManager::install(REMOTE_SERVER_PROJECT_ID, Arc::new(session.clone()), cx)
        });

        session.add_request_handler(cx.weak_entity(), Self::handle_list_remote_directory);
        session.add_request_handler(cx.weak_entity(), Self::handle_get_path_metadata);
        session.add_request_handler(cx.weak_entity(), Self::handle_shutdown_remote_server);
        session.add_request_handler(cx.weak_entity(), Self::handle_ping);
        session.add_request_handler(cx.weak_entity(), Self::handle_restore_buffer_snapshot);
        session.add_request_handler(cx.weak_entity(), Self::handle_get_processes);
        session.add_request_handler(cx.weak_entity(), Self::handle_get_remote_profiling_data);

        session.add_entity_request_handler(Self::handle_add_worktree);
        session.add_request_handler(cx.weak_entity(), Self::handle_remove_worktree);

        session.add_entity_request_handler(Self::handle_open_buffer_by_path);
        session.add_entity_request_handler(Self::handle_open_new_buffer);
        session.add_entity_request_handler(Self::handle_find_search_candidates);
        session.add_entity_request_handler(Self::handle_open_server_settings);
        session.add_entity_request_handler(Self::handle_get_directory_environment);
        session.add_entity_message_handler(Self::handle_toggle_lsp_logs);
        session.add_entity_request_handler(Self::handle_open_image_by_path);
        session.add_entity_request_handler(Self::handle_trust_worktrees);
        session.add_entity_request_handler(Self::handle_restrict_worktrees);
        session.add_entity_request_handler(Self::handle_download_file_by_path);

        session.add_entity_message_handler(Self::handle_find_search_candidates_cancel);
        session.add_entity_request_handler(BufferStore::handle_update_buffer);
        session.add_entity_message_handler(BufferStore::handle_close_buffer);

        session.add_request_handler(cx.weak_entity(), Self::handle_sync_extensions);
        session.add_request_handler(cx.weak_entity(), Self::handle_install_extension);

        session.add_request_handler(cx.weak_entity(), Self::handle_spawn_kernel);
        session.add_request_handler(cx.weak_entity(), Self::handle_kill_kernel);

        session.add_entity_request_handler(Self::handle_spawn_terminal);
        session.add_entity_message_handler(Self::handle_terminal_input);
        session.add_entity_message_handler(Self::handle_ack_terminal_output);
        session.add_entity_message_handler(Self::handle_resize_terminal);
        session.add_entity_message_handler(Self::handle_close_terminal);
        session.add_entity_request_handler(Self::handle_list_terminals);
        session.add_entity_request_handler(Self::handle_attach_terminal);

        BufferStore::init(&session);
        WorktreeStore::init(&session);
        SettingsObserver::init(&session);
        LspStore::init(&session);
        TaskStore::init(Some(&session));
        ToolchainStore::init(&session);
        DapStore::init(&session, cx);
        // todo(debugger): Re init breakpoint store when we set it up for collab
        BreakpointStore::init(&session);
        GitStore::init(&session);
        AgentServerStore::init_headless(&session);
        ContextServerStore::init_headless(&session);

        HeadlessProject {
            next_entry_id: Default::default(),
            session,
            settings_observer,
            fs,
            worktree_store,
            buffer_store,
            lsp_store,
            task_store,
            dap_store,
            breakpoint_store,
            agent_server_store,
            context_server_store,
            languages,
            extensions,
            git_store,
            environment,
            profiling_collector: gpui::ProfilingCollector::new(startup_time),
            _toolchain_store: toolchain_store,
            kernels: Default::default(),
            pty_manager,
            shutdown_request_handler: None,
            sandbox: None,
            hub: None,
            participants: Default::default(),
            retained_buffers: Vec::new(),
            edited_buffers: HashSet::default(),
            worktree_add_lock: Arc::default(),
        }
    }

    /// Serve mode: switches on everything sandbox-specific. Called at most once per
    /// process, right after construction (registering a handler twice panics, and
    /// The shared project and runtime survive participant reloads.
    /// every fresh session). Returns the control channel for the loopback listener.
    pub fn enable_sandbox(
        &mut self,
        config: SandboxConfig,
        cx: &mut Context<Self>,
    ) -> Arc<ControlChannel> {
        if let Some(sandbox) = &self.sandbox {
            debug_assert!(false, "enable_sandbox runs at most once per process");
            log::error!("enable_sandbox called twice; keeping the existing runtime");
            return sandbox.control.clone();
        }
        let session = self.session.clone();
        let client_state =
            cx.new(|cx| ClientStateStore::new(self.fs.clone(), config.client_state_dir, cx));
        // The stopping wait must end only on the D6 flush (tagged `stopping`), never on a
        // ticker save that was already in flight when the notice went out.
        let (control, mut install_requests) = ControlChannel::new(
            session.clone(),
            config.control_secret.clone(),
            cx.background_executor().clone(),
        );
        let ports = PortForwarder::new(
            config.supervisor_http,
            config.supervisor_url,
            config.control_secret,
        );
        let extension_dir = self.extensions.read(cx).extension_dir.clone();
        let extensions = cx.new(|_| {
            SandboxExtensions::new(
                self.extensions.clone(),
                config.registry,
                self.fs.clone(),
                extension_dir,
            )
        });

        session.add_request_handler(
            client_state.downgrade(),
            ClientStateStore::handle_save_client_state,
        );
        session.add_request_handler(
            client_state.downgrade(),
            ClientStateStore::handle_load_client_state,
        );
        session.add_request_handler(cx.weak_entity(), PortForwarder::handle_forward_port);
        session.add_request_handler(cx.weak_entity(), PortForwarder::handle_unforward_port);
        session.add_request_handler(
            extensions.downgrade(),
            SandboxExtensions::handle_list_extensions,
        );
        session.add_request_handler(
            extensions.downgrade(),
            SandboxExtensions::handle_install_registry_extension,
        );
        session.add_request_handler(
            extensions.downgrade(),
            SandboxExtensions::handle_uninstall_extension,
        );

        extensions
            .update(cx, |extensions, cx| extensions.load_installed_from_disk(cx))
            .detach_and_log_err(cx);

        let install_drain = cx.spawn({
            let extensions = extensions.clone();
            async move |_, cx| {
                while let Some(ControlEvent::InstallExtensions(ids)) =
                    futures::StreamExt::next(&mut install_requests).await
                {
                    for id in ids {
                        let id: Arc<str> = id.into();
                        // The installed check happens under the extensions' operation
                        // lock, after the startup scan that holds it has populated the
                        // installed set.
                        let install = extensions.update(cx, |extensions, cx| {
                            extensions.install_if_missing(id.clone(), cx)
                        });
                        if let Err(error) = install.await {
                            log::error!(
                                "installing extension {id} for the supervisor failed: {error:#}"
                            );
                        }
                    }
                }
            }
        });

        let extension_events = ExtensionEvents::try_global(cx).map(|events| {
            cx.subscribe(&events, |this, _, event, cx| {
                if matches!(event, extension::Event::ExtensionsInstalledChanged) {
                    this.send_extensions_changed(cx);
                }
            })
        });

        self.sandbox = Some(SandboxRuntime {
            client_state,
            control: control.clone(),
            ports,
            extensions,
            _install_drain: install_drain,
            _extension_events: extension_events,
        });
        control
    }

    /// `SyncExtensions`: the SSH store's sync in `run` mode. In serve mode the desktop's
    /// list is not authoritative for a sandbox, so the sync is additive and never removes a
    /// registry-installed extension (`SandboxExtensions::handle_sync_extensions`).
    async fn handle_sync_extensions(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::SyncExtensions>,
        cx: AsyncApp,
    ) -> Result<proto::SyncExtensionsResponse> {
        let (store, sandbox) = this.read_with(&cx, |this, _| {
            (
                this.extensions.clone(),
                this.sandbox
                    .as_ref()
                    .map(|sandbox| sandbox.extensions.clone()),
            )
        });
        match sandbox {
            Some(sandbox) => SandboxExtensions::handle_sync_extensions(sandbox, envelope, cx).await,
            None => HeadlessExtensionStore::handle_sync_extensions(store, envelope, cx).await,
        }
    }

    /// `InstallExtension` (the SSH upload path): the SSH store's install in `run` mode. In
    /// serve mode the id, version and upload directory are validated first
    /// (`SandboxExtensions::install_uploaded`), so a hostile client cannot name a directory
    /// outside the uploads directory or an id that escapes the extensions directory.
    async fn handle_install_extension(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::InstallExtension>,
        cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let (store, sandbox) = this.read_with(&cx, |this, _| {
            (
                this.extensions.clone(),
                this.sandbox
                    .as_ref()
                    .map(|sandbox| sandbox.extensions.clone()),
            )
        });
        match sandbox {
            Some(sandbox) => {
                SandboxExtensions::handle_install_extension(sandbox, envelope, cx).await
            }
            None => HeadlessExtensionStore::handle_install_extension(store, envelope, cx).await,
        }
    }

    /// Serve mode: called after every attach, fresh or reconnect. Replays the last port
    /// picture and a pending `Resumed`, then sends the current `ExtensionsChanged`.
    /// Idempotent: every message carries the full picture.
    pub fn on_session_attached(&mut self, cx: &mut Context<Self>) {
        let Some(sandbox) = &self.sandbox else {
            return;
        };
        sandbox.control.replay_after_attach();
        self.send_extensions_changed(cx);
    }

    pub fn add_participant(
        &mut self,
        peer: proto::PeerId,
        participant: &str,
        cx: &mut Context<Self>,
    ) {
        if let Some(sandbox) = &self.sandbox {
            sandbox.client_state.update(cx, |store, cx| {
                store.register_participant(peer, participant, cx)
            });
        }
    }

    pub fn reset_participant(&mut self, peer: proto::PeerId, cx: &mut Context<Self>) {
        // Reset only the participant's snapshot ledger, not the VM-owned buffers or LSPs.
        self.buffer_store
            .update(cx, |store, _| store.forget_shared_buffers_for(&peer));
    }

    pub fn participant_attached(&mut self, peer: proto::PeerId, cx: &mut Context<Self>) {
        self.participants.insert(peer);
        if let Some(hub) = &self.hub {
            hub.set_active(peer, true);
        }
        if let Some(sandbox) = &self.sandbox {
            sandbox.control.set_participant_saves(
                peer,
                sandbox
                    .client_state
                    .read(cx)
                    .participant_stopping_versions(peer, cx),
            );
        }
        let Ok(client) = self.session.for_peer(peer) else {
            return;
        };
        for participant in &self.participants {
            let collaborator = proto::Collaborator {
                peer_id: Some(*participant),
                replica_id: participant.id,
                user_id: participant.id as u64,
                is_host: false,
                committer_name: None,
                committer_email: None,
            };
            if *participant == peer {
                self.session
                    .send(proto::AddProjectCollaborator {
                        project_id: REMOTE_SERVER_PROJECT_ID,
                        collaborator: Some(collaborator),
                    })
                    .log_err();
            } else {
                client
                    .send(proto::AddProjectCollaborator {
                        project_id: REMOTE_SERVER_PROJECT_ID,
                        collaborator: Some(collaborator),
                    })
                    .log_err();
            }
        }
        let worktrees: Vec<_> = self.worktree_store.read(cx).worktrees().collect();
        // On a cold VM the browser adds its roots before replaying pre-boot messages.
        // Replaying an empty pre-boot project afterward would delete those new roots.
        if !worktrees.is_empty() {
            client
                .send(proto::UpdateProject {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    worktrees: worktrees
                        .iter()
                        .map(|w| w.read(cx).metadata_proto())
                        .collect(),
                })
                .log_err();
        }
        for worktree in worktrees {
            let worktree = worktree.read(cx);
            let update = worktree
                .snapshot()
                .build_initial_update(REMOTE_SERVER_PROJECT_ID, worktree.id().to_proto());
            for chunk in proto::split_worktree_update(update) {
                client.send(chunk).log_err();
            }
        }
        self.settings_observer
            .read(cx)
            .send_initial_state(&client, cx);
        self.lsp_store
            .read(cx)
            .send_initial_state(REMOTE_SERVER_PROJECT_ID, &client);
        self.git_store
            .read(cx)
            .send_initial_state(REMOTE_SERVER_PROJECT_ID, &client, cx);
        self.on_session_attached(cx);
    }

    pub fn participant_detached(&mut self, peer: proto::PeerId, _cx: &mut Context<Self>) {
        self.participants.remove(&peer);
        if let Some(sandbox) = &self.sandbox {
            sandbox.control.set_participant_saves(peer, None);
        }
        if let Some(hub) = &self.hub {
            hub.set_active(peer, false);
            for terminal in self.pty_manager.list() {
                if hub.owns_terminal(terminal.terminal_id, peer) {
                    self.pty_manager.detach(terminal.terminal_id);
                }
            }
        }
        self.session
            .send(proto::RemoveProjectCollaborator {
                project_id: REMOTE_SERVER_PROJECT_ID,
                peer_id: Some(peer),
            })
            .log_err();
    }

    /// Sends the installed set to the client and reports it to the supervisor.
    fn send_extensions_changed(&self, cx: &mut Context<Self>) {
        let Some(sandbox) = &self.sandbox else {
            return;
        };
        let records = sandbox.extensions.read(cx).installed_extension_records();
        self.session
            .send(proto::ExtensionsChanged {
                project_id: REMOTE_SERVER_PROJECT_ID,
                installed: records
                    .iter()
                    .map(InstalledExtensionRecord::to_proto)
                    .collect(),
            })
            .log_err();
        let ids: Vec<String> = records.iter().map(|record| record.id.to_string()).collect();
        let ports = sandbox.ports.clone();
        cx.background_spawn(async move {
            let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
            ports.report_installed_extensions(&ids).await.log_err();
        })
        .detach();
    }

    /// Serve mode: after `/files` wrote `abs_paths`, rescans them in their worktrees and
    /// tells the client which project paths changed. Paths outside every worktree are
    /// skipped.
    pub fn notify_files_uploaded(
        this: WeakEntity<Self>,
        abs_paths: Vec<PathBuf>,
        cx: &mut AsyncApp,
    ) -> Task<Result<()>> {
        cx.spawn(async move |cx| {
            let (session, per_worktree) = this.update(cx, |this, cx| {
                let worktree_store = this.worktree_store.read(cx);
                let mut per_worktree: HashMap<WorktreeId, (Entity<Worktree>, Vec<Arc<RelPath>>)> =
                    HashMap::default();
                for abs_path in &abs_paths {
                    match worktree_store.find_worktree(abs_path, cx) {
                        Some((worktree, rel_path)) => {
                            let id = worktree.read(cx).id();
                            per_worktree
                                .entry(id)
                                .or_insert_with(|| (worktree, Vec::new()))
                                .1
                                .push(rel_path);
                        }
                        None => log::debug!("uploaded path {abs_path:?} is outside every worktree"),
                    }
                }
                (this.session.clone(), per_worktree)
            })?;

            let mut paths = Vec::new();
            for (worktree_id, (worktree, rel_paths)) in per_worktree {
                let barrier = worktree.update(cx, |worktree, _| {
                    worktree
                        .as_local()
                        .map(|local| local.refresh_entries_for_paths(rel_paths.clone()))
                });
                if let Some(mut barrier) = barrier {
                    futures::StreamExt::next(&mut barrier).await;
                }
                paths.extend(rel_paths.into_iter().map(|rel_path| proto::ProjectPath {
                    worktree_id: worktree_id.to_proto(),
                    path: rel_path.as_unix_str().to_owned(),
                }));
            }
            if !paths.is_empty() {
                session.send(proto::FilesUploaded {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    paths,
                })?;
            }
            Ok(())
        })
    }

    fn on_buffer_event(
        &mut self,
        buffer: Entity<Buffer>,
        event: &BufferEvent,
        cx: &mut Context<Self>,
    ) {
        if let BufferEvent::Operation {
            operation,
            is_local,
        } = event
        {
            if matches!(operation, language::Operation::Buffer(_)) {
                self.edited_buffers
                    .insert(buffer.read(cx).remote_id().to_proto());
            }
            // Received batches (text and selections) are relayed by BufferStore.
            if !is_local {
                return;
            }
            cx.background_spawn(self.session.request(proto::UpdateBuffer {
                project_id: REMOTE_SERVER_PROJECT_ID,
                buffer_id: buffer.read(cx).remote_id().to_proto(),
                operations: vec![serialize_operation(operation)],
            }))
            .detach()
        }
    }

    fn on_lsp_store_event(
        &mut self,
        lsp_store: Entity<LspStore>,
        event: &LspStoreEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            LspStoreEvent::LanguageServerAdded(id, name, worktree_id) => {
                let log_store = cx
                    .try_global::<GlobalLogStore>()
                    .map(|lsp_logs| lsp_logs.0.clone());
                if let Some(log_store) = log_store {
                    log_store.update(cx, |log_store, cx| {
                        log_store.add_language_server(
                            LanguageServerKind::LocalSsh {
                                lsp_store: self.lsp_store.downgrade(),
                            },
                            *id,
                            Some(name.clone()),
                            *worktree_id,
                            lsp_store.read(cx).language_server_for_id(*id),
                            cx,
                        );
                    });
                }
            }
            LspStoreEvent::LanguageServerRemoved(id) => {
                let log_store = cx
                    .try_global::<GlobalLogStore>()
                    .map(|lsp_logs| lsp_logs.0.clone());
                if let Some(log_store) = log_store {
                    log_store.update(cx, |log_store, cx| {
                        log_store.remove_language_server(*id, cx);
                    });
                }
                self.session
                    .send(proto::UpdateLanguageServer {
                        project_id: REMOTE_SERVER_PROJECT_ID,
                        server_name: None,
                        language_server_id: id.to_proto(),
                        variant: Some(proto::update_language_server::Variant::Removed(
                            proto::ServerRemoved {},
                        )),
                    })
                    .log_err();
            }
            LspStoreEvent::LanguageServerUpdate {
                language_server_id,
                name,
                message,
            } => {
                self.session
                    .send(proto::UpdateLanguageServer {
                        project_id: REMOTE_SERVER_PROJECT_ID,
                        server_name: name.as_ref().map(|name| name.to_string()),
                        language_server_id: language_server_id.to_proto(),
                        variant: Some(message.clone()),
                    })
                    .log_err();
            }
            LspStoreEvent::Notification(message) => {
                self.session
                    .send(proto::Toast {
                        project_id: REMOTE_SERVER_PROJECT_ID,
                        notification_id: "lsp".to_string(),
                        message: message.clone(),
                    })
                    .log_err();
            }
            LspStoreEvent::LanguageServerPrompt(prompt) => {
                let request = self.session.request(proto::LanguageServerPromptRequest {
                    project_id: REMOTE_SERVER_PROJECT_ID,
                    actions: prompt
                        .actions
                        .iter()
                        .map(|action| action.title.to_string())
                        .collect(),
                    level: Some(prompt_to_proto(prompt)),
                    lsp_name: prompt.lsp_name.clone(),
                    message: prompt.message.clone(),
                });
                let prompt = prompt.clone();
                cx.background_spawn(async move {
                    let response = request.await?;
                    if let Some(action_response) = response.action_response {
                        prompt.respond(action_response as usize).await;
                    }
                    anyhow::Ok(())
                })
                .detach();
            }
            _ => {}
        }
    }

    pub async fn handle_add_worktree(
        this: Entity<Self>,
        message: TypedEnvelope<proto::AddWorktree>,
        mut cx: AsyncApp,
    ) -> Result<proto::AddWorktreeResponse> {
        use client::ErrorCodeExt;
        let lock = this.read_with(&cx, |this, _| this.worktree_add_lock.clone());
        let guard = lock.lock_owned().await;
        let fs = this.read_with(&cx, |this, _| this.fs.clone());
        let path = PathBuf::from(shellexpand::tilde(&message.payload.path).to_string());

        let canonicalized = match fs.canonicalize(&path).await {
            Ok(path) => path,
            Err(e) => {
                let mut parent = path
                    .parent()
                    .ok_or(e)
                    .with_context(|| format!("{path:?} does not exist"))?;
                if parent == Path::new("") {
                    parent = util::paths::home_dir();
                }
                let parent = fs.canonicalize(parent).await.map_err(|_| {
                    anyhow!(
                        proto::ErrorCode::DevServerProjectPathDoesNotExist
                            .with_tag("path", path.to_string_lossy().as_ref())
                    )
                })?;
                if let Some(file_name) = path.file_name() {
                    parent.join(file_name)
                } else {
                    parent
                }
            }
        };
        if let Some(response) = this.read_with(&cx, |this, cx| {
            this.worktree_store
                .read(cx)
                .worktrees()
                .find_map(|worktree| {
                    let worktree = worktree.read(cx);
                    (worktree.abs_path().as_ref() == canonicalized.as_path()).then(|| {
                        proto::AddWorktreeResponse {
                            worktree_id: worktree.id().to_proto(),
                            canonicalized_path: canonicalized.to_string_lossy().into_owned(),
                            root_repo_common_dir: worktree
                                .root_repo_common_dir()
                                .map(|p| p.to_string_lossy().into_owned()),
                            root_repo_is_linked_worktree: worktree.root_repo_is_linked_worktree(),
                        }
                    })
                })
        }) {
            return Ok(response);
        }
        let next_worktree_id = this
            .update(&mut cx, |this, cx| {
                this.worktree_store
                    .update(cx, |worktree_store, _| worktree_store.next_worktree_id())
            })
            .await?;
        let worktree = this
            .read_with(&cx.clone(), |this, _| {
                Worktree::local(
                    Arc::from(canonicalized.as_path()),
                    message.payload.visible,
                    this.fs.clone(),
                    this.next_entry_id.clone(),
                    true,
                    next_worktree_id,
                    &mut cx,
                )
            })
            .await?;

        let response = this.read_with(&cx, |_, cx| {
            let worktree = worktree.read(cx);
            proto::AddWorktreeResponse {
                worktree_id: worktree.id().to_proto(),
                canonicalized_path: canonicalized.to_string_lossy().into_owned(),
                root_repo_common_dir: worktree
                    .root_repo_common_dir()
                    .map(|p| p.to_string_lossy().into_owned()),
                root_repo_is_linked_worktree: worktree.root_repo_is_linked_worktree(),
            }
        });

        // We spawn this asynchronously, so that we can send the response back
        // *before* `worktree_store.add()` can send out UpdateProject requests
        // to the client about the new worktree.
        //
        // That lets the client manage the reference/handles of the newly-added
        // worktree, before getting interrupted by an UpdateProject request.
        //
        // This fixes the problem of the client sending the AddWorktree request,
        // headless project sending out a project update, client receiving it
        // and immediately dropping the reference of the new client, causing it
        // to be dropped on the headless project, and the client only then
        // receiving a response to AddWorktree.
        cx.spawn(async move |cx| {
            let _guard = guard;
            this.update(cx, |this, cx| {
                this.worktree_store.update(cx, |worktree_store, cx| {
                    worktree_store.add(&worktree, cx);
                });
            });
        })
        .detach();

        Ok(response)
    }

    pub async fn handle_remove_worktree(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::RemoveWorktree>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        // The VM owns collaborative roots. Dropping a tab's local worktree handles must
        // not tear down scanners, buffers or language servers used by other participants.
        if envelope.original_sender_id.is_some_and(|peer| peer.id >= 8) {
            return Ok(proto::Ack {});
        }
        let worktree_id = WorktreeId::from_proto(envelope.payload.worktree_id);
        this.update(&mut cx, |this, cx| {
            this.worktree_store.update(cx, |worktree_store, cx| {
                worktree_store.remove_worktree(worktree_id, cx);
            });
        });
        Ok(proto::Ack {})
    }

    /// Serve mode: installs the callback `handle_shutdown_remote_server` runs instead of
    /// quitting the process.
    pub fn set_shutdown_request_handler(&mut self, handler: Arc<dyn Fn() + Send + Sync>) {
        self.shutdown_request_handler = Some(handler);
    }

    pub async fn handle_open_buffer_by_path(
        this: Entity<Self>,
        message: TypedEnvelope<proto::OpenBufferByPath>,
        mut cx: AsyncApp,
    ) -> Result<proto::OpenBufferResponse> {
        let peer_id = message.original_sender_id.unwrap_or(message.sender_id);
        let worktree_id = WorktreeId::from_proto(message.payload.worktree_id);
        let path = RelPath::from_unix_str(&message.payload.path)?.into();
        let (buffer_store, buffer) = this.update(&mut cx, |this, cx| {
            let buffer_store = this.buffer_store.clone();
            let buffer = this.buffer_store.update(cx, |buffer_store, cx| {
                buffer_store.open_buffer(ProjectPath { worktree_id, path }, cx)
            });
            (buffer_store, buffer)
        });

        let buffer = buffer.await?;
        let buffer_id = buffer.read_with(&cx, |b, _| b.remote_id());
        buffer_store.update(&mut cx, |buffer_store, cx| {
            buffer_store
                .create_buffer_for_peer(&buffer, peer_id, cx)
                .detach_and_log_err(cx);
        });

        Ok(proto::OpenBufferResponse {
            buffer_id: buffer_id.to_proto(),
        })
    }

    pub async fn handle_open_image_by_path(
        this: Entity<Self>,
        message: TypedEnvelope<proto::OpenImageByPath>,
        mut cx: AsyncApp,
    ) -> Result<proto::OpenImageResponse> {
        let peer_id = message.original_sender_id.unwrap_or(message.sender_id);
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let worktree_id = WorktreeId::from_proto(message.payload.worktree_id);
        let path = RelPath::from_unix_str(&message.payload.path)?;
        let project_id = message.payload.project_id;
        use proto::create_image_for_peer::Variant;

        let (worktree_store, session) = this.read_with(&cx, |this, _| {
            (this.worktree_store.clone(), this.session.clone())
        });

        let worktree = worktree_store
            .read_with(&cx, |store, cx| store.worktree_for_id(worktree_id, cx))
            .context("worktree not found")?;

        let load_task = worktree.update(&mut cx, |worktree, cx| {
            worktree.load_binary_file(path.as_ref(), cx)
        });

        let loaded_file = load_task.await?;
        let content = loaded_file.content;
        let file = loaded_file.file;

        let proto_file = worktree.read_with(&cx, |_worktree, cx| file.to_proto(cx));
        let image_id =
            ImageId::from(NonZeroU64::new(NEXT_ID.fetch_add(1, Ordering::Relaxed)).unwrap());

        let format = image::guess_format(&content)
            .map(|f| format!("{:?}", f).to_lowercase())
            .unwrap_or_else(|_| "unknown".to_string());

        let state = proto::ImageState {
            id: image_id.to_proto(),
            file: Some(proto_file),
            content_size: content.len() as u64,
            format,
        };

        session.send(proto::CreateImageForPeer {
            project_id,
            peer_id: Some(peer_id),
            variant: Some(Variant::State(state)),
        })?;

        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
        for chunk in content.chunks(CHUNK_SIZE) {
            session.send(proto::CreateImageForPeer {
                project_id,
                peer_id: Some(peer_id),
                variant: Some(Variant::Chunk(proto::ImageChunk {
                    image_id: image_id.to_proto(),
                    data: chunk.to_vec(),
                })),
            })?;
        }

        Ok(proto::OpenImageResponse {
            image_id: image_id.to_proto(),
        })
    }

    pub async fn handle_trust_worktrees(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::TrustWorktrees>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let trusted_worktrees = cx
            .update(|cx| TrustedWorktrees::try_get_global(cx))
            .context("missing trusted worktrees")?;
        let worktree_store = this.read_with(&cx, |project, _| project.worktree_store.clone());
        trusted_worktrees.update(&mut cx, |trusted_worktrees, cx| {
            trusted_worktrees.trust(
                &worktree_store,
                envelope
                    .payload
                    .trusted_paths
                    .into_iter()
                    .filter_map(PathTrust::from_proto)
                    .collect(),
                cx,
            );
        });
        Ok(proto::Ack {})
    }

    pub async fn handle_restrict_worktrees(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::RestrictWorktrees>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let trusted_worktrees = cx
            .update(|cx| TrustedWorktrees::try_get_global(cx))
            .context("missing trusted worktrees")?;
        let worktree_store = this.read_with(&cx, |project, _| project.worktree_store.downgrade());
        trusted_worktrees.update(&mut cx, |trusted_worktrees, cx| {
            let restricted_paths = envelope
                .payload
                .worktree_ids
                .into_iter()
                .map(WorktreeId::from_proto)
                .map(PathTrust::Worktree)
                .collect::<HashSet<_>>();
            trusted_worktrees.restrict(worktree_store, restricted_paths, cx);
        });
        Ok(proto::Ack {})
    }

    pub async fn handle_download_file_by_path(
        this: Entity<Self>,
        message: TypedEnvelope<proto::DownloadFileByPath>,
        mut cx: AsyncApp,
    ) -> Result<proto::DownloadFileResponse> {
        let peer_id = message.original_sender_id.unwrap_or(message.sender_id);
        log::debug!(
            "handle_download_file_by_path: received request: {:?}",
            message.payload
        );

        let worktree_id = WorktreeId::from_proto(message.payload.worktree_id);
        let path = RelPath::from_unix_str(&message.payload.path)?;
        let project_id = message.payload.project_id;
        let file_id = message.payload.file_id;
        log::debug!(
            "handle_download_file_by_path: worktree_id={:?}, path={:?}, file_id={}",
            worktree_id,
            path,
            file_id
        );
        use proto::create_file_for_peer::Variant;

        let (worktree_store, session): (Entity<WorktreeStore>, AnyProtoClient) = this
            .read_with(&cx, |this, _| {
                (this.worktree_store.clone(), this.session.clone())
            });

        let worktree = worktree_store
            .read_with(&cx, |store, cx| store.worktree_for_id(worktree_id, cx))
            .context("worktree not found")?;

        let download_task = worktree.update(&mut cx, |worktree: &mut Worktree, cx| {
            worktree.load_binary_file(path.as_ref(), cx)
        });

        let downloaded_file = download_task.await?;
        let content = downloaded_file.content;
        let file = downloaded_file.file;
        log::debug!(
            "handle_download_file_by_path: file loaded, content_size={}",
            content.len()
        );

        let proto_file = worktree.read_with(&cx, |_worktree: &Worktree, cx| file.to_proto(cx));
        log::debug!(
            "handle_download_file_by_path: using client-provided file_id={}",
            file_id
        );

        let state = proto::FileState {
            id: file_id,
            file: Some(proto_file),
            content_size: content.len() as u64,
        };

        log::debug!("handle_download_file_by_path: sending State message");
        session.send(proto::CreateFileForPeer {
            project_id,
            peer_id: Some(peer_id),
            variant: Some(Variant::State(state)),
        })?;

        const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
        let num_chunks = content.len().div_ceil(CHUNK_SIZE);
        log::debug!(
            "handle_download_file_by_path: sending {} chunks",
            num_chunks
        );
        for (i, chunk) in content.chunks(CHUNK_SIZE).enumerate() {
            log::trace!(
                "handle_download_file_by_path: sending chunk {}/{}, size={}",
                i + 1,
                num_chunks,
                chunk.len()
            );
            session.send(proto::CreateFileForPeer {
                project_id,
                peer_id: Some(peer_id),
                variant: Some(Variant::Chunk(proto::FileChunk {
                    file_id,
                    data: chunk.to_vec(),
                })),
            })?;
        }

        log::debug!(
            "handle_download_file_by_path: returning file_id={}",
            file_id
        );
        Ok(proto::DownloadFileResponse { file_id })
    }

    pub async fn handle_open_new_buffer(
        this: Entity<Self>,
        message: TypedEnvelope<proto::OpenNewBuffer>,
        mut cx: AsyncApp,
    ) -> Result<proto::OpenBufferResponse> {
        let peer_id = message.original_sender_id.unwrap_or(message.sender_id);
        let (buffer_store, buffer) = this.update(&mut cx, |this, cx| {
            let buffer_store = this.buffer_store.clone();
            let buffer = this.buffer_store.update(cx, |buffer_store, cx| {
                buffer_store.create_buffer(None, true, cx)
            });
            (buffer_store, buffer)
        });

        let buffer = buffer.await?;
        let buffer_id = buffer.read_with(&cx, |b, _| b.remote_id());
        buffer_store.update(&mut cx, |buffer_store, cx| {
            buffer_store
                .create_buffer_for_peer(&buffer, peer_id, cx)
                .detach_and_log_err(cx);
        });

        Ok(proto::OpenBufferResponse {
            buffer_id: buffer_id.to_proto(),
        })
    }

    async fn handle_toggle_lsp_logs(
        _: Entity<Self>,
        envelope: TypedEnvelope<proto::ToggleLspLogs>,
        cx: AsyncApp,
    ) -> Result<()> {
        let server_id = LanguageServerId::from_proto(envelope.payload.server_id);
        cx.update(|cx| {
            let log_store = cx
                .try_global::<GlobalLogStore>()
                .map(|global_log_store| global_log_store.0.clone())
                .context("lsp logs store is missing")?;
            let toggled_log_kind =
                match proto::toggle_lsp_logs::LogType::try_from(envelope.payload.log_type)
                    .ok()
                    .context("invalid log type")?
                {
                    proto::toggle_lsp_logs::LogType::Log => LogKind::Logs,
                    proto::toggle_lsp_logs::LogType::Trace => LogKind::Trace,
                    proto::toggle_lsp_logs::LogType::Rpc => LogKind::Rpc,
                };
            log_store.update(cx, |log_store, _| {
                log_store.toggle_lsp_logs(server_id, envelope.payload.enabled, toggled_log_kind);
            });
            anyhow::Ok(())
        })?;

        Ok(())
    }

    async fn handle_open_server_settings(
        this: Entity<Self>,
        message: TypedEnvelope<proto::OpenServerSettings>,
        mut cx: AsyncApp,
    ) -> Result<proto::OpenBufferResponse> {
        let peer_id = message.original_sender_id.unwrap_or(message.sender_id);
        let settings_path = paths::settings_file();
        let (worktree, path) = this
            .update(&mut cx, |this, cx| {
                this.worktree_store.update(cx, |worktree_store, cx| {
                    worktree_store.find_or_create_worktree(settings_path, false, cx)
                })
            })
            .await?;

        let (buffer, buffer_store) = this.update(&mut cx, |this, cx| {
            let buffer = this.buffer_store.update(cx, |buffer_store, cx| {
                buffer_store.open_buffer(
                    ProjectPath {
                        worktree_id: worktree.read(cx).id(),
                        path,
                    },
                    cx,
                )
            });

            (buffer, this.buffer_store.clone())
        });

        let buffer = buffer.await?;

        let buffer_id = cx.update(|cx| {
            if buffer.read(cx).is_empty() {
                buffer.update(cx, |buffer, cx| {
                    buffer.edit([(0..0, initial_server_settings_content())], None, cx)
                });
            }

            let buffer_id = buffer.read(cx).remote_id();

            buffer_store.update(cx, |buffer_store, cx| {
                buffer_store
                    .create_buffer_for_peer(&buffer, peer_id, cx)
                    .detach_and_log_err(cx);
            });

            buffer_id
        });

        Ok(proto::OpenBufferResponse {
            buffer_id: buffer_id.to_proto(),
        })
    }

    async fn handle_spawn_kernel(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::SpawnKernel>,
        cx: AsyncApp,
    ) -> Result<proto::SpawnKernelResponse> {
        let fs = this.update(&mut cx.clone(), |this, _| this.fs.clone());

        let mut ports = Vec::new();
        for _ in 0..5 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            ports.push(port);
        }

        let connection_info = serde_json::json!({
            "shell_port": ports[0],
            "iopub_port": ports[1],
            "stdin_port": ports[2],
            "control_port": ports[3],
            "hb_port": ports[4],
            "ip": "127.0.0.1",
            "key": uuid::Uuid::new_v4().to_string(),
            "transport": "tcp",
            "signature_scheme": "hmac-sha256",
            "kernel_name": envelope.payload.kernel_name,
        });

        let connection_file_content = serde_json::to_string_pretty(&connection_info)?;
        let kernel_id = uuid::Uuid::new_v4().to_string();

        let connection_file_path = std::env::temp_dir().join(format!("kernel-{}.json", kernel_id));
        fs.save(
            &connection_file_path,
            &connection_file_content.as_str().into(),
            language::LineEnding::Unix,
        )
        .await?;

        let working_directory = if envelope.payload.working_directory.is_empty() {
            std::env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        } else {
            Some(envelope.payload.working_directory)
        };

        // Spawn kernel (Assuming python for now, or we'd need to parse kernelspec logic here or pass the command)

        // Spawn kernel
        let spawn_kernel = |binary: &str, args: &[String]| {
            let mut command = smol::process::Command::new(binary);

            if !args.is_empty() {
                for arg in args {
                    if arg == "{connection_file}" {
                        command.arg(&connection_file_path);
                    } else {
                        command.arg(arg);
                    }
                }
            } else {
                command
                    .arg("-m")
                    .arg("ipykernel_launcher")
                    .arg("-f")
                    .arg(&connection_file_path);
            }

            // This ensures subprocesses spawned from the kernel use the correct Python environment
            let python_bin_dir = std::path::Path::new(binary).parent();
            if let Some(bin_dir) = python_bin_dir {
                if let Some(path_var) = std::env::var_os("PATH") {
                    let mut paths = std::env::split_paths(&path_var).collect::<Vec<_>>();
                    paths.insert(0, bin_dir.to_path_buf());
                    if let Ok(new_path) = std::env::join_paths(paths) {
                        command.env("PATH", new_path);
                    }
                }

                if let Some(venv_root) = bin_dir.parent() {
                    command.env("VIRTUAL_ENV", venv_root.to_string_lossy().to_string());
                }
            }

            if let Some(wd) = &working_directory {
                command.current_dir(wd);
            }
            command.spawn()
        };

        // We need to manage the child process lifecycle
        let child = if !envelope.payload.command.is_empty() {
            spawn_kernel(&envelope.payload.command, &envelope.payload.args).context(format!(
                "failed to spawn kernel process (command: {})",
                envelope.payload.command
            ))?
        } else if let Some(venv_python) = working_directory
            .as_ref()
            .and_then(|wd| find_venv_python(wd))
        {
            let path_str = venv_python.to_string_lossy().to_string();
            spawn_kernel(&path_str, &[]).context(format!(
                "failed to spawn kernel process (venv: {})",
                path_str
            ))?
        } else {
            spawn_kernel("python3", &[])
                .or_else(|_| spawn_kernel("python", &[]))
                .context("failed to spawn kernel process (tried python3 and python)")?
        };

        this.update(&mut cx.clone(), |this, _cx| {
            this.kernels.insert(kernel_id.clone(), child);
        });

        Ok(proto::SpawnKernelResponse {
            kernel_id,
            connection_file: connection_file_content,
        })
    }

    async fn handle_kill_kernel(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::KillKernel>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let kernel_id = envelope.payload.kernel_id;
        let child = this.update(&mut cx, |this, _| this.kernels.remove(&kernel_id));
        if let Some(mut child) = child {
            child.kill().log_err();
        }
        Ok(proto::Ack {})
    }

    async fn handle_find_search_candidates(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::FindSearchCandidates>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        use futures::stream::StreamExt as _;

        let peer_id = envelope.original_sender_id.unwrap_or(envelope.sender_id);
        let message = envelope.payload;
        let query = SearchQuery::from_proto(
            message.query.context("missing query field")?,
            PathStyle::local(),
        )?;

        let project_id = message.project_id;
        let buffer_store = this.read_with(&cx, |this, _| this.buffer_store.clone());
        let handle = message.handle;
        let _buffer_store = buffer_store.clone();
        let client = this.read_with(&cx, |this, _| this.session.for_peer(peer_id))?;
        let task = cx.spawn(async move |cx| {
            let results = this.update(cx, |this, cx| {
                project::Search::local(
                    this.fs.clone(),
                    this.buffer_store.clone(),
                    this.worktree_store.clone(),
                    message.limit as _,
                    cx,
                )
                .into_handle(query, cx)
                .matching_buffers(cx)
            });
            let (batcher, batches) =
                project::project_search::AdaptiveBatcher::new(cx.background_executor());
            let mut new_matches = Box::pin(results.rx);

            let sender_task = cx.background_executor().spawn({
                let client = client.clone();
                async move {
                    let mut batches = std::pin::pin!(batches);
                    while let Some(buffer_ids) = batches.next().await {
                        client
                            .request(proto::FindSearchCandidatesChunk {
                                handle,
                                peer_id: Some(peer_id),
                                project_id,
                                variant: Some(
                                    proto::find_search_candidates_chunk::Variant::Matches(
                                        proto::FindSearchCandidatesMatches { buffer_ids },
                                    ),
                                ),
                            })
                            .await?;
                    }
                    anyhow::Ok(())
                }
            });

            while let Some((buffer, _)) = new_matches.next().await {
                let _ = buffer_store
                    .update(cx, |this, cx| {
                        this.create_buffer_for_peer(&buffer, peer_id, cx)
                    })
                    .await;
                let buffer_id = buffer.read_with(cx, |this, _| this.remote_id().to_proto());
                batcher.push(buffer_id).await;
            }
            batcher.flush().await;

            sender_task.await?;

            client
                .request(proto::FindSearchCandidatesChunk {
                    handle,
                    peer_id: Some(peer_id),
                    project_id,
                    variant: Some(proto::find_search_candidates_chunk::Variant::Done(
                        proto::FindSearchCandidatesDone {},
                    )),
                })
                .await?;
            anyhow::Ok(())
        });
        _buffer_store.update(&mut cx, |this, _| {
            this.register_ongoing_project_search((peer_id, handle), task);
        });

        Ok(proto::Ack {})
    }

    // Goes from client to host.
    async fn handle_find_search_candidates_cancel(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::FindSearchCandidatesCancelled>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        let buffer_store = this.read_with(&mut cx, |this, _| this.buffer_store.clone());
        BufferStore::handle_find_search_candidates_cancel(buffer_store, envelope, cx).await
    }

    async fn handle_list_remote_directory(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::ListRemoteDirectory>,
        cx: AsyncApp,
    ) -> Result<proto::ListRemoteDirectoryResponse> {
        use smol::stream::StreamExt;
        let fs = cx.read_entity(&this, |this, _| this.fs.clone());
        let expanded = PathBuf::from(shellexpand::tilde(&envelope.payload.path).to_string());
        let check_info = envelope
            .payload
            .config
            .as_ref()
            .is_some_and(|config| config.is_dir);

        let mut entries = Vec::new();
        let mut entry_info = Vec::new();
        let mut response = fs.read_dir(&expanded).await?;
        while let Some(path) = response.next().await {
            let path = path?;
            if let Some(file_name) = path.file_name() {
                entries.push(file_name.to_string_lossy().into_owned());
                if check_info {
                    let is_dir = fs.is_dir(&path).await;
                    entry_info.push(proto::EntryInfo { is_dir });
                }
            }
        }
        Ok(proto::ListRemoteDirectoryResponse {
            entries,
            entry_info,
        })
    }

    async fn handle_get_path_metadata(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::GetPathMetadata>,
        cx: AsyncApp,
    ) -> Result<proto::GetPathMetadataResponse> {
        let fs = cx.read_entity(&this, |this, _| this.fs.clone());
        let expanded = PathBuf::from(shellexpand::tilde(&envelope.payload.path).to_string());

        let metadata = fs.metadata(&expanded).await?;
        let is_dir = metadata.map(|metadata| metadata.is_dir).unwrap_or(false);

        Ok(proto::GetPathMetadataResponse {
            exists: metadata.is_some(),
            is_dir,
            path: expanded.to_string_lossy().into_owned(),
        })
    }

    async fn handle_shutdown_remote_server(
        this: Entity<Self>,
        _envelope: TypedEnvelope<proto::ShutdownRemoteServer>,
        cx: AsyncApp,
    ) -> Result<proto::Ack> {
        if let Some(on_shutdown) =
            this.read_with(&cx, |this, _| this.shutdown_request_handler.clone())
        {
            // serve: the broker flushes this Ack, then closes the session.
            on_shutdown();
            return Ok(proto::Ack {});
        }
        cx.spawn(async move |cx| {
            cx.update(|cx| {
                // TODO: This is a hack, because in a headless project, shutdown isn't executed
                // when calling quit, but it should be.
                cx.shutdown();
                cx.quit();
            })
        })
        .detach();

        Ok(proto::Ack {})
    }

    pub async fn handle_ping(
        _this: Entity<Self>,
        _envelope: TypedEnvelope<proto::Ping>,
        _cx: AsyncApp,
    ) -> Result<proto::Ack> {
        log::debug!("Received ping from client");
        Ok(proto::Ack {})
    }

    async fn handle_get_processes(
        _this: Entity<Self>,
        _envelope: TypedEnvelope<proto::GetProcesses>,
        _cx: AsyncApp,
    ) -> Result<proto::GetProcessesResponse> {
        let mut processes = Vec::new();
        let refresh_kind = RefreshKind::nothing().with_processes(
            ProcessRefreshKind::nothing()
                .without_tasks()
                .with_cmd(UpdateKind::Always),
        );

        for process in System::new_with_specifics(refresh_kind)
            .processes()
            .values()
        {
            let name = process.name().to_string_lossy().into_owned();
            let command = process
                .cmd()
                .iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect::<Vec<_>>();

            processes.push(proto::ProcessInfo {
                pid: process.pid().as_u32(),
                name,
                command,
            });
        }

        processes.sort_by_key(|p| p.name.clone());

        Ok(proto::GetProcessesResponse { processes })
    }

    async fn handle_get_remote_profiling_data(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::GetRemoteProfilingData>,
        cx: AsyncApp,
    ) -> Result<proto::GetRemoteProfilingDataResponse> {
        let foreground_only = envelope.payload.foreground_only;

        let (deltas, now_nanos) = cx.update(|cx| {
            let timings = if foreground_only {
                vec![gpui::profiler::get_current_thread_timings(
                    TasksIncluded::OnlyCompleted,
                )]
            } else {
                gpui::profiler::get_all_timings(TasksIncluded::OnlyCompleted)
            };
            this.update(cx, |this, _cx| {
                let deltas = this.profiling_collector.collect_unseen(timings);
                let now_nanos = Instant::now()
                    .duration_since(this.profiling_collector.startup_time())
                    .as_nanos() as u64;
                (deltas, now_nanos)
            })
        });

        let threads = deltas
            .into_iter()
            .map(|delta| proto::RemoteProfilingThread {
                thread_name: delta.thread_name,
                thread_id: delta.thread_id,
                timings: delta
                    .new_timings
                    .into_iter()
                    .map(|t| proto::RemoteProfilingTiming {
                        location: Some(proto::RemoteProfilingLocation {
                            file: t.location.file.to_string(),
                            line: t.location.line,
                            column: t.location.column,
                        }),
                        start_nanos: t.start as u64,
                        duration_nanos: t.duration as u64,
                    })
                    .collect(),
            })
            .collect();

        Ok(proto::GetRemoteProfilingDataResponse { threads, now_nanos })
    }

    async fn handle_get_directory_environment(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::GetDirectoryEnvironment>,
        mut cx: AsyncApp,
    ) -> Result<proto::DirectoryEnvironment> {
        let shell = task::shell_from_proto(envelope.payload.shell.context("missing shell")?)?;
        let directory = PathBuf::from(envelope.payload.directory);
        let environment = this
            .update(&mut cx, |this, cx| {
                this.environment.update(cx, |environment, cx| {
                    environment.local_directory_environment(&shell, directory.into(), cx)
                })
            })
            .await
            .context("failed to get directory environment")?
            .into_iter()
            .collect();
        Ok(proto::DirectoryEnvironment { environment })
    }

    /// Longest client-controlled string accepted on `SpawnTerminal`. A terminal
    /// entry lives for the process's lifetime and its title/cwd are echoed by
    /// `ListTerminals`, so an oversized value would poison the inventory (and
    /// could push a `ListTerminalsResponse` past the 16 MiB frame ceiling) for
    /// the rest of the session.
    const MAX_SPAWN_STRING_BYTES: usize = 8 * 1024;
    /// Largest environment map accepted on `SpawnTerminal`.
    const MAX_SPAWN_ENV_ENTRIES: usize = 4096;

    async fn handle_spawn_terminal(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::SpawnTerminal>,
        mut cx: AsyncApp,
    ) -> Result<proto::SpawnTerminalResponse> {
        let peer = envelope.original_sender_id.unwrap_or(envelope.sender_id);
        let payload = envelope.payload;
        if let Some(title) = &payload.title {
            anyhow::ensure!(
                title.len() <= Self::MAX_SPAWN_STRING_BYTES,
                "terminal title is too long"
            );
        }
        if let Some(task_id) = &payload.task_id {
            anyhow::ensure!(
                task_id.len() <= Self::MAX_SPAWN_STRING_BYTES,
                "terminal task id is too long"
            );
        }
        if let Some(working_directory) = &payload.working_directory {
            anyhow::ensure!(
                working_directory.len() <= Self::MAX_SPAWN_STRING_BYTES,
                "terminal working directory is too long"
            );
        }
        anyhow::ensure!(
            payload.env.len() <= Self::MAX_SPAWN_ENV_ENTRIES,
            "terminal environment has too many entries"
        );
        anyhow::ensure!(
            payload
                .env
                .iter()
                .all(|(key, value)| key.len() + value.len() <= Self::MAX_SPAWN_STRING_BYTES),
            "terminal environment entry is too long"
        );
        let shell = task::shell_from_proto(payload.shell.context("missing shell")?)?;
        let options = pty::SpawnOptions {
            shell,
            working_directory: payload.working_directory.map(PathBuf::from),
            env: payload.env.into_iter().collect(),
            cols: terminal_dimension(payload.cols),
            rows: terminal_dimension(payload.rows),
            task_id: payload.task_id,
            title: payload.title,
        };
        let terminal_id = this.update(&mut cx, |this, _| {
            let id = this.pty_manager.spawn(options)?;
            if let Some(hub) = &this.hub {
                hub.own_terminal(id, peer);
            }
            anyhow::Ok(id)
        })?;
        Ok(proto::SpawnTerminalResponse { terminal_id })
    }

    async fn handle_terminal_input(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::TerminalInput>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        this.read_with(&cx, |this, _| {
            this.check_terminal_peer(
                envelope.payload.terminal_id,
                envelope.original_sender_id.unwrap_or(envelope.sender_id),
            )
        })?;
        let proto::TerminalInput {
            terminal_id, data, ..
        } = envelope.payload;
        this.update(&mut cx, |this, _| this.pty_manager.write(terminal_id, data))
    }

    async fn handle_ack_terminal_output(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::AckTerminalOutput>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        this.read_with(&cx, |this, _| {
            this.check_terminal_peer(
                envelope.payload.terminal_id,
                envelope.original_sender_id.unwrap_or(envelope.sender_id),
            )
        })?;
        let proto::AckTerminalOutput {
            terminal_id,
            offset,
            ..
        } = envelope.payload;
        this.update(&mut cx, |this, _| this.pty_manager.ack(terminal_id, offset));
        Ok(())
    }

    async fn handle_resize_terminal(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::ResizeTerminal>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        this.read_with(&cx, |this, _| {
            this.check_terminal_peer(
                envelope.payload.terminal_id,
                envelope.original_sender_id.unwrap_or(envelope.sender_id),
            )
        })?;
        let proto::ResizeTerminal {
            terminal_id,
            cols,
            rows,
            ..
        } = envelope.payload;
        this.update(&mut cx, |this, _| {
            this.pty_manager.resize(
                terminal_id,
                terminal_dimension(cols),
                terminal_dimension(rows),
            )
        })
    }

    async fn handle_close_terminal(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::CloseTerminal>,
        mut cx: AsyncApp,
    ) -> Result<()> {
        this.read_with(&cx, |this, _| {
            this.check_terminal_peer(
                envelope.payload.terminal_id,
                envelope.original_sender_id.unwrap_or(envelope.sender_id),
            )
        })?;
        let terminal_id = envelope.payload.terminal_id;
        // The client's `Drop` legitimately sends a close after the exit path
        // already removed the entry.
        if let Err(error) = this.update(&mut cx, |this, _| this.pty_manager.close(terminal_id)) {
            log::debug!("ignoring CloseTerminal for {terminal_id}: {error:#}");
        }
        Ok(())
    }

    async fn handle_list_terminals(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::ListTerminals>,
        mut cx: AsyncApp,
    ) -> Result<proto::ListTerminalsResponse> {
        let peer = envelope.original_sender_id.unwrap_or(envelope.sender_id);
        let terminals = this.update(&mut cx, |this, _| {
            this.pty_manager
                .list()
                .into_iter()
                .filter(|terminal| this.check_terminal_peer(terminal.terminal_id, peer).is_ok())
                .collect()
        });
        Ok(proto::ListTerminalsResponse { terminals })
    }

    async fn handle_attach_terminal(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::AttachTerminal>,
        mut cx: AsyncApp,
    ) -> Result<proto::AttachTerminalResponse> {
        this.read_with(&cx, |this, _| {
            this.check_terminal_peer(
                envelope.payload.terminal_id,
                envelope.original_sender_id.unwrap_or(envelope.sender_id),
            )
        })?;
        let proto::AttachTerminal {
            terminal_id,
            from_offset,
            cols,
            rows,
            ..
        } = envelope.payload;
        let outcome = this.update(&mut cx, |this, _| {
            this.pty_manager.attach(
                terminal_id,
                from_offset,
                terminal_dimension(cols),
                terminal_dimension(rows),
            )
        })?;
        Ok(proto::AttachTerminalResponse {
            replayed_from: outcome.replayed_from,
            end_offset: outcome.end_offset,
            exit: outcome.exit,
        })
    }

    fn check_terminal_peer(&self, terminal: u64, peer: proto::PeerId) -> Result<()> {
        anyhow::ensure!(
            self.hub
                .as_ref()
                .is_none_or(|hub| hub.owns_terminal(terminal, peer)),
            "terminal belongs to another participant"
        );
        Ok(())
    }
}

/// Clamps a wire window dimension into the `u16` range a PTY accepts, never zero.
fn terminal_dimension(value: u32) -> u16 {
    value.clamp(1, u16::MAX as u32) as u16
}

fn prompt_to_proto(
    prompt: &project::LanguageServerPromptRequest,
) -> proto::language_server_prompt_request::Level {
    match prompt.level {
        PromptLevel::Info => proto::language_server_prompt_request::Level::Info(
            proto::language_server_prompt_request::Info {},
        ),
        PromptLevel::Warning => proto::language_server_prompt_request::Level::Warning(
            proto::language_server_prompt_request::Warning {},
        ),
        PromptLevel::Critical => proto::language_server_prompt_request::Level::Critical(
            proto::language_server_prompt_request::Critical {},
        ),
    }
}

fn find_venv_python(working_directory: &str) -> Option<std::path::PathBuf> {
    let wd = std::path::Path::new(working_directory);
    for dir_name in &[".venv", "venv", ".env", "env"] {
        let venv_dir = wd.join(dir_name);
        let has_pyvenv_cfg = venv_dir.join("pyvenv.cfg").is_file();
        let has_activate = venv_dir.join("bin").join("activate").is_file();
        if has_pyvenv_cfg || has_activate {
            let python = venv_dir.join("bin").join("python");
            if python.is_file() {
                return Some(python);
            }
            let python3 = venv_dir.join("bin").join("python3");
            if python3.is_file() {
                return Some(python3);
            }
        }
    }
    None
}
