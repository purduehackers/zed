//! The boot sequence behind `start()` (BUILD-SPEC 3.4): assets → app → settings →
//! connect → database → registries → window → ready, each stage reported to the shell, the
//! whole sequence under a 90 s timer. Also the post-boot entry points the shell drives:
//! [`flush_client_state`], [`set_hidden`], [`has_unsaved_changes`].

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use db::client_state::{ClientStateEvent, ClientStateStore};
use fs::{Fs, WasmFs};
use futures::channel::oneshot;
use gpui::{App, AppContext as _, AsyncApp, Entity, WeakEntity};
use gpui_platform::WebBackendPreference;
use project::lifecycle::LifecycleKind;
use release_channel::AppCommitSha;
use session::Session;
use workspace::{
    Workspace,
    client_state::RemoteClientStateSink,
    notifications::{
        NotificationId, show_app_notification, simple_message_notification::MessageNotification,
    },
};
use zed_web_core::{AssetPack, Backend, BootConfig, BootError, BootStage};

use crate::assets::WebAssets;
use crate::bridge::{self, JsHost};
use crate::init::{self, BuildInfo};
use crate::{connect, keymap, web_settings};

/// The whole boot must reach `ready` within this budget.
const BOOT_TIMEOUT: Duration = Duration::from_secs(90);
/// How often the main thread drains panics raised on worker threads.
const WORKER_PANIC_POLL: Duration = Duration::from_secs(1);

struct BootState {
    workspace: WeakEntity<Workspace>,
    /// The one window (the test hooks drive editor and terminal actions through it).
    #[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
    window: gpui::WindowHandle<workspace::MultiWorkspace>,
}

thread_local! {
    static STARTED: Cell<bool> = const { Cell::new(false) };
    static ASYNC_APP: RefCell<Option<AsyncApp>> = const { RefCell::new(None) };
    static STATE: RefCell<Option<BootState>> = const { RefCell::new(None) };
    /// The session's `RemoteClient`, set as soon as the first dial succeeds (before the
    /// window exists), so the test hooks can report the connection state during boot.
    static REMOTE: RefCell<Option<WeakEntity<remote::RemoteClient>>> = const { RefCell::new(None) };
    /// The running `boot_in_app` task; dropped (cancelled) by the boot timer so a slow
    /// first dial cannot open the window and emit `ready` after the shell was told
    /// `boot_timeout`.
    static BOOT_TASK: RefCell<Option<gpui::Task<()>>> = const { RefCell::new(None) };
}

type Completion = Rc<RefCell<Option<oneshot::Sender<Result<(), BootError>>>>>;

fn complete(completion: &Completion, result: Result<(), BootError>) {
    if let Some(sender) = completion.borrow_mut().take() {
        sender.send(result).ok();
    }
}

/// `ZS_BUILD_ID` baked at compile time, `"dev"` for a local build.
pub fn build_id() -> &'static str {
    option_env!("ZS_BUILD_ID").unwrap_or("dev")
}

fn backend(backend: Backend) -> WebBackendPreference {
    match backend {
        Backend::Auto => WebBackendPreference::Auto,
        Backend::WebGpu => WebBackendPreference::WebGpu,
        Backend::WebGl => WebBackendPreference::WebGl,
    }
}

/// `smol::spawn` runs on gpui's background executor (b5 §4): installed as the first statement
/// inside `run_embedded`, before any `cx.spawn` or timer.
struct GpuiSmolRuntime(gpui::BackgroundExecutor);

impl smol::runtime::Runtime for GpuiSmolRuntime {
    fn schedule(&self, runnable: smol::runtime::Runnable) {
        self.0
            .spawn(async move {
                runnable.run();
            })
            .detach();
    }
}

/// Runs the boot sequence. Single-shot; resolves at `ready` or with the first failure.
pub async fn run(config: BootConfig, assets_tar: Vec<u8>, host: JsHost) -> Result<(), BootError> {
    if STARTED.with(|started| started.replace(true)) {
        return Err(BootError::new(
            "cancelled",
            "start() was already called; reload the page to boot again",
        ));
    }
    host.install();
    bridge::install_panic_hook();
    #[cfg(feature = "test-hooks")]
    crate::test_hooks::install();
    bridge::progress(BootStage::Booting, "");

    // 1. The asset pack, parsed on the main thread before the app starts.
    bridge::progress(BootStage::Assets, "");
    let pack = AssetPack::from_tar(&assets_tar)
        .map_err(|error| BootError::new("bad_assets", format!("{error:#}")))?;
    drop(assets_tar);
    let assets = WebAssets::new(pack);
    log::info!("asset pack: {} files", assets.len());

    // 2. The app. `Application::run` drops the app once the launch callback returns (which
    //    is immediate on the web), so the embedded form's handle is held forever instead.
    let (tx, rx) = oneshot::channel::<Result<(), BootError>>();
    let completion: Completion = Rc::new(RefCell::new(Some(tx)));
    let app = gpui_platform::application_with_web_backend(backend(config.backend))
        .with_assets(assets.clone());
    let handle = app.run_embedded({
        let completion = completion.clone();
        let assets = assets.clone();
        move |cx| {
            smol::runtime::install_runtime(Arc::new(GpuiSmolRuntime(
                cx.background_executor().clone(),
            )))
            .ok();
            if !smol::runtime::runtime_installed() {
                complete(
                    &completion,
                    Err(BootError::new(
                        "runtime_missing",
                        "the smol runtime adapter could not be installed",
                    )),
                );
                return;
            }
            // Static constructors (`inventory` registries: migrations, settings, actions)
            // must have run: the loader calls `__wasm_call_ctors` before `start()`.
            if db::registered_migration_count() == 0 {
                complete(
                    &completion,
                    Err(BootError::new(
                        "ctors_missing",
                        "no database migrations are registered: static constructors did not run",
                    )),
                );
                return;
            }
            ASYNC_APP.with(|slot| *slot.borrow_mut() = Some(cx.to_async()));

            cx.spawn({
                let completion = completion.clone();
                async move |cx| {
                    cx.background_executor().timer(BOOT_TIMEOUT).await;
                    let boot_task = BOOT_TASK.with(|task| task.borrow_mut().take());
                    if boot_task.is_none() {
                        // Boot already finished (or failed); nothing to cancel.
                        return;
                    }
                    complete(
                        &completion,
                        Err(BootError::new(
                            "boot_timeout",
                            format!(
                                "boot did not reach ready within {}s",
                                BOOT_TIMEOUT.as_secs()
                            ),
                        )),
                    );
                    drop(boot_task);
                }
            })
            .detach();

            cx.spawn(async move |cx| {
                loop {
                    cx.background_executor().timer(WORKER_PANIC_POLL).await;
                    bridge::report_pending_worker_panics();
                }
            })
            .detach();

            let boot_task = cx.spawn(async move |cx| {
                let result = boot_in_app(config, assets, cx).await;
                // Boot is over: nothing is left for the timer to cancel.
                if let Some(task) = BOOT_TASK.with(|task| task.borrow_mut().take()) {
                    task.detach();
                }
                complete(&completion, result);
            });
            BOOT_TASK.with(|task| *task.borrow_mut() = Some(boot_task));
        }
    });
    std::mem::forget(handle);

    let result = match rx.await {
        Ok(result) => result,
        Err(_) => Err(BootError::cancelled()),
    };
    if let Err(error) = &result {
        bridge::progress(BootStage::Failed, error.code);
        bridge::report_error("boot", &error.to_string(), "");
    }
    result
}

async fn boot_in_app(
    config: BootConfig,
    assets: WebAssets,
    cx: &mut AsyncApp,
) -> Result<(), BootError> {
    let host_os = keymap::host_os(&config);
    log::info!("host os: {}", host_os.as_str());
    #[cfg(feature = "test-hooks")]
    crate::test_hooks::record_host_os(host_os);
    // 3a. Settings and keymap seeded into the in-memory filesystem (D11).
    bridge::progress(BootStage::Settings, "");
    let fs: Arc<WasmFs> = cx.update(|cx| WasmFs::new(cx.background_executor().clone()));
    assets.set_fs(fs.clone());
    web_settings::seed_config_files(&fs, &config.settings_json, &config.keymap_json);
    cx.update(|cx| crate::files::init(fs.clone(), cx));
    let fs_dyn: Arc<dyn Fs> = fs.clone();

    // 3b. Everything that precedes the dial.
    let build = BuildInfo {
        id: build_id(),
        commit_sha: option_env!("ZED_COMMIT_SHA").map(|sha| AppCommitSha::new(sha.to_string())),
    };
    let (client, extension_host_proxy) = cx
        .update(|cx| {
            init::init_before_connect(
                fs_dyn.clone(),
                &assets,
                host_os,
                &config.settings_json,
                &build,
                cx,
            )
        })
        .map_err(|error| BootError::new("settings", format!("{error:#}")))?;

    // 3c. Dial.
    bridge::set_session(config.connect.clone());
    bridge::progress(BootStage::Connecting, "");
    let remote = connect::connect(&config.workspace.id, &config.connect, cx).await?;
    REMOTE.with(|slot| *slot.borrow_mut() = Some(remote.downgrade()));

    // 3d. The client-state image, restored off the main thread (D7), then the stores that
    //     depend on it.
    bridge::progress(BootStage::Database, "");
    let proto = cx.update(|cx| remote.read(cx).proto_client());
    let (image, loaded_version) = workspace::client_state::load_client_state(&proto)
        .await
        .map_err(BootError::database)?;
    // Before the image is consumed: no image means nobody has opened this workspace in a
    // browser yet, which is what decides the default dock layout in `workspace_chrome`.
    crate::workspace_chrome::set_first_run(image.is_none());
    let (app_db, outcome) = cx
        .background_executor()
        .spawn(db::AppDatabase::open_with_image(image))
        .await;
    cx.update(|cx| {
        cx.set_global(db::AppDatabase(app_db.0.clone()));
        db::kvp::GlobalKeyValueStore::init(&app_db);
    });
    let session = Session::new(
        uuid::Uuid::new_v4().to_string(),
        db::kvp::KeyValueStore::from_app_db(&app_db),
    )
    .await;
    let sink = RemoteClientStateSink::new(proto.clone(), build.id.to_string());
    let store = cx.update(|cx| {
        let store = cx.new(|cx| ClientStateStore::new(&app_db, sink, loaded_version, outcome, cx));
        db::client_state::set_global_store(store.clone(), cx);
        cx.subscribe(&store, |_store, event, cx| {
            #[cfg(feature = "test-hooks")]
            crate::test_hooks::record_client_state_event(event, _store.read(cx).is_hidden());
            match event {
                ClientStateEvent::ReadOnly(reason) => notify_read_only(reason, cx),
                ClientStateEvent::SaveFailed(error) => {
                    log::warn!("client state not saved: {error}")
                }
                ClientStateEvent::Stale { server_version } => {
                    log::warn!("client state was stale; adopted server version {server_version}")
                }
                ClientStateEvent::Saved { version } => {
                    log::debug!("client state saved as {version}")
                }
            }
        })
        .detach();
        connect::observe(remote.clone(), store.clone(), cx);
        store
    });

    // 3e. Registries, panels and the workspace chrome; the settings save-back.
    bridge::progress(BootStage::Languages, "");
    let app_state = cx.update(|cx| {
        init::init_after_db(
            client,
            fs_dyn.clone(),
            assets,
            extension_host_proxy,
            session,
            cx,
        )
    });
    cx.update(|cx| web_settings::install_save_back(fs.clone(), cx));

    // 3f. The window and the workspace around the existing client (D16), then the unsaved
    //     buffers of the previous session come back dirty (D6) after the editor items were
    //     restored.
    bridge::progress(BootStage::Window, "");
    let opened = connect::open_remote_workspace(remote.clone(), &config, app_state, cx).await?;
    STATE.with(|state| {
        *state.borrow_mut() = Some(BootState {
            workspace: opened.workspace.downgrade(),
            window: opened.window,
        })
    });
    cx.update(|cx| {
        let project = opened.workspace.read(cx).project().clone();
        if let Some(remote) = project.read(cx).remote_extension_store().cloned() {
            extension_host::ExtensionStore::global(cx)
                .update(cx, |store, cx| store.attach(remote, cx));
        }
        let workspace = opened.workspace.downgrade();
        cx.subscribe(&project, move |_, event, cx| {
            if let project::Event::LifecycleNotice { kind, seconds } = event {
                handle_lifecycle(*kind, *seconds, workspace.clone(), cx);
            }
        })
        .detach();
        // What the server replayed right after `HelloAck` (the port picture, a pending
        // `Resumed`) arrived before the project's handlers existed; deliver it now that
        // they do and the lifecycle subscription above is in place.
        remote.read(cx).replay_unhandled_messages(cx);
    });
    let restored = opened
        .window
        .update(cx, |_, window, cx| {
            workspace::client_state::restore_unsaved_buffers(&opened.workspace, window, cx)
        })
        .map_err(BootError::window)?
        .await;
    match restored {
        Ok(count) if count > 0 => log::info!("restored {count} unsaved buffer(s)"),
        Ok(_) => {}
        Err(error) => log::warn!("unsaved buffers were not restored: {error:#}"),
    }

    // 3g. The open task awaited worktree creation and item restoration.
    bridge::progress(BootStage::Ready, "");
    let _ = store;
    Ok(())
}

fn notify_read_only(reason: &str, cx: &mut App) {
    struct ReadOnlyClientState;
    log::warn!("client state is read-only: {reason}");
    let message: gpui::SharedString = format!(
        "Saved layout from a newer build could not be loaded; layout changes are not being saved.\n{reason}"
    )
    .into();
    show_app_notification(
        NotificationId::unique::<ReadOnlyClientState>(),
        cx,
        move |cx| {
            cx.new(|cx| {
                MessageNotification::new(message.clone(), cx)
                    .primary_message("Discard saved layout")
                    .primary_on_click(|_, cx| {
                        if let Some(store) = db::client_state::global_store(cx) {
                            store.update(cx, |store, _| store.allow_overwrite());
                        }
                    })
            })
        },
    );
}

/// Forwards a lifecycle notice to the shell; on `Stopping` runs, in order, the pending
/// settings saves, the unsaved-buffer snapshot (D6) and the stopping flush (D7) inside the
/// server's stopping window. The client never writes to the workspace filesystem.
fn handle_lifecycle(
    kind: LifecycleKind,
    seconds: u32,
    workspace: WeakEntity<Workspace>,
    cx: &mut App,
) {
    bridge::lifecycle(kind, seconds);
    #[cfg(feature = "test-hooks")]
    crate::test_hooks::record_lifecycle(kind, seconds);
    if kind != LifecycleKind::Stopping {
        return;
    }
    cx.spawn(async move |cx| {
        if let Err(error) = cx.update(|cx| web_settings::flush_pending_saves(cx)).await {
            log::warn!("stopping settings flush failed: {error:#}");
        }
        if let Some(workspace) = workspace.upgrade() {
            match cx
                .update(|cx| workspace::client_state::snapshot_unsaved_buffers(&workspace, cx))
                .await
            {
                Ok(count) => log::info!("snapshotted {count} unsaved buffer(s) for the stop"),
                Err(error) => log::warn!("unsaved buffers were not snapshotted: {error:#}"),
            }
        }
        if let Err(error) = cx
            .update(|cx| db::client_state::flush_client_state_for_stop(cx))
            .await
        {
            log::warn!("stopping flush failed: {error:#}");
        }
    })
    .detach();
}

pub(crate) fn async_app() -> Result<AsyncApp> {
    ASYNC_APP
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| anyhow::anyhow!("the app has not started"))
}

/// The session's `RemoteClient`, once the first dial succeeded (test hooks).
#[cfg(feature = "test-hooks")]
pub(crate) fn remote_client() -> Option<Entity<remote::RemoteClient>> {
    REMOTE.with(|slot| slot.borrow().as_ref().and_then(|remote| remote.upgrade()))
}

/// The workspace window, once open (test hooks).
#[cfg(feature = "test-hooks")]
pub(crate) fn window() -> Option<gpui::WindowHandle<workspace::MultiWorkspace>> {
    STATE.with(|state| state.borrow().as_ref().map(|state| state.window))
}

/// The hidden-tab and `pagehide` flush: pending settings saves, then the image. A stopped
/// or read-only store (`flush_now` returns `Err`) is not an error for the caller. Settings
/// failures still reject; the pagehide caller explicitly treats the operation as best effort.
pub async fn flush_client_state() -> Result<()> {
    let cx = async_app()?;
    let settings_result = cx.update(|cx| web_settings::flush_pending_saves(cx)).await;
    if let Err(error) = cx
        .update(|cx| db::client_state::flush_client_state(cx))
        .await
    {
        log::debug!("client state not flushed: {error:#}");
    }
    settings_result
}

/// `document.hidden` changed: the store shortens its interval and flushes when hidden and
/// restores the interval when visible; pending settings saves drain when hidden.
pub fn set_hidden(hidden: bool) {
    let Ok(cx) = async_app() else {
        return;
    };
    #[cfg(feature = "test-hooks")]
    let record = crate::test_hooks::record_visibility(hidden);
    cx.update(|cx| {
        let settings = hidden.then(|| web_settings::flush_pending_saves(cx));
        // Update the hidden interval immediately, but completion covers both the settings
        // requests and the database image. A caller observing this flush may then reload.
        let task = db::client_state::set_hidden(hidden, cx);
        cx.spawn(async move |_cx| {
            let settings_result = match settings {
                Some(settings) => settings.await,
                None => Ok(()),
            };
            let image_result = task.await;
            let result = settings_result.and(image_result);
            if let Err(error) = &result {
                log::debug!("set_hidden({hidden}): {error:#}");
            }
            #[cfg(feature = "test-hooks")]
            {
                let version = _cx.update(|cx| {
                    db::client_state::global_store(cx).map(|store| store.read(cx).version())
                });
                crate::test_hooks::record_visibility_flush(record, &result, version);
            }
        })
        .detach();
    });
}

/// Whether the workspace has a dirty item.
pub fn has_unsaved_changes() -> bool {
    let Ok(cx) = async_app() else {
        return false;
    };
    let Some(workspace) = STATE.with(|state| {
        state
            .borrow()
            .as_ref()
            .and_then(|state| state.workspace.upgrade())
    }) else {
        return false;
    };
    cx.update(|cx| workspace.read(cx).items(cx).any(|item| item.is_dirty(cx)))
}

/// Reachable from the shell through [`has_unsaved_changes`]; kept for the chrome's `Quit`.
pub fn workspace() -> Option<Entity<Workspace>> {
    STATE.with(|state| {
        state
            .borrow()
            .as_ref()
            .and_then(|state| state.workspace.upgrade())
    })
}
