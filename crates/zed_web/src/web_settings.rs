//! Settings and keymap in the browser: the web defaults, seeding the two documents the
//! shell provides into the in-memory `WasmFs` (D11: `/home/web/.config/zed/*`), the
//! `SettingsStore` with the user document applied synchronously, and the save-back of
//! `settings.json`/`keymap.json` to the control plane (b6 §7 item 8: the only two documents
//! stored in v0).

use std::{
    cell::RefCell,
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};

use collections::HashMap;
use fs::{Fs, WasmFs};
use futures::{FutureExt as _, StreamExt as _, future::Shared};
use gpui::{App, AppContext as _, AsyncApp, Task, UpdateGlobal as _};
use settings::{SettingsFile, SettingsParseResult, SettingsStore};
use workspace::notifications::{
    NotificationId, dismiss_app_notification, show_app_notification,
    simple_message_notification::MessageNotification,
};
use zed_web_core::DocumentKind;

use crate::bridge;

/// Per-document debounce between a `WasmFs` write and the `saveDocument` call.
const SAVE_DEBOUNCE: Duration = Duration::from_millis(750);
/// The `WasmFs` watch latency on the config directory.
const WATCH_LATENCY: Duration = Duration::from_millis(500);

type SaveTask = Shared<Task<Result<(), String>>>;

thread_local! {
    static FS: RefCell<Option<Arc<WasmFs>>> = const { RefCell::new(None) };
    static WATCHER: RefCell<Option<Task<()>>> = const { RefCell::new(None) };
    static PENDING: RefCell<HashMap<DocumentKind, Task<()>>> = RefCell::new(HashMap::default());
    // A flush can cancel a debounce after its host request has already started. Keep that
    // request alive, and let concurrent hidden/pagehide/STOPPING flushes await the same work.
    static IN_FLIGHT: RefCell<Vec<SaveTask>> = const { RefCell::new(Vec::new()) };
}

/// Upstream defaults plus browser-only overrides, parsed once for the settings store.
fn web_default_settings() -> &'static str {
    static DEFAULTS: LazyLock<String> = LazyLock::new(|| {
        zed_web_core::merge_web_defaults(&settings::default_settings())
            .expect("the shipped default settings must merge with the web overrides")
    });
    &DEFAULTS
}

/// Creates `settings.json` and `keymap.json` under `paths::config_dir()` from the
/// shell-provided documents (an empty string still creates the file, empty) and the empty
/// directories whose watchers must start cleanly. Synchronous: no async `Fs` call happens
/// before the app loop runs.
pub fn seed_config_files(fs: &Arc<WasmFs>, settings_json: &str, keymap_json: &str) {
    for dir in [
        paths::config_dir(),
        paths::data_dir(),
        paths::snippets_dir(),
        paths::prompts_dir(),
        paths::themes_dir(),
        paths::languages_dir(),
    ] {
        fs.insert_dir(dir);
    }
    fs.insert_file(paths::settings_file(), settings_json.as_bytes().to_vec());
    fs.insert_file(paths::keymap_file(), keymap_json.as_bytes().to_vec());
    // Seeding marks both files dirty like any write; the documents came from the control
    // plane, so saving them back would overwrite a newer copy from another tab (v0 is
    // last-writer-wins) and spend two PUTs inside the stopping window. Only writes made
    // after boot are saved back ([`flush_pending_saves`]).
    let seeded = fs.take_dirty();
    log::debug!("seeded {} config file(s)", seeded.len());
}

/// Applies the shell document synchronously, then watches in-memory settings files.
pub fn init(fs: Arc<dyn Fs>, settings_json: &str, cx: &mut App) {
    let store = SettingsStore::new(cx, web_default_settings());
    cx.set_global(store);
    SettingsStore::observe_active_settings_profile_name(cx).detach();

    if !settings_json.trim().is_empty() {
        let result = SettingsStore::update_global(cx, |store, cx| {
            store.set_user_settings(settings_json, cx)
        });
        notify_parse_result(SettingsFile::User, &result, cx);
    }

    SettingsStore::update_global(cx, move |store, cx| {
        store.watch_settings_files(fs, cx, |file, result, cx| {
            notify_parse_result(file, &result, cx);
        });
    });
}

fn notify_parse_result(file: SettingsFile, result: &SettingsParseResult, cx: &mut App) {
    let is_user = matches!(file, SettingsFile::User);
    let id = NotificationId::Named(format!("failed-to-parse-settings-{is_user}").into());
    match &result.parse_status {
        settings::ParseStatus::Failed { error } => {
            let which = if is_user { "user" } else { "global" };
            log::error!("failed to load {which} settings: {error}");
            let message = format!("Invalid {which} settings file\n{error}");
            show_app_notification(id, cx, move |cx| {
                cx.new(|cx| MessageNotification::new(message.clone(), cx))
            });
        }
        settings::ParseStatus::Success => dismiss_app_notification(&id, cx),
        settings::ParseStatus::Unchanged => {}
    }
}

fn document_kind_for(path: &Path) -> Option<DocumentKind> {
    if path == paths::settings_file().as_path() {
        Some(DocumentKind::Settings)
    } else if path == paths::keymap_file().as_path() {
        Some(DocumentKind::Keymap)
    } else {
        None
    }
}

fn path_for(kind: DocumentKind) -> &'static Path {
    match kind {
        DocumentKind::Settings => paths::settings_file(),
        DocumentKind::Keymap => paths::keymap_file(),
    }
}

/// Watches `paths::config_dir()` and saves `settings.json`/`keymap.json` through
/// `host.saveDocument` after a per-document debounce. Other files under the config
/// directory live only in the `WasmFs` for the tab's lifetime (v0).
pub fn install_save_back(fs: Arc<WasmFs>, cx: &mut App) {
    FS.with(|slot| *slot.borrow_mut() = Some(fs.clone()));
    let watcher = cx.spawn(async move |cx| {
        let (mut events, _watcher) = fs.watch(paths::config_dir(), WATCH_LATENCY).await;
        while let Some(batch) = events.next().await {
            for event in batch {
                let Some(kind) = document_kind_for(&event.path) else {
                    continue;
                };
                cx.update(|cx| schedule_save(kind, fs.clone(), cx));
            }
        }
    });
    WATCHER.with(|slot| *slot.borrow_mut() = Some(watcher));
}

fn schedule_save(kind: DocumentKind, fs: Arc<WasmFs>, cx: &mut App) {
    let task = cx.spawn(async move |cx| {
        cx.background_executor().timer(SAVE_DEBOUNCE).await;
        // A document that cannot be read (removed, or a directory in its place) must not
        // reach the control plane as an empty file.
        match fs.load(path_for(kind)).await {
            Ok(text) => {
                // save_now reports failures to the user; explicit flushes also receive them.
                cx.update(|cx| begin_save(kind, text, cx)).await.ok();
            }
            Err(error) => log::warn!(
                "{} changed but could not be read; not saved: {error:#}",
                kind.as_str()
            ),
        }
    });
    PENDING.with(|pending| {
        pending.borrow_mut().insert(kind, task);
    });
}

fn begin_save(kind: DocumentKind, text: String, cx: &mut App) -> SaveTask {
    let task = cx
        .spawn(async move |cx| save_now(kind, text, cx).await)
        .shared();
    IN_FLIGHT.with(|saves| {
        let mut saves = saves.borrow_mut();
        saves.retain(|save| save.peek().is_none());
        saves.push(task.clone());
    });
    task
}

async fn save_now(kind: DocumentKind, text: String, cx: &mut AsyncApp) -> Result<(), String> {
    match bridge::save_document(kind, text).await {
        Ok(()) => {
            log::debug!("saved {}", kind.as_str());
            Ok(())
        }
        Err(error) => {
            log::warn!("{} was not saved: {error:#}", kind.as_str());
            let name = match kind {
                DocumentKind::Settings => "Settings",
                DocumentKind::Keymap => "Keymap",
            };
            let message = format!("{name} were not saved: {error:#}");
            cx.update(|cx| {
                let id =
                    NotificationId::Named(format!("save-back-failed-{}", kind.as_str()).into());
                show_app_notification(id, cx, move |cx| {
                    cx.new(|cx| MessageNotification::new(message.clone(), cx))
                });
            });
            Err(format!("{} was not saved: {error:#}", kind.as_str()))
        }
    }
}

/// Drains `WasmFs::take_dirty()` and saves any `settings.json`/`keymap.json` entry at once,
/// cancelling that document's pending debounce, so a write made inside the debounce window
/// is not lost when the tab is hidden or the workspace stops. Entries for other files are
/// dropped. Also joins saves already in flight, including ones an overlapping flush started
/// before it drained the dirty list. Resolves only after every save is answered; a genuine
/// save failure is reported to the user and returned to the flush caller.
pub fn flush_pending_saves(cx: &mut App) -> Task<anyhow::Result<()>> {
    let Some(fs) = FS.with(|slot| slot.borrow().clone()) else {
        return Task::ready(Ok(()));
    };
    let mut documents: Vec<(DocumentKind, String)> = Vec::new();
    for file in fs.take_dirty() {
        let Some(kind) = document_kind_for(&file.path) else {
            continue;
        };
        let Some(contents) = file.contents else {
            continue;
        };
        documents.retain(|(existing, _)| *existing != kind);
        documents.push((kind, String::from_utf8_lossy(&contents).into_owned()));
    }
    PENDING.with(|pending| {
        let mut pending = pending.borrow_mut();
        for (kind, _) in &documents {
            pending.remove(kind);
        }
    });
    for (kind, text) in documents {
        let _ = begin_save(kind, text, cx);
    }
    let saves = IN_FLIGHT.with(|saves| saves.borrow().clone());
    cx.spawn(async move |_| {
        // Await all documents even if one fails, so an error saving settings does not cancel
        // an otherwise valid keymap save inside the stopping window.
        for result in futures::future::join_all(saves).await {
            result.map_err(anyhow::Error::msg)?;
        }
        Ok(())
    })
}
