//! Keymaps in the browser: the host OS is chosen at runtime (a Windows user in Chrome gets
//! the Windows chords even though the binary targets wasm), the desktop files load leniently
//! (they bind actions of crates the browser excludes), and the `web.json` layer of D12 loads
//! last so its browser-reserved-chord alternatives and `null` unbindings win.

use anyhow::Context as _;
use futures::{StreamExt as _, channel::mpsc, select_biased};
use gpui::{App, AppContext as _, KeyBinding, SharedString, Task};
use migrator::migrate_keymap;
use project::DisableAiSettings;
use settings::{
    BaseKeymap, KeybindSource, KeymapFile, KeymapFileLoadResult, KeymapOs, Settings as _,
    SettingsAssets, SettingsStore, VIM_KEYMAP_PATH, WEB_KEYMAP_PATH,
};
use util::asset_str;
use vim_mode_setting::{HelixModeSetting, VimModeSetting};
use workspace::notifications::{
    NotificationId, dismiss_app_notification, show_app_notification,
    simple_message_notification::MessageNotification,
};
use zed_web_core::{BootConfig, HostOs};

/// The host OS: the boot config's `hostOs` override, else `navigator.platform` /
/// `navigator.userAgent`.
pub fn host_os(config: &BootConfig) -> HostOs {
    if let Some(os) = config.host_os.as_deref().and_then(HostOs::from_override) {
        return os;
    }
    let (platform, user_agent) = web_sys::window()
        .map(|window| {
            let navigator = window.navigator();
            #[allow(deprecated)]
            let platform = navigator.platform().unwrap_or_default();
            (platform, navigator.user_agent().unwrap_or_default())
        })
        .unwrap_or_default();
    HostOs::from_platform(&platform, &user_agent)
}

fn keymap_os(os: HostOs) -> KeymapOs {
    match os {
        HostOs::Mac => KeymapOs::Mac,
        HostOs::Windows => KeymapOs::Windows,
        HostOs::Linux => KeymapOs::Linux,
    }
}

/// Loads a shipped keymap that may bind actions the browser build does not register
/// (`collab_panel::*`, `onboarding::*`, `welcome::*`, ...): the bindings that resolve are
/// kept, the loader's message about the rest is appended to `dropped`.
fn load_lenient(
    asset_path: &str,
    source: KeybindSource,
    cx: &mut App,
    dropped: &mut Vec<String>,
) -> Vec<KeyBinding> {
    let content = asset_str::<SettingsAssets>(asset_path);
    let mut key_bindings = match KeymapFile::load(content.as_ref(), cx) {
        KeymapFileLoadResult::Success { key_bindings } => key_bindings,
        KeymapFileLoadResult::SomeFailedToLoad {
            key_bindings,
            error_message,
        } => {
            dropped.push(format!("{asset_path}: {}", error_message.0));
            key_bindings
        }
        KeymapFileLoadResult::JsonParseFailure { error } => {
            log::error!("JSON parse error in built-in keymap {asset_path:?}: {error}");
            Vec::new()
        }
    };
    for key_binding in &mut key_bindings {
        key_binding.set_meta(source.meta());
    }
    key_bindings
}

/// Loads a layer whose every binding must resolve (the override layers this build owns);
/// the desktop panics on the same failure, here it fails the boot ([`install`]).
fn load_strict(asset_path: &str, cx: &mut App) -> anyhow::Result<Vec<KeyBinding>> {
    KeymapFile::load_asset(asset_path, Some(KeybindSource::Default), cx)
        .with_context(|| format!("built-in keymap {asset_path:?}"))
}

/// Port of the desktop's `load_default_keymap` with the host OS chosen at runtime, lenient
/// loading of the desktop files and the web layer last. Order: default → base → vim/helix (if
/// enabled) → specific-overrides → web (D12); the user keymap follows in [`reload_keymaps`].
/// Returns the loader messages about bindings dropped from the lenient layers; a strict
/// layer (the specific overrides, `web.json`) that fails to load is an error.
pub fn load_default_keymap(os: HostOs, cx: &mut App) -> anyhow::Result<Vec<String>> {
    let mut dropped = Vec::new();
    let base_keymap = *BaseKeymap::get_global(cx);
    if base_keymap == BaseKeymap::None {
        return Ok(dropped);
    }
    let os = keymap_os(os);

    let bindings = load_lenient(
        settings::default_keymap_path_for(os),
        KeybindSource::Default,
        cx,
        &mut dropped,
    );
    cx.bind_keys(filter_disabled_ai_bindings(bindings, cx));

    if let Some(asset_path) = base_keymap.asset_path_for(os) {
        let bindings = load_lenient(asset_path, KeybindSource::Base, cx, &mut dropped);
        cx.bind_keys(filter_disabled_ai_bindings(bindings, cx));
    }

    if VimModeSetting::get_global(cx).0 || HelixModeSetting::get_global(cx).0 {
        let bindings = load_lenient(VIM_KEYMAP_PATH, KeybindSource::Vim, cx, &mut dropped);
        cx.bind_keys(filter_disabled_ai_bindings(bindings, cx));
    }

    // `load_strict` needs `&mut App` too, so each layer is read into a local before
    // `bind_keys` takes its own mutable borrow.
    let overrides = load_strict(settings::specific_overrides_keymap_path_for(os), cx)?;
    cx.bind_keys(overrides);
    // The browser layer wins over every layer above it (D12).
    let web_bindings = load_strict(WEB_KEYMAP_PATH, cx)?;
    cx.bind_keys(web_bindings);
    Ok(dropped)
}

/// Port of the desktop's `reload_keymaps`: clear, defaults, user bindings as
/// `KeybindSource::User`, then notify the keymap editor. No menus or dock menu on the web.
pub fn reload_keymaps(os: HostOs, mut user_key_bindings: Vec<KeyBinding>, cx: &mut App) {
    cx.clear_key_bindings();
    // The strict layers were validated at boot ([`install`]); the embedded files cannot
    // change afterwards, so a failure here can only be logged.
    if let Err(error) = load_default_keymap(os, cx) {
        log::error!("failed to reload the built-in keymaps: {error:#}");
    }
    for key_binding in &mut user_key_bindings {
        key_binding.set_meta(KeybindSource::User.meta());
    }
    cx.bind_keys(filter_disabled_ai_bindings(user_key_bindings, cx));
    keymap_editor::KeymapEventChannel::trigger_keymap_changed(cx);
}

struct KeymapParseErrorNotification;

/// Port of the desktop's `handle_keymap_file_changes`: observes the base keymap, vim/helix
/// mode, `disable_ai` and the keyboard mapper, consumes the user keymap watcher, migrates the
/// user keymap in memory and reports parse errors as workspace notifications. Fails when a
/// strict built-in layer (`web.json`, the specific overrides) does not load, which is a
/// broken bundle rather than a user error.
pub fn install(
    os: HostOs,
    mut user_keymap_file_rx: mpsc::UnboundedReceiver<String>,
    user_keymap_watcher: Task<()>,
    cx: &mut App,
) -> anyhow::Result<()> {
    let (base_keymap_tx, mut base_keymap_rx) = mpsc::unbounded::<()>();
    let (keyboard_layout_tx, mut keyboard_layout_rx) = mpsc::unbounded::<()>();
    let mut old_base_keymap = *BaseKeymap::get_global(cx);
    let mut old_vim_enabled = VimModeSetting::get_global(cx).0;
    let mut old_helix_enabled = HelixModeSetting::get_global(cx).0;
    let mut old_disable_ai = DisableAiSettings::get_global(cx).disable_ai;

    cx.observe_global::<SettingsStore>(move |cx| {
        let new_base_keymap = *BaseKeymap::get_global(cx);
        let new_vim_enabled = VimModeSetting::get_global(cx).0;
        let new_helix_enabled = HelixModeSetting::get_global(cx).0;
        let new_disable_ai = DisableAiSettings::get_global(cx).disable_ai;
        if new_base_keymap != old_base_keymap
            || new_vim_enabled != old_vim_enabled
            || new_helix_enabled != old_helix_enabled
            || new_disable_ai != old_disable_ai
        {
            old_base_keymap = new_base_keymap;
            old_vim_enabled = new_vim_enabled;
            old_helix_enabled = new_helix_enabled;
            old_disable_ai = new_disable_ai;
            base_keymap_tx.unbounded_send(()).ok();
        }
    })
    .detach();

    let mut current_mapping = cx.keyboard_mapper().get_key_equivalents().cloned();
    cx.on_keyboard_layout_change(move |cx| {
        let next_mapping = cx.keyboard_mapper().get_key_equivalents();
        if current_mapping.as_ref() != next_mapping {
            current_mapping = next_mapping.cloned();
            keyboard_layout_tx.unbounded_send(()).ok();
        }
    })
    .detach();

    let dropped = load_default_keymap(os, cx)?;
    if !dropped.is_empty() {
        // Expected on every boot: the shipped keymaps bind actions the browser build leaves out
        // (`zed::Hide`, `repl::Run`, ...), and web.json unbinds the meaningful chords (D12), so
        // the list is diagnostic detail rather than a warning worth a console entry per session.
        log::debug!(
            "bindings to actions the browser build does not register were dropped from the shipped keymaps:\n{}",
            dropped.join("\n")
        );
    }

    let notification_id = NotificationId::unique::<KeymapParseErrorNotification>();
    cx.spawn(async move |cx| {
        let _user_keymap_watcher = user_keymap_watcher;
        let mut user_keymap_content = String::new();
        loop {
            select_biased! {
                _ = base_keymap_rx.next() => {},
                _ = keyboard_layout_rx.next() => {},
                content = user_keymap_file_rx.next() => {
                    if let Some(content) = content {
                        user_keymap_content = match migrate_keymap(&content) {
                            Ok(Some(migrated)) => migrated,
                            _ => content,
                        };
                    }
                }
            };
            cx.update(|cx| match KeymapFile::load(&user_keymap_content, cx) {
                KeymapFileLoadResult::Success { key_bindings } => {
                    reload_keymaps(os, key_bindings, cx);
                    dismiss_app_notification(&notification_id, cx);
                }
                KeymapFileLoadResult::SomeFailedToLoad {
                    key_bindings,
                    error_message,
                } => {
                    if !key_bindings.is_empty() {
                        reload_keymaps(os, key_bindings, cx);
                    }
                    show_keymap_error(
                        notification_id.clone(),
                        format!(
                            "Error in user keymap file. Bindings not reloaded.\n\n{}",
                            error_message.0
                        ),
                        cx,
                    );
                }
                KeymapFileLoadResult::JsonParseFailure { error } => show_keymap_error(
                    notification_id.clone(),
                    format!("JSON parse error in keymap file. Bindings not reloaded.\n\n{error}"),
                    cx,
                ),
            });
        }
    })
    .detach();
    Ok(())
}

fn show_keymap_error(id: NotificationId, message: String, cx: &mut App) {
    let message: SharedString = message.into();
    show_app_notification(id, cx, move |cx| {
        cx.new(|cx| MessageNotification::new(message.clone(), cx))
    });
}

/// Namespaces of actions that are part of an AI feature; dropped when `disable_ai` is set so
/// lower-precedence defaults fire instead of a silently no-op handler.
const AI_ACTION_NAMESPACES: &[&str] = &[
    "acp::",
    "agent::",
    "assistant::",
    "edit_prediction::",
    "inline_assistant::",
    "zeta::",
];

fn filter_disabled_ai_bindings(bindings: Vec<KeyBinding>, cx: &App) -> Vec<KeyBinding> {
    if !DisableAiSettings::get_global(cx).disable_ai {
        return bindings;
    }
    bindings
        .into_iter()
        .filter(|binding| {
            let name = binding.action().name();
            !AI_ACTION_NAMESPACES
                .iter()
                .any(|namespace| name.starts_with(namespace))
        })
        .collect()
}
