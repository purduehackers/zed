mod base_keymap_setting;
mod content_into_gpui;
mod editable_setting_control;
mod editorconfig_store;
mod granted_write_path;
mod keymap_file;
mod settings_file;
mod settings_store;
mod vscode_import;

pub use settings_macros::RegisterSetting;

pub mod settings_content {
    pub use ::settings_content::*;
}

pub mod fallible_options {
    pub use ::settings_content::{FallibleOption, parse_json};
}

#[doc(hidden)]
pub mod private {
    pub use crate::settings_store::{RegisteredSetting, SettingValue};
    pub use inventory;
}

use gpui::{App, Global};

use std::env;
use std::{borrow::Cow, fmt, str};
use util::asset_str;

pub use ::settings_content::*;
pub use base_keymap_setting::*;
pub use content_into_gpui::IntoGpui;
pub use editable_setting_control::*;
pub use editorconfig_store::{
    Editorconfig, EditorconfigEvent, EditorconfigProperties, EditorconfigStore,
};
pub use granted_write_path::GrantedWritePath;
pub use keymap_file::{
    KeyBindingValidator, KeyBindingValidatorRegistration, KeybindSource, KeybindUpdateOperation,
    KeybindUpdateTarget, KeymapFile, KeymapFileLoadResult,
};
pub use settings_file::*;
pub use settings_json::*;
pub use settings_store::{
    DefaultSemanticTokenRules, InvalidSettingsError, LSP_SETTINGS_SCHEMA_URL_PREFIX,
    LocalSettingsKind, LocalSettingsPath, MigrationStatus, Settings, SettingsFile,
    SettingsJsonSchemaParams, SettingsKey, SettingsLocation, SettingsParseResult, SettingsStore,
};

pub use vscode_import::{VsCodeSettings, VsCodeSettingsSource};

pub use keymap_file::ActionSequence;

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveSettingsProfileName(pub String);

impl Global for ActiveSettingsProfileName {}

pub trait UserSettingsContentExt {
    fn for_profile(&self, cx: &App) -> Option<&SettingsProfile>;
    fn for_release_channel(&self) -> Option<&SettingsContent>;
    fn for_os(&self) -> Option<&SettingsContent>;
}

impl UserSettingsContentExt for UserSettingsContent {
    fn for_profile(&self, cx: &App) -> Option<&SettingsProfile> {
        let Some(active_profile) = cx.try_global::<ActiveSettingsProfileName>() else {
            return None;
        };
        self.profiles.get(&active_profile.0)
    }

    fn for_release_channel(&self) -> Option<&SettingsContent> {
        self.release_channel_overrides
            .get_by_key(release_channel::RELEASE_CHANNEL.dev_name())
    }

    fn for_os(&self) -> Option<&SettingsContent> {
        self.platform_overrides.get_by_key(env::consts::OS)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct WorktreeId(usize);

impl From<WorktreeId> for usize {
    fn from(value: WorktreeId) -> Self {
        value.0
    }
}

impl WorktreeId {
    pub fn from_usize(handle_id: usize) -> Self {
        Self(handle_id)
    }

    pub fn from_proto(id: u64) -> Self {
        Self(id as usize)
    }

    pub fn to_proto(self) -> u64 {
        self.0 as u64
    }

    pub fn to_usize(self) -> usize {
        self.0
    }
}

impl fmt::Display for WorktreeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

// Dev builds read the checkout's files at runtime instead of embedding them;
// see the `assets` crate for the rationale.
util::fs_embed! {
    pub struct SettingsAssets,
    crate_relative = "../../assets",
    root_relative = "assets",
    include = ["settings/*", "keymaps/*"],
    exclude = ["*.DS_Store"],
}

pub fn init(cx: &mut App) {
    let settings = SettingsStore::new(cx, &default_settings());
    cx.set_global(settings);
    SettingsStore::observe_active_settings_profile_name(cx).detach();
}

pub fn default_settings() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/default.json")
}

pub fn default_semantic_token_rules() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/default_semantic_token_rules.json")
}

#[cfg(target_os = "macos")]
pub const DEFAULT_KEYMAP_PATH: &str = "keymaps/default-macos.json";

#[cfg(target_os = "windows")]
pub const DEFAULT_KEYMAP_PATH: &str = "keymaps/default-windows.json";

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub const DEFAULT_KEYMAP_PATH: &str = "keymaps/default-linux.json";

/// The operating system a keymap file set is written for. Desktop builds resolve it at
/// compile time ([`KeymapOs::current`]); the browser build picks the user's host at runtime
/// so a Windows or Linux user gets their chords even though the binary targets wasm.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KeymapOs {
    /// `keymaps/default-macos.json` and `keymaps/macos/*`.
    Mac,
    /// `keymaps/default-windows.json` and `keymaps/linux/*`.
    Windows,
    /// `keymaps/default-linux.json` and `keymaps/linux/*`.
    Linux,
}

impl KeymapOs {
    /// The keymap OS of the compile target (the one the `*_KEYMAP_PATH` constants use).
    pub const fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::Mac
        } else if cfg!(target_os = "windows") {
            Self::Windows
        } else {
            Self::Linux
        }
    }
}

/// The default keymap asset for `os`; [`DEFAULT_KEYMAP_PATH`] is this for [`KeymapOs::current`].
pub fn default_keymap_path_for(os: KeymapOs) -> &'static str {
    match os {
        KeymapOs::Mac => "keymaps/default-macos.json",
        KeymapOs::Windows => "keymaps/default-windows.json",
        KeymapOs::Linux => "keymaps/default-linux.json",
    }
}

/// The specific-overrides asset for `os`; [`SPECIFIC_OVERRIDES_KEYMAP_PATH`] is this for
/// [`KeymapOs::current`].
pub fn specific_overrides_keymap_path_for(os: KeymapOs) -> &'static str {
    match os {
        KeymapOs::Mac => "keymaps/specific-overrides-macos.json",
        KeymapOs::Windows | KeymapOs::Linux => "keymaps/specific-overrides.json",
    }
}

/// Browser-only layer that remaps the chords the browser reserves (Cmd/Ctrl+W, T, N, Q,
/// Shift+T, Ctrl+Tab; BUILD-SPEC 3.6, D12). One file for every host OS: on a given host only
/// one chord family (`ctrl-` or `cmd-`) is reachable, and the other family's `null`
/// unbindings are no-ops there. Loaded by the browser entry crate last, after vim and
/// specific-overrides, and only on wasm; never referenced by the desktop keymap loader.
pub const WEB_KEYMAP_PATH: &str = "keymaps/web.json";

pub fn default_keymap() -> Cow<'static, str> {
    asset_str::<SettingsAssets>(DEFAULT_KEYMAP_PATH)
}

pub const VIM_KEYMAP_PATH: &str = "keymaps/vim.json";

pub fn vim_keymap() -> Cow<'static, str> {
    asset_str::<SettingsAssets>(VIM_KEYMAP_PATH)
}

/// Specific keybinding overrides. Loaded after the base keymap so they win over
/// conflicting base-keymap (and default `Editor`) bindings for the same chords,
/// while still allowing user keymaps (loaded last) to override them. Shared
/// across features - prefer adding a context block here over creating another
/// override keymap file.
#[cfg(target_os = "macos")]
pub const SPECIFIC_OVERRIDES_KEYMAP_PATH: &str = "keymaps/specific-overrides-macos.json";

#[cfg(not(target_os = "macos"))]
pub const SPECIFIC_OVERRIDES_KEYMAP_PATH: &str = "keymaps/specific-overrides.json";

pub fn initial_user_settings_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_user_settings.json")
}

pub fn initial_server_settings_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_server_settings.json")
}

pub fn initial_project_settings_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_local_settings.json")
}

pub fn initial_keymap_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("keymaps/initial.json")
}

pub fn initial_tasks_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_tasks.json")
}

pub fn initial_worktree_setup_tasks_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_worktree_setup_tasks.json")
}

pub fn initial_debug_tasks_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_debug_tasks.json")
}

pub fn initial_local_debug_tasks_content() -> Cow<'static, str> {
    asset_str::<SettingsAssets>("settings/initial_local_debug_tasks.json")
}

#[cfg(test)]
mod keymap_path_tests {
    use super::*;

    #[test]
    fn keymap_paths_by_os() {
        assert_eq!(
            default_keymap_path_for(KeymapOs::Mac),
            "keymaps/default-macos.json"
        );
        assert_eq!(
            default_keymap_path_for(KeymapOs::Windows),
            "keymaps/default-windows.json"
        );
        assert_eq!(
            default_keymap_path_for(KeymapOs::Linux),
            "keymaps/default-linux.json"
        );
        assert_eq!(default_keymap_path_for(KeymapOs::current()), DEFAULT_KEYMAP_PATH);
        assert!(specific_overrides_keymap_path_for(KeymapOs::Mac).ends_with("-macos.json"));
        assert_eq!(
            specific_overrides_keymap_path_for(KeymapOs::current()),
            SPECIFIC_OVERRIDES_KEYMAP_PATH
        );
        assert_eq!(WEB_KEYMAP_PATH, "keymaps/web.json");
        assert!(SettingsAssets::get(WEB_KEYMAP_PATH).is_some());
        assert!(KeymapFile::parse(&asset_str::<SettingsAssets>(WEB_KEYMAP_PATH)).is_ok());
    }

    #[test]
    fn base_keymap_asset_path_matches_host() {
        for (_, base_keymap) in BaseKeymap::OPTIONS {
            assert_eq!(
                base_keymap.asset_path(),
                base_keymap.asset_path_for(KeymapOs::current()),
                "{base_keymap:?}"
            );
        }
        assert!(BaseKeymap::TextMate.asset_path_for(KeymapOs::Linux).is_none());
        assert!(BaseKeymap::TextMate.asset_path_for(KeymapOs::Mac).is_some());
        assert!(BaseKeymap::Zed.asset_path_for(KeymapOs::Windows).is_none());
    }
}
