//! The browser's default-settings overrides (b7 §4.5), deep-merged over
//! `assets/settings/default.json` before the `SettingsStore` is built, then the AI proxy
//! overrides of [`crate::ai_proxy`] (b11 §4.3) on top, cached per control-plane origin.

use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

/// JSONC overrides applied over the shipped defaults: telemetry off, in-canvas prompts and
/// path pickers (no native dialogs in a tab), no updater, no session restore (the shell
/// decides what opens), server-side window decorations, every worktree trusted (D41).
pub const WEB_SETTINGS_OVERRIDES: &str = r#"{
  "telemetry": { "diagnostics": false, "metrics": false },
  // open_path_prompt against the sandbox (BUILD-SPEC 3.6 "Dialogs")
  "use_system_path_prompts": false,
  // ui_prompt in-canvas prompts
  "use_system_prompts": false,
  "auto_update": false,
  "restore_on_startup": "none",
  "window_decorations": "server",
  // The sandbox is the user's own VM, so "Restricted Mode" and its trust prompt only get in
  // the way; the workspace trusts every worktree it opens (D41).
  "session": { "trust_all_worktrees": true }
}"#;

/// The operator's own Zed layout, applied after [`WEB_SETTINGS_OVERRIDES`] so a workspace
/// opens looking like their desktop editor. Unlike the block above, nothing here is required
/// for the browser to work: it is taste, and the user's own `settings.json` still wins over
/// all of it.
///
/// Deliberately absent, because the bundle does not ship the assets they name: `ui_font_family`
/// and `buffer_font_family` (the tarball carries Lilex and IBM Plex Sans, plus Lilex Nerd
/// Font Mono for the terminal) and the
/// `Bearded Icons` icon theme. Font *sizes* carry over, so the proportions match even though
/// the faces differ. The theme is Ayu Dark, which the bundle does pack (`one`, `ayu` and
/// `gruvbox`); the operator's `Kintsugi` family is an extension and is not there.
/// Language server, formatter, proxy and SSH settings are desktop concerns and stay out.
pub const WEB_LAYOUT_DEFAULTS: &str = r#"{
  // AI off for the whole editor: no agent panel, no edit predictions, no registry fetches.
  "disable_ai": true,
  "theme": { "mode": "dark", "dark": "Ayu Dark", "light": "Ayu Light" },
  "ui_font_size": 15,
  "buffer_font_size": 14,
  "buffer_line_height": "comfortable",
  "vim_mode": true,
  // A browser tab can be closed at any moment; saving on focus change is the desktop setting
  // and happens to be the safer one here too.
  "autosave": "on_focus_change",
  "diff_view_style": "unified",
  "bottom_dock_layout": "contained",
  "diagnostics": { "inline": { "enabled": true } },
  "git": { "inline_blame": { "show_commit_summary": true } },
  "tab_bar": { "show": true },
  "tabs": { "file_icons": false, "git_status": false },
  // The tab is the window: no menus, no window controls, and nothing about the machine or the
  // account. One workspace is one project, so the host and project name say nothing a person
  // does not already know from the page they opened. Branch and worktree stay: those change.
  "title_bar": {
    "show_branch_status_icon": true,
    "show_menus": false,
    "show_project_items": false,
    "show_onboarding_banner": false,
    "show_user_picture": false,
    "show_sign_in": false,
    "show_user_menu": false,
    "show_branch_name": true,
    "show_worktree_name": true
  },
  "project_panel": {
    "dock": "left",
    // First-run setup opens it explicitly; returning tabs restore their saved dock state.
    "starts_open": false,
    "diagnostic_badges": false,
    "hide_hidden": false,
    "hide_root": true,
    "hide_gitignore": false,
    "bold_folder_labels": true,
    "entry_spacing": "comfortable"
  },
  "git_panel": {
    "dock": "left",
    "show_count_badge": true,
    "collapse_untracked_diff": true,
    "tree_view": true,
    "status_style": "icon"
  },
  "outline_panel": { "dock": "left" },
  "collaboration_panel": { "button": false, "dock": "left" },
  "terminal": {
    "font_family": "Lilex Nerd Font Mono",
    "dock": "right",
    "show_count_badge": false,
    "toolbar": { "breadcrumbs": true }
  },
  "agent": { "dock": "right", "enabled": false }
}"#;

/// Deep-merges [`WEB_SETTINGS_OVERRIDES`] and then [`WEB_LAYOUT_DEFAULTS`] over `base_json`
/// (all JSONC) and returns plain JSON. Objects merge recursively; every other value in the
/// overrides replaces the base.
pub fn merge_web_defaults(base_json: &str) -> anyhow::Result<String> {
    let mut base: serde_json::Value = serde_json_lenient::from_str(base_json)
        .map_err(|error| anyhow::anyhow!("default settings are not valid JSONC: {error}"))?;
    let overrides: serde_json::Value = serde_json_lenient::from_str(WEB_SETTINGS_OVERRIDES)
        .map_err(|error| anyhow::anyhow!("web overrides are not valid JSONC: {error}"))?;
    deep_merge(&mut base, overrides);
    let layout: serde_json::Value = serde_json_lenient::from_str(WEB_LAYOUT_DEFAULTS)
        .map_err(|error| anyhow::anyhow!("web layout defaults are not valid JSONC: {error}"))?;
    deep_merge(&mut base, layout);
    Ok(serde_json::to_string_pretty(&base)?)
}

/// The complete web defaults for one control-plane origin: [`merge_web_defaults`] over
/// `base_json`, then [`crate::ai_proxy::merge_ai_proxy_defaults`] with `origin`. Computed
/// once per origin and leaked, so the `&'static str` the `SettingsStore` wants survives; a
/// cache keyed on the origin (rather than a single `OnceLock`) keeps a second boot with a
/// different origin in the same process — a test harness — from reading the first one's URLs.
pub fn web_defaults_for_origin(origin: &str, base_json: &str) -> anyhow::Result<&'static str> {
    static CACHE: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("the web defaults cache is poisoned"))?;
    if let Some(cached) = cache.get(origin) {
        return Ok(cached);
    }
    let merged = crate::ai_proxy::merge_ai_proxy_defaults(&merge_web_defaults(base_json)?, origin)?;
    let leaked: &'static str = Box::leak(merged.into_boxed_str());
    cache.insert(origin.to_owned(), leaked);
    Ok(leaked)
}

pub(crate) fn deep_merge(base: &mut serde_json::Value, overrides: serde_json::Value) {
    match (base, overrides) {
        (serde_json::Value::Object(base), serde_json::Value::Object(overrides)) => {
            for (key, value) in overrides {
                match base.get_mut(&key) {
                    Some(existing) => deep_merge(existing, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, overrides) => *base = overrides,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_keymap_has_browser_safe_entry_points() {
        let sections: serde_json::Value =
            serde_json_lenient::from_str(include_str!("../../../assets/keymaps/web.json")).unwrap();
        let bindings = &sections
            .as_array()
            .unwrap()
            .iter()
            .find(|section| section["context"] == "Workspace")
            .unwrap()["bindings"];
        for (key, action) in [
            ("f1", "command_palette::Toggle"),
            ("alt-shift-p", "command_palette::Toggle"),
            ("alt-p", "file_finder::Toggle"),
            ("ctrl-`", "terminal_panel::Toggle"),
        ] {
            assert_eq!(bindings[key], action, "{key}");
        }
    }

    #[test]
    fn terminal_font_contains_nerd_glyphs_in_every_face() {
        let fonts: [&[u8]; 4] = [
            include_bytes!("../../../assets/fonts/lilex-nerd/LilexNerdFontMono-Regular.ttf"),
            include_bytes!("../../../assets/fonts/lilex-nerd/LilexNerdFontMono-Bold.ttf"),
            include_bytes!("../../../assets/fonts/lilex-nerd/LilexNerdFontMono-Italic.ttf"),
            include_bytes!("../../../assets/fonts/lilex-nerd/LilexNerdFontMono-BoldItalic.ttf"),
        ];
        for bytes in fonts {
            let face = ttf_parser::Face::parse(bytes, 0).unwrap();
            assert!(
                face.names()
                    .into_iter()
                    .any(|name| { name.to_string().as_deref() == Some("Lilex Nerd Font Mono") })
            );
            for glyph in ['A', '\u{e0b0}', '\u{e0a0}', '\u{f07b}', '\u{f308}'] {
                assert!(face.glyph_index(glyph).is_some(), "missing glyph {glyph:?}");
            }
        }
    }

    #[test]
    fn merge_keeps_default_keys() {
        let defaults = settings::default_settings();
        let merged = merge_web_defaults(&defaults).unwrap();
        let merged: serde_json::Value = serde_json::from_str(&merged).unwrap();
        let original: serde_json::Value = serde_json_lenient::from_str(&defaults).unwrap();

        assert_eq!(merged["telemetry"]["metrics"], serde_json::json!(false));
        assert_eq!(merged["telemetry"]["diagnostics"], serde_json::json!(false));
        assert_eq!(merged["use_system_path_prompts"], serde_json::json!(false));
        assert_eq!(merged["use_system_prompts"], serde_json::json!(false));
        assert_eq!(merged["restore_on_startup"], serde_json::json!("none"));
        assert_eq!(
            merged["session"]["trust_all_worktrees"],
            serde_json::json!(true)
        );
        // The layout block is applied on top and must survive the merge intact.
        assert_eq!(merged["disable_ai"], serde_json::json!(true));
        assert_eq!(merged["vim_mode"], serde_json::json!(true));
        assert_eq!(merged["theme"]["mode"], serde_json::json!("dark"));
        assert_eq!(merged["project_panel"]["dock"], serde_json::json!("left"));
        assert_eq!(merged["project_panel"]["starts_open"], serde_json::json!(false));
        assert_eq!(merged["terminal"]["dock"], serde_json::json!("right"));
        assert_eq!(merged["terminal"]["font_family"], "Lilex Nerd Font Mono");
        // The tab is the window: the machine and project name go, the git identity stays.
        assert_eq!(
            merged["title_bar"]["show_project_items"],
            serde_json::json!(false)
        );
        assert_eq!(
            merged["title_bar"]["show_branch_name"],
            serde_json::json!(true)
        );
        assert_eq!(
            merged["title_bar"]["show_worktree_name"],
            serde_json::json!(true)
        );
        assert_eq!(merged["title_bar"]["show_menus"], serde_json::json!(false));
        // Both theme names must be ones the asset tarball actually packs.
        assert_eq!(merged["theme"]["dark"], serde_json::json!("Ayu Dark"));
        assert_eq!(merged["theme"]["light"], serde_json::json!("Ayu Light"));
        // The bundle ships neither of the operator's fonts, so the faces stay as shipped.
        assert_ne!(merged["ui_font_family"], serde_json::json!("Ioskeley Mono"));
        for key in original.as_object().unwrap().keys() {
            assert!(
                merged.get(key).is_some(),
                "top-level key {key} lost in merge"
            );
        }
    }

    /// The merged document is what `zed_web` hands to `SettingsStore::new` as the defaults;
    /// a key the store rejects would fail the browser boot at `settings::init` time.
    #[gpui::test]
    fn web_defaults_parse_in_store(cx: &mut gpui::App) {
        let merged = merge_web_defaults(&settings::default_settings()).unwrap();
        // `SettingsStore::new` parses the defaults into `SettingsContent` and panics on a
        // document it cannot use; every override must survive that parse.
        let store = settings::SettingsStore::new(cx, &merged);
        let telemetry = &store.raw_default_settings().telemetry;
        assert_eq!(
            telemetry.as_ref().and_then(|telemetry| telemetry.metrics),
            Some(false)
        );
    }

    #[test]
    fn defaults_are_cached_per_origin() {
        let defaults = settings::default_settings();
        let a = web_defaults_for_origin("https://a.example.com", &defaults).unwrap();
        let b = web_defaults_for_origin("https://b.example.com", &defaults).unwrap();
        let a_again = web_defaults_for_origin("https://a.example.com", &defaults).unwrap();
        assert!(
            std::ptr::eq(a, a_again),
            "the same origin must hit the cache"
        );
        assert!(!std::ptr::eq(a, b));
        assert!(a.contains("https://a.example.com/api/ai/openai"));
        assert!(!a.contains("b.example.com"));
        assert!(b.contains("https://b.example.com/api/ai/codestral"));
        let parsed: serde_json::Value = serde_json::from_str(b).unwrap();
        assert_eq!(parsed["telemetry"]["metrics"], serde_json::json!(false));
        assert_eq!(
            parsed["language_models"]["ollama"]["api_url"],
            serde_json::json!("http://localhost:11434")
        );
    }

    #[test]
    fn merge_is_recursive() {
        let merged =
            merge_web_defaults(r#"{ "telemetry": { "metrics": true, "extra": 1 }, "a": 1 }"#)
                .unwrap();
        let merged: serde_json::Value = serde_json::from_str(&merged).unwrap();
        assert_eq!(merged["telemetry"]["extra"], serde_json::json!(1));
        assert_eq!(merged["telemetry"]["metrics"], serde_json::json!(false));
        assert_eq!(merged["a"], serde_json::json!(1));
    }
}
