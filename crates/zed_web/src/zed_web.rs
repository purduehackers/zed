#![cfg(target_family = "wasm")]

//! The browser entry crate: the analogue of `crates/zed` for the `wasm32-unknown-unknown`
//! bundle (BUILD-SPEC 3.1). It wires registries, panels, keymaps, themes and the workspace
//! in the desktop order (minus the crates the browser excludes), seeds settings and keymap
//! from the shell-provided JSON into the in-memory `WasmFs`, opens the remote project over
//! the WebSocket transport, and exposes the wasm-bindgen bridge the shell page drives
//! (CONTRACTS.md §8.4): [`start`], [`flush_client_state`], [`set_hidden`],
//! [`has_unsaved_changes`], [`build_id`].
//!
//! Every dependency but `zed_web_core` sits in the wasm target table and this root is
//! `#![cfg(target_family = "wasm")]`, so native workspace commands see an empty crate.

mod assets;
mod boot;
mod bridge;
mod clipboard;
mod connect;
mod debugger;
mod export;
mod extensions;
mod files;
mod init;
mod keymap;
#[cfg(feature = "test-hooks")]
mod test_hooks;
mod web_settings;
mod window;
mod workspace_chrome;

pub use bridge::{current_session, ensure_fresh_token};
pub use zed_web_core::{BootConfig, BootError, BootStage, ConnectInfo, HostOs};

use wasm_bindgen::prelude::*;

/// Entry point called by the shell page after wasm-bindgen's `init()`. Single-shot: a
/// second call rejects. Resolves once the workspace window is open and the remote project
/// has opened its paths (the `ready` stage), or rejects with a `{ code, message }` object
/// whose `code` is one of the [`BootError`] codes.
///
/// `assets` is the asset pack tarball (fonts, icons, images, themes, sounds); it is copied
/// once and never run through `JSON.stringify`. `host` is the `ZsHost` object of
/// CONTRACTS.md §8.4.
#[wasm_bindgen]
pub async fn start(
    config_json: String,
    assets: js_sys::Uint8Array,
    host: JsValue,
) -> Result<(), JsValue> {
    gpui_platform::web_init();
    let config =
        zed_web_core::parse_boot_config(&config_json).map_err(bridge::boot_error("bad_config"))?;
    let host = bridge::JsHost::from_js(host).map_err(bridge::boot_error("bad_host"))?;
    boot::run(config, assets.to_vec(), host)
        .await
        .map_err(bridge::js_error)
}

/// Flushes the pending settings/keymap saves and the client-state SQLite image now. The
/// shell calls it on `visibilitychange` → hidden (a D7 trigger) and best-effort on
/// `pagehide`. Resolves even when nothing could be flushed (a stopped session).
#[wasm_bindgen]
pub async fn flush_client_state() -> Result<(), JsValue> {
    boot::flush_client_state()
        .await
        .map_err(bridge::boot_error("database"))
}

/// `document.hidden` changed: shortens (hidden) or restores (visible) the client-state save
/// interval and, when hidden, flushes pending saves and the image.
#[wasm_bindgen]
pub fn set_hidden(hidden: bool) {
    boot::set_hidden(hidden)
}

/// Whether any open workspace has a dirty item; the loader's `beforeunload` guard
/// (BUILD-SPEC 13 "stop with unsaved buffers").
#[wasm_bindgen]
pub fn has_unsaved_changes() -> bool {
    boot::has_unsaved_changes()
}

/// The build identity baked at compile time (`ZS_BUILD_ID`, BUILD-SPEC 11.2), `"dev"` for
/// a local build; the shell compares it with the workspace's `client_build`.
#[wasm_bindgen]
pub fn build_id() -> String {
    boot::build_id().to_string()
}

/// Drives the existing Zed update button from the shell's background download.
#[wasm_bindgen]
pub fn set_update_status(status_json: String) -> Result<(), JsValue> {
    let status = serde_json::from_str(&status_json)
        .map_err(anyhow::Error::from)
        .map_err(bridge::boot_error("bad_config"))?;
    boot::async_app()
        .map_err(bridge::boot_error("runtime_missing"))?
        .update(|cx| title_bar::set_web_update_status(status, cx));
    Ok(())
}
