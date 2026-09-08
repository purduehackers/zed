#![cfg(target_family = "wasm")]

//! GPUI's browser platform uses one document-owned canvas and supports one top-level window.
//! Browser WebGPU is preferred by default, with an automatic WebGL2 fallback. Applications can
//! force either backend with [`WebBackendPreference`]. Opening a second top-level window, or
//! reopening one after it closes, returns [`WebWindowError`].

mod accessibility;
pub mod clipboard;
mod dispatcher;
mod display;
mod events;
pub mod files;
mod http_client;
mod ime_mirror;
mod keyboard;
mod logging;
mod platform;
mod window;

pub use accessibility::{set_text_input_label, toggle_screen_reader_mode};
pub use dispatcher::WebDispatcher;
pub use display::WebDisplay;
pub use gpui_wgpu::WebBackendPreference;
pub use http_client::{FetchCredentials, FetchHttpClient};
pub use keyboard::WebKeyboardLayout;
pub use logging::init_logging;
pub use platform::{WebPlatform, WebWindowError};
pub use window::WebWindow;
