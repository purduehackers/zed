#[cfg(not(target_family = "wasm"))]
mod capability_granter;
mod compatibility;
pub mod extension_settings;
#[cfg(all(test, not(target_family = "wasm")))]
mod extension_store_test;
#[cfg(not(target_family = "wasm"))]
pub mod headless_host;
mod language_assets;
#[cfg(not(target_family = "wasm"))]
pub mod wasm_host;

pub use compatibility::{is_suppressed_extension, is_version_compatible, schema_version_range};

#[cfg(not(target_family = "wasm"))]
use compatibility::{CURRENT_SCHEMA_VERSION, SUPPRESSED_EXTENSIONS};
#[cfg(not(target_family = "wasm"))]
use language_assets::{discover_query_files, load_plugin_language};
#[cfg(not(target_family = "wasm"))]
include!("native.rs");

#[cfg(target_family = "wasm")]
mod web;
#[cfg(target_family = "wasm")]
pub use web::*;
