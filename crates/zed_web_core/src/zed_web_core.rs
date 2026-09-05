//! Target-neutral half of the browser entry crate (`zed_web`): the boot configuration and
//! progress vocabulary shared with the shell page, the in-memory asset pack, host-OS
//! detection, the web default settings and the AI-proxy rules (b11). Everything here is pure
//! Rust with no JS or GPUI dependency so it is unit-tested natively
//! (`cargo test -p zed_web_core`).

pub mod ai_proxy;
pub mod asset_pack;
pub mod boot_config;
pub mod host_os;
pub mod web_settings;

pub use asset_pack::AssetPack;
pub use boot_config::{
    Backend, BootConfig, BootError, BootStage, ConnectInfo, DocumentKind, WorkspaceTarget,
    parse_boot_config,
};
pub use host_os::HostOs;
pub use web_settings::{WEB_SETTINGS_OVERRIDES, merge_web_defaults, web_defaults_for_origin};
