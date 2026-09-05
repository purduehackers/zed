#![allow(clippy::disallowed_methods, reason = "build scripts are exempt")]
use std::process::Command;

// Zed's version is the desktop crate's (`crates/zed/Cargo.toml`), never this crate's, so the
// version the browser client reports to the server cannot drift from it (BUILD-SPEC 11.2).
const ZED_MANIFEST: &str = include_str!("../zed/Cargo.toml");

fn main() {
    let zed_cargo_toml: cargo_toml::Manifest =
        toml::from_str(ZED_MANIFEST).expect("failed to parse zed Cargo.toml");
    println!(
        "cargo:rustc-env=ZED_PKG_VERSION={}",
        zed_cargo_toml.package.unwrap().version.unwrap()
    );

    println!("cargo:rerun-if-changed=../../.git/logs/HEAD");
    if let Some(output) = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
    {
        let git_sha = String::from_utf8_lossy(&output.stdout);
        println!("cargo:rustc-env=ZED_COMMIT_SHA={}", git_sha.trim());
    }
    // The bundle's build id (`<zed-commit>-<patch>`, BUILD-SPEC 11.2); `script/build-web`
    // sets it, `build_id()` falls back to "dev".
    println!("cargo:rerun-if-env-changed=ZS_BUILD_ID");
    if let Ok(build_id) = std::env::var("ZS_BUILD_ID") {
        println!("cargo:rustc-env=ZS_BUILD_ID={build_id}");
    }
}
