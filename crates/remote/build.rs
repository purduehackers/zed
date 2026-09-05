fn main() {
    // `transport/websocket/wire.rs` reads `option_env!("ZS_BUILD_ID")`; without this cargo
    // would keep a cached rlib with a stale build id after the environment changes.
    println!("cargo:rerun-if-env-changed=ZS_BUILD_ID");
}
