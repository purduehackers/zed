//! `smol` for Zed Codespaces. Native targets: smol 2.0.2's public surface, assembled from the
//! same crates smol re-exports. wasm32-unknown-unknown: only the surface a browser-set crate
//! references exists — `fs`, `net`, `process` as stubs returning `io::ErrorKind::Unsupported`,
//! `spawn`, and `runtime`, through which the entry crate hands `spawn` gpui's background
//! executor (`zed_web`, b7 §3.27). `Timer`, `block_on`, `unblock`, `Unblock` and `Async` are
//! deliberately absent on wasm so that a new use fails to compile instead of blocking or
//! trapping the worker; timers and blocking belong to gpui's executor there.
// smol 2.0.2 carries this too; the facade stands in for it in every native binary.
#![forbid(unsafe_code)]
#[doc(inline)]
pub use async_executor::{Executor, LocalExecutor, Task};
#[doc(inline)]
pub use futures_lite::{future, io, pin, prelude, ready, stream};
#[doc(inline)]
pub use {async_channel as channel, async_lock as lock};

#[cfg(not(target_family = "wasm"))]
#[doc(inline)]
pub use {
    async_fs as fs,
    async_io::{Async, Timer, block_on},
    async_net as net, async_process as process,
    blocking::{Unblock, unblock},
};
#[cfg(not(target_family = "wasm"))]
mod native_spawn; // verbatim copy of smol-2.0.2/src/spawn.rs
#[cfg(not(target_family = "wasm"))]
pub use native_spawn::spawn;

#[cfg(target_family = "wasm")]
mod wasm;
#[cfg(target_family = "wasm")]
pub use wasm::{fs, net, process, spawn};

/// `smol::runtime`: where the wasm `spawn` hands its runnables (installed once by `zed_web`).
#[cfg(target_family = "wasm")]
pub mod runtime;
// The same file, compiled natively only for its unit test.
#[cfg(all(test, not(target_family = "wasm")))]
mod runtime;

/// Proves the native facade is smol: every re-exported type is the crates-io type, and the
/// executor, timer, filesystem and process halves actually run.
#[cfg(all(test, not(target_family = "wasm")))]
mod native_tests {
    use std::time::{Duration, Instant};

    #[test]
    fn type_identity() {
        fn f(t: crate::Task<u8>) -> async_task::Task<u8> {
            t
        }
        fn g(t: crate::Timer) -> async_io::Timer {
            t
        }
        let _ = f;
        let _ = g;
    }

    #[test]
    fn spawn_and_timer() {
        crate::block_on(async {
            let t = crate::spawn(async { 1 + 2 });
            crate::Timer::at(Instant::now() + Duration::from_millis(5)).await;
            assert_eq!(t.await, 3);
        });
    }

    #[test]
    fn fs_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("roundtrip.txt");
        crate::block_on(async {
            crate::fs::write(&path, b"hello").await.unwrap();
            let read = crate::fs::read_to_string(&path).await.unwrap();
            assert_eq!(read, "hello");
        });
    }

    #[cfg(unix)]
    #[test]
    fn process_output() {
        crate::block_on(async {
            let output = crate::process::Command::new("true").output().await.unwrap();
            assert!(output.status.success());
        });
    }
}
