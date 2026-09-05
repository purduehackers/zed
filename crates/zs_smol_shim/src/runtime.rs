//! `smol::runtime` on wasm32-unknown-unknown: the slot through which the entry crate hands
//! `smol::spawn` gpui's background executor. Target-neutral so it can be unit-tested natively;
//! only exported as `smol::runtime` on wasm. Deliberately the minimum: no `Timer`, no
//! `block_on`, no executor of its own.

use std::sync::{Arc, OnceLock};

pub use async_task::Runnable;

/// Where `smol::spawn` hands its runnables once the app is up. The entry crate installs one
/// adapter over gpui's `BackgroundExecutor` as the first statement inside `run_embedded`
/// (b7 §3.27 step 2), before any `cx.spawn` or timer.
pub trait Runtime: Send + Sync + 'static {
    /// Run `runnable` (one poll of a `smol::spawn` future) on the runtime's executor. May be
    /// called from any thread, including gpui_web background workers; must not block.
    fn schedule(&self, runnable: Runnable);
}

static RUNTIME: OnceLock<Arc<dyn Runtime>> = OnceLock::new();

/// Installs the process-wide runtime. `Err(runtime)` hands the argument back if one is already
/// installed (b7 calls `.ok()` on the result and then asserts `runtime_installed()`).
pub fn install_runtime(runtime: Arc<dyn Runtime>) -> Result<(), Arc<dyn Runtime>> {
    RUNTIME.set(runtime)
}

/// `true` once `install_runtime` has succeeded.
pub fn runtime_installed() -> bool {
    RUNTIME.get().is_some()
}

/// `Some` after installation; read by `wasm::spawn::schedule`.
pub(crate) fn installed() -> Option<&'static Arc<dyn Runtime>> {
    RUNTIME.get()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingRuntime {
        scheduled: AtomicUsize,
    }

    impl Runtime for CountingRuntime {
        fn schedule(&self, runnable: Runnable) {
            self.scheduled.fetch_add(1, Ordering::SeqCst);
            runnable.run();
        }
    }

    #[test]
    fn runtime_install_once() {
        let first: Arc<CountingRuntime> = Arc::new(CountingRuntime::default());
        assert!(!runtime_installed());
        assert!(install_runtime(first.clone()).is_ok());
        assert!(runtime_installed());

        let second: Arc<dyn Runtime> = Arc::new(CountingRuntime::default());
        let rejected = install_runtime(second.clone()).unwrap_err();
        assert!(Arc::ptr_eq(&rejected, &second));

        let (runnable, task) = async_task::spawn(async { 1 }, |_runnable: Runnable| {});
        installed().unwrap().schedule(runnable);
        assert_eq!(first.scheduled.load(Ordering::SeqCst), 1);
        assert_eq!(futures_lite::future::block_on(task), 1);
    }
}
