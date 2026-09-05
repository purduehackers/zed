//! Serializes every SQLite call in the browser build (SQLite is compiled with
//! `SQLITE_THREADSAFE=0` there) and is also the write queue's lock
//! (`thread_safe_connection::wasm_lock_queue`), so sqlez holds exactly one lock on
//! wasm and there is no lock order to reason about. It is a spin lock rather than a
//! `parking_lot` mutex because a contended `parking_lot` lock parks with
//! `Atomics.wait`, which traps on the browser main thread; critical sections are
//! short (one statement, or one queued write) and never span an `.await`.
//!
//! The lock is reentrant: the thread that holds it may take it again (nested
//! `Statement::prepare` calls from `with_savepoint`, `sql_has_syntax_error`'s scratch
//! connection, or a `write` issued from inside another write's closure), and the
//! outermost guard releases it.
//!
//! Compiled on wasm and, so it is unit-tested natively, under `cfg(test)` and the
//! `test-support` feature.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// The token of the thread holding the lock; 0 means free.
static OWNER: AtomicU64 = AtomicU64::new(0);
/// How many guards the owner currently holds.
static DEPTH: AtomicUsize = AtomicUsize::new(0);
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static TOKEN: u64 = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
}

/// Holds the process-wide SQLite lock; dropping the outermost guard releases it.
#[must_use = "the lock is released as soon as the guard is dropped"]
pub(crate) struct Guard(());

/// Acquires the process-wide SQLite lock, spinning while another thread holds it.
/// Reentrant on the owning thread.
pub(crate) fn lock() -> Guard {
    let me = TOKEN.with(|token| *token);
    if OWNER.load(Ordering::Acquire) == me {
        DEPTH.fetch_add(1, Ordering::Relaxed);
        return Guard(());
    }
    while OWNER
        .compare_exchange_weak(0, me, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::hint::spin_loop();
    }
    DEPTH.store(1, Ordering::Relaxed);
    Guard(())
}

impl Drop for Guard {
    fn drop(&mut self) {
        if DEPTH.fetch_sub(1, Ordering::Relaxed) == 1 {
            OWNER.store(0, Ordering::Release);
        }
    }
}

/// Whether the calling thread currently holds the lock.
pub fn is_held_by_current_thread() -> bool {
    OWNER.load(Ordering::Acquire) == TOKEN.with(|token| *token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::UnsafeCell, thread};

    #[test]
    fn reentrant_same_thread() {
        assert!(!is_held_by_current_thread());
        let outer = lock();
        assert!(is_held_by_current_thread());
        {
            let _inner = lock();
            assert!(is_held_by_current_thread());
        }
        assert!(
            is_held_by_current_thread(),
            "dropping the inner guard must not release the lock"
        );
        drop(outer);
        assert!(!is_held_by_current_thread());
    }

    /// A counter that is only sound to touch under the lock; the test would be
    /// racy (and would fail under `cargo test`'s repeated increments) if the lock
    /// did not serialize the threads.
    struct Unsynchronized(UnsafeCell<u64>);
    unsafe impl Sync for Unsynchronized {}

    #[test]
    fn serializes_across_threads() {
        static COUNTER: Unsynchronized = Unsynchronized(UnsafeCell::new(0));
        const THREADS: usize = 8;
        const INCREMENTS: u64 = 10_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                thread::spawn(|| {
                    for _ in 0..INCREMENTS {
                        let _guard = lock();
                        let nested = lock();
                        // SAFETY: the lock is held, so no other thread touches the cell.
                        unsafe {
                            let value = COUNTER.0.get();
                            *value = (*value).wrapping_add(1);
                        }
                        drop(nested);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("a locking thread panicked");
        }

        let _guard = lock();
        // SAFETY: the lock is held.
        let total = unsafe { *COUNTER.0.get() };
        assert_eq!(total, THREADS as u64 * INCREMENTS);
    }
}
