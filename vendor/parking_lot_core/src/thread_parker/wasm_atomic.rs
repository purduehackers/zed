// Copyright 2016 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use core::{
    arch::wasm32,
    cell::Cell,
    sync::atomic::{AtomicI32, Ordering},
};
use std::time::{Duration, Instant};
use std::{convert::TryFrom, thread};

#[cfg(feature = "wasm-test-hooks")]
static MAIN_THREAD_PARKS: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

/// Number of untimed parks that entered the browser main-thread spin path.
/// Test-only: a worker can release a held mutex once the main thread really contends.
#[cfg(feature = "wasm-test-hooks")]
pub fn main_thread_park_count() -> usize {
    MAIN_THREAD_PARKS.load(Ordering::Acquire)
}

std::thread_local! {
    // Zed Codespaces (vendor/README.md): browsers forbid `memory.atomic.wait32` on the main
    // thread. The instruction throws a JS exception that unwinds straight through the wasm
    // frames without running a single destructor, so a contended lock on that thread leaked
    // every guard on the stack (gpui's `App` borrow included). The embedder marks that thread
    // and its parks become spin loops instead.
    static CANNOT_WAIT: Cell<bool> = const { Cell::new(false) };
}

/// Marks the calling thread as one that must never block in `memory.atomic.wait32` (the
/// browser's main thread): from now on its parks spin until unparked.
pub fn mark_current_thread_cannot_wait() {
    CANNOT_WAIT.with(|flag| flag.set(true));
}

#[inline]
fn cannot_wait() -> bool {
    CANNOT_WAIT.with(|flag| flag.get())
}

// Helper type for putting a thread to sleep until some other thread wakes it up
pub struct ThreadParker {
    parked: AtomicI32,
}

const UNPARKED: i32 = 0;
const PARKED: i32 = 1;

impl super::ThreadParkerT for ThreadParker {
    type UnparkHandle = UnparkHandle;

    const IS_CHEAP_TO_CONSTRUCT: bool = true;

    #[inline]
    fn new() -> ThreadParker {
        ThreadParker {
            parked: AtomicI32::new(UNPARKED),
        }
    }

    #[inline]
    unsafe fn prepare_park(&self) {
        self.parked.store(PARKED, Ordering::Relaxed);
    }

    #[inline]
    unsafe fn timed_out(&self) -> bool {
        self.parked.load(Ordering::Relaxed) == PARKED
    }

    #[inline]
    unsafe fn park(&self) {
        if cannot_wait() {
            #[cfg(feature = "wasm-test-hooks")]
            MAIN_THREAD_PARKS.fetch_add(1, Ordering::Release);
            while self.parked.load(Ordering::Acquire) == PARKED {
                core::hint::spin_loop();
            }
            return;
        }
        while self.parked.load(Ordering::Acquire) == PARKED {
            let r = wasm32::memory_atomic_wait32(self.ptr(), PARKED, -1);
            // we should have either woken up (0) or got a not-equal due to a
            // race (1). We should never time out (2)
            debug_assert!(r == 0 || r == 1);
        }
    }

    #[inline]
    unsafe fn park_until(&self, timeout: Instant) -> bool {
        if cannot_wait() {
            while self.parked.load(Ordering::Acquire) == PARKED {
                if timeout.checked_duration_since(Instant::now()).is_none() {
                    return false;
                }
                core::hint::spin_loop();
            }
            return true;
        }
        while self.parked.load(Ordering::Acquire) == PARKED {
            if let Some(left) = timeout.checked_duration_since(Instant::now()) {
                let nanos_left = i64::try_from(left.as_nanos()).unwrap_or(i64::max_value());
                let r = wasm32::memory_atomic_wait32(self.ptr(), PARKED, nanos_left);
                debug_assert!(r == 0 || r == 1 || r == 2);
            } else {
                return false;
            }
        }
        true
    }

    #[inline]
    unsafe fn unpark_lock(&self) -> UnparkHandle {
        // We don't need to lock anything, just clear the state
        self.parked.store(UNPARKED, Ordering::Release);
        UnparkHandle(self.ptr())
    }
}

impl ThreadParker {
    #[inline]
    fn ptr(&self) -> *mut i32 {
        &self.parked as *const AtomicI32 as *mut i32
    }
}

pub struct UnparkHandle(*mut i32);

impl super::UnparkHandleT for UnparkHandle {
    #[inline]
    unsafe fn unpark(self) {
        let num_notified = wasm32::memory_atomic_notify(self.0 as *mut i32, 1);
        debug_assert!(num_notified == 0 || num_notified == 1);
    }
}

#[inline]
pub fn thread_yield() {
    thread::yield_now();
}
