//! Deterministically exercises `Shared::record_waker` while a worker owns its notifier
//! mutex. Only compiled with `zed_web/test-hooks`; no App borrow crosses the probe.

use std::{
    future::Future as _,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

use anyhow::{Result, ensure};
use futures::{FutureExt as _, channel::oneshot, future};
use js_sys::Object;
use wasm_bindgen::JsValue;

use super::{boot, js_sleep, set};

const DEADLINE: Duration = Duration::from_secs(10);
static ACTIVE: AtomicBool = AtomicBool::new(false);

struct ActiveProbe;

impl Drop for ActiveProbe {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::Release);
    }
}

#[derive(Default)]
struct Gate {
    cloned: AtomicBool,
    lock_held: AtomicBool,
    main_polling: AtomicBool,
    park_baseline: AtomicUsize,
    observed_park: AtomicBool,
    timed_out: AtomicBool,
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl futures::task::ArcWake for WakeCount {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::Release);
    }
}

// Shared calls Waker::clone while holding its notifier mutex. The first clone made by
// the worker waits until the main thread enters the actual D42 parker branch. The
// deadline always releases the worker, even when testing the unfixed std-mutex path.
unsafe fn clone_waker(data: *const ()) -> RawWaker {
    // SAFETY: every raw pointer owns an Arc<Gate>; borrow that ownership until cloning it.
    let gate = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Gate>()) });
    if !gate.cloned.swap(true, Ordering::AcqRel) {
        let started = web_time::Instant::now();
        gate.lock_held.store(true, Ordering::Release);
        loop {
            if gate.main_polling.load(Ordering::Acquire)
                && parking_lot_core::main_thread_park_count()
                    > gate.park_baseline.load(Ordering::Acquire)
            {
                gate.observed_park.store(true, Ordering::Release);
                break;
            }
            if started.elapsed() >= DEADLINE {
                gate.timed_out.store(true, Ordering::Release);
                break;
            }
            std::hint::spin_loop();
        }
    }
    raw_waker(Arc::clone(&gate))
}

unsafe fn wake(data: *const ()) {
    // This probe's permanently pending future never needs a wakeup. Consume ownership.
    unsafe { drop_waker(data) };
}

unsafe fn wake_by_ref(_: *const ()) {}

unsafe fn drop_waker(data: *const ()) {
    // SAFETY: consume precisely the Arc ownership supplied to this RawWaker.
    drop(unsafe { Arc::from_raw(data.cast::<Gate>()) });
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

fn raw_waker(gate: Arc<Gate>) -> RawWaker {
    RawWaker::new(Arc::into_raw(gate).cast(), &VTABLE)
}

pub(super) async fn probe() -> Result<JsValue> {
    ensure!(
        ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok(),
        "a shared contention probe is already running"
    );
    let _active = ActiveProbe;
    // Take only an executor clone, not an App update/borrow, across the synchronous poll.
    let executor = boot::async_app()?.background_executor().clone();
    let main_thread = std::thread::current().id();
    let gate = Arc::new(Gate::default());
    let mut shared = future::pending::<()>().shared();
    let mut worker_shared = shared.clone();
    let worker_gate = gate.clone();
    let worker = executor.spawn(async move {
        let different_thread = std::thread::current().id() != main_thread;
        // SAFETY: VTABLE implements Arc ownership and does not access thread-local data.
        let waker = unsafe { Waker::from_raw(raw_waker(worker_gate)) };
        let pending = Pin::new(&mut worker_shared)
            .poll(&mut Context::from_waker(&waker))
            .is_pending();
        drop(worker_shared);
        drop(waker);
        (different_thread, pending)
    });

    let started = web_time::Instant::now();
    while !gate.lock_held.load(Ordering::Acquire) {
        ensure!(
            started.elapsed() < DEADLINE,
            "worker never held the Shared notifier mutex"
        );
        // State-based waiting only: this delay does not manufacture the contention.
        js_sleep(Duration::from_millis(1)).await;
    }
    ensure!(
        !gate.timed_out.load(Ordering::Acquire),
        "worker released before the main poll"
    );
    let baseline = parking_lot_core::main_thread_park_count();
    gate.park_baseline.store(baseline, Ordering::Release);
    // No await or other foreground work between this flag and the exact Shared poll.
    gate.main_polling.store(true, Ordering::Release);
    let main_pending = Pin::new(&mut shared)
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
        .is_pending();
    let parks = parking_lot_core::main_thread_park_count() - baseline;
    let (different_thread, worker_pending) = worker.await;
    drop(shared);
    ensure!(different_thread, "the probe did not execute on a worker");
    ensure!(
        !gate.timed_out.load(Ordering::Acquire),
        "worker's contention deadline expired"
    );
    ensure!(
        gate.observed_park.load(Ordering::Acquire) && parks > 0,
        "the main Shared poll never entered the D42 parker"
    );
    ensure!(
        main_pending && worker_pending,
        "the pending Shared future unexpectedly completed"
    );
    let wakers_released = Arc::strong_count(&gate) == 1;
    ensure!(wakers_released, "dropping Shared retained the probe waker");

    // Also exercise Pending -> Ready, wakeup, cloning a completed Shared, and dropping a
    // pending clone. This checks the notifier change preserves ordinary Shared semantics.
    let (send, receive) = oneshot::channel::<u32>();
    let mut completing = receive
        .map(|value| value.expect("probe sender is retained"))
        .shared();
    let second = completing.clone();
    drop(completing.clone());
    let wake_count = Arc::new(WakeCount::default());
    let completion_waker = futures::task::waker(wake_count.clone());
    ensure!(
        Pin::new(&mut completing)
            .poll(&mut Context::from_waker(&completion_waker))
            .is_pending(),
        "the completion probe was not initially pending"
    );
    send.send(47)
        .map_err(|_| anyhow::anyhow!("the Shared receiver was dropped"))?;
    let completion_wakes = wake_count.0.load(Ordering::Acquire);
    ensure!(
        completion_wakes > 0,
        "Shared did not notify its registered waker"
    );
    let (first_value, second_value) = future::join(completing.clone(), second).await;
    let completed_clone_value = completing.clone().await;
    ensure!(
        first_value == 47 && second_value == 47 && completed_clone_value == 47,
        "Shared completion values differed"
    );
    drop(completing);

    let out = Object::new();
    set(&out, "mainThreadParks", parks as f64);
    set(
        &out,
        "workerObservedPark",
        gate.observed_park.load(Ordering::Acquire),
    );
    set(&out, "workerPending", worker_pending);
    set(&out, "mainPending", main_pending);
    set(&out, "wakersReleased", wakers_released);
    set(&out, "completionWakes", completion_wakes as f64);
    set(&out, "completionValue", first_value);
    set(&out, "completedCloneValue", completed_clone_value);
    Ok(out.into())
}
