use super::*;
use crate::{future::poll_fn, task::noop_waker, FutureExt};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex as StdMutex,
};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn registered_clone_drop_wake_and_completion() {
    let ready = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let inner_waker = Arc::new(StdMutex::new(None::<Waker>));
    let mut first = {
        let ready = ready.clone();
        let polls = polls.clone();
        let inner_waker = inner_waker.clone();
        poll_fn(move |cx| {
            polls.fetch_add(1, Ordering::SeqCst);
            *inner_waker.lock().unwrap() = Some(cx.waker().clone());
            if ready.load(Ordering::SeqCst) {
                Poll::Ready(42)
            } else {
                Poll::Pending
            }
        })
        .shared()
    };
    let mut dropped = first.clone();
    let mut late = first.clone();
    let first_count = Arc::new(WakeCount::default());
    let dropped_count = Arc::new(WakeCount::default());
    let first_waker = waker_ref(&first_count);
    let dropped_waker = waker_ref(&dropped_count);
    let mut first_cx = Context::from_waker(&first_waker);
    let mut dropped_cx = Context::from_waker(&dropped_waker);

    assert!(Pin::new(&mut first).poll(&mut first_cx).is_pending());
    assert!(Pin::new(&mut dropped).poll(&mut dropped_cx).is_pending());
    drop(dropped);
    inner_waker.lock().unwrap().as_ref().unwrap().wake_by_ref();
    assert_eq!(first_count.0.load(Ordering::SeqCst), 1);
    assert_eq!(dropped_count.0.load(Ordering::SeqCst), 0);

    ready.store(true, Ordering::SeqCst);
    assert_eq!(Pin::new(&mut first).poll(&mut first_cx), Poll::Ready(42));
    assert!(first.is_terminated());
    assert_eq!(late.peek(), Some(&42));
    let polls_before_late = polls.load(Ordering::SeqCst);
    assert_eq!(Pin::new(&mut late).poll(&mut first_cx), Poll::Ready(42));
    assert_eq!(polls.load(Ordering::SeqCst), polls_before_late);
}

#[test]
fn native_waker_mutex_retains_poisoning() {
    let mut shared = crate::future::pending::<()>().shared();
    let notifier = shared.inner.as_ref().unwrap().notifier.clone();
    assert!(std::thread::spawn(move || {
        let _guard = notifier.wakers.lock().unwrap();
        panic!("poison the native waker mutex");
    })
    .join()
    .is_err());

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = Pin::new(&mut shared).poll(&mut cx);
    }))
    .is_err());
}
