use std::future::Future;

/// Runs `future` to completion on the installed `smol::runtime::Runtime` (gpui's background
/// executor once `zed_web` has installed it, b7 §3.27) or, before that, on the current thread's
/// JS event loop. Exists so that `git::repository::untracked_files_for_checkpoint`
/// (RealGitRepository-only) keeps compiling; live browser code should still prefer gpui's
/// executor directly (`cx.background_spawn`).
pub fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> async_task::Task<T> {
    let (runnable, task) = async_task::spawn(future, schedule);
    runnable.schedule();
    task
}

fn schedule(runnable: async_task::Runnable) {
    match crate::runtime::installed() {
        // gpui's executor, any thread.
        Some(runtime) => runtime.schedule(runnable),
        // Before install: the current thread's event loop (main thread only, in practice).
        None => super::js_set_timeout(
            Box::new(move || {
                runnable.run();
            }),
            0,
        ),
    }
}
