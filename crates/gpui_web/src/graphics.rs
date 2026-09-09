//! Recover the browser's graphics device without recreating the GPUI window or editor.

use crate::{events::EventListenerHandle, window::WebWindowInner};
use futures::{
    FutureExt, StreamExt,
    channel::mpsc,
    future::{AbortHandle, Abortable},
};
use gpui::{BackgroundExecutor, DevicePixels, RequestFrameOptions, Size};
use gpui_wgpu::{PreparedWebGraphics, WebBackendPreference, WgpuBackend, WgpuContext};
use std::{
    cell::RefCell, collections::VecDeque, future::Future, rc::Rc, sync::Mutex, time::Duration,
};
use wasm_bindgen::JsCast;
use web_time::Instant;

enum GraphicsEvent {
    DeviceLost(u64),
    ContextLost,
    ContextRestored,
}

pub(crate) struct GraphicsRecovery {
    task: AbortHandle,
    _listeners: Vec<EventListenerHandle>,
}

impl GraphicsRecovery {
    pub(crate) fn new(
        inner: &Rc<WebWindowInner>,
        context: &WgpuContext,
        shared_context: Rc<RefCell<Option<WgpuContext>>>,
        executor: BackgroundExecutor,
    ) -> Self {
        let backend = match context.backend() {
            WgpuBackend::BrowserWebGpu => WebBackendPreference::WebGpu,
            _ => WebBackendPreference::WebGl,
        };
        // Loss/restoration can arrive from both wgpu and the canvas. Bound the queue,
        // coalesce a loss episode, and reject notifications from obsolete devices.
        let (sender, mut events) = mpsc::channel(4);
        watch_device(context, sender.clone(), 0);
        let mut listeners = Vec::new();
        for name in ["webglcontextlost", "webglcontextrestored"] {
            let weak = Rc::downgrade(inner);
            let mut sender = sender.clone();
            listeners.push(EventListenerHandle::add(
                inner.canvas.as_ref(),
                name,
                move |event| {
                    let Some(inner) = weak.upgrade() else { return };
                    let event: &web_sys::Event = event.unchecked_ref();
                    if name == "webglcontextlost" {
                        event.prevent_default(); // Permit the browser to restore the context.
                        inner.graphics_ready.set(false);
                        sender.try_send(GraphicsEvent::ContextLost).ok();
                    } else {
                        sender.try_send(GraphicsEvent::ContextRestored).ok();
                    }
                },
            ));
        }
        inner
            .canvas
            .set_attribute("data-gpui-graphics", "ready")
            .ok();
        let (task, registration) = AbortHandle::new_pair();
        let weak = Rc::downgrade(inner);
        wasm_bindgen_futures::spawn_local(async move {
            let _ = Abortable::new(
                async move {
                    let mut generation = 0;
                    let mut recent_losses = VecDeque::new();
                    while let Some(event) = events.next().await {
                        match event {
                            GraphicsEvent::DeviceLost(id) if id != generation => continue,
                            GraphicsEvent::ContextRestored => continue,
                            _ => {}
                        }
                        let Some(inner) = weak.upgrade() else { break };
                        status(&inner, "recovering");
                        recent_losses
                            .retain(|time: &Instant| time.elapsed() < Duration::from_secs(60));
                        recent_losses.push_back(Instant::now());
                        if recent_losses.len() > 3 {
                            log::error!(
                                "Browser graphics repeatedly failed; automatic recovery stopped"
                            );
                            status(&inner, "failed");
                            break;
                        }
                        inner.state.borrow_mut().renderer.destroy();
                        if backend == WebBackendPreference::WebGl {
                            let restored = timeout(
                                async {
                                    while let Some(event) = events.next().await {
                                        if matches!(event, GraphicsEvent::ContextRestored) {
                                            return Ok(());
                                        }
                                    }
                                    anyhow::bail!("Browser graphics event stream closed")
                                },
                                &executor,
                            )
                            .await;
                            if let Err(error) = restored.and_then(|result| result) {
                                log::error!("Browser WebGL restoration failed: {error:#}");
                                status(&inner, "failed");
                                break;
                            }
                        }
                        let mut recovered = false;
                        for attempt in 0..3 {
                            if attempt > 0 {
                                executor
                                    .timer(Duration::from_millis(250 * (1 << attempt)))
                                    .await;
                            }
                            let prepared =
                                timeout(WgpuContext::new_web(&inner.canvas, backend), &executor)
                                    .await
                                    .and_then(|result| result);
                            let result =
                                prepared.and_then(|PreparedWebGraphics { context, surface }| {
                                    if context.device_lost() {
                                        anyhow::bail!("Replacement GPU device was lost")
                                    }
                                    let max_size = context.device.limits().max_texture_dimension_2d;
                                    let (width, height) = inner.last_physical_size.get();
                                    let size = Size {
                                        width: DevicePixels(width.max(1).min(max_size) as i32),
                                        height: DevicePixels(height.max(1).min(max_size) as i32),
                                    };
                                    {
                                        let mut state = inner.state.borrow_mut();
                                        state.renderer.recover_web(&context, surface, size)?;
                                        state.max_texture_dimension = max_size;
                                    }
                                    generation += 1;
                                    watch_device(&context, sender.clone(), generation);
                                    *shared_context.borrow_mut() = Some(context);
                                    Ok(())
                                });
                            match result {
                                Ok(()) => {
                                    recovered = true;
                                    break;
                                }
                                Err(error) => log::warn!(
                                    "Browser graphics recovery attempt {}: {error:#}",
                                    attempt + 1
                                ),
                            }
                        }
                        if !recovered {
                            status(&inner, "failed");
                            break;
                        }
                        status(&inner, "ready");
                        // Repaint every view: the old scene contains invalid atlas tile IDs.
                        inner.with_callback(
                            |callbacks| &mut callbacks.request_frame,
                            |callback| {
                                callback(RequestFrameOptions {
                                    require_presentation: true,
                                    force_render: true,
                                });
                            },
                        );
                        inner.wake_frame_loop();
                        log::info!("Browser graphics recovered without restarting the editor");
                    }
                },
                registration,
            )
            .await;
        });
        Self {
            task,
            _listeners: listeners,
        }
    }
}

fn watch_device(context: &WgpuContext, sender: mpsc::Sender<GraphicsEvent>, generation: u64) {
    let mut initial = sender.clone();
    let sender = Mutex::new(sender);
    context.on_web_device_lost(move || {
        if let Ok(mut sender) = sender.lock() {
            sender.try_send(GraphicsEvent::DeviceLost(generation)).ok();
        }
    });
    if context.device_lost() {
        initial.try_send(GraphicsEvent::DeviceLost(generation)).ok();
    }
}

fn status(inner: &WebWindowInner, status: &str) {
    inner.graphics_ready.set(status == "ready");
    inner
        .canvas
        .set_attribute("data-gpui-graphics", status)
        .ok();
    if let Ok(event) = web_sys::Event::new("gpui-graphics-state") {
        inner.browser_window.dispatch_event(&event).ok();
    }
}

async fn timeout<T>(
    future: impl Future<Output = T>,
    executor: &BackgroundExecutor,
) -> anyhow::Result<T> {
    let future = future.fuse();
    let timeout = executor.timer(Duration::from_secs(20)).fuse();
    futures::pin_mut!(future, timeout);
    futures::select_biased! {
        result = future => Ok(result),
        _ = timeout => anyhow::bail!("Browser graphics recovery timed out"),
    }
}

impl Drop for GraphicsRecovery {
    fn drop(&mut self) {
        self.task.abort();
    }
}
