//! Resolve browser clipboard access before replaying unchanged Zed paste actions.

use gpui::{Action, App, AppContext, ClipboardReadError, Window};
use gpui_web::clipboard;
use ui::InteractiveElement;
use wasm_bindgen::{JsCast, closure::Closure};
use workspace::{
    Workspace,
    notifications::{
        NotificationId, show_app_notification, simple_message_notification::MessageNotification,
    },
};

struct ClipboardError;

pub fn init(cx: &mut App) {
    let app = cx.to_async();
    clipboard::on_error(move |message| {
        app.update(|cx| {
            show_app_notification(NotificationId::unique::<ClipboardError>(), cx, move |cx| {
                cx.new(|cx| MessageNotification::new(message.clone(), cx))
            });
        });
    });
    let app = cx.to_async();
    clipboard::on_paste(move || {
        let target = app.update(|cx| {
            let handle = cx.active_window()?;
            let focus = handle
                .update(cx, |_, window, cx| window.focused(cx))
                .ok()
                .flatten()?;
            Some((handle, focus))
        });
        target.map(|(handle, focus)| {
            let app = app.clone();
            Box::new(move |item| {
                app.update(|cx| {
                    handle
                        .update(cx, |_, window, cx| {
                            if focus.is_focused(window) {
                                clipboard::with_read(item, || {
                                    focus.dispatch_action(&editor::actions::Paste, window, cx)
                                });
                            }
                        })
                        .ok();
                });
            }) as clipboard::PasteTarget
        })
    });
    cx.observe_new(|workspace: &mut Workspace, _, _| {
        workspace.register_action_renderer(|div, _, _, _| {
            div.capture_action(paste::<editor::actions::Paste>)
                .capture_action(paste::<terminal::Paste>)
                .capture_action(paste::<terminal::PasteText>)
                .capture_action(|action: &vim::WebPaste, window, cx| {
                    if vim::web_uses_system_clipboard(cx) {
                        paste(action, window, cx);
                    }
                })
        });
    })
    .detach();

    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return;
    };
    for (name, cut) in [("copy", false), ("cut", true)] {
        let app = cx.to_async();
        let document = document.clone();
        let target = document.clone();
        let listener = Closure::<dyn FnMut(web_sys::ClipboardEvent)>::new(
            move |event: web_sys::ClipboardEvent| {
                if !document
                    .active_element()
                    .is_some_and(|element| element.has_attribute("data-gpui-input"))
                {
                    return;
                }
                let Some(data) = event.clipboard_data() else {
                    return;
                };
                event.prevent_default();
                clipboard::with_copy_event(data, || {
                    app.update(|cx| {
                        if let Some(handle) = cx.active_window() {
                            handle
                                .update(cx, |_, window, cx| {
                                    if let Some(focus) = window.focused(cx) {
                                        let action: Box<dyn Action> = if cut {
                                            Box::new(editor::actions::Cut)
                                        } else {
                                            Box::new(editor::actions::Copy)
                                        };
                                        focus.dispatch_action(action.as_ref(), window, cx);
                                    }
                                })
                                .ok();
                        }
                    });
                });
            },
        );
        target
            .add_event_listener_with_callback(name, listener.as_ref().unchecked_ref())
            .expect("clipboard listener");
        // The browser entry point is single-shot and lives for the document lifetime.
        listener.forget();
    }
}

fn paste<A: Action>(action: &A, window: &mut Window, cx: &mut App) {
    if clipboard::read().is_some() {
        return;
    }
    cx.stop_propagation();
    if clipboard::request_native_paste() {
        return;
    }
    let Some(focus) = window.focused(cx) else {
        return;
    };
    let action = action.boxed_clone();
    // Start the permission-gated read now, while the gesture is still active.
    let read = cx.read_from_clipboard_async();
    window.spawn(cx, async move |cx| {
        match read.await {
            Ok(Some(item)) => {
                cx.update(|window, cx| {
                    // A delayed permission prompt must never paste into a different field.
                    if focus.is_focused(window) {
                        clipboard::with_read(item, || focus.dispatch_action(action.as_ref(), window, cx));
                    }
                }).ok();
            }
            Ok(None) => {},
            Err(error) => {
                let message = match error {
                    ClipboardReadError::Denied(_) | ClipboardReadError::Unavailable =>
                        "Paste was blocked by the browser. Allow clipboard access or use the browser's Edit → Paste command.".to_string(),
                    ClipboardReadError::UnsupportedContent => "This clipboard content cannot be pasted here.".to_string(),
                };
                clipboard::report_error(message);
            }
        }
    }).detach();
}
