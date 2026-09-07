//! Browser clipboard operations. Synchronous reads are scoped to an observed paste,
//! never a stale mirror of another application's system clipboard.

use std::cell::{Cell, RefCell};

use gpui::{ClipboardEntry, ClipboardItem, ImageFormat};
use wasm_bindgen::JsValue;

thread_local! {
    static READING: RefCell<Option<ClipboardItem>> = const { RefCell::new(None) };
    static COPYING: RefCell<Option<web_sys::DataTransfer>> = const { RefCell::new(None) };
    static LAST_COPY: RefCell<Option<ClipboardItem>> = const { RefCell::new(None) };
    static ERROR_HANDLER: RefCell<Option<Box<dyn FnMut(String)>>> = RefCell::new(None);
    static PASTE_TARGET: RefCell<Option<Box<dyn FnMut() -> Option<PasteTarget>>>> = RefCell::new(None);
    static NATIVE_KEY: Cell<bool> = const { Cell::new(false) };
    static NATIVE_REQUESTED: Cell<bool> = const { Cell::new(false) };
}

pub type PasteTarget = Box<dyn FnOnce(ClipboardItem)>;

pub fn on_paste(handler: impl FnMut() -> Option<PasteTarget> + 'static) {
    PASTE_TARGET.with(|slot| *slot.borrow_mut() = Some(Box::new(handler)));
}

pub(crate) fn paste_target() -> Option<PasteTarget> {
    PASTE_TARGET.with(|slot| slot.borrow_mut().as_mut().and_then(|handler| handler()))
}

pub(crate) fn begin_key(native_paste: bool) {
    NATIVE_KEY.set(native_paste);
    NATIVE_REQUESTED.set(false);
}

pub fn request_native_paste() -> bool {
    let native = NATIVE_KEY.get();
    if native {
        NATIVE_REQUESTED.set(true);
    }
    native
}

pub(crate) fn end_key() -> bool {
    NATIVE_KEY.set(false);
    NATIVE_REQUESTED.replace(false)
}

pub fn on_error(handler: impl FnMut(String) + 'static) {
    ERROR_HANDLER.with(|slot| *slot.borrow_mut() = Some(Box::new(handler)));
}

pub fn report_error(message: String) {
    log::warn!("{message}");
    // A synchronous platform write may be inside a GPUI App borrow.
    wasm_bindgen_futures::spawn_local(async move {
        ERROR_HANDLER.with(|slot| {
            if let Some(handler) = slot.borrow_mut().as_mut() {
                handler(message);
            }
        });
    });
}

pub fn read() -> Option<ClipboardItem> {
    READING.with(|slot| slot.borrow().clone())
}

pub fn with_read<T>(item: ClipboardItem, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<ClipboardItem>);
    impl Drop for Restore {
        fn drop(&mut self) {
            READING.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(READING.with(|slot| slot.replace(Some(item))));
    f()
}

pub fn with_copy_event<T>(data: web_sys::DataTransfer, f: impl FnOnce() -> T) -> T {
    let previous = COPYING.with(|slot| slot.replace(Some(data)));
    let result = f();
    COPYING.with(|slot| *slot.borrow_mut() = previous);
    result
}

/// Retain local multi-cursor/linewise metadata only when the real clipboard text
/// still matches. Never deserialize untrusted custom metadata into editor offsets.
pub(crate) fn restore_metadata(mut item: ClipboardItem) -> ClipboardItem {
    LAST_COPY.with(|slot| {
        if let Some(copied) = slot.borrow().as_ref()
            && item
                .entries
                .iter()
                .all(|entry| matches!(entry, ClipboardEntry::String(_)))
            && copied
                .entries
                .iter()
                .all(|entry| matches!(entry, ClipboardEntry::String(_)))
            && item.text().is_some()
            && item.text() == copied.text()
        {
            item = copied.clone();
        }
    });
    item
}

pub(crate) fn write(item: ClipboardItem) {
    let text = item.text();
    let event = COPYING.with(|slot| slot.borrow().clone());
    if let Some(data) = event {
        if let Some(text) = text {
            match data.set_data("text/plain", &text) {
                Ok(()) => LAST_COPY.with(|slot| *slot.borrow_mut() = Some(item)),
                Err(error) => report_error(format!(
                    "Copy failed: {}",
                    crate::platform::js_error_message(&error)
                )),
            }
        }
        return;
    }

    let Some(window) = web_sys::window() else {
        return;
    };
    let navigator = window.navigator();
    if !js_sys::Reflect::get(navigator.as_ref(), &"clipboard".into())
        .is_ok_and(|value| !value.is_null() && !value.is_undefined())
    {
        report_error("Clipboard unavailable. Use a secure browser tab.".into());
        return;
    }

    let image = item.entries.iter().find_map(|entry| match entry {
        ClipboardEntry::Image(image) if !image.bytes.is_empty() => Some(image.clone()),
        _ => None,
    });
    let write = if let Some(image) = image {
        let data = js_sys::Object::new();
        // Promise-valued ClipboardItems keep write() inside the user gesture,
        // even when an image needs conversion to the interoperable PNG format.
        let png = wasm_bindgen_futures::future_to_promise(async move {
            let bytes = if image.format == ImageFormat::Png {
                image.bytes
            } else {
                let decoded = image::load_from_memory(&image.bytes)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                let mut output = std::io::Cursor::new(Vec::new());
                decoded
                    .write_to(&mut output, image::ImageFormat::Png)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                output.into_inner()
            };
            let parts = js_sys::Array::of1(&js_sys::Uint8Array::from(bytes.as_slice()));
            let options = web_sys::BlobPropertyBag::new();
            options.set_type("image/png");
            web_sys::Blob::new_with_u8_array_sequence_and_options(&parts, &options)
                .map(JsValue::from)
        });
        js_sys::Reflect::set(&data, &"image/png".into(), &png).ok();
        if let Some(text) = text {
            let parts = js_sys::Array::of1(&JsValue::from_str(&text));
            let options = web_sys::BlobPropertyBag::new();
            options.set_type("text/plain");
            if let Ok(blob) = web_sys::Blob::new_with_str_sequence_and_options(&parts, &options) {
                js_sys::Reflect::set(
                    &data,
                    &"text/plain".into(),
                    &js_sys::Promise::resolve(&blob),
                )
                .ok();
            }
        }
        match web_sys::ClipboardItem::new_with_record_from_str_to_blob_promise(&data) {
            Ok(value) => navigator.clipboard().write(&js_sys::Array::of1(&value)),
            Err(error) => {
                report_error(format!(
                    "Copy failed: {}",
                    crate::platform::js_error_message(&error)
                ));
                return;
            }
        }
    } else if let Some(text) = text {
        navigator.clipboard().write_text(&text)
    } else {
        return;
    };

    wasm_bindgen_futures::spawn_local(async move {
        match wasm_bindgen_futures::JsFuture::from(write).await {
            Ok(_) => LAST_COPY.with(|slot| *slot.borrow_mut() = Some(item)),
            Err(error) => report_error(format!(
                "Copy failed: {}. Check this site's clipboard permission and try again.",
                crate::platform::js_error_message(&error)
            )),
        }
    });
}
