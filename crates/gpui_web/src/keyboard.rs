use gpui::PlatformKeyboardLayout;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
};
use wasm_bindgen::JsCast;

use crate::events::EventListenerHandle;

pub struct WebKeyboardLayout(String);

impl PlatformKeyboardLayout for WebKeyboardLayout {
    fn id(&self) -> &str {
        &self.0
    }

    fn name(&self) -> &str {
        "Browser keyboard"
    }
}

/// The browser exposes key legends, not an OS layout identifier or Cocoa's
/// localized menu equivalents. Keep binding keys logical; only recover the
/// unmodified key when macOS Option has replaced it with text such as π.
#[derive(Default)]
pub(crate) struct WebKeyboard {
    keys: RefCell<BTreeMap<String, String>>,
    observed: RefCell<BTreeMap<(String, bool), String>>,
    request: Cell<u64>,
    changed: RefCell<Option<Box<dyn FnMut()>>>,
    listeners: RefCell<Vec<EventListenerHandle>>,
}

impl WebKeyboard {
    pub(crate) fn new(window: &web_sys::Window) -> Rc<Self> {
        let this = Rc::new(Self::default());
        let keyboard = js_sys::Reflect::get(&window.navigator(), &"keyboard".into())
            .ok()
            .filter(|value| !value.is_null() && !value.is_undefined());
        if let Some(keyboard) = keyboard {
            // Shipped Chromium exposes getLayoutMap without Keyboard being an
            // EventTarget yet. Read the map independently of layoutchange.
            let mut targets = vec![(window.clone().into(), "focus")];
            if let Ok(keyboard) = keyboard.dyn_into::<web_sys::EventTarget>() {
                targets.push((keyboard, "layoutchange"));
            }
            for (target, event) in targets {
                let weak = Rc::downgrade(&this);
                this.listeners.borrow_mut().push(EventListenerHandle::add(
                    &target,
                    event,
                    move |_| {
                        if let Some(this) = weak.upgrade() {
                            this.observed.borrow_mut().clear();
                            if event == "layoutchange" {
                                this.keys.borrow_mut().clear();
                                this.notify();
                            }
                            this.refresh();
                        }
                    },
                ));
            }
            this.refresh();
        }
        this
    }

    fn refresh(self: &Rc<Self>) {
        let request = self.request.get().wrapping_add(1);
        self.request.set(request);
        let weak = Rc::downgrade(self);
        wasm_bindgen_futures::spawn_local(async move {
            // Optional API: Firefox/WebKit and denied requests still use actual
            // KeyboardEvent text, plus legends observed without Option held.
            let read = async {
                let navigator = web_sys::window().unwrap().navigator();
                let keyboard = js_sys::Reflect::get(&navigator, &"keyboard".into())?;
                let get = js_sys::Reflect::get(&keyboard, &"getLayoutMap".into())?
                    .dyn_into::<js_sys::Function>()?;
                let result = get.call0(&keyboard)?.dyn_into::<js_sys::Promise>()?;
                let map = wasm_bindgen_futures::JsFuture::from(result).await?;
                let mut keys = BTreeMap::new();
                if let Some(entries) = js_sys::try_iter(&map)? {
                    for entry in entries {
                        let pair = js_sys::Array::from(&entry?);
                        if let (Some(code), Some(key)) =
                            (pair.get(0).as_string(), pair.get(1).as_string())
                            && key.chars().count() == 1
                        {
                            keys.insert(code, key);
                        }
                    }
                }
                Ok::<_, wasm_bindgen::JsValue>(keys)
            };
            if let Ok(keys) = read.await
                && let Some(this) = weak.upgrade()
                && this.request.get() == request
            {
                let changed = *this.keys.borrow() != keys;
                if changed {
                    *this.keys.borrow_mut() = keys;
                    this.notify();
                }
            }
        });
    }

    pub(crate) fn observe(self: &Rc<Self>, event: &web_sys::KeyboardEvent) {
        if event.ctrl_key()
            || event.alt_key()
            || event.meta_key()
            || event.is_composing()
            || event.key_code() == 229
            || event.get_modifier_state("AltGraph")
            || event.get_modifier_state("CapsLock")
        {
            return;
        }
        let key = event.key();
        let code = event.code();
        if code.is_empty() || key.chars().count() != 1 {
            return;
        }
        let slot = (code, event.shift_key());
        let switched = self
            .observed
            .borrow()
            .get(&slot)
            .is_some_and(|old| old != &key);
        if switched {
            self.observed.borrow_mut().clear();
            self.keys.borrow_mut().clear();
            self.refresh();
            self.notify();
        }
        self.observed.borrow_mut().insert(slot, key);
    }

    pub(crate) fn option_key(&self, event: &web_sys::KeyboardEvent) -> Option<String> {
        let code = event.code();
        self.observed
            .borrow()
            .get(&(code.clone(), event.shift_key()))
            .cloned()
            .or_else(|| {
                self.keys
                    .borrow()
                    .get(&code)
                    .cloned()
                    .or_else(|| self.observed.borrow().get(&(code, false)).cloned())
                    // getLayoutMap supplies unshifted legends only. Letters
                    // keep a separate Shift modifier, but punctuation must be
                    // observed with Shift: guessing its shifted form can invoke
                    // the wrong binding on international layouts.
                    .filter(|key| {
                        !event.shift_key() || key.chars().all(|c| c.is_ascii_alphabetic())
                    })
            })
    }

    pub(crate) fn layout(&self) -> WebKeyboardLayout {
        let mut hash = DefaultHasher::new();
        self.keys.borrow().hash(&mut hash);
        WebKeyboardLayout(format!("web-{:016x}", hash.finish()))
    }

    pub(crate) fn on_change(&self, callback: Box<dyn FnMut()>) {
        *self.changed.borrow_mut() = Some(callback);
    }

    fn notify(self: &Rc<Self>) {
        // Observing a key can run inside GPUI dispatch. Notify on a later
        // microtask so reloading keybindings cannot re-enter App's borrow.
        let weak = Rc::downgrade(self);
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(this) = weak.upgrade() {
                let callback = this.changed.borrow_mut().take();
                if let Some(mut callback) = callback {
                    callback();
                    *this.changed.borrow_mut() = Some(callback);
                }
            }
        });
    }
}
