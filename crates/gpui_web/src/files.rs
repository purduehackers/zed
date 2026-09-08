//! Native platform file prompts and drops, backed by the application's virtual Fs.

use std::{cell::RefCell, collections::BTreeSet, path::PathBuf, rc::Rc};

use anyhow::{Context as _, Result};
use futures::channel::oneshot;
use gpui::PathPromptOptions;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(module = "/src/files.js")]
extern "C" {
    #[wasm_bindgen(js_name = droppedFiles)]
    fn dropped_files(data: &web_sys::DataTransfer) -> js_sys::Promise;
    #[wasm_bindgen(js_name = pickFiles)]
    fn pick_files(files: bool, directories: bool, multiple: bool) -> js_sys::Promise;
    #[wasm_bindgen(js_name = pickSave)]
    fn pick_save(name: &str) -> js_sys::Promise;
    #[wasm_bindgen(js_name = saveFile)]
    fn save_file(path: &str, bytes: &js_sys::Uint8Array) -> js_sys::Promise;
}

type Import = Rc<dyn Fn(Vec<(PathBuf, Option<Vec<u8>>)>) -> Result<()>>;
type ErrorHandler = Rc<dyn Fn(String)>;
thread_local! {
    static IMPORT: RefCell<Option<Import>> = const { RefCell::new(None) };
    static ERROR: RefCell<Option<ErrorHandler>> = const { RefCell::new(None) };
}

/// Install on the browser main thread, before opening the window.
pub fn init(
    import: impl Fn(Vec<(PathBuf, Option<Vec<u8>>)>) -> Result<()> + 'static,
    error: impl Fn(String) + 'static,
) {
    IMPORT.with(|slot| *slot.borrow_mut() = Some(Rc::new(import)));
    ERROR.with(|slot| *slot.borrow_mut() = Some(Rc::new(error)));
}

pub(crate) fn report_error(error: anyhow::Error) {
    log::error!("Browser file transfer failed: {error:#}");
    if let Some(handler) = ERROR.with(|slot| slot.borrow().clone()) {
        handler(format!("Could not import files: {error:#}"));
    }
}

async fn import(promise: js_sys::Promise) -> Result<Option<Vec<PathBuf>>> {
    let selected = JsFuture::from(promise).await.map_err(js_error)?;
    if selected.is_null() {
        return Ok(None);
    }
    let mut entries = Vec::new();
    let mut roots = BTreeSet::new();
    for entry in js_sys::Array::from(&selected) {
        let entry = js_sys::Array::from(&entry);
        let path = PathBuf::from(entry.get(0).as_string().context("Missing imported path")?);
        let relative = path.strip_prefix("/browser/imports")?;
        let mut parts = relative.components();
        let batch = parts.next().context("Missing import id")?;
        let root = parts.next().context("Missing imported file name")?;
        roots.insert(PathBuf::from("/browser/imports").join(batch).join(root));
        let bytes = entry.get(1);
        entries.push((
            path,
            (!bytes.is_null()).then(|| js_sys::Uint8Array::new(&bytes).to_vec()),
        ));
    }
    IMPORT
        .with(|slot| slot.borrow().clone())
        .context("Browser filesystem is not initialized")?(entries)?;
    Ok(Some(roots.into_iter().collect()))
}

pub(crate) fn drop_files(
    data: &web_sys::DataTransfer,
) -> impl Future<Output = Result<Option<Vec<PathBuf>>>> + use<> {
    let promise = dropped_files(data);
    import(promise)
}

pub(crate) fn prompt_for_paths(
    options: PathPromptOptions,
) -> oneshot::Receiver<Result<Option<Vec<PathBuf>>>> {
    let promise = pick_files(options.files, options.directories, options.multiple);
    let (sender, receiver) = oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        sender.send(import(promise).await).ok();
    });
    receiver
}

pub(crate) fn prompt_for_new_path(
    name: Option<&str>,
) -> oneshot::Receiver<Result<Option<PathBuf>>> {
    let promise = pick_save(name.unwrap_or("untitled.txt"));
    let (sender, receiver) = oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        sender
            .send(
                JsFuture::from(promise)
                    .await
                    .map_err(js_error)
                    .map(|path| path.as_string().map(PathBuf::from)),
            )
            .ok();
    });
    receiver
}

/// Only called on the main thread. Resolve after the browser has closed the writable
/// stream, so native Save keeps the buffer dirty when permission or writing fails.
pub async fn write(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let data = js_sys::Uint8Array::from(bytes);
    JsFuture::from(save_file(&path.to_string_lossy(), &data))
        .await
        .map_err(js_error)?;
    Ok(())
}

fn js_error(value: JsValue) -> anyhow::Error {
    anyhow::anyhow!(crate::platform::js_error_message(&value))
}
