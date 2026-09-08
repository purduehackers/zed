//! The browser's `AssetSource`: the fetched asset pack (fonts, icons, images, themes, sounds;
//! BUILD-SPEC 3.5) layered over the wasm-embedded `assets::Assets` (prompts, `*.md`).

use std::{
    borrow::Cow,
    path::Path,
    sync::{Arc, OnceLock},
};

use assets::Assets;
use gpui::{App, AssetSource, SharedString};
use zed_web_core::AssetPack;

/// Pack-backed asset source; cheap to clone (the pack is shared).
#[derive(Clone)]
pub struct WebAssets {
    pack: Arc<AssetPack>,
    fs: Arc<OnceLock<Arc<fs::WasmFs>>>,
}

impl WebAssets {
    /// Wraps a parsed pack.
    pub fn new(pack: AssetPack) -> Self {
        Self {
            pack: Arc::new(pack),
            fs: Arc::default(),
        }
    }

    pub fn set_fs(&self, fs: Arc<fs::WasmFs>) {
        self.fs.set(fs).ok();
    }

    /// Registers every `fonts/**/*.ttf` of the pack with the text system. The browser
    /// replacement for `assets::Assets::load_fonts`, which only enumerates the embedded set
    /// (empty on wasm); the web text system has no system fonts, so this must precede the
    /// first frame.
    pub fn load_fonts(&self, cx: &App) -> anyhow::Result<()> {
        let fonts: Vec<Cow<'static, [u8]>> = self
            .pack
            .list("fonts/")
            .into_iter()
            .filter(|path| path.ends_with(".ttf"))
            .filter_map(|path| self.pack.get(&path).map(|bytes| Cow::Owned(bytes.to_vec())))
            .collect();
        anyhow::ensure!(!fonts.is_empty(), "the asset pack carries no fonts");
        cx.text_system().add_fonts(fonts)
    }

    /// Number of files in the pack.
    pub fn len(&self) -> usize {
        self.pack.len()
    }
}

impl AssetSource for WebAssets {
    fn load(&self, path: &str) -> anyhow::Result<Option<Cow<'static, [u8]>>> {
        if path.starts_with("/extensions/browser/") {
            return self
                .fs
                .get()
                .map(|fs| fs.read_bytes(Path::new(path)).map(Cow::Owned))
                .transpose();
        }
        if let Some(bytes) = self.pack.get(path) {
            return Ok(Some(Cow::Owned(bytes.to_vec())));
        }
        Assets.load(path)
    }

    fn list(&self, path: &str) -> anyhow::Result<Vec<SharedString>> {
        let mut entries: Vec<SharedString> = self
            .pack
            .list(path)
            .into_iter()
            .map(SharedString::from)
            .collect();
        entries.extend(Assets.list(path)?);
        entries.sort();
        entries.dedup();
        Ok(entries)
    }
}
