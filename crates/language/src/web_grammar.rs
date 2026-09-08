//! Downloaded grammars retain the native parser/query API, but guest execution
//! uses a bounded interpreter. Never link extension code into the editor heap.

use crate::{GrammarHandle, ParseableLanguage, WASM_ENGINE};
use anyhow::{Result, ensure};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tree_sitter::{Language, WasmStore};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
type CachedGrammar = (Weak<()>, Language);
thread_local! {
    static GRAMMARS: RefCell<HashMap<usize, CachedGrammar>> = RefCell::new(HashMap::new());
}

pub(crate) fn load(name: String, bytes: Vec<u8>) -> Result<GrammarHandle> {
    ensure!(!name.contains('\0'), "invalid grammar name");
    ensure!(bytes.len() <= 64 * 1024 * 1024, "grammar exceeds 64 MiB");
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let lifetime = Arc::new(());
    let language = ParseableLanguage::from_resolver(Arc::new(move || {
        if let Some(language) =
            GRAMMARS.with_borrow(|cache| cache.get(&id).map(|(_, language)| language.clone()))
        {
            return Ok(language);
        }

        // A Language stays on the worker that created it. Its copied parse
        // tables outlive this loading store; each parser owns its own VM/store.
        let mut store = WasmStore::new(&WASM_ENGINE)?;
        let language = store.load_language(&name, &bytes)?;
        GRAMMARS.with_borrow_mut(|cache| {
            cache.retain(|_, (lifetime, _)| lifetime.strong_count() != 0);
            cache.insert(id, (Arc::downgrade(&lifetime), language.clone()));
        });
        Ok(language)
    }));
    // Report malformed modules before registering a usable grammar handle.
    language.resolve()?;
    Ok(language.into())
}
