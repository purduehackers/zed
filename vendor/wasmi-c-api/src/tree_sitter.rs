//! The three owned-reference/store operations Tree-sitter needs in addition to
//! Wasmi's standard C API. No pointer-layout casts or guest host capabilities.

use crate::{wasm_extern_t, wasm_func_t, wasm_ref_t, wasm_store_t};
use alloc::boxed::Box;
use wasmi::{Nullable, Ref};

/// The caller owns the returned reference and must use `wasm_ref_delete`.
#[unsafe(no_mangle)]
pub extern "C" fn ts_wasmi_func_to_ref(func: &wasm_func_t) -> Box<wasm_ref_t> {
    Box::new(wasm_ref_t {
        inner: Ref::Func(Nullable::Val(func.func())),
    })
}

/// Copies a function reference into an owned extern associated with its store.
#[unsafe(no_mangle)]
pub extern "C" fn ts_wasmi_ref_to_func(
    store: &wasm_store_t,
    value: &wasm_ref_t,
) -> Option<Box<wasm_extern_t>> {
    let Ref::Func(Nullable::Val(func)) = value.inner else {
        return None;
    };
    Some(Box::new(wasm_extern_t {
        store: store.inner.clone(),
        which: func.into(),
    }))
}

/// Bounds each guest initializer/lexer/scanner call, including infinite loops.
///
/// # Safety
/// Called only before entering Wasmi, never while a host callback has borrowed
/// its context. The store must belong to a fuel-enabled engine.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ts_wasmi_store_refuel(store: &mut wasm_store_t) -> bool {
    unsafe { store.inner.context_mut() }
        .set_fuel(5_000_000)
        .is_ok()
}
