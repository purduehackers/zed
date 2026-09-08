wasmi_c_api_impl 2.0.0 from crates.io, MIT/Apache-2.0.

`src/tree_sitter.rs` adds owned function-reference conversions (the standard C API
versions panic) and bounded-call fuel replenishment for Tree-sitter. The remaining
runtime is unmodified. Used only by the browser, without WASI or host I/O.
