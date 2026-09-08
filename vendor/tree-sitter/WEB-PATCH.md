Tree-sitter 43623ec9bf0eaaf7113285c46e8a09018f181b18 (0.27.0), MIT.

The browser uses Wasmi for downloaded grammars, preserving Tree-sitter's existing
isolated-memory loader, parser and query implementation. Native targets retain
Wasmtime. `binding_rust/wasmi/wasmtime.h` implements only the C interface used by
`wasm_store.c`; guest memory, function-table growth and per-call fuel are bounded.
The browser stdio patch supports the loader's sizing and bounded-string formats.
Cargo metadata is made standalone; grammar ABI and native parsing are unchanged.
