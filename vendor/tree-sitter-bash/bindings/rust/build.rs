fn main() {
    let src_dir = std::path::Path::new("src");

    let mut c_config = cc::Build::new();
    c_config
        .std("c11")
        .include(src_dir)
        .flag_if_supported("-Wno-unused-value");

    #[cfg(target_env = "msvc")]
    c_config.flag("-utf-8");

    if std::env::var("TARGET").unwrap() == "wasm32-unknown-unknown" {
        let Ok(wasm_headers) = std::env::var("DEP_TREE_SITTER_LANGUAGE_WASM_HEADERS") else {
            panic!("Environment variable DEP_TREE_SITTER_LANGUAGE_WASM_HEADERS must be set by the language crate");
        };

        c_config.include(&wasm_headers);

        // That header subset (the tree-sitter-language crate's `wasm/include`) has no
        // `isdigit`: its <ctype.h> declares only `isblank`/`isprint` and the runtime's
        // `lib/src/wasm-stdlib/imports.txt` provides none, because wasi-libc's own <ctype.h>
        // makes it an inline macro that never becomes an import. scanner.c calls it twice on
        // `lexer->lookahead` (an int32_t code point); `iswdigit` is declared in the subset's
        // <wctype.h>, is in the import set and gives the same answer for every code point,
        // so map the call for this target only. Native targets compile scanner.c untouched.
        c_config.define("isdigit", "iswdigit");
    }

    let parser_path = src_dir.join("parser.c");
    c_config.file(&parser_path);
    println!("cargo:rerun-if-changed={}", parser_path.to_str().unwrap());

    let scanner_path = src_dir.join("scanner.c");
    c_config.file(&scanner_path);
    println!("cargo:rerun-if-changed={}", scanner_path.to_str().unwrap());

    c_config.compile("tree-sitter-bash");
}
