fn main() {
    let block_dir = std::path::Path::new("tree-sitter-markdown").join("src");
    let inline_dir = std::path::Path::new("tree-sitter-markdown-inline").join("src");

    let mut c_config = cc::Build::new();
    c_config.std("c11").include(&block_dir);

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
        // makes it an inline macro that never becomes an import. tree-sitter-markdown's
        // scanner.c calls it once on `lexer->lookahead` (an int32_t code point); `iswdigit`
        // is declared in the subset's <wctype.h> (scanner.c includes it), is in the import
        // set and gives the same answer for every code point, so map the call for this
        // target only. Native targets compile both scanners untouched.
        c_config.define("isdigit", "iswdigit");
    }

    for path in &[
        block_dir.join("parser.c"),
        block_dir.join("scanner.c"),
        inline_dir.join("parser.c"),
        inline_dir.join("scanner.c"),
    ] {
        c_config.file(path);
        println!("cargo:rerun-if-changed={}", path.to_str().unwrap());
    }

    c_config.compile("tree-sitter-markdown");
}
