fn main() {
    let src_dir = std::path::Path::new("src");

    let mut c_config = cc::Build::new();
    c_config.std("c11").include(src_dir);

    #[cfg(target_env = "msvc")]
    c_config.flag("-utf-8");

    if std::env::var("TARGET").unwrap() == "wasm32-unknown-unknown" {
        let Ok(wasm_headers) = std::env::var("DEP_TREE_SITTER_LANGUAGE_WASM_HEADERS") else {
            panic!("Environment variable DEP_TREE_SITTER_LANGUAGE_WASM_HEADERS must be set by the language crate");
        };

        c_config.include(&wasm_headers);
        // Zed Codespaces: tree-sitter's freestanding wasm32 headers (the directory above)
        // declare `wchar_t` only in <wchar.h> and have no `static_assert` in <assert.h>;
        // scanner.c gets both from <stdlib.h>/<assert.h> on hosted libcs. Pre-include
        // the shim's <wchar.h> and spell the C11 keyword out for this target only.
        c_config.flag("-include").flag("wchar.h");
        c_config.define("static_assert", "_Static_assert");
    }

    let parser_path = src_dir.join("parser.c");
    c_config.file(&parser_path);
    println!("cargo:rerun-if-changed={}", parser_path.to_str().unwrap());

    let scanner_path = src_dir.join("scanner.c");
    c_config.file(&scanner_path);
    println!("cargo:rerun-if-changed={}", scanner_path.to_str().unwrap());

    c_config.compile("tree-sitter-cpp");
}
