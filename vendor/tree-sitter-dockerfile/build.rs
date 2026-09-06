fn main() {
    let mut build = cc::Build::new();
    build.std("c11").include("src").warnings(false);
    if std::env::var("TARGET").unwrap() == "wasm32-unknown-unknown" {
        build.include(std::env::var("DEP_TREE_SITTER_LANGUAGE_WASM_HEADERS").unwrap());
    }
    for file in ["src/parser.c", "src/scanner.c"] {
        build.file(file);
        println!("cargo:rerun-if-changed={file}");
    }
    build.compile("tree-sitter-dockerfile");
}
