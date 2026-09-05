#!/usr/bin/env bash
# Rewrites the wasm-bindgen (--target web) glue of the browser bundle in three ways
# (b7 §3.31 step 4b; carried from the reference fork, which needed it on wasm-bindgen 0.2.127):
#   1. the memory accessor falls back to `globalThis.__wbgSharedMemory`, and the shared
#      memory the glue allocates is stashed there for the worker bootstrap;
#   2. `globalThis.__zsCallCtors` runs `wasm.__wasm_call_ctors` exactly once (the threads
#      transform does not call static constructors; `inventory` registries need them);
#   3. the cached DataView accessors retry once on `RangeError` after shared memory grows.
# Every rewrite is guarded by a `grep -Fq` of its anchor: on glue that already handles the
# case the step logs and skips, so the script is a no-op rather than a failure.
set -euo pipefail
glue="${1:?usage: patch-wasm-bindgen-memory.sh <zed_web.js>}"
[[ -f "$glue" ]] || { echo "patch-wasm-bindgen-memory: $glue not found" >&2; exit 2; }

python3 - "$glue" <<'PY'
import re, sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
orig = src
log = lambda m: print(f"patch-wasm-bindgen-memory: {m}")

# 1. Shared-memory fallback: `memory = new WebAssembly.Memory({...shared: true})` → stash.
m = re.search(r"(\bmemory\s*=\s*)(new WebAssembly\.Memory\(\{[^}]*shared:\s*true[^}]*\}\))", src)
if m and "__wbgSharedMemory" not in src:
    src = src[:m.start()] + m.group(1) + "(globalThis.__wbgSharedMemory ??= " + m.group(2) + ")" + src[m.end():]
    log("shared-memory accessor: patched")
else:
    log("shared-memory accessor: no-op (anchor absent or already patched)")

# 2. One-shot ctor helper next to __wbindgen_start.
if "__zsCallCtors" not in src:
    anchor = re.search(r"\n(\s*)wasm\.__wbindgen_start\([^)]*\);", src)
    if anchor:
        indent = anchor.group(1)
        helper = (f"\n{indent}globalThis.__zsCallCtors = () => {{\n"
                  f"{indent}    if (globalThis.__zsCtorsRan) return;\n"
                  f"{indent}    globalThis.__zsCtorsRan = true;\n"
                  f"{indent}    if (typeof wasm.__wasm_call_ctors === 'function') wasm.__wasm_call_ctors();\n"
                  f"{indent}}};")
        src = src[:anchor.start()] + helper + src[anchor.start():]
        log("ctor helper: added")
    else:
        log("ctor helper: no-op (no __wbindgen_start anchor)")
else:
    log("ctor helper: no-op (already present)")

# 3. DataView retry after memory growth.
if "accessDataViewMemory0" not in src and "function getDataViewMemory0()" in src:
    src = src.replace(
        "function getDataViewMemory0() {",
        "function accessDataViewMemory0(fn) {\n"
        "    try { return fn(); } catch (e) {\n"
        "        if (!(e instanceof RangeError)) throw e;\n"
        "        cachedDataViewMemory0 = null;\n"
        "        return fn();\n"
        "    }\n"
        "}\n"
        "function getDataViewMemory0() {", 1)
    log("DataView retry: added")
else:
    log("DataView retry: no-op (anchor absent or already patched)")

if src != orig:
    open(path, "w", encoding="utf-8").write(src)
PY
