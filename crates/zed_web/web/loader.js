// Reference loader for the browser bundle (b7 §3.30; CONTRACTS.md §8.4). apps/web's editor
// route implements the same contract; this file drives the dev harness in index.html.
//
// Requirements this loader relies on:
//   - COOP same-origin / COEP require-corp on the document and every subresource, so
//     `crossOriginIsolated` is true (the bundle uses shared memory);
//   - `zed_web.js` served from a real same-origin URL with `Content-Type: text/javascript`:
//     the worker bootstrap of `wasm_thread` derives its import URL from that script;
//   - CSP `script-src 'self' 'wasm-unsafe-eval'; worker-src 'self' blob:` (the vendored
//     `wasm_thread` needs no `'unsafe-eval'`; script/build-web --serve sends this CSP).

const stageEl = document.getElementById("stage");
const detailEl = document.getElementById("detail");
const bootEl = document.getElementById("boot");

// The session token never stays in the URL: a `token=` query parameter is moved into
// sessionStorage and the URL rewritten without it (history and Referer never see it;
// the page also carries a no-referrer policy).
const TOKEN_KEY = "zs.token";
function takeTokenFromUrl() {
  const url = new URL(location.href);
  const token = url.searchParams.get("token");
  if (token !== null) {
    try {
      sessionStorage.setItem(TOKEN_KEY, token);
    } catch (error) {
      console.warn("[zed-web] sessionStorage unavailable; the token stays in memory only", error);
    }
    url.searchParams.delete("token");
    history.replaceState(history.state, "", url.toString());
  }
  return token;
}
const tokenFromUrl = takeTokenFromUrl();
function sessionToken() {
  if (tokenFromUrl !== null) return tokenFromUrl;
  try {
    return sessionStorage.getItem(TOKEN_KEY) ?? "";
  } catch {
    return "";
  }
}

function showStage(stage, detail) {
  if (stageEl) stageEl.textContent = stage;
  if (detailEl) detailEl.textContent = detail ?? "";
}

function showError(message) {
  if (stageEl) {
    stageEl.textContent = "failed";
    stageEl.className = "error";
  }
  if (detailEl) detailEl.textContent = message;
  if (bootEl) bootEl.hidden = false;
}

if (!globalThis.crossOriginIsolated) {
  showError(
    "This page is not cross-origin isolated (COOP/COEP headers missing), so the shared-memory build cannot run.",
  );
  throw new Error("crossOriginIsolated is false");
}

const params = new URLSearchParams(location.search);

/** The boot configuration of CONTRACTS.md §8.4, assembled from the query string and localStorage. */
function bootConfig(buildId) {
  const paths = params.getAll("path");
  return {
    buildId,
    connect: {
      wsUrl: params.get("ws") ?? "",
      token: sessionToken(),
      sessionId: params.get("session") ?? "",
    },
    workspace: { id: params.get("workspace") ?? "dev", paths },
    settingsJson: localStorage.getItem("zs.settings") ?? "",
    keymapJson: localStorage.getItem("zs.keymap") ?? "",
    backend: params.get("backend") ?? "auto",
    hostOs: params.get("os") ?? undefined,
  };
}

/** The `ZsHost` object: the dev harness's minimal implementation. */
const host = {
  bootProgress(stage, detail) {
    console.info("[zed-web]", stage, detail);
    showStage(stage, detail);
    if (stage === "ready" && bootEl) bootEl.hidden = true;
    if (stage === "stopped" || stage === "failed") showError(`${stage}: ${detail}`);
    if (stage === "reconnecting" && bootEl) bootEl.hidden = false;
  },
  async refreshConnectInfo() {
    // The dev harness has no control plane: reuse the query-string session. A real shell
    // re-POSTs /connect and rejects with { code: "unauthorized" | "stopped" | "unavailable" }.
    const { connect } = bootConfig("dev");
    if (!connect.wsUrl || !connect.token) {
      throw { code: "unavailable", message: "no connect info in the query string" };
    }
    return connect;
  },
  async saveDocument(kind, json) {
    localStorage.setItem(kind === "keymap" ? "zs.keymap" : "zs.settings", json);
  },
  reportError(kind, message, stack) {
    console.error("[zed-web]", kind, message, stack);
  },
  onLifecycle(kind, seconds) {
    console.info("[zed-web] lifecycle", kind, seconds);
  },
  onClosed(info) {
    console.info("[zed-web] closed", info.code, info.reason);
  },
};

async function main() {
  showStage("fetching editor");
  // The worker bootstrap reads this before it falls back to the stack-trace trick.
  globalThis.__zsBindgenShimUrl = new URL("./zed_web.js", import.meta.url).href;

  const [module, assets] = await Promise.all([
    import("./zed_web.js"),
    fetch("./zed-assets.tar").then(async (response) => {
      if (!response.ok) throw new Error(`zed-assets.tar: HTTP ${response.status}`);
      return new Uint8Array(await response.arrayBuffer());
    }),
  ]);
  const { default: init, start, flush_client_state, set_hidden, has_unsaved_changes, build_id } = module;

  // Memory is created by the glue from the module's import limits (128 MiB initial, 4 GiB
  // max from .cargo/config.toml); `thread_stack_size` stays at the glue default.
  const wasm = await init({ module_or_path: new URL("./zed_web_bg.wasm", import.meta.url) });
  // Static constructors (inventory registries) once, before start(); the threads transform
  // does not run them itself. The glue patch adds __zsCallCtors; the export is the fallback.
  if (typeof globalThis.__zsCallCtors === "function") globalThis.__zsCallCtors();
  else if (typeof wasm.__wasm_call_ctors === "function") wasm.__wasm_call_ctors();

  const buildId = build_id();
  console.info("[zed-web] build", buildId);

  document.addEventListener("visibilitychange", () => {
    set_hidden(document.hidden);
    if (document.hidden) flush_client_state().catch(() => {});
  });
  // Best-effort only: the executor may not tick again (the hidden flush is authoritative).
  window.addEventListener("pagehide", () => {
    flush_client_state().catch(() => {});
  });
  window.addEventListener("beforeunload", (event) => {
    if (has_unsaved_changes()) event.preventDefault();
  });
  document.addEventListener("fullscreenchange", () => {
    const keyboard = navigator.keyboard;
    if (!keyboard || typeof keyboard.lock !== "function") return;
    if (document.fullscreenElement) keyboard.lock(["KeyW", "KeyT", "KeyN", "KeyQ", "Tab"]).catch(() => {});
    else keyboard.unlock();
  });

  try {
    await start(JSON.stringify(bootConfig(buildId)), assets, host);
  } catch (error) {
    // { code, message }: session_busy → reload with takeover; unauthorized → sign in;
    // incompatible_server → reload; workspace_stopped/server_stopping → stopped state;
    // ctors_missing → call the ctor export and reload once.
    const code = error && typeof error === "object" && "code" in error ? error.code : "failed";
    const message = error && typeof error === "object" && "message" in error ? error.message : String(error);
    showError(`${code}: ${message}`);
    throw error;
  }
}

main().catch((error) => console.error("[zed-web] boot failed", error));
