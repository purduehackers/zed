//! The registry, store and panel initialization of `crates/zed/src/main.rs`, in the desktop
//! order minus the crates the browser excludes (the updater, extension host, collaboration,
//! audio, feedback, onboarding, the profiler). Items skipped on purpose are noted inline so
//! the sequence can be diffed against `main.rs`.

use std::sync::Arc;

use anyhow::Result;
use client::{Client, RefreshLlmTokenListener, UserStore};
use extension::ExtensionHostProxy;
use fs::Fs;
use gpui::{App, AppContext as _};
use language::LanguageRegistry;
use language_extension::LspAccess;
use node_runtime::NodeRuntime;
use project::{Project, trusted_worktrees};
use prompt_store::PromptBuilder;
use release_channel::{AppCommitSha, AppVersion};
use session::{AppSession, Session};
use settings::{Settings as _, SettingsStore};
use theme::{ActiveTheme as _, GlobalTheme, LoadThemes, ThemeRegistry};
use workspace::{AppState, WorkspaceDb, WorkspaceSettings, WorkspaceStore};
use zed_web_core::HostOs;

use crate::assets::WebAssets;
use crate::{ai, keymap, web_edit_prediction, web_settings, window, workspace_chrome};

/// Compile-time identity of this bundle.
pub struct BuildInfo {
    /// `ZS_BUILD_ID` (`<zed-commit>-<patch>`) or `"dev"`.
    pub id: &'static str,
    /// The checkout's commit, when the build script could read it.
    pub commit_sha: Option<AppCommitSha>,
}

fn platform_style(host_os: HostOs) -> ui::PlatformStyle {
    match host_os {
        HostOs::Mac => ui::PlatformStyle::Mac,
        HostOs::Windows => ui::PlatformStyle::Windows,
        HostOs::Linux => ui::PlatformStyle::Linux,
    }
}

/// Everything that must exist before the transport dials: fonts, the platform style, the
/// settings store and keymaps, the filesystem, the extension proxy, the AI-proxy
/// credentials provider and the `Client`. `origin` is the control plane's
/// (`scheme://host[:port]`): the seeded `api_url`s and the keys routes hang off it.
/// Returns the client and the proxy; the caller then connects and opens the database,
/// because `trusted_worktrees::init` and `workspace::init` need the `AppDatabase`.
pub fn init_before_connect(
    fs: Arc<dyn Fs>,
    assets: &WebAssets,
    host_os: HostOs,
    settings_json: &str,
    origin: &str,
    build: &BuildInfo,
    cx: &mut App,
) -> Result<(Arc<Client>, Arc<ExtensionHostProxy>)> {
    // 1. Fonts before the first frame: the web text system has no system fonts.
    assets.load_fonts(cx)?;

    // 2. The host's platform style (D12) before any window or key-binding element exists.
    ui::PlatformStyle::set_platform_style(platform_style(host_os));
    menu::init();
    zed_actions::init();

    // 3. Version and build identity; `gpui_tokio::init` is desktop-only.
    let app_version = AppVersion::load(
        env!("ZED_PKG_VERSION"),
        Some(build.id),
        build.commit_sha.clone(),
    );
    release_channel::init(app_version, cx);
    if let Some(commit_sha) = &build.commit_sha {
        AppCommitSha::set_global(commit_sha.clone(), cx);
    }

    // 4. Settings from the shell document, over defaults that point every bring-your-own-key
    //    model provider at `<origin>/api/ai/<provider>` (b11); `zlog_settings::init` is
    //    desktop-only.
    web_settings::init(fs.clone(), settings_json, origin, cx);

    // 5. Keymaps, watching the user keymap in the WasmFs.
    let (user_keymap_file_rx, user_keymap_watcher) = settings::watch_config_file(
        cx.background_executor(),
        fs.clone(),
        paths::keymap_file().clone(),
    );
    keymap::install(host_os, user_keymap_file_rx, user_keymap_watcher, cx)?;
    ui::on_new_scrollbars::<SettingsStore>(cx);

    // 6. The filesystem.
    <dyn Fs>::set_global(fs.clone(), cx);

    // 7. Git hosting providers.
    git::GitHostingProviderRegistry::set_global(
        Arc::new(git::GitHostingProviderRegistry::new()),
        cx,
    );
    git_hosting_providers::init(cx);

    // 8. The extension proxy (extensions themselves run in the sandbox, BUILD-SPEC 9).
    extension::init(cx);
    let extension_host_proxy = ExtensionHostProxy::global(cx);

    // 8a. The AI-proxy credentials provider (the `zed_credentials_provider` global) and the
    //     notice-raising HTTP client, before `Client::production` captures both (b11 §3.18).
    #[cfg_attr(not(feature = "test-hooks"), allow(unused_variables))]
    let ai_credentials = ai::install(origin, cx);
    #[cfg(feature = "test-hooks")]
    crate::test_hooks::record_ai_credentials(origin, ai_credentials);

    // 9. The collaboration client, transport-less in the browser: the base HTTP client is
    //    the `fetch`-backed one from `application_with_web_backend` (wrapped in 8a).
    //    Telemetry, ids and `authenticate` are skipped (telemetry is also off in the web
    //    defaults).
    let client = Client::production(cx);
    cx.set_http_client(client.http_client());
    Client::set_global(client.clone(), cx);

    // 10. Project and client globals.
    Project::init(&client, cx);
    client::init(&client, cx);
    feature_flags::FeatureFlagStore::init(cx);

    Ok((client, extension_host_proxy))
}

/// Everything after the client-state database is open: registries, stores, panels, actions
/// and the workspace chrome. `origin` is the same control-plane origin as in
/// [`init_before_connect`] (the proxied edit-prediction endpoints hang off it). Returns the
/// `AppState`.
pub fn init_after_db(
    client: Arc<Client>,
    fs: Arc<dyn Fs>,
    assets: WebAssets,
    extension_host_proxy: Arc<ExtensionHostProxy>,
    session: Session,
    origin: &str,
    cx: &mut App,
) -> Arc<AppState> {
    // 11. Trusted worktrees from the restored image.
    let db_trusted_paths = WorkspaceDb::global(cx)
        .fetch_trusted_worktrees()
        .unwrap_or_default();
    trusted_worktrees::init(db_trusted_paths, cx);

    // 12. Languages.
    let mut languages = LanguageRegistry::new(cx.background_executor().clone());
    languages.set_language_server_download_dir(paths::languages_dir().clone());
    let languages = Arc::new(languages);

    // 13. No local node: language servers run in the sandbox.
    let node_runtime = NodeRuntime::unavailable();

    // 14. Debug adapters and built-in languages.
    debug_adapter_extension::init(extension_host_proxy.clone(), cx);
    languages::init(languages.clone(), fs.clone(), node_runtime.clone(), cx);

    // 15. Stores and the LSP access for extension-provided language servers.
    let user_store = cx.new(|cx| UserStore::new(client.clone(), cx));
    let workspace_store = cx.new(|cx| WorkspaceStore::new(client.clone(), cx));
    language_extension::init(
        LspAccess::ViaWorkspaces({
            let workspace_store = workspace_store.clone();
            Arc::new(move |cx: &mut App| {
                workspace_store.update(cx, |workspace_store, cx| {
                    Ok(workspace_store
                        .workspaces()
                        .filter_map(|weak| weak.upgrade())
                        .map(|workspace: gpui::Entity<workspace::Workspace>| {
                            workspace.read(cx).project().read(cx).lsp_store()
                        })
                        .collect())
                })
            })
        }),
        extension_host_proxy.clone(),
        languages.clone(),
    );

    // 16. Debugger.
    debugger_ui::init(cx);
    debugger_tools::init(cx);

    // 17. The app state.
    let app_session = cx.new(|cx| AppSession::new(session, cx));
    let app_state = Arc::new(AppState {
        languages: languages.clone(),
        client: client.clone(),
        user_store: user_store.clone(),
        fs: fs.clone(),
        build_window_options: window::build_window_options,
        workspace_store,
        node_runtime,
        session: app_session,
    });
    AppState::set_global(app_state.clone(), cx);

    // 18. Skipped: `auto_update::init`, `auto_update_ui::init`, `reliability::init`,
    //     `extension_host::init` (desktop-only).
    dap_adapters::init(cx);

    // 19. Themes from the asset pack. `eager_load_active_theme_and_icon_theme` needs the
    //     extension store and `block_on`; the registry loads the pack's themes here instead.
    theme_settings::init(LoadThemes::All(Box::new(assets.clone())), cx);
    theme_extension::init(
        extension_host_proxy.clone(),
        ThemeRegistry::global(cx),
        cx.background_executor().clone(),
    );

    // 20.
    command_palette::init(cx);

    // 21. Copilot is in the closure through `language_models`, `settings_ui`,
    //     `edit_prediction` and `edit_prediction_ui` regardless; BUILD-SPEC 8 routes it
    //     through the sandbox language server.
    let copilot_chat_configuration = copilot_chat::CopilotChatConfiguration {
        enterprise_uri: language::language_settings::all_language_settings(None, cx)
            .edit_predictions
            .copilot
            .enterprise_uri
            .clone(),
    };
    copilot_chat::init(
        app_state.client.http_client(),
        zed_credentials_provider::global(cx),
        copilot_chat_configuration,
        cx,
    );
    copilot_ui::init(&app_state, cx);

    // 22. Language models: the API-key providers, whose `api_url`s were seeded to the proxy
    //     in step 4 and whose keys the step-8a provider answers. Bedrock, the ChatGPT
    //     subscription, extension providers and the `localhost` providers (Ollama, LM Studio,
    //     llama.cpp — unreachable under the editor CSP) are gated out in `language_models`.
    language_model::init(cx);
    RefreshLlmTokenListener::register(app_state.client.clone(), app_state.user_store.clone(), cx);
    language_models::init(app_state.user_store.clone(), app_state.client.clone(), cx);
    // 22a. An OpenCode model whose `custom_model_api_url` leaves the proxy is blocked by the
    //      editor CSP; say so once when such a model is selected (b11 §7 item 4b).
    ai::watch_default_model(origin, cx);

    // 23. Tools. `zed::telemetry_log` and `zed::remote_debug` are `crates/zed` modules
    //     (follow-up); `zed::edit_prediction_registry` has its browser form at step 30a.
    acp_tools::init(cx);
    edit_prediction_ui::init(cx);
    web_search::init(cx);
    web_search_providers::init(app_state.client.clone(), app_state.user_store.clone(), cx);
    snippet_provider::init(cx);

    // 24. Agent.
    let prompt_builder = PromptBuilder::load(app_state.fs.clone(), false, cx);
    project::AgentRegistryStore::init_global(
        cx,
        app_state.fs.clone(),
        app_state.client.http_client(),
    );
    agent_ui::init(
        app_state.fs.clone(),
        prompt_builder,
        app_state.languages.clone(),
        false,
        false,
        cx,
    );
    agent_settings::init_user_agents_md(app_state.fs.clone(), cx, |_, _| {});

    // 25. `dev_container::init` is desktop-only. `repl::init` waits for its tokio and
    //     aws-lc-rs edges (`jupyter-websocket-client`, `runtimelib`) to be gated for wasm.
    recent_projects::init(cx);

    // 26. Editor and viewers; `audio::init` is desktop-only.
    editor::init(cx);
    image_viewer::init(cx);
    diagnostics::init(cx);

    // 27. Workspace.
    workspace::init(app_state.clone(), cx);
    ui_prompt::init(cx);

    // 28. Navigation and panels.
    go_to_line::init(cx);
    file_finder::init(cx);
    tab_switcher::init(cx);
    outline::init(cx);
    call_hierarchy::init(cx);
    project_symbols::init(cx);
    project_panel::init(cx);
    outline_panel::init(cx);
    tasks_ui::init(cx);
    snippets_ui::init(cx);
    channel::init(&app_state.client, app_state.user_store.clone(), cx);
    search::init(cx);
    lsp_locations::init(cx);
    cx.set_global(workspace::PaneSearchBarCallbacks {
        setup_search_bar: |languages, toolbar, window, cx| {
            let search_bar = cx.new(|cx| search::BufferSearchBar::new(languages, window, cx));
            toolbar.update(cx, |toolbar, cx| {
                toolbar.add_item(search_bar, window, cx);
            });
        },
        wrap_div_with_search_actions: search::buffer_search::register_pane_search_actions,
    });

    // 29. Editing modes and selectors; `journal::init` is desktop-only.
    vim::init(cx);
    terminal_view::init(cx);
    encoding_selector::init(cx);
    language_selector::init(cx);
    line_ending_selector::init(cx);
    toolchain_selector::init(cx);
    theme_selector::init(cx);
    settings_profile_selector::init(cx);
    language_tools::init(cx);

    // 30. Skipped: `call`, `collab_ui`, `feedback`, `onboarding`, `extensions_ui` (awaits the
    //     remote extension store), `inspector_ui`, `miniprofiler_ui` (std `Instant`).
    notifications::init(app_state.client.clone(), app_state.user_store.clone(), cx);
    git_ui::init(cx);
    markdown_preview::init(cx);
    tabular_data_preview::init(cx);
    svg_preview::init(cx);
    settings_ui::init(cx);
    keymap_editor::init(cx);
    edit_prediction::init(cx);
    // 30a. The browser edit-prediction registry (b11 §3.19), after `edit_prediction::init`
    //      so the crate's globals exist before editors are observed.
    web_edit_prediction::init(
        app_state.client.clone(),
        app_state.user_store.clone(),
        origin,
        cx,
    );
    json_schema_store::init(cx);
    which_key::init(cx);

    // 31. On desktop `collab_ui::init` calls this; `collab_ui` is excluded here.
    title_bar::init(cx);

    // 32. Window background and text rendering follow the settings; the server-URL
    //     reconnect branch is desktop-only.
    cx.observe_global::<SettingsStore>(move |cx| {
        for &mut window in cx.windows().iter_mut() {
            let background_appearance = cx.theme().window_background_appearance();
            window
                .update(cx, |_, window, _| {
                    window.set_background_appearance(background_appearance)
                })
                .ok();
        }
        cx.set_text_rendering_mode(
            match WorkspaceSettings::get_global(cx).text_rendering_mode {
                settings::TextRenderingMode::PlatformDefault => {
                    gpui::TextRenderingMode::PlatformDefault
                }
                settings::TextRenderingMode::Subpixel => gpui::TextRenderingMode::Subpixel,
                settings::TextRenderingMode::Grayscale => gpui::TextRenderingMode::Grayscale,
            },
        );
    })
    .detach();

    // 33. Syntax theme.
    app_state.languages.set_theme(cx.theme().clone());
    cx.observe_global::<GlobalTheme>({
        let languages = app_state.languages.clone();
        move |cx| {
            languages.set_theme(cx.theme().clone());
        }
    })
    .detach();

    // 34. Status bar, toolbars, panels and actions on every workspace.
    workspace_chrome::init(app_state.clone(), cx);

    // 35.
    cx.activate(true);

    app_state
}
