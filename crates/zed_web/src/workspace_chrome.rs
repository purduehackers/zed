//! The per-window and per-workspace chrome of `crates/zed/src/zed.rs` (`initialize_workspace`,
//! `initialize_pane`, `initialize_panels`, `register_actions`) minus what the browser
//! excludes: no collab panel, no onboarding hint, no quick-action bar (a `crates/zed`
//! module), no telemetry, no migration banner, no file watcher, no GPU warning.

use std::cell::Cell;
use std::sync::Arc;

use agent_ui::AgentDiffToolbar;
use anyhow::Context as _;
use breadcrumbs::Breadcrumbs;
use debugger_ui::debugger_panel::DebugPanel;
use futures::FutureExt as _;
use git_ui::{
    branch_diff::BranchDiffToolbar,
    commit_view::CommitViewToolbar,
    git_panel::GitPanel,
    project_diff::ProjectDiffToolbar,
    solo_diff_view::{SoloDiffGitToolbar, SoloDiffStyleToolbar},
    staged_diff::StagedDiffToolbar,
    unstaged_diff::UnstagedDiffToolbar,
};
use gpui::{
    App, AppContext as _, AsyncWindowContext, Context, Entity, Focusable as _, ReadGlobal as _,
    Task, TaskExt as _, WeakEntity, Window,
};
use image_viewer::ImageInfo;
use language_onboarding::BasedPyrightBanner;
use language_tools::lsp_button::{self, LspButton};
use language_tools::lsp_log_view::LspLogToolbarItemView;
use outline_panel::OutlinePanel;
use project::DisableAiSettings;
use project_panel::ProjectPanel;
use search::project_search::ProjectSearchBar;
use settings::SettingsStore;
use sidebar::Sidebar;
use terminal_view::terminal_panel::TerminalPanel;
use ui::PopoverMenuHandle;
use util::ResultExt as _;
use workspace::{AppState, MultiWorkspace, Pane, Panel, Workspace};
use zed_actions::{GetMerch, OpenDocs, OpenStatusPage};
use zed_web_core::BootStage;

use crate::{bridge, web_settings};

const DOCS_URL: &str = "https://zed.dev/docs/";
const STATUS_URL: &str = "https://status.zed.dev";
const MERCH_URL: &str = "https://merch.zed.dev/";

thread_local! {
    /// Whether this tab restored no client state, i.e. nobody has opened this workspace in a
    /// browser before. Set from [`crate::boot`] before the window exists.
    static FIRST_RUN: Cell<bool> = const { Cell::new(false) };
}

/// Records whether the boot found a client-state image to restore. A workspace opened for the
/// first time has no serialized dock layout, so [`initialize_panels`] gives it the default one
/// instead of leaving the person staring at an empty pane.
pub fn set_first_run(first_run: bool) {
    FIRST_RUN.with(|flag| flag.set(first_run));
}

/// Installs the observers that dress every new `MultiWorkspace` and `Workspace`.
pub fn init(app_state: Arc<AppState>, cx: &mut App) {
    cx.on_action(|_: &zed_actions::Quit, cx| quit(cx));

    cx.observe_new(|_multi_workspace: &mut MultiWorkspace, window, cx| {
        let Some(window) = window else {
            return;
        };

        // A window never closes in the browser; if it ever did, flush best-effort (not one
        // of D7's triggers).
        window.on_window_should_close(cx, |_, cx| {
            db::client_state::flush_client_state(cx).detach();
            true
        });

        let multi_workspace_handle = cx.entity();
        cx.subscribe_in(
            &multi_workspace_handle,
            window,
            |this, _multi_workspace, event: &workspace::MultiWorkspaceEvent, window, cx| {
                let workspace::MultiWorkspaceEvent::ActiveWorkspaceChanged { source_workspace } =
                    event
                else {
                    return;
                };
                let active_workspace = this.workspace().clone();
                let source_workspace = source_workspace.clone();
                active_workspace.update(cx, |workspace, cx| {
                    if let Some(source) = &source_workspace
                        && let Some(panel) = workspace.panel::<agent_ui::AgentPanel>(cx)
                    {
                        panel.update(cx, |panel, cx| {
                            panel.initialize_from_source_workspace_if_needed(
                                source.clone(),
                                window,
                                cx,
                            );
                        });
                    }
                    ensure_agent_panel_for_workspace(workspace, source_workspace, window, cx)
                        .detach_and_log_err(cx);
                });
            },
        )
        .detach();

        let window_handle = window.window_handle();
        cx.defer(move |cx| {
            window_handle
                .update(cx, |_, window, cx| {
                    let sidebar =
                        cx.new(|cx| Sidebar::new(multi_workspace_handle.clone(), window, cx));
                    multi_workspace_handle.update(cx, |multi_workspace, cx| {
                        multi_workspace.register_sidebar(sidebar, cx);
                    });
                })
                .ok();
        });
    })
    .detach();

    cx.observe_new(move |workspace: &mut Workspace, window, cx| {
        let Some(window) = window else {
            return;
        };

        let workspace_handle = cx.entity();
        let center_pane = workspace.active_pane().clone();
        initialize_pane(workspace, &center_pane, window, cx);

        cx.subscribe_in(&workspace_handle, window, {
            move |workspace, _, event, window, cx| {
                if let workspace::Event::PaneAdded(pane) = event {
                    initialize_pane(workspace, pane, window, cx);
                }
            }
        })
        .detach();

        let edit_prediction_menu_handle = PopoverMenuHandle::default();
        let edit_prediction_ui = cx.new(|cx| {
            edit_prediction_ui::EditPredictionButton::new(
                app_state.fs.clone(),
                app_state.user_store.clone(),
                edit_prediction_menu_handle.clone(),
                workspace.project().clone(),
                cx,
            )
        });
        workspace.register_action({
            move |_, _: &edit_prediction_ui::ToggleMenu, window, cx| {
                edit_prediction_menu_handle.toggle(window, cx);
            }
        });

        let search_button = cx.new(|_| search::search_status_button::SearchButton::new());
        let diagnostic_summary =
            cx.new(|cx| diagnostics::items::DiagnosticIndicator::new(workspace, cx));
        let active_file_name = cx.new(|_| workspace::active_file_name::ActiveFileName::new());
        let activity_indicator = activity_indicator::ActivityIndicator::new(
            workspace,
            workspace.project().read(cx).languages().clone(),
            window,
            cx,
        );
        let active_buffer_encoding =
            cx.new(|_| encoding_selector::ActiveBufferEncoding::new(workspace));
        let active_buffer_language =
            cx.new(|_| language_selector::ActiveBufferLanguage::new(workspace));
        let active_toolchain_language =
            cx.new(|cx| toolchain_selector::ActiveToolchain::new(workspace, window, cx));
        let vim_mode_indicator = cx.new(|cx| vim::ModeIndicator::new(window, cx));
        let image_info = cx.new(|_cx| ImageInfo::new(workspace));

        let lsp_button_menu_handle = PopoverMenuHandle::default();
        let lsp_button =
            cx.new(|cx| LspButton::new(workspace, lsp_button_menu_handle.clone(), window, cx));
        workspace.register_action({
            move |_, _: &lsp_button::ToggleMenu, window, cx| {
                lsp_button_menu_handle.toggle(window, cx);
            }
        });

        let cursor_position =
            cx.new(|_| go_to_line::cursor_position::CursorPosition::new(workspace));
        let line_ending_indicator =
            cx.new(|_| line_ending_selector::LineEndingIndicator::default());
        let git_blame_status = cx.new(|_| git_ui::GitBlameStatus::default());
        let merge_conflict_indicator =
            cx.new(|cx| git_ui::MergeConflictIndicator::new(workspace, cx));
        workspace.status_bar().update(cx, |status_bar, cx| {
            status_bar.add_left_item(search_button, window, cx);
            status_bar.add_left_item(lsp_button, window, cx);
            status_bar.add_left_item(diagnostic_summary, window, cx);
            status_bar.add_left_item(active_file_name, window, cx);
            status_bar.add_left_item(git_blame_status, window, cx);
            status_bar.add_left_item(merge_conflict_indicator, window, cx);
            status_bar.add_left_item(activity_indicator, window, cx);
            status_bar.add_right_item(edit_prediction_ui, window, cx);
            status_bar.add_right_item(active_buffer_encoding, window, cx);
            status_bar.add_right_item(active_buffer_language, window, cx);
            status_bar.add_right_item(active_toolchain_language, window, cx);
            status_bar.add_right_item(line_ending_indicator, window, cx);
            status_bar.add_right_item(vim_mode_indicator, window, cx);
            status_bar.add_right_item(cursor_position, window, cx);
            status_bar.add_right_item(image_info, window, cx);
        });

        let panels_task = initialize_panels(window, cx);
        workspace.set_panels_task(panels_task);
        register_actions(workspace, window, cx);

        if !workspace.has_active_modal(window, cx) {
            workspace.focus_handle(cx).focus(window, cx);
        }
    })
    .detach();
}

/// `zed::Quit` in a tab: pending settings saves, the unsaved-buffer snapshot (D6, never a
/// write to the workspace filesystem), the client-state flush, then `stopped` with detail
/// `quit`; the shell takes it from there.
fn quit(cx: &mut App) {
    let workspace = crate::boot::workspace();
    cx.spawn(async move |cx| {
        cx.update(|cx| web_settings::flush_pending_saves(cx))
            .await
            .log_err();
        if let Some(workspace) = workspace {
            cx.update(|cx| workspace::client_state::snapshot_unsaved_buffers(&workspace, cx))
                .await
                .log_err();
        }
        cx.update(|cx| db::client_state::flush_client_state(cx))
            .await
            .ok();
        bridge::progress(BootStage::Stopped, "quit");
    })
    .detach();
}

fn initialize_panels(window: &mut Window, cx: &mut Context<Workspace>) -> Task<anyhow::Result<()>> {
    cx.spawn_in(window, async move |workspace_handle, cx| {
        // Joining peers receive root metadata before its entries. The default shell
        // needs the root entry to choose the repository rather than the sandbox home.
        let roots = workspace_handle.read_with(cx, |workspace, cx| {
            workspace.worktrees(cx).collect::<Vec<_>>()
        })?;
        for root in roots {
            if let Some(ready) = root.update(cx, |root, _| {
                root.as_remote_mut().map(|root| root.wait_for_snapshot(1))
            }) {
                ready.await?;
            }
        }
        let project_panel = ProjectPanel::load(workspace_handle.clone(), cx.clone());
        let outline_panel = OutlinePanel::load(workspace_handle.clone(), cx.clone());
        let terminal_panel = TerminalPanel::load(workspace_handle.clone(), cx.clone());
        let git_panel = GitPanel::load(workspace_handle.clone(), cx.clone());
        let debug_panel = DebugPanel::load(workspace_handle.clone(), cx);

        async fn add_panel_when_ready(
            panel_task: impl Future<Output = anyhow::Result<Entity<impl Panel>>> + 'static,
            workspace_handle: WeakEntity<Workspace>,
            mut cx: AsyncWindowContext,
        ) {
            if let Some(panel) = panel_task.await.context("failed to load panel").log_err() {
                workspace_handle
                    .update_in(&mut cx, |workspace, window, cx| {
                        workspace.add_panel(panel, window, cx);
                    })
                    .log_err();
            }
        }

        futures::join!(
            add_panel_when_ready(project_panel, workspace_handle.clone(), cx.clone()),
            add_panel_when_ready(outline_panel, workspace_handle.clone(), cx.clone()),
            add_panel_when_ready(terminal_panel, workspace_handle.clone(), cx.clone()),
            add_panel_when_ready(git_panel, workspace_handle.clone(), cx.clone()),
            add_panel_when_ready(debug_panel, workspace_handle.clone(), cx.clone()),
            initialize_agent_panel(workspace_handle.clone(), cx.clone()).map(|r| r.log_err()),
        );

        workspace_handle.update_in(cx, |workspace, window, cx| {
            workspace.finish_dock_restoration(cx);
            // A returning tab gets whatever layout it left behind, including a closed dock.
            // A workspace nobody has opened yet has no serialized layout at all, and an IDE
            // that opens to nothing reads as broken, so give it the file tree and a terminal.
            // `open_panel` activates without taking focus, and activating the terminal panel
            // with no terminals in it spawns one.
            if FIRST_RUN.with(|flag| flag.get()) {
                workspace.open_panel::<ProjectPanel>(window, cx);
                workspace.open_panel::<TerminalPanel>(window, cx);
            }
        })?;

        anyhow::Ok(())
    })
}

fn setup_or_teardown_ai_panel<P: Panel>(
    workspace: &mut Workspace,
    window: &mut Window,
    cx: &mut Context<Workspace>,
    load_panel: impl FnOnce(
        WeakEntity<Workspace>,
        AsyncWindowContext,
    ) -> Task<anyhow::Result<Entity<P>>>
    + 'static,
) -> Task<anyhow::Result<()>> {
    let disable_ai = SettingsStore::global(cx)
        .get::<DisableAiSettings>(None)
        .disable_ai;
    let existing_panel = workspace.panel::<P>(cx);
    match (disable_ai, existing_panel) {
        (false, None) => cx.spawn_in(window, async move |workspace, cx| {
            let panel = load_panel(workspace.clone(), cx.clone()).await?;
            workspace.update_in(cx, |workspace, window, cx| {
                let disable_ai = SettingsStore::global(cx)
                    .get::<DisableAiSettings>(None)
                    .disable_ai;
                let have_panel = workspace.panel::<P>(cx).is_some();
                if !disable_ai && !have_panel {
                    workspace.add_panel(panel, window, cx);
                }
            })
        }),
        (true, Some(existing_panel)) => {
            workspace.remove_panel::<P>(&existing_panel, window, cx);
            Task::ready(Ok(()))
        }
        _ => Task::ready(Ok(())),
    }
}

fn ensure_agent_panel_for_workspace(
    workspace: &mut Workspace,
    source_workspace: Option<WeakEntity<Workspace>>,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) -> Task<anyhow::Result<()>> {
    let task = setup_or_teardown_ai_panel(workspace, window, cx, move |workspace, cx| {
        agent_ui::AgentPanel::load(workspace, cx)
    });
    cx.spawn_in(window, async move |workspace, cx| {
        task.await?;
        workspace.update_in(cx, |workspace, window, cx| {
            if let Some(source_workspace) = source_workspace.clone()
                && let Some(panel) = workspace.panel::<agent_ui::AgentPanel>(cx)
            {
                panel.update(cx, |panel, cx| {
                    panel.initialize_from_source_workspace_if_needed(source_workspace, window, cx);
                });
            }
        })
    })
}

async fn initialize_agent_panel(
    workspace_handle: WeakEntity<Workspace>,
    mut cx: AsyncWindowContext,
) -> anyhow::Result<()> {
    workspace_handle
        .update_in(&mut cx, |workspace, window, cx| {
            ensure_agent_panel_for_workspace(workspace, None, window, cx)
        })?
        .await?;

    workspace_handle.update_in(&mut cx, |workspace, window, cx| {
        cx.observe_global_in::<SettingsStore>(window, move |workspace, window, cx| {
            ensure_agent_panel_for_workspace(workspace, None, window, cx).detach_and_log_err(cx);
        })
        .detach();
        workspace
            .register_action(agent_ui::AgentPanel::toggle_focus)
            .register_action(agent_ui::AgentPanel::focus)
            .register_action(agent_ui::AgentPanel::toggle)
            .register_action(agent_ui::InlineAssistant::inline_assist);
    })?;

    anyhow::Ok(())
}

fn register_actions(workspace: &mut Workspace, _: &mut Window, _: &mut Context<Workspace>) {
    workspace
        .register_action(|_, _: &OpenDocs, _, cx| cx.open_url(DOCS_URL))
        .register_action(|_, _: &OpenStatusPage, _, cx| cx.open_url(STATUS_URL))
        .register_action(|_, _: &GetMerch, _, cx| cx.open_url(MERCH_URL));
}

fn initialize_pane(
    workspace: &Workspace,
    pane: &Entity<Pane>,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    pane.update(cx, |pane, cx| {
        pane.toolbar().update(cx, |toolbar, cx| {
            let solo_diff_style_toolbar = cx.new(SoloDiffStyleToolbar::new);
            toolbar.add_item(solo_diff_style_toolbar, window, cx);
            let breadcrumbs = cx.new(|_| Breadcrumbs::new());
            toolbar.add_item(breadcrumbs, window, cx);
            let buffer_search_bar = cx.new(|cx| {
                search::BufferSearchBar::new(
                    Some(workspace.project().read(cx).languages().clone()),
                    window,
                    cx,
                )
            });
            toolbar.add_item(buffer_search_bar, window, cx);
            let diagnostic_editor_controls = cx.new(|_| diagnostics::ToolbarControls::new());
            toolbar.add_item(diagnostic_editor_controls, window, cx);
            let project_search_bar = cx.new(|_| ProjectSearchBar::new());
            toolbar.add_item(project_search_bar, window, cx);
            let lsp_log_item = cx.new(|_| LspLogToolbarItemView::new());
            toolbar.add_item(lsp_log_item, window, cx);
            let dap_log_item = cx.new(|_| debugger_tools::DapLogToolbarItemView::new());
            toolbar.add_item(dap_log_item, window, cx);
            let acp_tools_item = cx.new(|_| acp_tools::AcpToolsToolbarItemView::new());
            toolbar.add_item(acp_tools_item, window, cx);
            let syntax_tree_item = cx.new(|_| language_tools::SyntaxTreeToolbarItemView::new());
            toolbar.add_item(syntax_tree_item, window, cx);
            let highlights_tree_item =
                cx.new(|_| language_tools::HighlightsTreeToolbarItemView::new());
            toolbar.add_item(highlights_tree_item, window, cx);
            let project_diff_toolbar = cx.new(|cx| ProjectDiffToolbar::new(workspace, cx));
            toolbar.add_item(project_diff_toolbar, window, cx);
            let staged_diff_toolbar = cx.new(|cx| StagedDiffToolbar::new(workspace, cx));
            toolbar.add_item(staged_diff_toolbar, window, cx);
            let unstaged_diff_toolbar = cx.new(|cx| UnstagedDiffToolbar::new(workspace, cx));
            toolbar.add_item(unstaged_diff_toolbar, window, cx);
            let branch_diff_toolbar = cx.new(BranchDiffToolbar::new);
            toolbar.add_item(branch_diff_toolbar, window, cx);
            let solo_diff_git_toolbar = cx.new(SoloDiffGitToolbar::new);
            toolbar.add_item(solo_diff_git_toolbar, window, cx);
            let commit_view_toolbar = cx.new(|_| CommitViewToolbar::new());
            toolbar.add_item(commit_view_toolbar, window, cx);
            let agent_diff_toolbar = cx.new(AgentDiffToolbar::new);
            toolbar.add_item(agent_diff_toolbar, window, cx);
            let basedpyright_banner = cx.new(|cx| BasedPyrightBanner::new(workspace, cx));
            toolbar.add_item(basedpyright_banner, window, cx);
            let image_view_toolbar = cx.new(|_| image_viewer::ImageViewToolbarControls::new());
            toolbar.add_item(image_view_toolbar, window, cx);
        })
    });
}
