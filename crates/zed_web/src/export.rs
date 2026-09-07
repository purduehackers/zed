//! Browser download commands; archiving and delivery live in the Zedspaces host.

use std::cell::Cell;

use anyhow::{Context as _, Result};
use gpui::{App, AppContext, Context, PromptLevel, Window, actions};
use project_panel::ProjectPanel;
use workspace::{
    Pane, SaveIntent, Workspace,
    notifications::{NotificationId, simple_message_notification::MessageNotification},
};

actions!(web, [DownloadProjectZip]);

thread_local! {
    static DOWNLOADING: Cell<bool> = const { Cell::new(false) };
}

struct Download;
impl Drop for Download {
    fn drop(&mut self) {
        DOWNLOADING.set(false);
    }
}

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, _| {
        workspace.register_action(|workspace, _: &DownloadProjectZip, window, cx| {
            download(workspace, false, window, cx);
        });
        workspace.register_action(
            |workspace, _: &project_panel::DownloadFolderZip, window, cx| {
                download(workspace, true, window, cx);
            },
        );
    })
    .detach();
}

fn notify(workspace: &mut Workspace, message: String, cx: &mut Context<Workspace>) {
    workspace.show_notification(NotificationId::unique::<Download>(), cx, |cx| {
        cx.new(|cx| MessageNotification::new(message, cx))
    });
}

fn download(
    workspace: &mut Workspace,
    folder: bool,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    if DOWNLOADING.get() {
        return;
    }
    let project = workspace.project().clone();
    let path = if folder {
        workspace.panel::<ProjectPanel>(cx).and_then(|panel| {
            let path = panel.read(cx).selected_entry_project_path(cx)?;
            let project = project.read(cx);
            project.entry_for_path(&path, cx)?.is_dir().then_some(())?;
            project.absolute_path(&path, cx)
        })
    } else {
        let project = project.read(cx);
        let active = workspace
            .active_item(cx)
            .and_then(|item| item.project_path(cx));
        active
            .and_then(|path| project.worktree_for_id(path.worktree_id, cx))
            .or_else(|| project.visible_worktrees(cx).next())
            .map(|tree| tree.read(cx).abs_path().to_path_buf())
    };
    let Some(path) = path else {
        notify(workspace, "Select a project folder to download.".into(), cx);
        return;
    };
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let options = window.prompt(
        PromptLevel::Info,
        &format!("Download {name} as ZIP?"),
        Some("Includes working files, including uncommitted changes. Git metadata, links and special files are excluded. Ignored files may contain secrets or large build output. Limit: 256 MiB and 20,000 files. Files are read while the workspace stays live."),
        &["Project Files", "Include Ignored Files", "Cancel"],
        cx,
    );
    DOWNLOADING.set(true);
    let guard = Download;
    cx.spawn_in(window, async move |workspace, cx| {
        let _guard = guard;
        let result: Result<()> = async {
            let include_ignored = match options.await? {
                0 => false,
                1 => true,
                _ => return Ok(()),
            };
            let dirty = workspace.read_with(cx, |workspace, cx| {
                let mut seen = std::collections::HashSet::new();
                workspace.panes().iter().flat_map(|pane| {
                    pane.read(cx).items().filter_map(|item| {
                        (item.is_dirty(cx) && item.project_path(cx).is_some() && seen.insert(item.item_id()))
                            .then(|| (pane.clone(), item.boxed_clone()))
                    }).collect::<Vec<_>>()
                }).collect::<Vec<_>>()
            })?;
            if !dirty.is_empty() {
                let answer = cx.update(|window, cx| window.prompt(
                    PromptLevel::Warning,
                    "Save changes before downloading?",
                    Some("Only files saved in the project are included. Untitled buffers are not exported."),
                    &["Save and Download", "Download Saved Files", "Cancel"], cx,
                ))?.await?;
                match answer {
                    0 => for (pane, item) in dirty {
                        if !Pane::save_item(project.clone(), pane, item.as_ref(), SaveIntent::SaveAll, cx).await? {
                            return Ok(());
                        }
                    },
                    1 => {},
                    _ => return Ok(()),
                }
            }
            workspace.update(cx, |workspace, cx| notify(workspace, "Preparing ZIP download…".into(), cx))?;
            let message = crate::bridge::download_project(&path.to_string_lossy(), include_ignored).await
                .context("Could not download project")?;
            workspace.update(cx, |workspace, cx| notify(workspace, message, cx))?;
            Ok(())
        }.await;
        if let Err(error) = result {
            workspace.update(cx, |workspace, cx| notify(workspace, format!("{error:#}"), cx)).ok();
        }
    }).detach();
}
