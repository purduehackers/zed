//! Connect browser-selected files to the same Fs that native drop/import handlers use.

use std::{
    path::{Component, Path},
    sync::Arc,
};

use fs::WasmFs;
use gpui::{App, AppContext, Context, PathPromptOptions, Window, actions};
use project_panel::ProjectPanel;
use workspace::Workspace;
use workspace::notifications::{
    NotificationId, show_app_notification, simple_message_notification::MessageNotification,
};

struct FileTransferError;

actions!(web, [UploadFiles, UploadFolder]);

pub fn init(fs: Arc<WasmFs>, cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, _| {
        workspace.register_action(|workspace, _: &UploadFiles, window, cx| {
            upload(workspace, false, window, cx)
        });
        workspace.register_action(|workspace, _: &UploadFolder, window, cx| {
            upload(workspace, true, window, cx)
        });
    })
    .detach();
    let (sender, receiver) = async_channel::bounded::<fs::wasm_fs::ExternalFileWrite>(8);
    fs.set_write_through("/browser".into(), sender);
    cx.spawn(async move |_| {
        while let Ok(write) = receiver.recv().await {
            write
                .completion
                .send(gpui_web::files::write(&write.path, &write.bytes).await)
                .ok();
        }
    })
    .detach();
    let app = cx.to_async();
    gpui_web::files::init(
        move |entries| {
            // Validate the whole batch before modifying Fs. Browser-selected names
            // must never escape into settings or another import's virtual subtree.
            for (path, _) in &entries {
                let suffix = path.strip_prefix("/browser/imports")?;
                anyhow::ensure!(
                    suffix.components().count() >= 2
                        && suffix
                            .components()
                            .all(|part| matches!(part, Component::Normal(_))),
                    "Invalid browser import path"
                );
            }
            for (path, bytes) in entries {
                if let Some(bytes) = bytes {
                    fs.insert_file(&path, bytes);
                } else {
                    fs.insert_dir(Path::new(&path));
                }
            }
            Ok(())
        },
        move |message| {
            app.update(|cx| {
                show_app_notification(
                    NotificationId::unique::<FileTransferError>(),
                    cx,
                    move |cx| cx.new(|cx| MessageNotification::new(message.clone(), cx)),
                );
            });
        },
    );
}

fn upload(
    workspace: &mut Workspace,
    directory: bool,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    let Some(panel) = workspace.panel::<ProjectPanel>(cx) else {
        return;
    };
    let entry_id = panel
        .read(cx)
        .selected_entry(cx)
        .map(|(_, entry)| entry.id)
        .or_else(|| {
            workspace
                .project()
                .read(cx)
                .visible_worktrees(cx)
                .next()?
                .read(cx)
                .root_entry()
                .map(|entry| entry.id)
        });
    let Some(entry_id) = entry_id else {
        return;
    };
    let paths = cx.prompt_for_paths(PathPromptOptions {
        files: !directory,
        directories: directory,
        multiple: !directory,
        prompt: None,
    });
    cx.spawn_in(window, async move |workspace, cx| match paths.await {
        Ok(Ok(Some(paths))) => {
            panel
                .update_in(cx, |panel, window, cx| {
                    panel.drop_external_files(&paths, entry_id, window, cx)
                })
                .ok();
        }
        Ok(Err(error)) => {
            workspace
                .update(cx, |workspace, cx| workspace.show_error(error, cx))
                .ok();
        }
        _ => {}
    })
    .detach();
}
