use super::*;

impl HeadlessProject {
    pub(super) async fn handle_restore_buffer_snapshot(
        this: Entity<Self>,
        message: TypedEnvelope<proto::RestoreBufferSnapshot>,
        mut cx: AsyncApp,
    ) -> Result<proto::RestoreBufferSnapshotResponse> {
        let peer = message
            .original_sender_id
            .context("snapshot requires a participant")?;
        let snapshot = message.payload;
        anyhow::ensure!(snapshot.text.len() <= 8 * 1024 * 1024, "draft is too large");
        let (fs, recovery_path, version) = this.update(&mut cx, |this, cx| {
            anyhow::ensure!(
                this.participants.contains(&peer),
                "participant is not attached"
            );
            let buffer = this
                .buffer_store
                .read(cx)
                .get(language::BufferId::new(snapshot.buffer_id)?)
                .context("unknown draft buffer")?;
            let mut recovery_path = None;
            buffer.update(cx, |buffer, cx| -> Result<()> {
                log::debug!("participant draft: peer={} buffer={} current_bytes={} draft_bytes={} edited={} dirty={}",
                    peer.id, snapshot.buffer_id, buffer.len(), snapshot.text.len(),
                    this.edited_buffers.contains(&snapshot.buffer_id), buffer.is_dirty());
                if buffer.text() == snapshot.text {
                    return Ok(());
                }
                // This decision and the edit are one GPUI update. Two simultaneous
                // restores cannot both replace the initial text; later edits/saves win.
                if this.edited_buffers.contains(&snapshot.buffer_id) || buffer.is_dirty() {
                    let file = buffer
                        .file()
                        .and_then(|file| file.as_local())
                        .context("draft has no local file")?;
                    let path = file.abs_path(cx);
                    recovery_path = Some(path.with_file_name(format!(
                            "{}.zedspaces-recovered-{}.txt",
                            path.file_name()
                                .context("draft has no filename")?
                                .to_string_lossy(),
                            uuid::Uuid::new_v4()
                        )));
                } else {
                    this.edited_buffers.insert(snapshot.buffer_id);
                    if let Some(mtime) = snapshot.mtime {
                        buffer.did_reload(
                            buffer.version(),
                            buffer.line_ending(),
                            Some(mtime.into()),
                            cx,
                        );
                    }
                    buffer.set_text(snapshot.text.clone(), cx);
                    if let Some(entry) = buffer.peek_undo_stack() {
                        buffer.forget_transaction(entry.transaction_id());
                    }
                }
                Ok(())
            })?;
            Ok::<_, anyhow::Error>((
                this.fs.clone(),
                recovery_path,
                language::proto::serialize_version(&buffer.read(cx).version()),
            ))
        })?;
        if let Some(path) = &recovery_path {
            fs.atomic_write(path.clone(), snapshot.text).await?;
            log::warn!(
                "preserved an older participant draft at {} instead of replacing shared text",
                path.display()
            );
        }
        Ok(proto::RestoreBufferSnapshotResponse {
            version,
            recovery_path: recovery_path.map(|path| path.to_string_lossy().into_owned()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::FakeFs;
    use gpui::TestAppContext;
    use serde_json::json;
    use util::{path, rel_path::rel_path};

    #[gpui::test]
    async fn older_snapshot_never_overwrites_shared_edits_or_saves(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let fs = FakeFs::new(server_cx.executor());
        fs.insert_tree(path!("/code"), json!({"README.md": "disk"}))
            .await;
        let (project, headless) = crate::remote_editing_tests::init_test(&fs, cx, server_cx).await;
        let (worktree, _) = project
            .update(cx, |project, cx| {
                project.find_or_create_worktree(path!("/code"), true, cx)
            })
            .await
            .unwrap();
        let worktree_id = worktree.read_with(cx, |worktree, _| worktree.id());
        let buffer = project
            .update(cx, |project, cx| {
                project.open_buffer(
                    ProjectPath {
                        worktree_id,
                        path: rel_path("README.md").into(),
                    },
                    cx,
                )
            })
            .await
            .unwrap();
        let id = buffer.read_with(cx, |buffer, _| buffer.remote_id());
        let saved_mtime = buffer.read_with(cx, |buffer, _| buffer.saved_mtime());
        let peer = proto::PeerId { owner_id: 0, id: 8 };
        headless.update(server_cx, |this, _| {
            this.participants.insert(peer);
        });
        let request = || TypedEnvelope {
            sender_id: peer,
            original_sender_id: Some(peer),
            message_id: 1,
            received_at: Instant::now(),
            payload: proto::RestoreBufferSnapshot {
                project_id: REMOTE_SERVER_PROJECT_ID,
                buffer_id: id.to_proto(),
                text: "draft".into(),
                mtime: saved_mtime.map(Into::into),
            },
        };
        let restored = HeadlessProject::handle_restore_buffer_snapshot(
            headless.clone(),
            request(),
            server_cx.to_async(),
        )
        .await
        .unwrap();
        assert!(restored.recovery_path.is_none());
        server_cx.run_until_parked();
        cx.run_until_parked();
        assert!(
            buffer.read_with(cx, |buffer, _| buffer.is_dirty()),
            "the restored host draft remains unsaved in the client"
        );
        let duplicate = HeadlessProject::handle_restore_buffer_snapshot(
            headless.clone(),
            request(),
            server_cx.to_async(),
        )
        .await
        .unwrap();
        assert_eq!(
            duplicate, restored,
            "an identical snapshot does not duplicate the CRDT edit"
        );
        let (store, hosted) = headless.read_with(server_cx, |this, cx| {
            (
                this.buffer_store.clone(),
                this.buffer_store.read(cx).get(id).unwrap(),
            )
        });
        hosted.update(server_cx, |buffer, cx| buffer.set_text("newer", cx));
        store
            .update(server_cx, |store, cx| store.save_buffer(hosted.clone(), cx))
            .await
            .unwrap();
        server_cx.run_until_parked();
        let conflict = HeadlessProject::handle_restore_buffer_snapshot(
            headless,
            request(),
            server_cx.to_async(),
        )
        .await
        .unwrap();
        assert_eq!(
            hosted.read_with(server_cx, |buffer, _| buffer.text()),
            "newer"
        );
        assert_eq!(
            fs.load(Path::new(path!("/code/README.md"))).await.unwrap(),
            "newer"
        );
        assert_eq!(
            fs.load(Path::new(&conflict.recovery_path.unwrap()))
                .await
                .unwrap(),
            "draft"
        );
    }
}
