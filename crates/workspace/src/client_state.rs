//! The proto-aware half of client-state persistence (BUILD-SPEC 5.4, D6, D7): the sink that
//! ships `AppDatabase` images over the remote-server session, the boot-time loader, and the
//! `unsaved_buffers` snapshot taken on `LifecycleNotice STOPPING` and restored on the next
//! open. `db::client_state` owns the saver itself; the lifecycle toast is the shell page's
//! (BUILD-SPEC 7.6), fed by `project::Event::LifecycleNotice`.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context as _, Result};
use async_compression::{
    Level,
    futures::{bufread::GzipDecoder, write::GzipEncoder},
};
use client::{AnyProtoClient, proto};
use db::{
    client_state::{ClientStateSink, MAX_IMAGE_BYTES, SaveOutcome},
    sqlez::{domain::Domain, thread_safe_connection::ThreadSafeConnection},
    sqlez_macros::sql,
};
use fs::MTime;
use futures::{
    AsyncReadExt as _, AsyncWriteExt as _, FutureExt as _, future::BoxFuture, io::BufReader,
};
use gpui::{App, AppContext as _, Entity, Task, Window};
use util::ResultExt as _;
use web_time::{SystemTime, UNIX_EPOCH};

use crate::{Workspace, WorkspaceId, persistence::WorkspaceDb};

/// Upper bound on the sum of `text` over all `unsaved_buffers` rows of one snapshot; buffers
/// past it are skipped with a warning so the image stays under `MAX_IMAGE_BYTES`.
pub const UNSAVED_SNAPSHOT_LIMIT_BYTES: usize = 8 * 1024 * 1024;

const GZIP_LEVEL: Level = Level::Precise(3);

/// Ships images to the remote server as `SaveClientState` (gzip level 3).
pub struct RemoteClientStateSink {
    client: AnyProtoClient,
    project_id: u64,
    client_build: String,
}

impl RemoteClientStateSink {
    /// A sink over `client`, tagging saves with `client_build`.
    pub fn new(client: AnyProtoClient, client_build: String) -> Arc<Self> {
        Arc::new(Self {
            client,
            project_id: proto::REMOTE_SERVER_PROJECT_ID,
            client_build,
        })
    }
}

impl ClientStateSink for RemoteClientStateSink {
    fn save(
        &self,
        image: Vec<u8>,
        version: u64,
        stopping: bool,
    ) -> BoxFuture<'static, Result<SaveOutcome>> {
        let client = self.client.clone();
        let project_id = self.project_id;
        let client_build = self.client_build.clone();
        async move {
            let sqlite = gzip(&image).await?;
            let response = client
                .request(proto::SaveClientState {
                    project_id,
                    sqlite,
                    version,
                    gzip: true,
                    client_build: Some(client_build),
                    stopping,
                })
                .await
                .context("saving client state")?;
            Ok(SaveOutcome {
                accepted: response.accepted,
                version: response.version,
            })
        }
        .boxed()
    }

    fn current_version(&self) -> BoxFuture<'static, Result<u64>> {
        let client = self.client.clone();
        let project_id = self.project_id;
        async move {
            let response = client
                .request(proto::LoadClientState {
                    project_id,
                    metadata_only: true,
                })
                .await
                .context("querying the client-state version")?;
            Ok(response.version)
        }
        .boxed()
    }
}

/// Boot helper: requests the stored image before the `AppDatabase` is opened. Returns the
/// decompressed image (`None` when nothing is stored) and the stored version (0 when none).
pub async fn load_client_state(client: &AnyProtoClient) -> Result<(Option<Vec<u8>>, u64)> {
    let response = client
        .request(proto::LoadClientState {
            project_id: proto::REMOTE_SERVER_PROJECT_ID,
            metadata_only: false,
        })
        .await
        .context("loading client state")?;
    if let Some(stored_build) = &response.client_build
        && remote::websocket_wire::ZS_BUILD_ID.is_some_and(|build| build != stored_build)
    {
        log::info!(
            "client state was written by build {stored_build}; this is build {}",
            remote::websocket_wire::ZS_BUILD_ID.unwrap_or("unknown")
        );
    }
    if response.sqlite.is_empty() {
        return Ok((None, response.version));
    }
    let image = if response.gzip {
        gunzip(&response.sqlite, MAX_IMAGE_BYTES).await?
    } else {
        anyhow::ensure!(
            response.sqlite.len() <= MAX_IMAGE_BYTES,
            "stored client state is {} bytes, over the {MAX_IMAGE_BYTES} byte limit",
            response.sqlite.len()
        );
        response.sqlite
    };
    Ok((Some(image), response.version))
}

/// gzip-compresses `bytes` at [`GZIP_LEVEL`].
pub async fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzipEncoder::with_quality(Vec::new(), GZIP_LEVEL);
    encoder
        .write_all(bytes)
        .await
        .context("compressing the client-state image")?;
    encoder
        .close()
        .await
        .context("finishing the client-state image")?;
    Ok(encoder.into_inner())
}

/// Decompresses a gzip stream, refusing output past `limit` bytes.
pub async fn gunzip(bytes: &[u8], limit: usize) -> Result<Vec<u8>> {
    let decoder = GzipDecoder::new(BufReader::new(bytes));
    let mut image = Vec::new();
    decoder
        .take(limit as u64 + 1)
        .read_to_end(&mut image)
        .await
        .context("decompressing the client-state image")?;
    anyhow::ensure!(
        image.len() <= limit,
        "stored client state decompresses past the {limit} byte limit"
    );
    Ok(image)
}

/// The `unsaved_buffers` table (D6): dirty, path-backed buffers captured on `STOPPING`.
pub struct UnsavedBuffersDb(ThreadSafeConnection);

impl Domain for UnsavedBuffersDb {
    const NAME: &str = stringify!(UnsavedBuffersDb);

    const MIGRATIONS: &[&str] = &[sql!(
        CREATE TABLE unsaved_buffers(
            workspace_id INTEGER NOT NULL,
            abs_path TEXT NOT NULL,
            text TEXT NOT NULL,
            mtime_seconds INTEGER,
            mtime_nanos INTEGER,
            snapshot_at_unix_ms INTEGER NOT NULL,
            PRIMARY KEY(workspace_id, abs_path),
            FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
        ) STRICT;
    )];
}

db::static_connection!(UnsavedBuffersDb, [WorkspaceDb]);

/// One captured buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsavedBufferRow {
    /// Absolute path of the buffer's file.
    pub abs_path: PathBuf,
    /// The buffer's full text at capture time.
    pub text: String,
    /// `Buffer::saved_mtime`: the on-disk version the edits were made against (D6's
    /// "version"); a newer file on disk shows as a conflict after the restore.
    pub mtime: Option<MTime>,
    /// When the snapshot was taken.
    pub snapshot_at_unix_ms: u64,
}

type UnsavedBufferColumns = (String, String, Option<u64>, Option<u32>, u64);

impl UnsavedBuffersDb {
    /// Replaces the workspace's rows with `rows` in one write.
    pub async fn replace_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        rows: Vec<UnsavedBufferRow>,
    ) -> Result<()> {
        self.write(move |connection| {
            connection.with_savepoint("unsaved_buffers_replace", || {
                connection.exec_bound::<WorkspaceId>(
                    "DELETE FROM unsaved_buffers WHERE workspace_id = ?",
                )?(workspace_id)?;
                let mut insert = connection.exec_bound::<(WorkspaceId, UnsavedBufferColumns)>(
                    "INSERT INTO unsaved_buffers(workspace_id, abs_path, text, mtime_seconds, mtime_nanos, snapshot_at_unix_ms) VALUES (?, ?, ?, ?, ?, ?)",
                )?;
                for row in rows {
                    let (mtime_seconds, mtime_nanos) = match row
                        .mtime
                        .and_then(MTime::to_seconds_and_nanos_for_persistence)
                    {
                        Some((seconds, nanos)) => (Some(seconds), Some(nanos)),
                        None => (None, None),
                    };
                    insert((
                        workspace_id,
                        (
                            row.abs_path.to_string_lossy().into_owned(),
                            row.text,
                            mtime_seconds,
                            mtime_nanos,
                            row.snapshot_at_unix_ms,
                        ),
                    ))?;
                }
                Ok(())
            })
            .context("replacing unsaved_buffers rows")
        })
        .await
    }

    /// The workspace's rows, by path.
    pub fn rows_for_workspace(&self, workspace_id: WorkspaceId) -> Result<Vec<UnsavedBufferRow>> {
        let rows = self.select_bound::<WorkspaceId, UnsavedBufferColumns>(
            "SELECT abs_path, text, mtime_seconds, mtime_nanos, snapshot_at_unix_ms FROM unsaved_buffers WHERE workspace_id = ? ORDER BY abs_path",
        )?(workspace_id)
        .context("reading unsaved_buffers rows")?;
        Ok(rows
            .into_iter()
            .map(
                |(abs_path, text, mtime_seconds, mtime_nanos, snapshot_at_unix_ms)| {
                    UnsavedBufferRow {
                        abs_path: PathBuf::from(abs_path),
                        text,
                        mtime: mtime_seconds.map(|seconds| {
                            MTime::from_seconds_and_nanos(seconds, mtime_nanos.unwrap_or(0))
                        }),
                        snapshot_at_unix_ms,
                    }
                },
            )
            .collect())
    }

    /// Deletes the workspace's rows.
    pub async fn clear_for_workspace(&self, workspace_id: WorkspaceId) -> Result<()> {
        self.write(move |connection| {
            connection
                .exec_bound::<WorkspaceId>("DELETE FROM unsaved_buffers WHERE workspace_id = ?")?(
                workspace_id,
            )
            .context("clearing unsaved_buffers rows")
        })
        .await
    }
}

/// D6, on `LifecycleKind::Stopping` and before the client-state flush: every dirty,
/// path-backed buffer of the workspace's project becomes one `unsaved_buffers` row (untitled
/// buffers are covered by the editor's own item persistence). Nothing is written to the
/// workspace filesystem. Returns the number of rows written.
pub fn snapshot_unsaved_buffers(
    workspace: &Entity<Workspace>,
    cx: &mut App,
) -> Task<Result<usize>> {
    let workspace = workspace.read(cx);
    let Some(workspace_id) = workspace.database_id() else {
        return Task::ready(Ok(0));
    };
    let project = workspace.project().read(cx);
    let snapshot_at_unix_ms = unix_millis_now();
    let mut rows = Vec::new();
    let mut total_bytes = 0usize;
    for buffer in project.buffer_store().read(cx).buffers() {
        let buffer = buffer.read(cx);
        if !buffer.is_dirty() {
            continue;
        }
        let Some(file) = buffer.file() else {
            continue;
        };
        let abs_path = project
            .worktree_for_id(file.worktree_id(cx), cx)
            .map(|worktree| worktree.read(cx).absolutize(file.path()))
            .or_else(|| {
                let project_path = project.find_project_path(file.full_path(cx), cx)?;
                project.absolute_path(&project_path, cx)
            });
        let Some(abs_path) = abs_path else {
            continue;
        };
        let text = buffer.text();
        if total_bytes + text.len() > UNSAVED_SNAPSHOT_LIMIT_BYTES {
            log::warn!(
                "not snapshotting unsaved buffer {abs_path:?} ({} bytes): the snapshot would exceed {UNSAVED_SNAPSHOT_LIMIT_BYTES} bytes",
                text.len()
            );
            continue;
        }
        total_bytes += text.len();
        rows.push(UnsavedBufferRow {
            abs_path,
            text,
            mtime: buffer.saved_mtime(),
            snapshot_at_unix_ms,
        });
    }
    let count = rows.len();
    let db = UnsavedBuffersDb::global(cx);
    cx.background_spawn(async move {
        db.replace_for_workspace(workspace_id, rows).await?;
        Ok(count)
    })
}

/// D6, once per boot after the workspace's items have been restored: reopens each row's
/// buffer dirty with the stored text (the stored mtime becomes `saved_mtime`, so a file
/// changed on disk meanwhile shows as a conflict), makes it visible in a tab without
/// changing the active item, then clears the rows. Rows whose path is outside every open
/// worktree are dropped. Returns the number of buffers restored.
pub fn restore_unsaved_buffers(
    workspace: &Entity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) -> Task<Result<usize>> {
    let Some(workspace_id) = workspace.read(cx).database_id() else {
        return Task::ready(Ok(0));
    };
    let db = UnsavedBuffersDb::global(cx);
    let rows = match db.rows_for_workspace(workspace_id) {
        Ok(rows) => rows,
        Err(error) => return Task::ready(Err(error)),
    };
    if rows.is_empty() {
        return Task::ready(Ok(0));
    }
    let workspace = workspace.clone();
    window.spawn(cx, async move |cx| {
        let mut restored = 0;
        for row in rows {
            let project = workspace.read_with(cx, |workspace, _| workspace.project().clone());
            let Some(project_path) = project.read_with(cx, |project, cx| {
                project.find_project_path(&row.abs_path, cx)
            }) else {
                log::warn!(
                    "dropping unsaved buffer {:?}: its worktree is no longer open",
                    row.abs_path
                );
                continue;
            };
            let buffer = match project
                .update(cx, |project, cx| {
                    project.open_buffer(project_path.clone(), cx)
                })
                .await
            {
                Ok(buffer) => buffer,
                Err(error) => {
                    log::warn!(
                        "dropping unsaved buffer {:?}: opening it failed: {error:#}",
                        row.abs_path
                    );
                    continue;
                }
            };
            buffer.update(cx, |buffer, cx| {
                if buffer.text() == row.text {
                    return;
                }
                if row.mtime.is_some() {
                    buffer.did_reload(buffer.version(), buffer.line_ending(), row.mtime, cx);
                }
                buffer.set_text(row.text.clone(), cx);
                if let Some(entry) = buffer.peek_undo_stack() {
                    buffer.forget_transaction(entry.transaction_id());
                }
            });
            // A dirty buffer must be visible to be saved or discarded; the tab is added
            // without stealing the active item.
            let open = workspace.update_in(cx, |workspace, window, cx| {
                workspace.open_path_preview(project_path, None, false, false, false, window, cx)
            });
            match open {
                Ok(open) => {
                    open.await
                        .with_context(|| format!("showing restored buffer {:?}", row.abs_path))
                        .log_err();
                }
                Err(error) => log::warn!(
                    "showing restored buffer {:?} failed: {error:#}",
                    row.abs_path
                ),
            }
            restored += 1;
        }
        db.clear_for_workspace(workspace_id).await?;
        Ok(restored)
    })
}

fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init_test;
    use fs::{FakeFs, Fs as _};
    use gpui::{TestAppContext, VisualTestContext};
    use project::Project;
    use serde_json::json;
    use std::{path::Path, time::Duration};
    use util::{path, rel_path::rel_path};

    const WORKSPACE_ID: i64 = 7;

    async fn open_workspace<'a>(
        fs: &Arc<FakeFs>,
        cx: &'a mut TestAppContext,
    ) -> (
        Entity<Workspace>,
        Entity<Project>,
        &'a mut VisualTestContext,
    ) {
        let project = Project::test(fs.clone(), [path!("/root").as_ref()], cx).await;
        let (workspace, cx) =
            cx.add_window_view(|window, cx| Workspace::test_new(project.clone(), window, cx));
        workspace.update(cx, |workspace, _| {
            workspace.set_database_id(WorkspaceId::from_i64(WORKSPACE_ID));
        });
        // `unsaved_buffers` references `workspaces`; persist the workspace so the row exists.
        workspace
            .update_in(cx, |workspace, window, cx| {
                workspace.serialize_workspace_internal(window, cx)
            })
            .await;
        cx.run_until_parked();
        (workspace, project, cx)
    }

    async fn open_dirty(
        project: &Entity<Project>,
        path: &str,
        edit: &str,
        cx: &mut VisualTestContext,
    ) -> Entity<language::Buffer> {
        let buffer = project
            .update(cx, |project, cx| project.open_local_buffer(path, cx))
            .await
            .unwrap();
        buffer.update(cx, |buffer, cx| {
            buffer.edit([(0..0, edit)], None, cx);
        });
        cx.run_until_parked();
        buffer
    }

    fn rows(cx: &mut TestAppContext) -> Vec<UnsavedBufferRow> {
        cx.update(|cx| {
            UnsavedBuffersDb::global(cx)
                .rows_for_workspace(WorkspaceId::from_i64(WORKSPACE_ID))
                .unwrap()
        })
    }

    #[gpui::test]
    async fn snapshot_writes_dirty_path_backed_buffers_only(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            path!("/root"),
            json!({ "clean.rs": "fn clean() {}", "dirty.rs": "fn dirty() {}" }),
        )
        .await;
        let (workspace, project, cx) = open_workspace(&fs, cx).await;

        project
            .update(cx, |project, cx| {
                project.open_local_buffer(path!("/root/clean.rs"), cx)
            })
            .await
            .unwrap();
        let dirty = open_dirty(&project, path!("/root/dirty.rs"), "edited ", cx).await;
        let untitled = project.update(cx, |project, cx| {
            project.create_local_buffer("", None, false, cx)
        });
        untitled.update(cx, |buffer, cx| buffer.edit([(0..0, "scratch")], None, cx));
        assert!(untitled.read_with(cx, |buffer, _| buffer.is_dirty()));

        let written = cx
            .update(|_, cx| snapshot_unsaved_buffers(&workspace, cx))
            .await
            .unwrap();
        assert_eq!(written, 1);

        let rows = rows(cx);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].abs_path, Path::new(path!("/root/dirty.rs")));
        assert_eq!(rows[0].text, "edited fn dirty() {}");
        assert_eq!(
            rows[0].mtime,
            dirty.read_with(cx, |buffer, _| buffer.saved_mtime())
        );
        assert!(rows[0].mtime.is_some());
        assert_eq!(
            fs.load(Path::new(path!("/root/dirty.rs"))).await.unwrap(),
            "fn dirty() {}",
            "the snapshot never writes to disk"
        );
    }

    #[gpui::test]
    async fn snapshot_respects_limit(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(path!("/root"), json!({ "big.rs": "", "small.rs": "" }))
            .await;
        let (workspace, project, cx) = open_workspace(&fs, cx).await;

        // The buffer store only holds weak references: keep the buffers alive.
        let big = "x".repeat(UNSAVED_SNAPSHOT_LIMIT_BYTES + 1);
        let _big = open_dirty(&project, path!("/root/big.rs"), &big, cx).await;
        let _small = open_dirty(&project, path!("/root/small.rs"), "small", cx).await;

        let written = cx
            .update(|_, cx| snapshot_unsaved_buffers(&workspace, cx))
            .await
            .unwrap();
        assert_eq!(written, 1);
        let rows = rows(cx);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].abs_path, Path::new(path!("/root/small.rs")));
    }

    #[gpui::test]
    async fn restore_applies_text_and_marks_dirty(cx: &mut TestAppContext) {
        use crate::item::test::TestItem;

        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            path!("/root"),
            json!({ "dirty.rs": "fn dirty() {}", "other.rs": "fn other() {}" }),
        )
        .await;
        {
            let (workspace, project, cx) = open_workspace(&fs, cx).await;
            let _dirty = open_dirty(&project, path!("/root/dirty.rs"), "edited ", cx).await;
            let written = cx
                .update(|_, cx| snapshot_unsaved_buffers(&workspace, cx))
                .await
                .unwrap();
            assert_eq!(written, 1);
        }

        let (workspace, project, cx) = open_workspace(&fs, cx).await;
        // `TestItem` stands in for the editor item that shows a buffer in production, so the
        // restore can add a tab for the restored buffer.
        cx.update(|_, cx| crate::register_project_item::<TestItem>(cx));
        let other = workspace
            .update_in(cx, |workspace, window, cx| {
                let worktree_id = workspace
                    .project()
                    .read(cx)
                    .worktrees(cx)
                    .next()
                    .expect("the root worktree")
                    .read(cx)
                    .id();
                workspace.open_path_preview(
                    (worktree_id, rel_path("other.rs")),
                    None,
                    true,
                    false,
                    true,
                    window,
                    cx,
                )
            })
            .await
            .unwrap();
        // The buffer store only keeps weak references: hold the buffer like an item would.
        let buffer = project
            .update(cx, |project, cx| {
                project.open_local_buffer(path!("/root/dirty.rs"), cx)
            })
            .await
            .unwrap();
        buffer.read_with(cx, |buffer, _| assert!(!buffer.is_dirty()));
        let restored = cx
            .update(|window, cx| restore_unsaved_buffers(&workspace, window, cx))
            .await
            .unwrap();
        assert_eq!(restored, 1);
        buffer.read_with(cx, |buffer, _| {
            assert_eq!(buffer.text(), "edited fn dirty() {}");
            assert!(buffer.is_dirty());
            assert!(
                buffer.peek_undo_stack().is_none(),
                "undo must not revert to the disk text"
            );
        });
        workspace.update(cx, |workspace, cx| {
            let pane = workspace.active_pane().read(cx);
            let restored_tab = pane
                .items()
                .find(|item| {
                    item.project_path(cx)
                        .is_some_and(|path| path.path.as_unix_str() == "dirty.rs")
                })
                .expect("the active pane has a tab for the restored buffer");
            let active = pane.active_item().expect("an active item");
            assert_eq!(
                active.item_id(),
                other.item_id(),
                "the restored tab is added without becoming the active item"
            );
            assert_ne!(active.item_id(), restored_tab.item_id());
        });
        assert!(rows(cx).is_empty(), "rows are cleared after the restore");
        assert_eq!(
            fs.load(Path::new(path!("/root/dirty.rs"))).await.unwrap(),
            "fn dirty() {}"
        );
    }

    #[gpui::test]
    async fn restore_skips_identical_text(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(path!("/root"), json!({ "dirty.rs": "fn dirty() {}" }))
            .await;
        {
            let (workspace, project, cx) = open_workspace(&fs, cx).await;
            let _dirty = open_dirty(&project, path!("/root/dirty.rs"), "edited ", cx).await;
            let written = cx
                .update(|_, cx| snapshot_unsaved_buffers(&workspace, cx))
                .await
                .unwrap();
            assert_eq!(written, 1);
        }

        let (workspace, project, cx) = open_workspace(&fs, cx).await;
        // The editor item already restored the text from `editors.contents`.
        let buffer = open_dirty(&project, path!("/root/dirty.rs"), "edited ", cx).await;
        let version_before = buffer.read_with(cx, |buffer, _| buffer.version());
        let restored = cx
            .update(|window, cx| restore_unsaved_buffers(&workspace, window, cx))
            .await
            .unwrap();
        assert_eq!(restored, 1);
        buffer.read_with(cx, |buffer, _| {
            assert_eq!(buffer.text(), "edited fn dirty() {}");
            assert!(buffer.is_dirty());
            assert_eq!(buffer.version(), version_before, "no edit was applied");
        });
        assert!(rows(cx).is_empty());
    }

    #[gpui::test]
    async fn restore_marks_conflict_when_disk_changed(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(path!("/root"), json!({ "dirty.rs": "fn dirty() {}" }))
            .await;
        {
            let (workspace, project, cx) = open_workspace(&fs, cx).await;
            let _dirty = open_dirty(&project, path!("/root/dirty.rs"), "edited ", cx).await;
            let written = cx
                .update(|_, cx| snapshot_unsaved_buffers(&workspace, cx))
                .await
                .unwrap();
            assert_eq!(written, 1);
        }

        fs.set_next_mtime(SystemTime::now() + Duration::from_secs(60));
        fs.save(
            Path::new(path!("/root/dirty.rs")),
            &"fn changed_on_disk() {}".into(),
            language::LineEnding::Unix,
        )
        .await
        .unwrap();

        let (workspace, project, cx) = open_workspace(&fs, cx).await;
        let buffer = project
            .update(cx, |project, cx| {
                project.open_local_buffer(path!("/root/dirty.rs"), cx)
            })
            .await
            .unwrap();
        buffer.read_with(cx, |buffer, _| {
            assert_eq!(buffer.text(), "fn changed_on_disk() {}");
            assert!(!buffer.has_conflict());
        });
        let restored = cx
            .update(|window, cx| restore_unsaved_buffers(&workspace, window, cx))
            .await
            .unwrap();
        assert_eq!(restored, 1);
        cx.run_until_parked();
        buffer.read_with(cx, |buffer, _| {
            assert_eq!(buffer.text(), "edited fn dirty() {}");
            assert!(buffer.is_dirty());
            assert!(buffer.has_conflict(), "a newer file on disk is a conflict");
        });
    }

    #[gpui::test]
    async fn restore_drops_rows_for_closed_worktrees(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(path!("/root"), json!({ "a.rs": "" })).await;
        let (workspace, _project, cx) = open_workspace(&fs, cx).await;
        cx.update(|_, cx| {
            let db = UnsavedBuffersDb::global(cx);
            cx.background_spawn(async move {
                db.replace_for_workspace(
                    WorkspaceId::from_i64(WORKSPACE_ID),
                    vec![UnsavedBufferRow {
                        abs_path: PathBuf::from(path!("/elsewhere/file.rs")),
                        text: "gone".into(),
                        mtime: None,
                        snapshot_at_unix_ms: 1,
                    }],
                )
                .await
            })
        })
        .await
        .unwrap();
        assert_eq!(rows(cx).len(), 1);

        let restored = cx
            .update(|window, cx| restore_unsaved_buffers(&workspace, window, cx))
            .await
            .unwrap();
        assert_eq!(restored, 0);
        assert!(rows(cx).is_empty());
    }

    #[gpui::test]
    async fn gzip_round_trips_and_caps_output() {
        let image = b"SQLite format 3\0".repeat(64);
        let compressed = gzip(&image).await.unwrap();
        assert!(compressed.len() < image.len());
        assert_eq!(gunzip(&compressed, image.len()).await.unwrap(), image);
        assert!(gunzip(&compressed, image.len() - 1).await.is_err());
    }
}
