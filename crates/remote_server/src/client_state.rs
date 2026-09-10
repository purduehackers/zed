//! Server-side store of the client's database image (BUILD-SPEC 5.4, D7): the bytes of
//! `SaveClientState` land under the server data directory, inside the sandbox snapshot and
//! the rebuild tarball (D9), and come back through `LoadClientState` on the next open.
//! Each signed participant has its own image and version under `participants/<id>`.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result};
use fs::{CopyOptions, Fs, RenameOptions};
use futures::{FutureExt as _, future::Shared};
use gpui::{App, AppContext as _, AsyncApp, Context, Entity, Task, WeakEntity};
use rpc::{TypedEnvelope, proto};
use serde::{Deserialize, Serialize};
use util::ResultExt as _;

/// The stored image, byte for byte as received (gzip when `meta.json` says so).
pub const IMAGE_FILE: &str = "db.sqlite";
/// The previously accepted image, for manual recovery after a bad overwrite.
pub const PREV_IMAGE_FILE: &str = "db.sqlite.prev";
/// Version and encoding of the stored image.
pub const META_FILE: &str = "meta.json";
/// Largest accepted `SaveClientState.sqlite` (bytes on the wire, compressed when `gzip`).
pub const MAX_IMAGE_BYTES: usize = 12 * 1024 * 1024;
/// How far above the stored version a save may jump. The client counts by one, so a larger
/// jump is a bug or a hostile tab; accepting it (up to `u64::MAX`) would leave the store
/// refusing every later save of every tab, and the file survives rebuilds (D9).
pub const MAX_VERSION_ADVANCE: u64 = 1 << 32;
/// Longest `client_build` recorded in `meta.json`; longer or non-printable values are dropped.
pub const MAX_CLIENT_BUILD_BYTES: usize = 128;
/// Saves that may wait for the store at once; more are refused so a tab cannot pin
/// `MAX_PENDING_SAVES * MAX_IMAGE_BYTES` of payload behind the operation lock.
pub const MAX_PENDING_SAVES: usize = 2;

const TEMP_IMAGE_FILE: &str = "db.sqlite.tmp";

/// What `meta.json` records about the stored image.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientStateMeta {
    /// The client's version of the image; the store keeps the highest it has accepted.
    pub version: u64,
    /// When the image was accepted.
    pub saved_at_unix_ms: u64,
    /// Size of the stored bytes.
    pub bytes: u64,
    /// Whether the stored bytes are gzip-compressed.
    pub gzip: bool,
    /// Build id of the client that wrote it, for skew diagnostics.
    pub client_build: Option<String>,
}

/// The blob store behind `SaveClientState` / `LoadClientState`.
pub struct ClientStateStore {
    peers: collections::HashMap<proto::PeerId, ParticipantStore>,
    fs: Arc<dyn Fs>,
    dir: PathBuf,
    /// `None` until `loaded` resolves and when nothing is stored.
    current: Option<ClientStateMeta>,
    /// Reads `meta.json` once; every handler awaits it first so a request that races
    /// startup sees the stored version rather than 0.
    loaded: Shared<Task<()>>,
    /// Serializes saves (and the image read of a load) so a replayed request cannot
    /// interleave with a newer one and a load never sees a half-rotated image.
    operation_lock: Arc<futures::lock::Mutex<()>>,
    /// Saves waiting for or holding the operation lock.
    pending_saves: Arc<AtomicUsize>,
    saved_tx: watch::Sender<u64>,
    /// Keeps a receiver alive so `send` never reports the no-receiver error.
    _saved_rx: watch::Receiver<u64>,
    /// Versions of accepted saves that carried `stopping` (the D6 flush).
    stopping_saved_tx: watch::Sender<u64>,
    _stopping_saved_rx: watch::Receiver<u64>,
}

struct ParticipantStore {
    active: Option<Entity<ClientStateStore>>,
    // An in-flight save can outlive eviction. Reuse its store/lock if the participant
    // returns before it finishes; never create two writers for the same disk image.
    store: WeakEntity<ClientStateStore>,
}

/// Decrements the pending-save count when a save finishes, however it finishes.
struct PendingSaveGuard(Arc<AtomicUsize>);

impl Drop for PendingSaveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ClientStateStore {
    /// A store over `dir` (created on the first save). Reads the existing metadata in the
    /// background.
    pub fn new(fs: Arc<dyn Fs>, dir: PathBuf, cx: &mut Context<Self>) -> Self {
        let (saved_tx, saved_rx) = watch::channel(0);
        let (stopping_saved_tx, stopping_saved_rx) = watch::channel(0);
        let loaded = cx
            .spawn({
                let fs = fs.clone();
                let dir = dir.clone();
                async move |this, cx| {
                    let meta = read_meta(fs.as_ref(), &dir).await;
                    this.update(cx, |this, _| {
                        if let Some(meta) = meta {
                            this.saved_tx.send(meta.version).ok();
                            this.current = Some(meta);
                        }
                    })
                    .ok();
                }
            })
            .shared();
        Self {
            peers: Default::default(),
            fs,
            dir,
            current: None,
            loaded,
            operation_lock: Arc::default(),
            pending_saves: Arc::default(),
            saved_tx,
            _saved_rx: saved_rx,
            stopping_saved_tx,
            _stopping_saved_rx: stopping_saved_rx,
        }
    }

    /// The directory the images live in.
    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn register_participant(
        &mut self,
        peer: proto::PeerId,
        participant: &str,
        cx: &mut Context<Self>,
    ) {
        let dir = self.dir.join("participants").join(participant);
        let store = self
            .peers
            .get(&peer)
            .and_then(|entry| entry.store.upgrade())
            .unwrap_or_else(|| cx.new(|cx| Self::new(self.fs.clone(), dir, cx)));
        self.peers.insert(
            peer,
            ParticipantStore {
                store: store.downgrade(),
                active: Some(store),
            },
        );
    }

    pub fn release_participant(&mut self, peer: proto::PeerId) {
        if let Some(entry) = self.peers.get_mut(&peer) {
            entry.active = None;
        }
    }

    pub fn participant_stopping_versions(
        &self,
        peer: proto::PeerId,
        cx: &App,
    ) -> Option<watch::Receiver<u64>> {
        self.peers
            .get(&peer)
            .and_then(|entry| entry.store.upgrade())
            .map(|store| store.read(cx).stopping_saved_versions())
    }

    fn for_peer(this: Entity<Self>, peer: proto::PeerId, cx: &AsyncApp) -> Result<Entity<Self>> {
        this.read_with(cx, |store, _| {
            store
                .peers
                .get(&peer)
                .and_then(|entry| entry.store.upgrade())
        })
        .context("unknown client-state participant")
    }

    /// Metadata of the stored image, once loaded.
    pub fn current(&self) -> Option<&ClientStateMeta> {
        self.current.as_ref()
    }

    /// A receiver that observes every accepted version. Each awaiter should clone it:
    /// `changed()` takes `&mut self`.
    pub fn saved_versions(&self) -> watch::Receiver<u64> {
        self._saved_rx.clone()
    }

    /// A receiver that observes only the accepted saves tagged `stopping` (the control
    /// channel's stopping wait): a ticker save that was already in flight when the notice
    /// went out is accepted too, but must not end the wait, because it predates the
    /// `unsaved_buffers` snapshot (D6).
    pub fn stopping_saved_versions(&self) -> watch::Receiver<u64> {
        self._stopping_saved_rx.clone()
    }

    /// Resolves once the stored metadata has been read.
    pub fn loaded(&self) -> Shared<Task<()>> {
        self.loaded.clone()
    }

    /// Stores the image unless the server already holds an equal or newer version, in
    /// which case `accepted: false` and the current version are returned (a replayed
    /// envelope after a lost response lands here; the client converges on the returned
    /// version).
    pub async fn handle_save_client_state(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::SaveClientState>,
        cx: AsyncApp,
    ) -> Result<proto::SaveClientStateResponse> {
        let this = Self::for_peer(
            this,
            envelope.original_sender_id.unwrap_or(envelope.sender_id),
            &cx,
        )?;
        Self::save_image(this, envelope, cx).await
    }

    async fn save_image(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::SaveClientState>,
        mut cx: AsyncApp,
    ) -> Result<proto::SaveClientStateResponse> {
        let payload = envelope.payload;
        anyhow::ensure!(!payload.sqlite.is_empty(), "empty client-state image");
        anyhow::ensure!(
            payload.sqlite.len() <= MAX_IMAGE_BYTES,
            "client-state image is {} bytes, over the {MAX_IMAGE_BYTES} byte limit",
            payload.sqlite.len()
        );

        let (loaded, operation_lock, pending_saves) = this.read_with(&cx, |this, _| {
            (
                this.loaded(),
                this.operation_lock.clone(),
                this.pending_saves.clone(),
            )
        });
        if pending_saves.fetch_add(1, Ordering::AcqRel) >= MAX_PENDING_SAVES {
            pending_saves.fetch_sub(1, Ordering::AcqRel);
            anyhow::bail!("{MAX_PENDING_SAVES} client-state saves are already pending");
        }
        let _pending_guard = PendingSaveGuard(pending_saves);
        loaded.await;
        let _operation_guard = operation_lock.lock().await;

        let (fs, dir, current_version) = this.read_with(&cx, |this, _| {
            (
                this.fs.clone(),
                this.dir.clone(),
                this.current.as_ref().map_or(0, |meta| meta.version),
            )
        });
        if payload.version <= current_version {
            return Ok(proto::SaveClientStateResponse {
                accepted: false,
                version: current_version,
            });
        }
        anyhow::ensure!(
            payload.version <= current_version.saturating_add(MAX_VERSION_ADVANCE),
            "client-state version {} is more than {MAX_VERSION_ADVANCE} above the stored {current_version}",
            payload.version
        );

        fs.create_dir(&dir)
            .await
            .with_context(|| format!("creating {dir:?}"))?;
        let temp_path = dir.join(TEMP_IMAGE_FILE);
        let image_path = dir.join(IMAGE_FILE);
        let prev_path = dir.join(PREV_IMAGE_FILE);
        fs.write(&temp_path, &payload.sqlite)
            .await
            .with_context(|| format!("writing {temp_path:?}"))?;
        // Copy rather than rename into `.prev`, then replace the image in one rename: the
        // current image then exists at every instant, so a crash between the two steps (or
        // a load racing this save) never finds the store without `db.sqlite`.
        if fs.is_file(&image_path).await {
            fs.copy_file(
                &image_path,
                &prev_path,
                CopyOptions {
                    overwrite: true,
                    ignore_if_exists: false,
                },
            )
            .await
            .with_context(|| format!("keeping {image_path:?} as {prev_path:?}"))?;
        }
        fs.rename(
            &temp_path,
            &image_path,
            RenameOptions {
                overwrite: true,
                ignore_if_exists: false,
                create_parents: false,
            },
        )
        .await
        .with_context(|| format!("moving {temp_path:?} into place"))?;

        let meta = ClientStateMeta {
            version: payload.version,
            saved_at_unix_ms: unix_millis_now(),
            bytes: payload.sqlite.len() as u64,
            gzip: payload.gzip,
            client_build: sanitize_client_build(payload.client_build),
        };
        fs.atomic_write(dir.join(META_FILE), serde_json::to_string_pretty(&meta)?)
            .await
            .context("writing meta.json")?;

        this.update(&mut cx, |this, _| {
            this.current = Some(meta);
            this.saved_tx.send(payload.version).ok();
            if payload.stopping {
                this.stopping_saved_tx.send(payload.version).ok();
            }
        });
        Ok(proto::SaveClientStateResponse {
            accepted: true,
            version: payload.version,
        })
    }

    /// Returns the stored image (empty bytes and version 0 when nothing is stored; bytes
    /// omitted when `metadata_only`). Metadata and bytes are read under the operation
    /// lock, so they always describe the same accepted save.
    pub async fn handle_load_client_state(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::LoadClientState>,
        cx: AsyncApp,
    ) -> Result<proto::LoadClientStateResponse> {
        let this = Self::for_peer(
            this,
            envelope.original_sender_id.unwrap_or(envelope.sender_id),
            &cx,
        )?;
        Self::load_image(this, envelope, cx).await
    }

    async fn load_image(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::LoadClientState>,
        cx: AsyncApp,
    ) -> Result<proto::LoadClientStateResponse> {
        let (loaded, operation_lock) =
            this.read_with(&cx, |this, _| (this.loaded(), this.operation_lock.clone()));
        loaded.await;
        let _operation_guard = operation_lock.lock().await;
        let (fs, dir, current) = this.read_with(&cx, |this, _| {
            (this.fs.clone(), this.dir.clone(), this.current.clone())
        });
        let Some(meta) = current else {
            return Ok(proto::LoadClientStateResponse {
                sqlite: Vec::new(),
                version: 0,
                gzip: false,
                client_build: None,
            });
        };
        let sqlite = if envelope.payload.metadata_only {
            Vec::new()
        } else {
            let image_path = dir.join(IMAGE_FILE);
            fs.load_bytes(&image_path)
                .await
                .with_context(|| format!("reading {image_path:?}"))?
        };
        Ok(proto::LoadClientStateResponse {
            sqlite,
            version: meta.version,
            gzip: meta.gzip,
            client_build: meta.client_build,
        })
    }
}

async fn read_meta(fs: &dyn Fs, dir: &std::path::Path) -> Option<ClientStateMeta> {
    let meta_path = dir.join(META_FILE);
    if !fs.is_file(&meta_path).await {
        return None;
    }
    let text = fs
        .load(&meta_path)
        .await
        .with_context(|| format!("reading {meta_path:?}"))
        .log_err()?;
    let meta: ClientStateMeta = serde_json::from_str(&text)
        .with_context(|| format!("parsing {meta_path:?}"))
        .log_err()?;
    if !fs.is_file(&dir.join(IMAGE_FILE)).await {
        log::warn!("client-state metadata without an image in {dir:?}; ignoring it");
        return None;
    }
    Some(meta)
}

/// `client_build` is echoed to every later tab and written into `meta.json`; a value that
/// is not a short printable token is dropped rather than stored.
fn sanitize_client_build(client_build: Option<String>) -> Option<String> {
    let client_build = client_build?;
    if client_build.is_empty()
        || client_build.len() > MAX_CLIENT_BUILD_BYTES
        || client_build
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        log::warn!(
            "ignoring client_build of {} bytes: not a printable token",
            client_build.len()
        );
        return None;
    }
    Some(client_build)
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
    use fs::FakeFs;
    use gpui::TestAppContext;
    use serde_json::json;
    use std::time::Instant;
    use util::path;

    fn envelope<T>(payload: T) -> TypedEnvelope<T> {
        TypedEnvelope {
            sender_id: proto::REMOTE_SERVER_PEER_ID,
            original_sender_id: None,
            message_id: 1,
            payload,
            received_at: Instant::now(),
        }
    }

    fn save(version: u64, bytes: &[u8]) -> TypedEnvelope<proto::SaveClientState> {
        save_with_build(version, bytes, Some("build-1".into()), false)
    }

    fn save_with_build(
        version: u64,
        bytes: &[u8],
        client_build: Option<String>,
        stopping: bool,
    ) -> TypedEnvelope<proto::SaveClientState> {
        envelope(proto::SaveClientState {
            project_id: proto::REMOTE_SERVER_PROJECT_ID,
            sqlite: bytes.to_vec(),
            version,
            gzip: true,
            client_build,
            stopping,
        })
    }

    fn load(metadata_only: bool) -> TypedEnvelope<proto::LoadClientState> {
        envelope(proto::LoadClientState {
            project_id: proto::REMOTE_SERVER_PROJECT_ID,
            metadata_only,
        })
    }

    fn dir() -> PathBuf {
        PathBuf::from(path!("/data/server_state/client_state"))
    }

    fn store(fs: &Arc<FakeFs>, cx: &mut TestAppContext) -> Entity<ClientStateStore> {
        let fs: Arc<dyn Fs> = fs.clone();
        cx.new(|cx| ClientStateStore::new(fs, dir(), cx))
    }

    #[gpui::test]
    async fn participants_keep_independent_images_and_versions(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let root = store(&fs, cx);
        let a = proto::PeerId { owner_id: 0, id: 8 };
        let b = proto::PeerId { owner_id: 0, id: 9 };
        root.update(cx, |store, cx| {
            store.register_participant(a, "p_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", cx);
            store.register_participant(b, "p_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", cx);
        });
        for (peer, bytes) in [(a, b"layout-a"), (b, b"layout-b")] {
            let mut request = save(1, bytes);
            request.original_sender_id = Some(peer);
            assert!(
                ClientStateStore::handle_save_client_state(root.clone(), request, cx.to_async())
                    .await
                    .unwrap()
                    .accepted
            );
        }
        root.update(cx, |store, cx| {
            store.release_participant(a);
            store.release_participant(b);
            assert!(store.peers[&a].store.upgrade().is_none());
            assert!(store.peers[&b].store.upgrade().is_none());
            store.register_participant(a, "p_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", cx);
            store.register_participant(b, "p_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", cx);
        });
        for (peer, bytes) in [(a, b"layout-a"), (b, b"layout-b")] {
            let mut request = load(false);
            request.original_sender_id = Some(peer);
            let response =
                ClientStateStore::handle_load_client_state(root.clone(), request, cx.to_async())
                    .await
                    .unwrap();
            assert_eq!(response.sqlite, bytes);
            assert_eq!(response.version, 1);
        }
        assert!(
            ClientStateStore::handle_load_client_state(root, load(false), cx.to_async())
                .await
                .is_err()
        );
    }

    #[gpui::test]
    async fn rejoining_reuses_an_in_flight_store(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let root = store(&fs, cx);
        let peer = proto::PeerId { owner_id: 0, id: 8 };
        root.update(cx, |store, cx| {
            store.register_participant(peer, "p_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", cx);
            let in_flight = store.peers[&peer].store.upgrade().unwrap();
            store.release_participant(peer);
            store.register_participant(peer, "p_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", cx);
            assert_eq!(store.peers[&peer].store.upgrade().unwrap(), in_flight);
        });
    }

    #[gpui::test]
    async fn save_then_load_round_trips(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        let response =
            ClientStateStore::save_image(store.clone(), save(3, b"image-3"), cx.to_async())
                .await
                .unwrap();
        assert_eq!(
            response,
            proto::SaveClientStateResponse {
                accepted: true,
                version: 3
            }
        );

        let loaded = ClientStateStore::load_image(store, load(false), cx.to_async())
            .await
            .unwrap();
        assert_eq!(loaded.sqlite, b"image-3");
        assert_eq!(loaded.version, 3);
        assert!(loaded.gzip);
        assert_eq!(loaded.client_build.as_deref(), Some("build-1"));
        assert_eq!(
            fs.load_bytes(&dir().join(IMAGE_FILE)).await.unwrap(),
            b"image-3"
        );
    }

    #[gpui::test]
    async fn stale_version_returns_current(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        ClientStateStore::save_image(store.clone(), save(5, b"five"), cx.to_async())
            .await
            .unwrap();
        let response =
            ClientStateStore::save_image(store.clone(), save(5, b"other"), cx.to_async())
                .await
                .unwrap();
        assert_eq!(
            response,
            proto::SaveClientStateResponse {
                accepted: false,
                version: 5
            }
        );
        let response = ClientStateStore::save_image(store, save(2, b"older"), cx.to_async())
            .await
            .unwrap();
        assert!(!response.accepted);
        assert_eq!(
            fs.load_bytes(&dir().join(IMAGE_FILE)).await.unwrap(),
            b"five"
        );
    }

    #[gpui::test]
    async fn load_without_state_is_empty(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        let loaded = ClientStateStore::load_image(store, load(false), cx.to_async())
            .await
            .unwrap();
        assert!(loaded.sqlite.is_empty());
        assert_eq!(loaded.version, 0);
        assert!(!loaded.gzip);
        assert_eq!(loaded.client_build, None);
    }

    #[gpui::test]
    async fn state_survives_new_store_instance(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let first = store(&fs, cx);
        ClientStateStore::save_image(first, save(7, b"seven"), cx.to_async())
            .await
            .unwrap();

        let second = store(&fs, cx);
        cx.run_until_parked();
        assert_eq!(
            second.read_with(cx, |store, _| store.current().map(|meta| meta.version)),
            Some(7)
        );
        let loaded = ClientStateStore::load_image(second, load(false), cx.to_async())
            .await
            .unwrap();
        assert_eq!(loaded.sqlite, b"seven");
        assert_eq!(loaded.version, 7);
    }

    #[gpui::test]
    async fn load_immediately_after_new_sees_stored_version(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            dir(),
            json!({
                "db.sqlite": "stored",
                "meta.json": json!({
                    "version": 11,
                    "saved_at_unix_ms": 1,
                    "bytes": 6,
                    "gzip": false,
                    "client_build": null
                }).to_string(),
            }),
        )
        .await;
        let store = store(&fs, cx);
        // No `run_until_parked` between construction and the request: the `loaded` gate
        // must hold the request until the metadata is read.
        let loaded = ClientStateStore::load_image(store, load(true), cx.to_async())
            .await
            .unwrap();
        assert_eq!(loaded.version, 11);
        assert!(loaded.sqlite.is_empty());
    }

    #[gpui::test]
    async fn prev_image_kept(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        ClientStateStore::save_image(store.clone(), save(1, b"first"), cx.to_async())
            .await
            .unwrap();
        ClientStateStore::save_image(store, save(2, b"second"), cx.to_async())
            .await
            .unwrap();
        assert_eq!(
            fs.load_bytes(&dir().join(IMAGE_FILE)).await.unwrap(),
            b"second"
        );
        assert_eq!(
            fs.load_bytes(&dir().join(PREV_IMAGE_FILE)).await.unwrap(),
            b"first"
        );
        assert!(!fs.is_file(&dir().join(TEMP_IMAGE_FILE)).await);
    }

    #[gpui::test]
    async fn save_notifies_watch(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        let mut versions = store.read_with(cx, |store, _| store.saved_versions());
        let mut stopping_versions = store.read_with(cx, |store, _| store.stopping_saved_versions());
        assert_eq!(*versions.borrow(), 0);
        ClientStateStore::save_image(store.clone(), save(4, b"four"), cx.to_async())
            .await
            .unwrap();
        versions.changed().await.unwrap();
        assert_eq!(*versions.borrow(), 4);
        assert_eq!(
            *stopping_versions.borrow(),
            0,
            "a plain save is not a stopping save"
        );

        ClientStateStore::save_image(
            store,
            save_with_build(5, b"five", None, true),
            cx.to_async(),
        )
        .await
        .unwrap();
        assert_eq!(*versions.borrow(), 5);
        assert_eq!(*stopping_versions.borrow(), 5);
    }

    #[gpui::test]
    async fn rejects_far_future_versions_and_unprintable_client_builds(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        let error =
            ClientStateStore::save_image(store.clone(), save(u64::MAX, b"max"), cx.to_async())
                .await
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("above the stored"),
            "{error:#}"
        );
        assert!(!fs.is_file(&dir().join(IMAGE_FILE)).await);

        let response = ClientStateStore::save_image(
            store.clone(),
            save(MAX_VERSION_ADVANCE, b"edge"),
            cx.to_async(),
        )
        .await
        .unwrap();
        assert!(
            response.accepted,
            "a jump of exactly the window is accepted"
        );

        for bad in [
            "x".repeat(MAX_CLIENT_BUILD_BYTES + 1),
            "build\nwith\nnewlines".to_owned(),
            "build with spaces".to_owned(),
            String::new(),
        ] {
            let version = store.read_with(cx, |store, _| {
                store.current().map_or(0, |meta| meta.version)
            }) + 1;
            ClientStateStore::save_image(
                store.clone(),
                save_with_build(version, b"b", Some(bad), false),
                cx.to_async(),
            )
            .await
            .unwrap();
            let loaded = ClientStateStore::load_image(store.clone(), load(true), cx.to_async())
                .await
                .unwrap();
            assert_eq!(loaded.client_build, None);
        }
        let version = store.read_with(cx, |store, _| store.current().unwrap().version) + 1;
        ClientStateStore::save_image(
            store.clone(),
            save_with_build(version, b"b", Some("dev-abc123".into()), false),
            cx.to_async(),
        )
        .await
        .unwrap();
        let loaded = ClientStateStore::load_image(store, load(true), cx.to_async())
            .await
            .unwrap();
        assert_eq!(loaded.client_build.as_deref(), Some("dev-abc123"));
    }

    #[gpui::test]
    async fn metadata_only_omits_bytes(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        ClientStateStore::save_image(store.clone(), save(9, b"nine"), cx.to_async())
            .await
            .unwrap();
        let loaded = ClientStateStore::load_image(store, load(true), cx.to_async())
            .await
            .unwrap();
        assert!(loaded.sqlite.is_empty());
        assert_eq!(loaded.version, 9);
        assert!(loaded.gzip);
    }

    #[gpui::test]
    async fn rejects_empty_and_oversized_images(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let store = store(&fs, cx);
        assert!(
            ClientStateStore::save_image(store.clone(), save(1, b""), cx.to_async())
                .await
                .is_err()
        );
        let oversized = vec![0u8; MAX_IMAGE_BYTES + 1];
        assert!(
            ClientStateStore::save_image(store, save(1, &oversized), cx.to_async())
                .await
                .is_err()
        );
        assert!(!fs.is_file(&dir().join(IMAGE_FILE)).await);
    }
}
