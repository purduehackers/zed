//! Registry-driven extension management for the sandbox (BUILD-SPEC 5.6, 9): installs from
//! the Zed extension registry into `paths::remote_extensions_dir()` (inside the snapshot),
//! reloads what is on disk at startup, and answers the client's extension messages. Built
//! on the public API of `HeadlessExtensionStore`, which keeps running language servers and
//! wasm extensions; asset-only extensions (themes, grammars, snippets) live on disk for the
//! `/extensions/{id}/assets/*` route and are never loaded into the store.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use anyhow::{Context as _, Result};
use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive;
use cloud_api_types::{ExtensionMetadata, GetExtensionsResponse};
use extension::{ExtensionEvents, ExtensionManifest};
use extension_host::{
    headless_host::{ExtensionVersion, HeadlessExtensionStore},
    is_version_compatible, schema_version_range,
    wasm_host::wit::wasm_api_version_range,
};
use fs::{Fs, RemoveOptions, RenameOptions};
use futures::{AsyncRead, AsyncReadExt as _, StreamExt as _, io::BufReader};
use gpui::{App, AppContext as _, AsyncApp, Context, Entity, Task, WeakEntity};
use http_client::{AsyncBody, HttpClient as _, HttpClientWithUrl, Url};
use release_channel::ReleaseChannel;
use rpc::{TypedEnvelope, proto};
use util::ResultExt as _;

/// Largest registry download accepted (compressed bytes on the wire).
pub const MAX_REGISTRY_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024;
/// Most bytes a downloaded archive may expand to while it is unpacked: the download cap
/// bounds the gzip, not what it decodes to, and a hostile registry (`ZED_SERVER_URL` is
/// user-settable) could otherwise expand a small download until the server is killed.
pub const MAX_UNPACKED_BYTES: u64 = 256 * 1024 * 1024;
/// Largest registry listing accepted.
pub const MAX_REGISTRY_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
/// Longest `ListExtensions.search` forwarded to the registry.
pub const MAX_SEARCH_BYTES: usize = 256;
/// Subdirectory of the extensions directory where downloads are unpacked before the move
/// into place (never `remote_extensions_uploads_dir()`, which the store's sweep prunes).
pub const STAGING_DIR: &str = "staging";
/// `WasmHost`'s work directory inside the extensions directory; skipped by the disk scan.
const WORK_DIR: &str = "work";
const MANIFEST_FILE: &str = "extension.toml";
const MAX_EXTENSION_ID_BYTES: usize = 64;
const MAX_EXTENSION_VERSION_BYTES: usize = 80;
const MAX_VERSION_SUFFIX_BYTES: usize = 64;
const MAX_ERROR_BODY_BYTES: u64 = 4096;
const DOWNLOAD_CHUNK_BYTES: usize = 64 * 1024;

/// Whether `id` matches the extension-id grammar `^[a-z0-9][a-z0-9_-]{0,63}$`, the only
/// shape the registry issues. Rejecting `.`, `..`, separators and empty ids keeps every
/// path built from an id inside one extension's directory.
pub fn is_valid_extension_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_EXTENSION_ID_BYTES {
        return false;
    }
    let first_ok = bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit();
    first_ok
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

/// Whether `version` is a plain semantic version, `MAJOR.MINOR.PATCH` with an optional
/// `-pre` or `+build` suffix of at most 64 characters from `[0-9A-Za-z.-]`. A pinned
/// version is interpolated into the registry URL path, so anything else (`..`, `/`, `?`,
/// `#`) is refused before the URL is built.
pub fn is_valid_extension_version(version: &str) -> bool {
    if version.is_empty() || version.len() > MAX_EXTENSION_VERSION_BYTES {
        return false;
    }
    let (core, suffix) = match version.find(['-', '+']) {
        Some(index) => (&version[..index], &version[index + 1..]),
        None => (version, ""),
    };
    let mut numbers = core.split('.');
    let three_numbers = (0..3).all(|_| {
        numbers
            .next()
            .is_some_and(|number| !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()))
    }) && numbers.next().is_none();
    if !three_numbers {
        return false;
    }
    if version.len() > core.len() {
        if suffix.is_empty() || suffix.len() > MAX_VERSION_SUFFIX_BYTES {
            return false;
        }
        return suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'));
    }
    true
}

/// Where and how to reach the extension registry.
pub struct RegistryConfig {
    /// Client whose base URL is the Zed server URL (`ZED_SERVER_URL` or `https://zed.dev`);
    /// `build_zed_api_url` maps it to the API host.
    pub http: Arc<HttpClientWithUrl>,
    /// Selects the wasm API range advertised to the registry.
    pub release_channel: ReleaseChannel,
}

/// An installed extension as reported to the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledExtensionRecord {
    /// Registry id.
    pub id: Arc<str>,
    /// Installed version.
    pub version: Arc<str>,
    /// Display name.
    pub name: String,
    /// Description from the manifest.
    pub description: Option<String>,
    /// `ExtensionProvides` kebab-case names.
    pub provides: Vec<String>,
    /// Whether the last install of this id was a dev upload.
    pub dev: bool,
}

impl InstalledExtensionRecord {
    /// Derives the record from a manifest.
    pub fn from_manifest(manifest: &ExtensionManifest, dev: bool) -> Self {
        Self {
            id: manifest.id.clone(),
            version: manifest.version.clone(),
            name: manifest.name.clone(),
            description: manifest.description.clone(),
            provides: manifest
                .provides()
                .iter()
                .map(|provides| provides.to_string())
                .collect(),
            dev,
        }
    }

    /// The wire form.
    pub fn to_proto(&self) -> proto::InstalledExtension {
        proto::InstalledExtension {
            id: self.id.to_string(),
            version: self.version.to_string(),
            name: self.name.clone(),
            description: self.description.clone(),
            provides: self.provides.clone(),
            dev: self.dev,
        }
    }
}

/// The sandbox's extension manager.
pub struct SandboxExtensions {
    store: Entity<HeadlessExtensionStore>,
    registry: RegistryConfig,
    fs: Arc<dyn Fs>,
    extension_dir: PathBuf,
    /// Every installed extension's manifest, keyed by id; the source of the installed set.
    manifests: BTreeMap<Arc<str>, Arc<ExtensionManifest>>,
    /// Ids whose last install was a dev upload (in-memory only; the desktop re-syncs dev
    /// extensions on every connect).
    dev_ids: BTreeSet<Arc<str>>,
    /// Ids the store runs (languages, language servers): the ones an uninstall must unload
    /// through the store. An extension that is on disk but failed to load is installed
    /// without being here.
    store_loaded: BTreeSet<Arc<str>>,
    /// Serializes installs, uninstalls and the startup scan.
    operation_lock: Arc<futures::lock::Mutex<()>>,
}

impl SandboxExtensions {
    /// A manager over `store`, installing into `extension_dir` (the store's directory).
    pub fn new(
        store: Entity<HeadlessExtensionStore>,
        registry: RegistryConfig,
        fs: Arc<dyn Fs>,
        extension_dir: PathBuf,
    ) -> Self {
        Self {
            store,
            registry,
            fs,
            extension_dir,
            manifests: BTreeMap::new(),
            dev_ids: BTreeSet::new(),
            store_loaded: BTreeSet::new(),
            operation_lock: Arc::default(),
        }
    }

    /// The extensions directory.
    pub fn extension_dir(&self) -> &Path {
        &self.extension_dir
    }

    /// Whether `id` is installed.
    pub fn is_installed(&self, id: &str) -> bool {
        self.manifests.contains_key(id)
    }

    /// Whether the store runs `id` (as opposed to an asset-only extension that only lives
    /// on disk, or a runnable one that failed to load).
    pub fn is_loaded(&self, id: &str) -> bool {
        self.store_loaded.contains(id)
    }

    /// The installed set, in id order.
    pub fn installed_extension_records(&self) -> Vec<InstalledExtensionRecord> {
        self.manifests
            .values()
            .map(|manifest| {
                InstalledExtensionRecord::from_manifest(
                    manifest,
                    self.dev_ids.contains(&manifest.id),
                )
            })
            .collect()
    }

    /// Records `manifest` as installed (with `dev` for an upload the desktop marked so) and
    /// returns the manifest it replaces, for `restore_record` if the install then fails.
    fn record(
        &mut self,
        manifest: Arc<ExtensionManifest>,
        dev: bool,
    ) -> Option<Arc<ExtensionManifest>> {
        if dev {
            self.dev_ids.insert(manifest.id.clone());
        } else {
            self.dev_ids.remove(&manifest.id);
        }
        self.manifests.insert(manifest.id.clone(), manifest)
    }

    /// Undoes `record` after a failed install.
    fn restore_record(&mut self, id: &Arc<str>, previous: Option<Arc<ExtensionManifest>>) {
        match previous {
            Some(previous) => {
                self.manifests.insert(id.clone(), previous);
            }
            None => {
                self.manifests.remove(id);
                self.dev_ids.remove(id);
            }
        }
    }

    /// Scans `extension_dir/*/extension.toml` (skipping `work`, `staging` and any directory
    /// without a manifest) and loads the runnable ones into the store. Called once at
    /// startup; `ExtensionsInstalledChanged` fires once at the end.
    pub fn load_installed_from_disk(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let fs = self.fs.clone();
        let extension_dir = self.extension_dir.clone();
        let store = self.store.clone();
        let operation_lock = self.operation_lock.clone();
        cx.spawn(async move |this, cx| {
            let _operation_guard = operation_lock.lock().await;
            let manifests = scan_manifests(&fs, &extension_dir).await;
            let loadable: Vec<ExtensionVersion> = manifests
                .iter()
                .filter(|manifest| needs_store_load(manifest))
                .map(|manifest| registry_version(manifest))
                .collect();
            // Recorded before the store loads anything: the `ExtensionsInstalledChanged`
            // the store emits while loading is the first `ExtensionsChanged` the client and
            // the supervisor see, and it must carry the whole installed set.
            this.update(cx, |this, _| {
                this.manifests = manifests
                    .into_iter()
                    .map(|manifest| (manifest.id.clone(), manifest))
                    .collect();
            })?;
            let mut store_notified = false;
            if !loadable.is_empty() {
                let loadable_ids: Vec<Arc<str>> = loadable
                    .iter()
                    .map(|extension| Arc::from(extension.id.as_str()))
                    .collect();
                let missing = store
                    .update(cx, |store, cx| store.sync_extensions(loadable, cx))
                    .await
                    .context("loading installed extensions")?;
                for extension in &missing {
                    log::warn!(
                        "installed extension {} {} did not load; it stays on disk",
                        extension.id,
                        extension.version
                    );
                }
                let missing_ids: BTreeSet<&str> = missing
                    .iter()
                    .map(|extension| extension.id.as_str())
                    .collect();
                let loaded: Vec<Arc<str>> = loadable_ids
                    .into_iter()
                    .filter(|id| !missing_ids.contains(id.as_ref()))
                    .collect();
                store_notified = !loaded.is_empty();
                this.update(cx, |this, _| this.store_loaded.extend(loaded))?;
            }
            if !store_notified {
                this.update(cx, |_, cx| notify_installed_changed(cx))?;
            }
            Ok(())
        })
    }

    /// Searches the registry; suppressed and incompatible extensions are filtered out.
    pub fn search_registry(
        &self,
        search: Option<String>,
        cx: &Context<Self>,
    ) -> Task<Result<Vec<ExtensionMetadata>>> {
        let http = self.registry.http.clone();
        let release_channel = self.registry.release_channel;
        cx.background_spawn(async move {
            let max_schema_version = schema_version_range().end().to_string();
            let mut query = vec![("max_schema_version", max_schema_version.as_str())];
            if let Some(search) = search.as_deref() {
                query.push(("filter", search));
            }
            let url = http.build_zed_api_url("/extensions", &query)?;
            let mut response = http
                .get(url.as_ref(), AsyncBody::empty(), true)
                .await
                .context("querying the extension registry")?;
            let body = read_bounded(response.body_mut(), MAX_REGISTRY_RESPONSE_BYTES)
                .await
                .context("reading the extension registry response")?;
            anyhow::ensure!(
                response.status().is_success(),
                "extension registry answered {}: {}",
                response.status().as_u16(),
                String::from_utf8_lossy(&body[..body.len().min(MAX_ERROR_BODY_BYTES as usize)])
            );
            let mut parsed: GetExtensionsResponse =
                serde_json::from_slice(&body).context("parsing the extension registry response")?;
            parsed.data.retain(|extension| {
                !extension_host::is_suppressed_extension(&extension.id)
                    && is_version_compatible(release_channel, extension)
            });
            Ok(parsed.data)
        })
    }

    /// Downloads `id` (latest compatible unless `version` is pinned), unpacks it under
    /// `staging/`, validates the manifest and moves it into place; runnable extensions go
    /// through the store's `install_extension`. A replay of a finished install reinstalls
    /// the same version, which is harmless.
    pub fn install_from_registry(
        &mut self,
        id: Arc<str>,
        version: Option<Arc<str>>,
        cx: &mut Context<Self>,
    ) -> Task<Result<InstalledExtensionRecord>> {
        let install = self.spawn_registry_install(id.clone(), version, false, cx);
        cx.spawn(async move |_, _| {
            install
                .await?
                .with_context(|| format!("extension {id} was not installed"))
        })
    }

    /// The supervisor's install list (`POST /control/extensions`, resent on every boot):
    /// installs `id` from the registry unless it is already installed once the operation
    /// lock is held, so a list that arrives during the startup scan does not re-download
    /// what the scan is about to find. Resolves to `Ok(None)` when nothing was done.
    pub fn install_if_missing(
        &mut self,
        id: Arc<str>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Option<InstalledExtensionRecord>>> {
        self.spawn_registry_install(id, None, true, cx)
    }

    fn spawn_registry_install(
        &mut self,
        id: Arc<str>,
        version: Option<Arc<str>>,
        skip_if_installed: bool,
        cx: &mut Context<Self>,
    ) -> Task<Result<Option<InstalledExtensionRecord>>> {
        if !is_valid_extension_id(&id) {
            return Task::ready(Err(anyhow::anyhow!("invalid extension id {id:?}")));
        }
        if extension_host::is_suppressed_extension(&id) {
            return Task::ready(Err(anyhow::anyhow!(
                "extension {id} is built into the editor and cannot be installed"
            )));
        }
        if let Some(version) = &version
            && !is_valid_extension_version(version)
        {
            return Task::ready(Err(anyhow::anyhow!(
                "invalid extension version {version:?} for {id}"
            )));
        }
        let url = match &version {
            Some(version) => versioned_download_url(&self.registry.http, &id, version),
            None => latest_download_url(&self.registry.http, &id, self.registry.release_channel),
        };
        let url = match url {
            Ok(url) => url,
            Err(error) => return Task::ready(Err(error)),
        };
        let http = self.registry.http.clone();
        let fs = self.fs.clone();
        let extension_dir = self.extension_dir.clone();
        let store = self.store.clone();
        let operation_lock = self.operation_lock.clone();
        cx.spawn(async move |this, cx| {
            let _operation_guard = operation_lock.lock().await;
            if skip_if_installed && this.read_with(cx, |this, _| this.is_installed(&id))? {
                log::debug!("extension {id} is already installed; not reinstalling it");
                return Ok(None);
            }
            log::info!("installing extension {id} from the registry ({url})");
            let staging_dir = extension_dir
                .join(STAGING_DIR)
                .join(uuid::Uuid::new_v4().to_string());
            let result = async {
                let bytes = cx.background_spawn(download(http, url)).await?;
                fs.create_dir(&staging_dir)
                    .await
                    .with_context(|| format!("creating {staging_dir:?}"))?;
                unpack_archive(fs.as_ref(), &bytes, &staging_dir, MAX_UNPACKED_BYTES).await?;
                let manifest = Arc::new(
                    ExtensionManifest::load(fs.clone(), &staging_dir)
                        .await
                        .context("reading the downloaded extension's manifest")?,
                );
                anyhow::ensure!(
                    manifest.id == id,
                    "downloaded extension is {:?}, not {id:?}",
                    manifest.id
                );
                if let Some(version) = &version {
                    anyhow::ensure!(
                        manifest.version == *version,
                        "downloaded extension {id} is version {}, not {version}",
                        manifest.version
                    );
                }
                Self::install_unpacked(
                    &this,
                    &store,
                    &fs,
                    &extension_dir,
                    manifest.clone(),
                    registry_version(&manifest),
                    staging_dir.clone(),
                    cx,
                )
                .await?;
                anyhow::Ok(manifest)
            }
            .await;
            if result.is_err() {
                fs.remove_dir(
                    &staging_dir,
                    RemoveOptions {
                        recursive: true,
                        ignore_if_not_exists: true,
                    },
                )
                .await
                .log_err();
            }
            let manifest = result.with_context(|| format!("installing extension {id}"))?;
            Ok(Some(InstalledExtensionRecord::from_manifest(
                &manifest, false,
            )))
        })
    }

    /// The SSH-era `InstallExtension` in sandbox mode: installs the extension the client
    /// uploaded to `tmp_dir`, which must be a directory directly under
    /// `paths::remote_extensions_uploads_dir()` (after symlink resolution) whose manifest
    /// carries the requested id and version. The id and version are validated first, so a
    /// client cannot name a path outside the extensions directory or move an arbitrary
    /// directory into it. `dev` is recorded for the installed set.
    pub fn install_uploaded(
        &mut self,
        extension: ExtensionVersion,
        tmp_dir: PathBuf,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        if !is_valid_extension_id(&extension.id) {
            return Task::ready(Err(anyhow::anyhow!(
                "invalid extension id {:?}",
                extension.id
            )));
        }
        if !is_valid_extension_version(&extension.version) {
            return Task::ready(Err(anyhow::anyhow!(
                "invalid extension version {:?} for {}",
                extension.version,
                extension.id
            )));
        }
        let fs = self.fs.clone();
        let extension_dir = self.extension_dir.clone();
        let store = self.store.clone();
        let operation_lock = self.operation_lock.clone();
        cx.spawn(async move |this, cx| {
            let _operation_guard = operation_lock.lock().await;
            let id: Arc<str> = extension.id.as_str().into();
            // Checked before anything else: a directory that is not an upload is never
            // touched, not even by the cleanup below.
            let uploads_dir = paths::remote_extensions_uploads_dir();
            let canonical_tmp_dir = fs
                .canonicalize(&tmp_dir)
                .await
                .with_context(|| format!("resolving the upload directory {tmp_dir:?}"))?;
            let canonical_uploads_dir = fs
                .canonicalize(uploads_dir)
                .await
                .unwrap_or_else(|_| uploads_dir.clone());
            anyhow::ensure!(
                canonical_tmp_dir.parent() == Some(canonical_uploads_dir.as_path()),
                "upload directory {tmp_dir:?} is not directly under {uploads_dir:?}"
            );
            let result = async {
                let manifest = Arc::new(
                    ExtensionManifest::load(fs.clone(), &tmp_dir)
                        .await
                        .context("reading the uploaded extension's manifest")?,
                );
                anyhow::ensure!(
                    manifest.id == id,
                    "uploaded extension is {:?}, not {id:?}",
                    manifest.id
                );
                anyhow::ensure!(
                    manifest.version.as_ref() == extension.version,
                    "uploaded extension {id} is version {}, not {}",
                    manifest.version,
                    extension.version
                );
                Self::install_unpacked(
                    &this,
                    &store,
                    &fs,
                    &extension_dir,
                    manifest,
                    extension,
                    tmp_dir.clone(),
                    cx,
                )
                .await
            }
            .await;
            if result.is_err() {
                fs.remove_dir(
                    &tmp_dir,
                    RemoveOptions {
                        recursive: true,
                        ignore_if_not_exists: true,
                    },
                )
                .await
                .log_err();
            }
            result.with_context(|| format!("installing uploaded extension {id}"))
        })
    }

    /// Installs the unpacked extension at `source_dir` (consumed: moved into place, or
    /// removed by the store when its load fails): records the manifest, then loads a
    /// runnable extension through the store or moves an asset-only one into place, and
    /// rolls the record back if that fails.
    async fn install_unpacked(
        this: &WeakEntity<Self>,
        store: &Entity<HeadlessExtensionStore>,
        fs: &Arc<dyn Fs>,
        extension_dir: &Path,
        manifest: Arc<ExtensionManifest>,
        version: ExtensionVersion,
        source_dir: PathBuf,
        cx: &mut AsyncApp,
    ) -> Result<()> {
        let id = manifest.id.clone();
        let runnable = needs_store_load(&manifest);
        // Recorded before the store installs so the `ExtensionsInstalledChanged` it emits
        // already sees the new extension.
        let previous = this.update(cx, |this, _| this.record(manifest, version.dev))?;
        let install = async {
            if runnable {
                store
                    .update(cx, |store, cx| {
                        store.install_extension(version, source_dir.clone(), cx)
                    })
                    .await
            } else {
                move_into_place(fs.as_ref(), &source_dir, &extension_dir.join(id.as_ref())).await
            }
        }
        .await;
        match install {
            Ok(()) => this.update(cx, |this, cx| {
                if runnable {
                    this.store_loaded.insert(id);
                } else {
                    this.store_loaded.remove(&id);
                    notify_installed_changed(cx);
                }
            }),
            Err(error) => {
                this.update(cx, |this, _| this.restore_record(&id, previous))?;
                Err(error)
            }
        }
    }

    /// Removes `id` from the store and from disk. Idempotent: an id that is not installed
    /// resolves to `Ok` (a replayed request after a lost response lands here).
    pub fn uninstall_by_id(&mut self, id: Arc<str>, cx: &mut Context<Self>) -> Task<Result<()>> {
        if !is_valid_extension_id(&id) {
            return Task::ready(Err(anyhow::anyhow!("invalid extension id {id:?}")));
        }
        let fs = self.fs.clone();
        let extension_dir = self.extension_dir.clone();
        let store = self.store.clone();
        let operation_lock = self.operation_lock.clone();
        cx.spawn(async move |this, cx| {
            let _operation_guard = operation_lock.lock().await;
            let (removed, was_loaded, remaining) = this.update(cx, |this, _| {
                this.dev_ids.remove(&id);
                let removed = this.manifests.remove(&id).is_some();
                let was_loaded = this.store_loaded.remove(&id);
                let remaining: Vec<ExtensionVersion> = this
                    .manifests
                    .values()
                    .filter(|manifest| this.store_loaded.contains(&manifest.id))
                    .map(|manifest| registry_version(manifest))
                    .collect();
                (removed, was_loaded, remaining)
            })?;
            if was_loaded {
                // The store's sync unloads (and deletes from disk) every loaded extension
                // absent from the list, and notifies once.
                store
                    .update(cx, |store, cx| store.sync_extensions(remaining, cx))
                    .await
                    .with_context(|| format!("uninstalling extension {id}"))?;
            }
            // An extension the store never loaded (asset-only, or a runnable one whose load
            // failed) is not the store's to delete; for one it just unloaded this is a no-op.
            fs.remove_dir(
                &extension_dir.join(id.as_ref()),
                RemoveOptions {
                    recursive: true,
                    ignore_if_not_exists: true,
                },
            )
            .await
            .with_context(|| format!("removing extension {id}"))?;
            if removed && !was_loaded {
                this.update(cx, |_, cx| notify_installed_changed(cx))?;
            }
            Ok(())
        })
    }

    /// The SSH-era `SyncExtensions` in sandbox mode: additive and version-tolerant. Nothing
    /// is removed (the desktop's list is not authoritative for a sandbox, whose extensions
    /// come from the registry), and an installed id satisfies the request whatever its
    /// version; only dev extensions, which the desktop re-sends on every connect, and ids
    /// not installed at all are reported missing.
    pub fn missing_after_sync(&self, requested: Vec<ExtensionVersion>) -> Vec<ExtensionVersion> {
        requested
            .into_iter()
            .filter(|extension| extension.dev || !self.is_installed(&extension.id))
            .collect()
    }

    /// `ListExtensions` handler.
    pub async fn handle_list_extensions(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::ListExtensions>,
        mut cx: AsyncApp,
    ) -> Result<proto::ListExtensionsResponse> {
        if let Some(search) = &envelope.payload.search {
            anyhow::ensure!(
                search.len() <= MAX_SEARCH_BYTES,
                "search is {} bytes, over the {MAX_SEARCH_BYTES} byte limit",
                search.len()
            );
        }
        let installed = this.read_with(&cx, |this, _| {
            this.installed_extension_records()
                .iter()
                .map(InstalledExtensionRecord::to_proto)
                .collect()
        });
        let available = if envelope.payload.include_available {
            this.update(&mut cx, |this, cx| {
                this.search_registry(envelope.payload.search, cx)
            })
            .await?
            .into_iter()
            .map(available_to_proto)
            .collect()
        } else {
            Vec::new()
        };
        Ok(proto::ListExtensionsResponse {
            installed,
            available,
        })
    }

    /// `InstallRegistryExtension` handler.
    pub async fn handle_install_registry_extension(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::InstallRegistryExtension>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let id: Arc<str> = envelope.payload.id.into();
        anyhow::ensure!(is_valid_extension_id(&id), "invalid extension id {id:?}");
        this.update(&mut cx, |this, cx| {
            this.install_from_registry(id, envelope.payload.version.map(Into::into), cx)
        })
        .await?;
        Ok(proto::Ack {})
    }

    /// `UninstallExtension` handler.
    pub async fn handle_uninstall_extension(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::UninstallExtension>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let id: Arc<str> = envelope.payload.id.into();
        anyhow::ensure!(is_valid_extension_id(&id), "invalid extension id {id:?}");
        this.update(&mut cx, |this, cx| this.uninstall_by_id(id, cx))
            .await?;
        Ok(proto::Ack {})
    }

    /// `SyncExtensions` handler for sandbox mode: see `missing_after_sync`. The upload
    /// directory answered is the same one the SSH store names.
    pub async fn handle_sync_extensions(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::SyncExtensions>,
        cx: AsyncApp,
    ) -> Result<proto::SyncExtensionsResponse> {
        let requested: Vec<ExtensionVersion> = envelope
            .payload
            .extensions
            .into_iter()
            .map(|extension| ExtensionVersion {
                id: extension.id,
                version: extension.version,
                dev: extension.dev,
                content_fingerprint: extension.content_fingerprint,
            })
            .collect();
        let missing = this.read_with(&cx, |this, _| this.missing_after_sync(requested));
        Ok(proto::SyncExtensionsResponse {
            missing_extensions: missing
                .into_iter()
                .map(|extension| proto::Extension {
                    id: extension.id,
                    version: extension.version,
                    dev: extension.dev,
                    content_fingerprint: extension.content_fingerprint,
                })
                .collect(),
            tmp_dir: paths::remote_extensions_uploads_dir()
                .to_string_lossy()
                .to_string(),
        })
    }

    /// `InstallExtension` handler for sandbox mode: see `install_uploaded`.
    pub async fn handle_install_extension(
        this: Entity<Self>,
        envelope: TypedEnvelope<proto::InstallExtension>,
        mut cx: AsyncApp,
    ) -> Result<proto::Ack> {
        let extension = envelope
            .payload
            .extension
            .context("Invalid InstallExtension request")?;
        this.update(&mut cx, |this, cx| {
            this.install_uploaded(
                ExtensionVersion {
                    id: extension.id,
                    version: extension.version,
                    dev: extension.dev,
                    content_fingerprint: extension.content_fingerprint,
                },
                PathBuf::from(envelope.payload.tmp_dir),
                cx,
            )
        })
        .await?;
        Ok(proto::Ack {})
    }
}

/// The registry's latest-compatible download URL for `extension_id`.
pub fn latest_download_url(
    http: &HttpClientWithUrl,
    extension_id: &str,
    release_channel: ReleaseChannel,
) -> Result<Url> {
    let schema_versions = schema_version_range();
    let wasm_api_versions = wasm_api_version_range(release_channel);
    http.build_zed_api_url(
        &format!("/extensions/{extension_id}/download"),
        &[
            ("min_schema_version", &schema_versions.start().to_string()),
            ("max_schema_version", &schema_versions.end().to_string()),
            (
                "min_wasm_api_version",
                &wasm_api_versions.start().to_string(),
            ),
            ("max_wasm_api_version", &wasm_api_versions.end().to_string()),
        ],
    )
}

/// The registry's download URL for one pinned version; `Err` unless both the id and the
/// version have the shapes the registry issues (they become path segments).
pub fn versioned_download_url(
    http: &HttpClientWithUrl,
    extension_id: &str,
    version: &str,
) -> Result<Url> {
    anyhow::ensure!(
        is_valid_extension_id(extension_id),
        "invalid extension id {extension_id:?}"
    );
    anyhow::ensure!(
        is_valid_extension_version(version),
        "invalid extension version {version:?} for {extension_id}"
    );
    http.build_zed_api_url(
        &format!("/extensions/{extension_id}/{version}/download"),
        &[],
    )
}

/// Whether the store must load this extension (languages, language servers, debug adapters);
/// everything else is asset-only and lives on disk for the asset route.
fn needs_store_load(manifest: &ExtensionManifest) -> bool {
    !manifest.languages.is_empty() || manifest.allow_remote_load()
}

fn registry_version(manifest: &ExtensionManifest) -> ExtensionVersion {
    ExtensionVersion {
        id: manifest.id.to_string(),
        version: manifest.version.to_string(),
        dev: false,
        content_fingerprint: None,
    }
}

fn available_to_proto(extension: ExtensionMetadata) -> proto::AvailableExtension {
    proto::AvailableExtension {
        id: extension.id.to_string(),
        version: extension.manifest.version.to_string(),
        name: extension.manifest.name,
        description: extension.manifest.description,
        authors: extension.manifest.authors,
        repository: extension.manifest.repository,
        provides: extension
            .manifest
            .provides
            .iter()
            .map(|provides| provides.to_string())
            .collect(),
        download_count: extension.download_count,
    }
}

fn notify_installed_changed(cx: &mut App) {
    if let Some(events) = ExtensionEvents::try_global(cx) {
        events.update(cx, |events, cx| {
            events.emit(extension::Event::ExtensionsInstalledChanged, cx)
        });
    }
}

async fn scan_manifests(fs: &Arc<dyn Fs>, extension_dir: &Path) -> Vec<Arc<ExtensionManifest>> {
    let mut manifests = Vec::new();
    let Ok(mut entries) = fs.read_dir(extension_dir).await else {
        return manifests;
    };
    while let Some(entry) = entries.next().await {
        let Some(path) = entry.log_err() else {
            continue;
        };
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name == WORK_DIR || name == STAGING_DIR || !is_valid_extension_id(name) {
            continue;
        }
        if !fs.is_file(&path.join(MANIFEST_FILE)).await {
            continue;
        }
        match ExtensionManifest::load(fs.clone(), &path).await {
            Ok(manifest) if manifest.id.as_ref() == name => manifests.push(Arc::new(manifest)),
            Ok(manifest) => log::warn!(
                "extension directory {path:?} holds a manifest for {:?}; skipping it",
                manifest.id
            ),
            Err(error) => log::warn!("skipping extension directory {path:?}: {error:#}"),
        }
    }
    manifests
}

async fn download(http: Arc<HttpClientWithUrl>, url: Url) -> Result<Vec<u8>> {
    let mut response = http
        .get(url.as_ref(), AsyncBody::empty(), true)
        .await
        .context("downloading the extension")?;
    let status = response.status();
    if !status.is_success() {
        let body = read_bounded(response.body_mut(), MAX_ERROR_BODY_BYTES)
            .await
            .unwrap_or_default();
        anyhow::bail!(
            "extension registry answered {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        );
    }
    if let Some(content_length) = response
        .headers()
        .get(http_client::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
    {
        anyhow::ensure!(
            content_length <= MAX_REGISTRY_DOWNLOAD_BYTES,
            "extension download of {content_length} bytes exceeds the {MAX_REGISTRY_DOWNLOAD_BYTES} byte limit"
        );
    }
    let mut bytes = Vec::new();
    let mut chunk = vec![0u8; DOWNLOAD_CHUNK_BYTES];
    loop {
        let read = response
            .body_mut()
            .read(&mut chunk)
            .await
            .context("reading the extension download")?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_REGISTRY_DOWNLOAD_BYTES,
            "extension download exceeds the {MAX_REGISTRY_DOWNLOAD_BYTES} byte limit"
        );
    }
    Ok(bytes)
}

/// Reads at most `limit` bytes of `body`; `Err` when more arrive.
async fn read_bounded(body: &mut AsyncBody, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    body.take(limit + 1).read_to_end(&mut bytes).await?;
    anyhow::ensure!(
        bytes.len() as u64 <= limit,
        "response body exceeds the {limit} byte limit"
    );
    Ok(bytes)
}

/// Fails a read once more than `remaining` bytes have come out of the archive decoder:
/// the download cap bounds the gzip, not what it decodes to.
struct BoundedReader<R> {
    inner: R,
    remaining: u64,
    limit: u64,
}

impl<R: AsyncRead + Unpin> AsyncRead for BoundedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let read = std::task::ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        if read as u64 > this.remaining {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "extension archive expands past the {} byte limit",
                    this.limit
                ),
            )));
        }
        this.remaining -= read as u64;
        Poll::Ready(Ok(read))
    }
}

/// Unpacks a gzip tarball through `Fs`, skipping entries that would escape `dest`
/// (absolute paths, `..`) and anything that is not a file or directory. `Err` once the
/// archive has expanded to more than `max_unpacked` bytes.
async fn unpack_archive(fs: &dyn Fs, bytes: &[u8], dest: &Path, max_unpacked: u64) -> Result<()> {
    let decoder = BoundedReader {
        inner: GzipDecoder::new(BufReader::new(bytes)),
        remaining: max_unpacked,
        limit: max_unpacked,
    };
    let mut entries = Archive::new(decoder)
        .entries()
        .context("reading the extension archive")?;
    while let Some(entry) = entries.next().await {
        let entry = entry.context("reading an extension archive entry")?;
        let path = PathBuf::from(entry.path()?.as_os_str());
        let Some(relative) = safe_relative_path(&path) else {
            log::warn!("skipping extension archive entry {path:?}");
            continue;
        };
        let target = dest.join(&relative);
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            fs.create_dir(&target)
                .await
                .with_context(|| format!("creating {target:?}"))?;
        } else if entry_type.is_file() {
            if let Some(parent) = target.parent() {
                fs.create_dir(parent)
                    .await
                    .with_context(|| format!("creating {parent:?}"))?;
            }
            let mut contents = Vec::new();
            Box::pin(entry)
                .read_to_end(&mut contents)
                .await
                .with_context(|| format!("reading archive entry {path:?}"))?;
            fs.write(&target, &contents)
                .await
                .with_context(|| format!("writing {target:?}"))?;
        } else {
            log::debug!("skipping extension archive entry {path:?} of type {entry_type:?}");
        }
    }
    Ok(())
}

/// `path` with `.` components dropped, provided every remaining component is a plain name.
fn safe_relative_path(path: &Path) -> Option<PathBuf> {
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => relative.push(name),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (!relative.as_os_str().is_empty()).then_some(relative)
}

async fn move_into_place(fs: &dyn Fs, staging_dir: &Path, installed_dir: &Path) -> Result<()> {
    fs.remove_dir(
        installed_dir,
        RemoveOptions {
            recursive: true,
            ignore_if_not_exists: true,
        },
    )
    .await
    .with_context(|| format!("removing {installed_dir:?}"))?;
    fs.rename(
        staging_dir,
        installed_dir,
        RenameOptions {
            overwrite: true,
            ignore_if_exists: false,
            create_parents: true,
        },
    )
    .await
    .with_context(|| format!("moving {staging_dir:?} to {installed_dir:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::futures::write::GzipEncoder;
    use extension::ExtensionHostProxy;
    use fs::FakeFs;
    use futures::AsyncWriteExt as _;
    use gpui::TestAppContext;
    use http_client::{FakeHttpClient, Response};
    use node_runtime::NodeRuntime;
    use serde_json::json;
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };
    use util::path;

    fn extensions_dir() -> PathBuf {
        PathBuf::from(path!("/extensions"))
    }

    fn manifest(id: &str, version: &str, extra: &str) -> String {
        format!(
            "id = \"{id}\"\nname = \"{id} name\"\nversion = \"{version}\"\nschema_version = 1\n{extra}\n"
        )
    }

    fn theme_manifest(id: &str, version: &str) -> String {
        manifest(id, version, &format!("themes = [\"themes/{id}.json\"]"))
    }

    async fn tarball(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = async_tar::Builder::new(Vec::new());
        for (path, contents) in entries {
            let mut header = async_tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            if path.starts_with('/') || path.contains("..") {
                // The builder refuses escaping names; a hostile archive carries them anyway,
                // so write the name field of the header directly.
                header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
                header.set_cksum();
                builder.append(&header, *contents).await.unwrap();
            } else {
                header.set_cksum();
                builder
                    .append_data(&mut header, path, *contents)
                    .await
                    .unwrap();
            }
        }
        let tar = builder.into_inner().await.unwrap();
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(&tar).await.unwrap();
        encoder.close().await.unwrap();
        encoder.into_inner()
    }

    /// A registry answering with `(status, body)` for every request whose path starts with
    /// the given prefix (first match wins), 404 otherwise; records every request path.
    fn registry(
        routes: Vec<(&'static str, u16, Vec<u8>)>,
    ) -> (Arc<HttpClientWithUrl>, Arc<Mutex<Vec<String>>>) {
        let paths = Arc::new(Mutex::new(Vec::new()));
        let client = FakeHttpClient::create({
            let paths = paths.clone();
            move |request| {
                let paths = paths.clone();
                let routes = routes.clone();
                async move {
                    let path = request.uri().path().to_owned();
                    paths.lock().unwrap().push(path.clone());
                    let (status, body) = routes
                        .iter()
                        .find(|(prefix, _, _)| path.starts_with(prefix))
                        .map(|(_, status, body)| (*status, body.clone()))
                        .unwrap_or((404, Vec::new()));
                    Ok(Response::builder()
                        .status(status)
                        .body(AsyncBody::from(body))?)
                }
            }
        });
        (client, paths)
    }

    fn sandbox_over(
        fs: &Arc<FakeFs>,
        registry: Arc<HttpClientWithUrl>,
        cx: &mut TestAppContext,
    ) -> (Entity<SandboxExtensions>, Arc<AtomicUsize>) {
        let (sandbox, installed_changed, _) = sandbox_with_languages(fs, registry, cx);
        (sandbox, installed_changed)
    }

    /// Like `sandbox_over`, also returning the language registry the store's proxy
    /// registers languages into, so a test can see what the store loaded.
    fn sandbox_with_languages(
        fs: &Arc<FakeFs>,
        registry: Arc<HttpClientWithUrl>,
        cx: &mut TestAppContext,
    ) -> (
        Entity<SandboxExtensions>,
        Arc<AtomicUsize>,
        Arc<language::LanguageRegistry>,
    ) {
        let installed_changed = Arc::new(AtomicUsize::new(0));
        cx.update({
            let installed_changed = installed_changed.clone();
            move |cx| {
                if !cx.has_global::<settings::SettingsStore>() {
                    settings::init(cx);
                }
                release_channel::init(semver::Version::new(0, 0, 0), cx);
                if ExtensionEvents::try_global(cx).is_none() {
                    extension::init(cx);
                }
                let events = ExtensionEvents::try_global(cx).unwrap();
                cx.subscribe(&events, move |_, event, _| {
                    if matches!(event, extension::Event::ExtensionsInstalledChanged) {
                        installed_changed.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .detach();
            }
        });
        let fs: Arc<dyn Fs> = fs.clone();
        let languages = Arc::new(language::LanguageRegistry::test(cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());
        language_extension::init(
            language_extension::LspAccess::Noop,
            proxy.clone(),
            languages.clone(),
        );
        let store = cx.update(|cx| {
            HeadlessExtensionStore::new(
                fs.clone(),
                FakeHttpClient::with_404_response(),
                extensions_dir(),
                proxy,
                NodeRuntime::unavailable(),
                cx,
            )
        });
        let sandbox = cx.new(|_| {
            SandboxExtensions::new(
                store,
                RegistryConfig {
                    http: registry,
                    release_channel: ReleaseChannel::Dev,
                },
                fs,
                extensions_dir(),
            )
        });
        (sandbox, installed_changed, languages)
    }

    const FOO_LANGUAGE_CONFIG: &str =
        "name = \"Foo\"\ngrammar = \"foo\"\npath_suffixes = [\"foo\"]\n";

    fn language_manifest(id: &str, version: &str) -> String {
        manifest(id, version, "languages = [\"languages/foo\"]")
    }

    /// A language extension `id` (registering the language `Foo`) as a registry tarball.
    async fn language_tarball(id: &str, version: &str) -> Vec<u8> {
        tarball(&[
            ("extension.toml", language_manifest(id, version).as_bytes()),
            ("languages/foo/config.toml", FOO_LANGUAGE_CONFIG.as_bytes()),
        ])
        .await
    }

    fn language_names(languages: &language::LanguageRegistry) -> Vec<String> {
        languages
            .language_names()
            .into_iter()
            .map(|name| name.as_ref().to_owned())
            .collect()
    }

    fn envelope<T>(payload: T) -> TypedEnvelope<T> {
        TypedEnvelope {
            sender_id: proto::REMOTE_SERVER_PEER_ID,
            original_sender_id: None,
            message_id: 1,
            payload,
            received_at: Instant::now(),
        }
    }

    #[test]
    fn is_valid_extension_id_grammar() {
        for id in ["toml", "a-b_c1", "9x", &"a".repeat(64)] {
            assert!(is_valid_extension_id(id), "{id:?}");
        }
        for id in [
            "",
            "A",
            "../x",
            "a/b",
            "-a",
            "_a",
            "a.b",
            "a b",
            &"a".repeat(65),
        ] {
            assert!(!is_valid_extension_id(id), "{id:?}");
        }
    }

    #[gpui::test]
    async fn install_theme_only_extension_lands_on_disk(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let tarball = tarball(&[
            (
                "extension.toml",
                theme_manifest("mytheme", "0.1.0").as_bytes(),
            ),
            ("themes/mytheme.json", b"{}"),
        ])
        .await;
        let (registry, paths) = registry(vec![("/extensions/mytheme/download", 200, tarball)]);
        let (sandbox, installed_changed) = sandbox_over(&fs, registry, cx);

        let record = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("mytheme".into(), None, cx)
            })
            .await
            .unwrap();
        assert_eq!(record.id.as_ref(), "mytheme");
        assert_eq!(record.version.as_ref(), "0.1.0");
        assert_eq!(record.provides, vec!["themes".to_string()]);
        assert!(!record.dev);
        assert!(
            fs.is_file(&extensions_dir().join("mytheme/themes/mytheme.json"))
                .await
        );
        assert!(
            fs.is_file(&extensions_dir().join("mytheme/extension.toml"))
                .await
        );
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);
        assert_eq!(
            sandbox.read_with(cx, |sandbox, _| sandbox.installed_extension_records()),
            vec![record]
        );
        let paths = paths.lock().unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "/extensions/mytheme/download");
        drop(paths);

        // Nothing is left in staging.
        let staging = extensions_dir().join(STAGING_DIR);
        let mut leftovers = Vec::new();
        if let Ok(mut entries) = fs.read_dir(&staging).await {
            while let Some(entry) = entries.next().await {
                leftovers.push(entry.unwrap());
            }
        }
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[gpui::test]
    async fn install_pinned_version_uses_versioned_url_and_checks_version(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let tarball = tarball(&[
            ("extension.toml", theme_manifest("foo", "1.2.4").as_bytes()),
            ("themes/foo.json", b"{}"),
        ])
        .await;
        let (registry, paths) = registry(vec![("/extensions/foo/", 200, tarball)]);
        let (sandbox, installed_changed) = sandbox_over(&fs, registry, cx);

        let error = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("foo".into(), Some("1.2.3".into()), cx)
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("1.2.4"), "{error:#}");
        assert_eq!(paths.lock().unwrap()[0], "/extensions/foo/1.2.3/download");
        assert!(!fs.is_dir(&extensions_dir().join("foo")).await);
        assert!(sandbox.read_with(cx, |sandbox, _| !sandbox.is_installed("foo")));
        assert_eq!(installed_changed.load(Ordering::SeqCst), 0);
    }

    #[gpui::test]
    async fn install_rejects_mismatched_manifest_id(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let tarball = tarball(&[
            ("extension.toml", theme_manifest("bar", "1.0.0").as_bytes()),
            ("themes/bar.json", b"{}"),
        ])
        .await;
        let (registry, _) = registry(vec![("/extensions/foo/download", 200, tarball)]);
        let (sandbox, _) = sandbox_over(&fs, registry, cx);

        let error = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("foo".into(), None, cx)
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("\"bar\""), "{error:#}");
        assert!(!fs.is_dir(&extensions_dir().join("foo")).await);
        assert!(!fs.is_dir(&extensions_dir().join("bar")).await);
    }

    #[gpui::test]
    async fn install_rejects_oversized_and_ignores_traversing_entries(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let oversized = FakeHttpClient::create(|_| async move {
            Ok(Response::builder()
                .status(200)
                .header(
                    "content-length",
                    (MAX_REGISTRY_DOWNLOAD_BYTES + 1).to_string(),
                )
                .body(AsyncBody::from("tiny"))?)
        });
        let (sandbox, _) = sandbox_over(&fs, oversized, cx);
        let error = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("big".into(), None, cx)
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("limit"), "{error:#}");

        let tarball = tarball(&[
            (
                "extension.toml",
                theme_manifest("mytheme", "0.1.0").as_bytes(),
            ),
            ("../escape.txt", b"escaped"),
            ("/abs.txt", b"absolute"),
            ("themes/mytheme.json", b"{}"),
        ])
        .await;
        let (registry, _) = registry(vec![("/extensions/mytheme/download", 200, tarball)]);
        let (sandbox, _) = sandbox_over(&fs, registry, cx);
        sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("mytheme".into(), None, cx)
            })
            .await
            .unwrap();
        assert!(
            fs.is_file(&extensions_dir().join("mytheme/themes/mytheme.json"))
                .await
        );
        assert!(!fs.is_file(Path::new(path!("/escape.txt"))).await);
        assert!(!fs.is_file(Path::new(path!("/extensions/escape.txt"))).await);
        assert!(!fs.is_file(Path::new(path!("/abs.txt"))).await);
    }

    #[gpui::test]
    async fn install_suppressed_and_traversal_ids_rejected(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let (registry, paths) = registry(vec![]);
        let (sandbox, _) = sandbox_over(&fs, registry, cx);

        let error = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("snippets".into(), None, cx)
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("built into"), "{error:#}");
        for id in ["../../..", "a/b", ""] {
            let error = sandbox
                .update(cx, |sandbox, cx| {
                    sandbox.install_from_registry(id.into(), None, cx)
                })
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("invalid extension id"),
                "{error:#}"
            );
            let error = sandbox
                .update(cx, |sandbox, cx| sandbox.uninstall_by_id(id.into(), cx))
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("invalid extension id"),
                "{error:#}"
            );
        }
        assert!(
            paths.lock().unwrap().is_empty(),
            "no registry call was made"
        );
        assert!(fs.files().is_empty(), "the filesystem is untouched");
    }

    #[gpui::test]
    async fn uninstall_removes_dir_and_record(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let tarball = tarball(&[
            (
                "extension.toml",
                theme_manifest("mytheme", "0.1.0").as_bytes(),
            ),
            ("themes/mytheme.json", b"{}"),
        ])
        .await;
        let (registry, _) = registry(vec![("/extensions/mytheme/download", 200, tarball)]);
        let (sandbox, installed_changed) = sandbox_over(&fs, registry, cx);
        sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("mytheme".into(), None, cx)
            })
            .await
            .unwrap();
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);

        sandbox
            .update(cx, |sandbox, cx| {
                sandbox.uninstall_by_id("mytheme".into(), cx)
            })
            .await
            .unwrap();
        assert!(!fs.is_dir(&extensions_dir().join("mytheme")).await);
        assert!(sandbox.read_with(cx, |sandbox, _| {
            sandbox.installed_extension_records().is_empty()
        }));
        assert_eq!(installed_changed.load(Ordering::SeqCst), 2);

        // Uninstalling again (a replayed request) is a no-op.
        sandbox
            .update(cx, |sandbox, cx| {
                sandbox.uninstall_by_id("mytheme".into(), cx)
            })
            .await
            .unwrap();
        assert_eq!(installed_changed.load(Ordering::SeqCst), 2);
    }

    #[gpui::test]
    async fn load_installed_from_disk_scans_manifests(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            extensions_dir(),
            json!({
                "mytheme": {
                    "extension.toml": theme_manifest("mytheme", "0.1.0"),
                    "themes": { "mytheme.json": "{}" }
                },
                "work": { "leftover.wasm": "" },
                "staging": { "abc": { "extension.toml": theme_manifest("abc", "1.0.0") } },
                "no-manifest": { "readme.md": "" },
                "wrong-id": { "extension.toml": theme_manifest("other", "1.0.0") },
                "Invalid.Name": { "extension.toml": theme_manifest("Invalid.Name", "1.0.0") }
            }),
        )
        .await;
        let (registry, _) = registry(vec![]);
        let (sandbox, installed_changed) = sandbox_over(&fs, registry, cx);

        sandbox
            .update(cx, |sandbox, cx| sandbox.load_installed_from_disk(cx))
            .await
            .unwrap();
        let records = sandbox.read_with(cx, |sandbox, _| sandbox.installed_extension_records());
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0].id.as_ref(), "mytheme");
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn list_extensions_includes_available_when_requested(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let listing = json!({
            "data": [{
                "id": "html",
                "name": "HTML",
                "version": "2.0.0",
                "description": "markup",
                "authors": ["zed"],
                "repository": "https://example.com/html",
                "schema_version": 1,
                "wasm_api_version": null,
                "provides": ["languages"],
                "published_at": "2024-01-01T00:00:00Z",
                "download_count": 10
            }, {
                "id": "snippets",
                "name": "Snippets",
                "version": "1.0.0",
                "description": null,
                "authors": [],
                "repository": "https://example.com/snippets",
                "schema_version": 1,
                "wasm_api_version": null,
                "provides": [],
                "published_at": "2024-01-01T00:00:00Z",
                "download_count": 1
            }]
        })
        .to_string()
        .into_bytes();
        let (registry, paths) = registry(vec![("/extensions", 200, listing)]);
        let (sandbox, _) = sandbox_over(&fs, registry.clone(), cx);

        let response = SandboxExtensions::handle_list_extensions(
            sandbox.clone(),
            envelope(proto::ListExtensions {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                search: None,
                include_available: false,
            }),
            cx.to_async(),
        )
        .await
        .unwrap();
        assert!(response.installed.is_empty());
        assert!(response.available.is_empty());
        assert!(
            paths.lock().unwrap().is_empty(),
            "no registry call without include_available"
        );

        let response = SandboxExtensions::handle_list_extensions(
            sandbox,
            envelope(proto::ListExtensions {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                search: Some("html".into()),
                include_available: true,
            }),
            cx.to_async(),
        )
        .await
        .unwrap();
        assert_eq!(
            response.available.len(),
            1,
            "suppressed extensions are filtered"
        );
        let available = &response.available[0];
        assert_eq!(available.id, "html");
        assert_eq!(available.version, "2.0.0");
        assert_eq!(available.name, "HTML");
        assert_eq!(available.description.as_deref(), Some("markup"));
        assert_eq!(available.authors, vec!["zed".to_string()]);
        assert_eq!(available.provides, vec!["languages".to_string()]);
        assert_eq!(available.download_count, 10);
        assert_eq!(
            paths.lock().unwrap().as_slice(),
            &["/extensions".to_string()]
        );

        let error = SandboxExtensions::handle_list_extensions(
            sandbox_over(&fs, registry, cx).0,
            envelope(proto::ListExtensions {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
                search: Some("x".repeat(MAX_SEARCH_BYTES + 1)),
                include_available: true,
            }),
            cx.to_async(),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("limit"), "{error:#}");
        assert_eq!(
            paths.lock().unwrap().len(),
            1,
            "an over-long search never reaches the registry"
        );
    }

    #[test]
    fn is_valid_extension_version_grammar() {
        for version in [
            "0.1.0",
            "1.2.3",
            "10.20.30",
            "1.0.0-beta.1",
            "1.0.0+build-7",
        ] {
            assert!(is_valid_extension_version(version), "{version:?}");
        }
        for version in [
            "",
            "1",
            "1.2",
            "1.2.3.4",
            "v1.2.3",
            "1.2.3/",
            "../../internal/endpoint?x=",
            "1.2.3-",
            "1.2.3-a/b",
            "1.2.3#frag",
            &format!("1.2.3-{}", "a".repeat(MAX_VERSION_SUFFIX_BYTES + 1)),
        ] {
            assert!(!is_valid_extension_version(version), "{version:?}");
        }
    }

    #[gpui::test]
    async fn install_language_extension_loads_into_store(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let tarball = language_tarball("foo", "1.0.0").await;
        let (registry, _) = registry(vec![("/extensions/foo/download", 200, tarball)]);
        let (sandbox, installed_changed, languages) = sandbox_with_languages(&fs, registry, cx);

        let record = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("foo".into(), None, cx)
            })
            .await
            .unwrap();
        assert_eq!(record.version.as_ref(), "1.0.0");
        assert_eq!(record.provides, vec!["languages".to_string()]);
        assert!(
            fs.is_file(&extensions_dir().join("foo/languages/foo/config.toml"))
                .await
        );
        assert!(sandbox.read_with(cx, |sandbox, _| sandbox.is_loaded("foo")));
        assert!(
            language_names(&languages).contains(&"Foo".to_string()),
            "the store registered the extension's language: {:?}",
            language_names(&languages)
        );
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);

        sandbox
            .update(cx, |sandbox, cx| sandbox.uninstall_by_id("foo".into(), cx))
            .await
            .unwrap();
        assert!(!fs.is_dir(&extensions_dir().join("foo")).await);
        assert!(sandbox.read_with(cx, |sandbox, _| {
            !sandbox.is_loaded("foo") && !sandbox.is_installed("foo")
        }));
        assert!(
            !language_names(&languages).contains(&"Foo".to_string()),
            "the store unloaded the language"
        );
        assert_eq!(
            installed_changed.load(Ordering::SeqCst),
            2,
            "one notification per install and per uninstall"
        );
    }

    #[gpui::test]
    async fn startup_scan_notifies_with_the_full_installed_set(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            extensions_dir(),
            json!({
                "foo": {
                    "extension.toml": language_manifest("foo", "1.0.0"),
                    "languages": { "foo": { "config.toml": FOO_LANGUAGE_CONFIG } }
                },
                "mytheme": {
                    "extension.toml": theme_manifest("mytheme", "0.1.0"),
                    "themes": { "mytheme.json": "{}" }
                }
            }),
        )
        .await;
        let (registry, _) = registry(vec![]);
        let (sandbox, installed_changed, languages) = sandbox_with_languages(&fs, registry, cx);

        // What a subscriber (the `ExtensionsChanged` sender) sees at each notification.
        let seen: Arc<Mutex<Vec<Vec<String>>>> = Arc::default();
        cx.update({
            let seen = seen.clone();
            let sandbox = sandbox.clone();
            move |cx| {
                let events = ExtensionEvents::try_global(cx).unwrap();
                cx.subscribe(&events, move |_, event, cx| {
                    if matches!(event, extension::Event::ExtensionsInstalledChanged) {
                        let ids = sandbox
                            .read(cx)
                            .installed_extension_records()
                            .into_iter()
                            .map(|record| record.id.to_string())
                            .collect();
                        seen.lock().unwrap().push(ids);
                    }
                })
                .detach();
            }
        });

        sandbox
            .update(cx, |sandbox, cx| sandbox.load_installed_from_disk(cx))
            .await
            .unwrap();
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[vec!["foo".to_string(), "mytheme".to_string()]],
            "the store's notification already carries the whole installed set"
        );
        assert!(sandbox.read_with(cx, |sandbox, _| sandbox.is_loaded("foo")));
        assert!(sandbox.read_with(cx, |sandbox, _| !sandbox.is_loaded("mytheme")));
        assert!(language_names(&languages).contains(&"Foo".to_string()));
    }

    #[gpui::test]
    async fn uninstall_removes_a_runnable_extension_that_failed_to_load(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        // A language extension without its language config: the store cannot load it.
        fs.insert_tree(
            extensions_dir(),
            json!({
                "foo": {
                    "extension.toml": language_manifest("foo", "1.0.0"),
                    "languages": { "foo": { "readme.md": "" } }
                }
            }),
        )
        .await;
        let (registry, _) = registry(vec![]);
        let (sandbox, installed_changed) = sandbox_over(&fs, registry, cx);

        sandbox
            .update(cx, |sandbox, cx| sandbox.load_installed_from_disk(cx))
            .await
            .unwrap();
        assert!(sandbox.read_with(cx, |sandbox, _| {
            sandbox.is_installed("foo") && !sandbox.is_loaded("foo")
        }));
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);

        sandbox
            .update(cx, |sandbox, cx| sandbox.uninstall_by_id("foo".into(), cx))
            .await
            .unwrap();
        assert!(
            !fs.is_dir(&extensions_dir().join("foo")).await,
            "the directory is removed even though the store never loaded it"
        );
        assert!(sandbox.read_with(cx, |sandbox, _| {
            sandbox.installed_extension_records().is_empty()
        }));
        assert_eq!(
            installed_changed.load(Ordering::SeqCst),
            2,
            "the client learns about the removal"
        );
    }

    #[gpui::test]
    async fn install_if_missing_skips_installed_ids(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let tarball = tarball(&[
            (
                "extension.toml",
                theme_manifest("mytheme", "0.1.0").as_bytes(),
            ),
            ("themes/mytheme.json", b"{}"),
        ])
        .await;
        let (registry, paths) = registry(vec![("/extensions/mytheme/download", 200, tarball)]);
        let (sandbox, installed_changed) = sandbox_over(&fs, registry, cx);

        let record = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_if_missing("mytheme".into(), cx)
            })
            .await
            .unwrap();
        assert!(record.is_some(), "a missing extension is installed");
        assert_eq!(paths.lock().unwrap().len(), 1);

        let record = sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_if_missing("mytheme".into(), cx)
            })
            .await
            .unwrap();
        assert!(record.is_none(), "an installed extension is left alone");
        assert_eq!(paths.lock().unwrap().len(), 1, "no second download");
        assert_eq!(installed_changed.load(Ordering::SeqCst), 1);

        // A plain install of an installed id still reinstalls (the client's explicit
        // request may be an upgrade).
        sandbox
            .update(cx, |sandbox, cx| {
                sandbox.install_from_registry("mytheme".into(), None, cx)
            })
            .await
            .unwrap();
        assert_eq!(paths.lock().unwrap().len(), 2);
    }

    #[gpui::test]
    async fn install_if_missing_waits_for_the_startup_scan(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            extensions_dir(),
            json!({
                "mytheme": {
                    "extension.toml": theme_manifest("mytheme", "0.1.0"),
                    "themes": { "mytheme.json": "{}" }
                }
            }),
        )
        .await;
        let (registry, paths) = registry(vec![]);
        let (sandbox, _) = sandbox_over(&fs, registry, cx);

        // The supervisor's list arrives while the scan runs (the scan holds the lock).
        let scan = sandbox.update(cx, |sandbox, cx| sandbox.load_installed_from_disk(cx));
        let install = sandbox.update(cx, |sandbox, cx| {
            sandbox.install_if_missing("mytheme".into(), cx)
        });
        scan.await.unwrap();
        assert!(install.await.unwrap().is_none());
        assert!(
            paths.lock().unwrap().is_empty(),
            "the extension the scan found is not downloaded again"
        );
    }

    #[gpui::test]
    async fn pinned_version_must_be_a_semantic_version(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let (registry, paths) = registry(vec![]);
        let (sandbox, _) = sandbox_over(&fs, registry, cx);
        for version in ["../../internal/endpoint?x=", "1.2.3/../../x", "v1"] {
            let error = sandbox
                .update(cx, |sandbox, cx| {
                    sandbox.install_from_registry("toml".into(), Some(version.into()), cx)
                })
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("invalid extension version"),
                "{error:#}"
            );
        }
        assert!(
            paths.lock().unwrap().is_empty(),
            "no registry call was made"
        );
    }

    #[gpui::test]
    async fn unpack_archive_stops_past_the_expansion_limit(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let zeros = vec![0u8; 64 * 1024];
        let tarball = tarball(&[
            ("extension.toml", theme_manifest("big", "0.1.0").as_bytes()),
            ("big.bin", zeros.as_slice()),
        ])
        .await;
        assert!(
            tarball.len() < 4 * 1024,
            "the zeros compress well: {} bytes",
            tarball.len()
        );

        let dest = extensions_dir().join(STAGING_DIR).join("bounded");
        let error = unpack_archive(fs.as_ref(), &tarball, &dest, 16 * 1024)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("limit"), "{error:#}");
        assert!(!fs.is_file(&dest.join("big.bin")).await);

        unpack_archive(fs.as_ref(), &tarball, &dest, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(
            fs.load_bytes(&dest.join("big.bin")).await.unwrap().len(),
            zeros.len()
        );
    }

    #[gpui::test]
    async fn sandbox_sync_is_additive_and_uploads_are_validated(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let theme = tarball(&[
            (
                "extension.toml",
                theme_manifest("mytheme", "0.1.0").as_bytes(),
            ),
            ("themes/mytheme.json", b"{}"),
        ])
        .await;
        let foo = language_tarball("foo", "1.0.0").await;
        let (registry, _) = registry(vec![
            ("/extensions/mytheme/download", 200, theme),
            ("/extensions/foo/download", 200, foo),
        ]);
        let (sandbox, installed_changed, languages) = sandbox_with_languages(&fs, registry, cx);
        for id in ["mytheme", "foo"] {
            sandbox
                .update(cx, |sandbox, cx| {
                    sandbox.install_from_registry(id.into(), None, cx)
                })
                .await
                .unwrap();
        }
        assert_eq!(installed_changed.load(Ordering::SeqCst), 2);

        let sync = |extensions: Vec<proto::Extension>, cx: &mut TestAppContext| {
            SandboxExtensions::handle_sync_extensions(
                sandbox.clone(),
                envelope(proto::SyncExtensions { extensions }),
                cx.to_async(),
            )
        };
        let extension = |id: &str, version: &str, dev: bool| proto::Extension {
            id: id.into(),
            version: version.into(),
            dev,
            content_fingerprint: None,
        };

        // The desktop's empty list removes nothing.
        let response = sync(vec![], cx).await.unwrap();
        assert!(response.missing_extensions.is_empty());
        assert_eq!(
            response.tmp_dir,
            paths::remote_extensions_uploads_dir().to_string_lossy()
        );
        cx.run_until_parked();
        assert!(fs.is_dir(&extensions_dir().join("mytheme")).await);
        assert!(fs.is_dir(&extensions_dir().join("foo")).await);
        assert!(sandbox.read_with(cx, |sandbox, _| sandbox.is_loaded("foo")));
        assert!(language_names(&languages).contains(&"Foo".to_string()));
        assert_eq!(
            installed_changed.load(Ordering::SeqCst),
            2,
            "nothing changed"
        );

        // Installed ids satisfy the request at any version; unknown ids and dev
        // extensions are reported missing.
        let response = sync(
            vec![
                extension("foo", "9.9.9", false),
                extension("mytheme", "0.0.1", false),
                extension("bar", "1.0.0", false),
                extension("baz", "1.0.0", true),
            ],
            cx,
        )
        .await
        .unwrap();
        let missing: Vec<&str> = response
            .missing_extensions
            .iter()
            .map(|extension| extension.id.as_str())
            .collect();
        assert_eq!(missing, vec!["bar", "baz"]);
        assert!(sandbox.read_with(cx, |sandbox, _| sandbox.is_installed("foo")));

        // Uploads: the id grammar and the upload directory are enforced before anything
        // is moved or removed.
        let workspace = PathBuf::from(path!("/workspaces/repo"));
        fs.insert_tree(
            &workspace,
            json!({ "extension.toml": theme_manifest("evil", "0.0.1"), "src": { "main.rs": "" } }),
        )
        .await;
        let upload =
            |id: &str, version: &str, dev: bool, tmp_dir: PathBuf, cx: &mut TestAppContext| {
                SandboxExtensions::handle_install_extension(
                    sandbox.clone(),
                    envelope(proto::InstallExtension {
                        extension: Some(extension(id, version, dev)),
                        tmp_dir: tmp_dir.to_string_lossy().into_owned(),
                    }),
                    cx.to_async(),
                )
            };
        let error = upload(
            "../../../../workspaces/repo",
            "0.0.1",
            true,
            workspace.clone(),
            cx,
        )
        .await
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("invalid extension id"),
            "{error:#}"
        );
        let error = upload("evil", "0.0.1", true, workspace.clone(), cx)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("not directly under"),
            "{error:#}"
        );
        let error = upload("evil", "0.0.1/../x", true, workspace.clone(), cx)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("invalid extension version"),
            "{error:#}"
        );
        assert!(
            fs.is_file(&workspace.join("src/main.rs")).await
                && fs.is_file(&workspace.join("extension.toml")).await,
            "a rejected upload touches nothing"
        );
        assert!(!fs.is_dir(&extensions_dir().join("evil")).await);
        assert!(sandbox.read_with(cx, |sandbox, _| !sandbox.is_installed("evil")));

        // A directory the client uploaded under the uploads directory installs, and its
        // `dev` flag reaches the installed set.
        let uploaded = paths::remote_extensions_uploads_dir().join("up");
        fs.insert_tree(
            &uploaded,
            json!({ "extension.toml": theme_manifest("up", "0.2.0"), "themes": { "up.json": "{}" } }),
        )
        .await;
        let error = upload("up", "0.3.0", true, uploaded.clone(), cx)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("version"), "{error:#}");
        assert!(
            !fs.is_dir(&uploaded).await,
            "a rejected upload is cleaned up"
        );
        fs.insert_tree(
            &uploaded,
            json!({ "extension.toml": theme_manifest("up", "0.2.0"), "themes": { "up.json": "{}" } }),
        )
        .await;
        upload("up", "0.2.0", true, uploaded.clone(), cx)
            .await
            .unwrap();
        assert!(
            fs.is_file(&extensions_dir().join("up/themes/up.json"))
                .await
        );
        assert!(
            !fs.is_dir(&uploaded).await,
            "the upload was moved into place"
        );
        let records = sandbox.read_with(cx, |sandbox, _| sandbox.installed_extension_records());
        let up = records
            .iter()
            .find(|record| record.id.as_ref() == "up")
            .expect("the upload is installed");
        assert!(up.dev);
        assert_eq!(installed_changed.load(Ordering::SeqCst), 3);
    }
}
