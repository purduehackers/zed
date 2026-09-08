//! Browser-facing store for the native Extensions UI. Executable extension code
//! stays in the remote host; only declarative editor assets enter this process.

pub use crate::extension_settings::ExtensionSettings;
use crate::language_assets::{discover_query_files, load_plugin_language};
use anyhow::{Context as _, Result, anyhow, ensure};
use cloud_api_types::{ExtensionMetadata, ExtensionProvides};
use collections::{BTreeMap, BTreeSet};
pub use extension::ExtensionManifest;
use extension::{
    ExtensionGrammarProxy, ExtensionHostProxy, ExtensionLanguageProxy, ExtensionSnippetProxy,
    ExtensionThemeProxy,
};
use fs::{Fs, RemoveOptions};
use futures::FutureExt;
use gpui::UpdateGlobal;
use gpui::{
    App, AppContext, Context, Entity, EventEmitter, Global, Subscription, Task, TaskExt, actions,
};
use language::{LanguageConfig, QueryFiles};
use project::remote_extension_store::{
    InstalledExtension, RemoteExtensionEvent, RemoteExtensionStore,
};
use settings::{SemanticTokenRules, Settings, SettingsStore};
use std::{path::PathBuf, sync::Arc};

type AssetDownload = dyn Fn(Arc<str>, &mut App) -> Task<Result<PathBuf>> + Send + Sync;
type DevInstall = dyn Fn(PathBuf, &mut App) -> Task<Result<()>> + Send + Sync;
type ReportError = dyn Fn(String, &mut App) + Send + Sync;

/// Platform services: authenticated asset transfer and sandbox compilation.
pub struct BrowserExtensionBackend {
    pub download: Arc<AssetDownload>,
    pub install_dev: Arc<DevInstall>,
    pub report_error: Arc<ReportError>,
}

#[derive(Clone, Copy)]
pub enum ExtensionOperation {
    Install,
    Upgrade,
    Remove,
}

#[derive(Clone)]
pub struct ExtensionIndexEntry {
    pub manifest: Arc<ExtensionManifest>,
    pub dev: bool,
    revision: u64,
}

#[derive(Clone)]
pub enum Event {
    ExtensionsUpdated,
    ExtensionInstalled(Arc<str>),
    ExtensionUninstalled(Arc<str>),
    ExtensionFailedToLoad(Arc<str>),
}

struct LanguageAsset {
    config: LanguageConfig,
    path: PathBuf,
    queries: QueryFiles,
    rules: Option<SemanticTokenRules>,
}

struct Assets {
    root: PathBuf,
    languages: Vec<LanguageAsset>,
    themes: BTreeMap<Arc<str>, PathBuf>,
    icon_themes: BTreeMap<Arc<str>, PathBuf>,
    snippets: BTreeMap<PathBuf, String>,
}

pub struct ExtensionStore {
    fs: Arc<dyn Fs>,
    proxy: Arc<ExtensionHostProxy>,
    backend: BrowserExtensionBackend,
    remote: Option<Entity<RemoteExtensionStore>>,
    installed: BTreeMap<Arc<str>, ExtensionIndexEntry>,
    assets: BTreeMap<Arc<str>, Assets>,
    outstanding: BTreeMap<Arc<str>, ExtensionOperation>,
    syncing: bool,
    sync_again: bool,
    reload: BTreeSet<Arc<str>>,
    subscription: Option<Subscription>,
}

struct GlobalExtensionStore(Entity<ExtensionStore>);
impl Global for GlobalExtensionStore {}
impl EventEmitter<Event> for ExtensionStore {}
actions!(zed, [ReloadExtensions]);

pub fn init(fs: Arc<dyn Fs>, backend: BrowserExtensionBackend, cx: &mut App) {
    ExtensionSettings::register(cx);
    let store = cx.new(|cx| ExtensionStore {
        fs,
        proxy: ExtensionHostProxy::global(cx),
        backend,
        remote: None,
        installed: BTreeMap::default(),
        assets: BTreeMap::default(),
        outstanding: BTreeMap::default(),
        syncing: false,
        sync_again: false,
        reload: BTreeSet::default(),
        subscription: None,
    });
    cx.set_global(GlobalExtensionStore(store));
    cx.on_action(|_: &ReloadExtensions, cx| {
        ExtensionStore::global(cx).update(cx, |this, cx| {
            this.reload.extend(this.installed.keys().cloned());
            this.refresh(cx).detach_and_log_err(cx);
        });
    });
}

impl ExtensionStore {
    pub fn global(cx: &App) -> Entity<Self> {
        cx.global::<GlobalExtensionStore>().0.clone()
    }
    pub fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalExtensionStore>().map(|g| g.0.clone())
    }
    pub fn installed_extensions(&self) -> &BTreeMap<Arc<str>, ExtensionIndexEntry> {
        &self.installed
    }
    pub fn outstanding_operations(&self) -> &BTreeMap<Arc<str>, ExtensionOperation> {
        &self.outstanding
    }
    pub fn dev_extensions(&self) -> impl Iterator<Item = &Arc<ExtensionManifest>> {
        self.installed
            .values()
            .filter(|entry| entry.dev)
            .map(|entry| &entry.manifest)
    }
    pub fn extension_manifest_for_id(&self, id: &str) -> Option<&Arc<ExtensionManifest>> {
        self.installed.get(id).map(|entry| &entry.manifest)
    }
    pub fn extension_themes<'a>(&'a self, id: &str) -> impl Iterator<Item = &'a Arc<str>> {
        self.assets
            .get(id)
            .into_iter()
            .flat_map(|assets| assets.themes.keys())
    }
    pub fn extension_icon_themes<'a>(&'a self, id: &str) -> impl Iterator<Item = &'a Arc<str>> {
        self.assets
            .get(id)
            .into_iter()
            .flat_map(|assets| assets.icon_themes.keys())
    }

    pub fn attach(&mut self, remote: Entity<RemoteExtensionStore>, cx: &mut Context<Self>) {
        self.subscription = Some(cx.subscribe(&remote, |this, _, event, cx| {
            if matches!(event, RemoteExtensionEvent::InstalledChanged) {
                this.synchronize(cx);
            }
        }));
        self.remote = Some(remote);
        let refresh = self.refresh(cx);
        cx.spawn(async move |this, cx| {
            refresh.await?;
            this.update(cx, |this, cx| this.apply_startup_settings(cx))?
                .await
        })
        .detach_and_log_err(cx);
    }

    fn remote(&self) -> Result<Entity<RemoteExtensionStore>> {
        self.remote
            .clone()
            .context("The workspace is not connected")
    }

    fn apply_startup_settings(&self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let remote = self.remote();
        let settings = ExtensionSettings::get_global(cx).clone();
        let channel = release_channel::ReleaseChannel::global(cx);
        cx.spawn(async move |this, cx| {
            let remote = remote?;
            for (id, enabled) in &settings.auto_install_extensions {
                if !enabled || crate::is_suppressed_extension(id) {
                    continue;
                }
                let missing = remote.read_with(cx, |remote, _| {
                    !remote.installed().iter().any(|entry| entry.id == *id)
                });
                if missing {
                    if let Err(error) = remote
                        .update(cx, |remote, cx| {
                            remote.install_automatically(id.clone(), None, None, cx)
                        })
                        .await
                    {
                        log::warn!("Automatically installing {id}: {error:#}");
                    }
                }
            }
            remote.update(cx, |remote, cx| remote.refresh(cx)).await?;
            let installed = remote.read_with(cx, |remote, _| remote.installed().to_vec());
            for entry in installed {
                if entry.dev || !settings.should_auto_update(&entry.id) {
                    continue;
                }
                let Ok(current) = entry.version.parse::<semver::Version>() else {
                    continue;
                };
                let result = async {
                    let versions = remote
                        .update(cx, |remote, cx| {
                            remote.search(None, Some(entry.id.to_string()), BTreeSet::default(), cx)
                        })
                        .await?;
                    let latest = versions
                        .into_iter()
                        .filter(|version| crate::is_version_compatible(channel, version))
                        .filter_map(|version| {
                            Some((
                                version.manifest.version.parse::<semver::Version>().ok()?,
                                version.manifest.version,
                            ))
                        })
                        .filter(|(version, _)| *version > current)
                        .max_by(|a, b| a.0.cmp(&b.0));
                    if let Some((_, version)) = latest {
                        remote
                            .update(cx, |remote, cx| {
                                remote.install_automatically(
                                    entry.id.clone(),
                                    Some(version),
                                    Some(entry.revision),
                                    cx,
                                )
                            })
                            .await?;
                    }
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                if let Err(error) = result {
                    log::warn!("Checking updates for {}: {error:#}", entry.id);
                }
            }
            this.update(cx, |this, cx| this.refresh(cx))?.await
        })
    }

    fn refresh(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let remote = self.remote();
        cx.spawn(async move |this, cx| {
            remote?.update(cx, |remote, cx| remote.refresh(cx)).await?;
            this.update(cx, |this, cx| this.synchronize(cx))?;
            Ok(())
        })
    }

    pub fn fetch_extensions(
        &self,
        search: Option<&str>,
        provides: Option<&BTreeSet<ExtensionProvides>>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Vec<ExtensionMetadata>>> {
        self.fetch(
            search.map(str::to_owned),
            None,
            provides.cloned().unwrap_or_default(),
            cx,
        )
    }
    pub fn fetch_extension_versions(
        &self,
        id: &str,
        cx: &mut Context<Self>,
    ) -> Task<Result<Vec<ExtensionMetadata>>> {
        self.fetch(None, Some(id.to_owned()), BTreeSet::default(), cx)
    }
    fn fetch(
        &self,
        search: Option<String>,
        id: Option<String>,
        provides: BTreeSet<ExtensionProvides>,
        cx: &mut Context<Self>,
    ) -> Task<Result<Vec<ExtensionMetadata>>> {
        match self.remote() {
            Ok(remote) => remote.update(cx, |remote, cx| remote.search(search, id, provides, cx)),
            Err(error) => Task::ready(Err(error)),
        }
    }

    pub fn install_extension(&mut self, id: Arc<str>, version: Arc<str>, cx: &mut Context<Self>) {
        self.operate(id, Some(version), ExtensionOperation::Install, cx)
            .detach_and_log_err(cx);
    }
    pub fn install_latest_extension(&mut self, id: Arc<str>, cx: &mut Context<Self>) {
        self.operate(id, None, ExtensionOperation::Install, cx)
            .detach_and_log_err(cx);
    }
    pub fn upgrade_extension(
        &mut self,
        id: Arc<str>,
        version: Arc<str>,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        self.operate(id, Some(version), ExtensionOperation::Upgrade, cx)
    }
    pub fn uninstall_extension(
        &mut self,
        id: Arc<str>,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        self.operate(id, None, ExtensionOperation::Remove, cx)
    }
    fn operate(
        &mut self,
        id: Arc<str>,
        version: Option<Arc<str>>,
        operation: ExtensionOperation,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        if self.outstanding.contains_key(&id) {
            return Task::ready(Err(anyhow!("An operation on {id} is already running")));
        }
        let remote = self.remote();
        self.outstanding.insert(id.clone(), operation);
        cx.notify();
        cx.spawn(async move |this, cx| {
            let result = async {
                let remote = remote?;
                remote
                    .update(cx, |remote, cx| match operation {
                        ExtensionOperation::Remove => remote.uninstall(id.clone(), cx),
                        _ => remote.install(id.clone(), version, cx),
                    })
                    .await?;
                remote.update(cx, |remote, cx| remote.refresh(cx)).await
            }
            .await;
            this.update(cx, |this, cx| {
                if !this.syncing {
                    this.outstanding.remove(&id);
                }
                if let Err(error) = &result {
                    (this.backend.report_error)(format!("Extension {id}: {error:#}"), cx);
                }
                this.synchronize(cx);
                cx.notify();
            })?;
            result
        })
    }

    pub fn install_dev_extension(
        &mut self,
        path: PathBuf,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        let install = self.backend.install_dev.clone();
        cx.spawn(async move |this, cx| {
            cx.update(|cx| install(path, cx)).await?;
            this.update(cx, |this, cx| this.refresh(cx))?.await
        })
    }
    pub fn rebuild_dev_extension(&mut self, id: Arc<str>, cx: &mut Context<Self>) {
        // Browser selections are snapshots, not ongoing filesystem access.
        let selected = cx.prompt_for_paths(gpui::PathPromptOptions {
            files: false,
            directories: true,
            multiple: false,
            prompt: Some(format!("Rebuild {id}: select its source folder").into()),
        });
        let fs = self.fs.clone();
        cx.spawn(async move |this, cx| {
            let Some(path) = selected.await??.and_then(|paths| paths.into_iter().next()) else {
                return Ok(());
            };
            let manifest = ExtensionManifest::load(fs, &path).await?;
            ensure!(
                manifest.id == id,
                "Selected extension is {}, not {id}",
                manifest.id
            );
            this.update(cx, |this, cx| this.install_dev_extension(path, cx))?
                .await
        })
        .detach_and_log_err(cx);
    }

    // One synchronizer per store. Events arriving during downloads schedule another
    // pass; stale results cannot resurrect an uninstalled/replaced extension.
    fn synchronize(&mut self, cx: &mut Context<Self>) {
        if self.syncing {
            self.sync_again = true;
            return;
        }
        let Some(remote) = self.remote.clone() else {
            return;
        };
        self.syncing = true;
        cx.spawn(async move |this, cx| {
            loop {
                let pending = this.update(cx, |this, cx| {
                    this.sync_again = false;
                    let desired = remote.read(cx).installed().to_vec();
                    let removed = this
                        .installed
                        .keys()
                        .filter(|id| !desired.iter().any(|record| record.id == **id))
                        .cloned()
                        .collect::<Vec<_>>();
                    for id in removed {
                        this.remove(&id, cx);
                    }
                    desired
                        .into_iter()
                        .filter(|record| {
                            this.reload.remove(&record.id)
                                || this.installed.get(&record.id).is_none_or(|entry| {
                                    entry.manifest.version != record.version
                                        || entry.dev != record.dev
                                        || entry.revision != record.revision
                                })
                        })
                        .collect::<Vec<_>>()
                })?;
                for record in pending {
                    this.update(cx, |this, cx| {
                        this.outstanding
                            .entry(record.id.clone())
                            .or_insert(ExtensionOperation::Install);
                        cx.notify();
                    })?;
                    let result = async {
                        let (download, fs, proxy) = this.update(cx, |this, cx| {
                            (
                                (this.backend.download)(record.id.clone(), cx),
                                this.fs.clone(),
                                this.proxy.clone(),
                            )
                        })?;
                        let root = download.await?;
                        let prepared =
                            prepare(fs.clone(), proxy.clone(), root.clone(), &record).await;
                        let (entry, assets) = match prepared {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                remove_directory(fs, root).await;
                                return Err(error);
                            }
                        };
                        let current =
                            remote.read_with(cx, |remote, _| remote.installed().contains(&record));
                        if !current {
                            remove_directory(fs, root).await;
                            return Ok(());
                        }
                        let themes = assets.themes.values().cloned().collect::<BTreeSet<_>>();
                        let icons = assets
                            .icon_themes
                            .values()
                            .cloned()
                            .collect::<BTreeSet<_>>();
                        // All paths and metadata were validated before replacing the old
                        // set. The single synchronizer serializes subsequent removals.
                        this.update(cx, |this, cx| {
                            this.remove(&record.id, cx);
                            this.installed.insert(record.id.clone(), entry);
                            this.assets.insert(record.id.clone(), assets);
                            this.register_languages(cx);
                        })?;
                        let loaded = async {
                            for path in themes {
                                proxy.load_user_theme(path, fs.clone()).await?;
                            }
                            for path in icons {
                                proxy
                                    .load_icon_theme(path, root.clone(), fs.clone())
                                    .await?;
                            }
                            this.update(cx, |this, _| this.register_snippets())??;
                            Ok::<_, anyhow::Error>(())
                        }
                        .await;
                        if let Err(error) = loaded {
                            // Do not mark a partial asset set as loaded; Reload Extensions
                            // can retry it even when the installed version is unchanged.
                            this.update(cx, |this, cx| this.remove(&record.id, cx))?;
                            return Err(error);
                        }
                        this.update(cx, |this, cx| {
                            this.proxy.reload_current_theme(cx);
                            this.proxy.reload_current_icon_theme(cx);
                            cx.emit(Event::ExtensionInstalled(record.id.clone()));
                            cx.emit(Event::ExtensionsUpdated);
                            cx.notify();
                        })?;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await;
                    if let Err(error) = result {
                        this.update(cx, |this, cx| {
                            (this.backend.report_error)(
                                format!("Loading extension {}: {error:#}", record.id),
                                cx,
                            );
                            cx.emit(Event::ExtensionFailedToLoad(record.id.clone()));
                        })?;
                    }
                }
                if !this.update(cx, |this, cx| {
                    if this.sync_again {
                        true
                    } else {
                        this.syncing = false;
                        this.outstanding
                            .retain(|id, _| remote.read(cx).is_pending(id));
                        this.proxy.set_extensions_loaded();
                        cx.notify();
                        false
                    }
                })? {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .detach_and_log_err(cx);
    }

    fn remove(&mut self, id: &Arc<str>, cx: &mut Context<Self>) {
        let Some(assets) = self.assets.remove(id) else {
            return;
        };
        let entry = self.installed.remove(id).unwrap();
        self.proxy.remove_languages(
            &assets
                .languages
                .iter()
                .map(|language| language.config.name.clone())
                .collect::<Vec<_>>(),
            &entry.manifest.grammars.keys().cloned().collect::<Vec<_>>(),
        );
        self.proxy
            .remove_user_themes(assets.themes.keys().cloned().map(Into::into).collect());
        self.proxy
            .remove_icon_themes(assets.icon_themes.keys().cloned().map(Into::into).collect());
        for path in assets.snippets.keys() {
            if let Err(error) = self.proxy.register_snippet(path, "{}") {
                log::warn!("Removing extension snippets: {error:#}");
            }
        }
        if let Err(error) = self.register_snippets() {
            log::warn!("Restoring remaining extension snippets: {error:#}");
        }
        SettingsStore::update_global(cx, |store, cx| {
            for language in &assets.languages {
                store.remove_language_semantic_token_rules(language.config.name.as_ref(), cx);
            }
        });
        self.register_languages(cx);
        self.proxy.reload_current_theme(cx);
        self.proxy.reload_current_icon_theme(cx);
        let fs = self.fs.clone();
        cx.background_spawn(remove_directory(fs, assets.root))
            .detach();
        cx.emit(Event::ExtensionUninstalled(id.clone()));
        cx.emit(Event::ExtensionsUpdated);
        cx.notify();
    }

    fn register_languages(&self, cx: &mut App) {
        self.proxy.register_grammars(
            self.installed
                .iter()
                .flat_map(|(id, entry)| {
                    let root = &self.assets[id].root;
                    entry.manifest.grammars.keys().map(move |name| {
                        (
                            name.clone(),
                            root.join("grammars").join(format!("{name}.wasm")),
                        )
                    })
                })
                .collect(),
        );
        for assets in self.assets.values() {
            for language in &assets.languages {
                let fs = self.fs.clone();
                let path = language.path.clone();
                let queries = language.queries;
                let registered = self.proxy.register_language(
                    language.config.name.clone(),
                    language.config.grammar.clone(),
                    language.config.matcher.clone(),
                    language.config.hidden,
                    Arc::new(move || {
                        let fs = fs.clone();
                        let path = path.clone();
                        async move { load_plugin_language(fs, &path, Some(queries)).await }.boxed()
                    }),
                );
                if registered && let Some(rules) = &language.rules {
                    SettingsStore::update_global(cx, |store, cx| {
                        store.set_language_semantic_token_rules(
                            language.config.name.0.clone(),
                            rules.clone(),
                            cx,
                        )
                    });
                }
            }
        }
    }

    fn register_snippets(&self) -> Result<()> {
        // The native registry is keyed by language, not extension. Restore the
        // remaining sets after removal of another provider of the same language.
        for assets in self.assets.values() {
            for (path, contents) in &assets.snippets {
                self.proxy.register_snippet(path, contents)?;
            }
        }
        Ok(())
    }
}

async fn remove_directory(fs: Arc<dyn Fs>, root: PathBuf) {
    if let Err(error) = fs
        .remove_dir(
            &root,
            RemoveOptions {
                recursive: true,
                ignore_if_not_exists: true,
            },
        )
        .await
    {
        log::warn!("Removing browser extension cache: {error:#}");
    }
}

async fn prepare(
    fs: Arc<dyn Fs>,
    proxy: Arc<ExtensionHostProxy>,
    root: PathBuf,
    record: &InstalledExtension,
) -> Result<(ExtensionIndexEntry, Assets)> {
    let manifest = ExtensionManifest::load(fs.clone(), &root).await?;
    ensure!(
        manifest.id == record.id && manifest.version == record.version,
        "Extension changed during download; reload extensions to retry"
    );
    let mut assets = Assets {
        root,
        languages: Vec::new(),
        themes: BTreeMap::default(),
        icon_themes: BTreeMap::default(),
        snippets: BTreeMap::default(),
    };
    for path in &manifest.languages {
        let path = assets.root.join(path.as_std_path());
        let config: LanguageConfig =
            toml::from_str(&fs.load(&path.join(LanguageConfig::FILE_NAME)).await?)?;
        let rules_path = path.join(SemanticTokenRules::FILE_NAME);
        let rules = if fs.is_file(&rules_path).await {
            Some(SemanticTokenRules::parse(&fs.load(&rules_path).await?)?)
        } else {
            None
        };
        let queries = discover_query_files(fs.clone(), &path).await?;
        assets.languages.push(LanguageAsset {
            config,
            path,
            queries,
            rules,
        });
    }
    for path in &manifest.themes {
        let path = assets.root.join(path.as_std_path());
        for name in proxy.list_theme_names(path.clone(), fs.clone()).await? {
            assets.themes.insert(name.into(), path.clone());
        }
    }
    for path in &manifest.icon_themes {
        let path = assets.root.join(path.as_std_path());
        for name in proxy
            .list_icon_theme_names(path.clone(), fs.clone())
            .await?
        {
            assets.icon_themes.insert(name.into(), path.clone());
        }
    }
    for path in manifest
        .snippets
        .iter()
        .flat_map(|snippets| snippets.paths())
    {
        ensure!(
            !path.is_absolute()
                && path
                    .components()
                    .all(|part| matches!(part, std::path::Component::Normal(_))),
            "Invalid extension snippet path"
        );
        let path = assets.root.join(path);
        let contents = fs.load(&path).await?;
        assets.snippets.insert(path, contents);
    }
    for name in manifest.grammars.keys() {
        ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "Invalid grammar name"
        );
        ensure!(
            fs.is_file(&assets.root.join("grammars").join(format!("{name}.wasm")))
                .await,
            "Missing grammar {name}"
        );
    }
    Ok((
        ExtensionIndexEntry {
            manifest: Arc::new(manifest),
            dev: record.dev,
            revision: record.revision,
        },
        assets,
    ))
}
