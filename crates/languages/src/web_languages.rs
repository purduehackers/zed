//! Adapters for the pinned tools already installed in the Zedspaces base image.
//! Grammars run in the tab; these processes run only in the remote project.
use anyhow::{Result, bail};
use async_trait::async_trait;
use collections::HashMap;
use gpui::AsyncApp;
use language::{LanguageName, LspAdapter, LspAdapterDelegate, LspInstaller, Toolchain};
use lsp::{LanguageServerBinary, LanguageServerName};
use serde_json::{Value, json};
use std::{future::Future, path::PathBuf, sync::Arc};

pub(crate) struct InstalledLsp {
    name: &'static str,
    binary: &'static str,
    language: &'static str,
    language_id: &'static str,
    arguments: &'static [&'static str],
}

impl InstalledLsp {
    pub const ASTRO: Self = Self {
        name: "astro-language-server",
        binary: "astro-ls",
        language: "Astro",
        language_id: "astro",
        arguments: &["--stdio"],
    };
    pub const DOCKERFILE: Self = Self {
        name: "dockerfile-language-server",
        binary: "docker-langserver",
        language: "Dockerfile",
        language_id: "dockerfile",
        arguments: &["--stdio"],
    };
    pub const HTML: Self = Self {
        name: "vscode-html-language-server",
        binary: "vscode-html-language-server",
        language: "HTML",
        language_id: "html",
        arguments: &["--stdio"],
    };
    pub const TOML: Self = Self {
        name: "taplo",
        binary: "taplo",
        language: "TOML",
        language_id: "toml",
        arguments: &["lsp", "stdio"],
    };

    // Paths are supplied by the workspace image, not the browser's filesystem.
    async fn astro_modules(&self, delegate: &Arc<dyn LspAdapterDelegate>) -> Option<PathBuf> {
        if self.name != Self::ASTRO.name {
            return None;
        }
        delegate
            .shell_env()
            .await
            .get("ZS_NODE_MODULES_DIR")
            .map(PathBuf::from)
    }
}

#[async_trait(?Send)]
impl LspAdapter for InstalledLsp {
    fn name(&self) -> LanguageServerName {
        LanguageServerName::new_static(self.name)
    }

    fn language_ids(&self) -> HashMap<LanguageName, String> {
        [(self.language.into(), self.language_id.into())]
            .into_iter()
            .collect()
    }

    async fn initialization_options(
        self: Arc<Self>,
        delegate: &Arc<dyn LspAdapterDelegate>,
        _: &mut AsyncApp,
    ) -> Result<Option<Value>> {
        Ok(self.astro_modules(delegate).await.map(|modules| {
            json!({
                "provideFormatter": true,
                "typescript": { "tsdk": modules.join("typescript/lib") }
            })
        }))
    }

    async fn additional_initialization_options(
        self: Arc<Self>,
        target: LanguageServerName,
        delegate: &Arc<dyn LspAdapterDelegate>,
    ) -> Result<Option<Value>> {
        if target != LanguageServerName::new_static("typescript-language-server") {
            return Ok(None);
        }
        Ok(self.astro_modules(delegate).await.map(|modules| {
            json!({
                "plugins": [{ "name": "@astrojs/ts-plugin", "location": modules.parent() }]
            })
        }))
    }

    async fn additional_workspace_configuration(
        self: Arc<Self>,
        target: LanguageServerName,
        delegate: &Arc<dyn LspAdapterDelegate>,
        _: &mut AsyncApp,
    ) -> Result<Option<Value>> {
        if target != LanguageServerName::new_static("vtsls") {
            return Ok(None);
        }
        Ok(self.astro_modules(delegate).await.map(|modules| {
            json!({
                "vtsls": { "tsserver": { "globalPlugins": [{
                    "name": "@astrojs/ts-plugin", "location": modules.parent(),
                    "enableForWorkspaceTypeScriptVersions": true
                }] } }
            })
        }))
    }
}

impl LspInstaller for InstalledLsp {
    type BinaryVersion = ();

    async fn check_if_user_installed(
        &self,
        delegate: &Arc<dyn LspAdapterDelegate>,
        _: Option<Toolchain>,
        _: &AsyncApp,
    ) -> Option<LanguageServerBinary> {
        Some(LanguageServerBinary {
            path: delegate.which(self.binary.as_ref()).await?,
            arguments: self.arguments.iter().copied().map(Into::into).collect(),
            env: Some(delegate.shell_env().await),
        })
    }

    async fn fetch_latest_server_version(
        &self,
        _: &Arc<dyn LspAdapterDelegate>,
        _: bool,
        _: &mut AsyncApp,
    ) -> Result<()> {
        bail!("{} must be installed in the workspace image", self.binary)
    }

    fn fetch_server_binary(
        &self,
        _: (),
        _: PathBuf,
        _: &Arc<dyn LspAdapterDelegate>,
    ) -> impl Send + Future<Output = Result<LanguageServerBinary>> + use<> {
        let binary = self.binary;
        async move { bail!("{binary} must be installed in the workspace image") }
    }

    async fn cached_server_binary(
        &self,
        _: PathBuf,
        _: &dyn LspAdapterDelegate,
    ) -> Option<LanguageServerBinary> {
        None
    }
}
