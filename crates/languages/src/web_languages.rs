//! Adapters for the pinned tools already installed in the Zedspaces base image.
//! Grammars run in the tab; these processes run only in the remote project.
use anyhow::{Result, bail};
use async_trait::async_trait;
use collections::HashMap;
use gpui::AsyncApp;
use language::{LanguageName, LspAdapter, LspAdapterDelegate, LspInstaller, Toolchain};
use lsp::{LanguageServerBinary, LanguageServerName};
use std::{future::Future, path::PathBuf, sync::Arc};

pub(crate) struct InstalledLsp {
    name: &'static str,
    binary: &'static str,
    language: &'static str,
    language_id: &'static str,
}

impl InstalledLsp {
    pub const DOCKERFILE: Self = Self {
        name: "dockerfile-language-server",
        binary: "docker-langserver",
        language: "Dockerfile",
        language_id: "dockerfile",
    };
    pub const HTML: Self = Self {
        name: "vscode-html-language-server",
        binary: "vscode-html-language-server",
        language: "HTML",
        language_id: "html",
    };
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
            arguments: vec!["--stdio".into()],
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
