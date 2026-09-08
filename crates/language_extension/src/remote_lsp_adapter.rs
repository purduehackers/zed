//! Client-side metadata and presentation for extension servers running in a sandbox.
use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use async_trait::async_trait;
use collections::HashMap;
use extension::{ExtensionManifest, LanguageServerLabelRequest, RemoteLanguageServerLabels};
use futures::{FutureExt, lock::OwnedMutexGuard};
use gpui::AsyncApp;
use language::{
    CodeLabel, DynLspInstaller, Language, LanguageName, LanguageServerBinaryLocations, LspAdapter,
    LspAdapterDelegate, Toolchain,
};
use lsp::{CodeActionKind, LanguageServerBinary, LanguageServerBinaryOptions, LanguageServerName};

use crate::extension_lsp_adapter::{
    labels_from_extension, lsp_completion_to_extension, symbol_kind_to_extension,
};

pub(crate) struct RemoteLspAdapter {
    pub manifest: Arc<ExtensionManifest>,
    pub labels: RemoteLanguageServerLabels,
    pub language_server_id: LanguageServerName,
}

#[async_trait(?Send)]
impl LspAdapter for RemoteLspAdapter {
    fn name(&self) -> LanguageServerName {
        self.language_server_id.clone()
    }

    fn is_extension(&self) -> bool {
        true
    }

    fn language_ids(&self) -> HashMap<LanguageName, String> {
        self.manifest.language_servers[&self.language_server_id]
            .language_ids
            .clone()
    }

    fn code_action_kinds(&self) -> Option<Vec<CodeActionKind>> {
        self.manifest.language_servers[&self.language_server_id]
            .code_action_kinds
            .clone()
            .or_else(|| {
                Some(vec![
                    CodeActionKind::EMPTY,
                    CodeActionKind::QUICKFIX,
                    CodeActionKind::REFACTOR,
                    CodeActionKind::REFACTOR_EXTRACT,
                    CodeActionKind::SOURCE,
                ])
            })
    }

    async fn labels_for_completions(
        self: Arc<Self>,
        completions: &[lsp::CompletionItem],
        language: &Arc<Language>,
    ) -> Result<Vec<Option<CodeLabel>>> {
        let request = LanguageServerLabelRequest::Completions(
            completions
                .iter()
                .cloned()
                .map(lsp_completion_to_extension)
                .collect(),
        );
        let labels = (self.labels)(self.language_server_id.clone(), request).await?;
        Ok(labels_from_extension(labels, language))
    }

    async fn labels_for_symbols(
        self: Arc<Self>,
        symbols: &[language::Symbol],
        language: &Arc<Language>,
    ) -> Result<Vec<Option<CodeLabel>>> {
        let request = LanguageServerLabelRequest::Symbols(
            symbols
                .iter()
                .map(|symbol| extension::Symbol {
                    name: symbol.name.clone(),
                    kind: symbol_kind_to_extension(symbol.kind),
                    container_name: symbol.container_name.clone(),
                })
                .collect(),
        );
        let labels = (self.labels)(self.language_server_id.clone(), request).await?;
        Ok(labels_from_extension(labels, language))
    }
}

// Remote LspStore owns process startup/configuration. A local launch is an invalid
// routing operation, not a request to download a second server into the browser.
#[async_trait(?Send)]
impl DynLspInstaller for RemoteLspAdapter {
    async fn try_fetch_server_binary(
        &self,
        _: &Arc<dyn LspAdapterDelegate>,
        _: PathBuf,
        _: bool,
        _: &mut AsyncApp,
    ) -> Result<LanguageServerBinary> {
        bail!("Extension language servers must run in the connected sandbox")
    }

    fn get_language_server_command(
        self: Arc<Self>,
        _: Arc<dyn LspAdapterDelegate>,
        _: Option<Toolchain>,
        _: LanguageServerBinaryOptions,
        _: OwnedMutexGuard<Option<(bool, LanguageServerBinary)>>,
        _: AsyncApp,
    ) -> LanguageServerBinaryLocations {
        async {
            (
                Err(anyhow::anyhow!(
                    "Extension language servers must run in the connected sandbox"
                )),
                None,
            )
        }
        .boxed_local()
    }
}
