//! Filesystem-backed language assets shared by native and browser hosts.
use anyhow::Result;
use fs::Fs;
use futures::{StreamExt, future::join_all};
use language::{
    LanguageConfig, LanguageQueries, LoadedLanguage, QueryFile, QueryFileContents, QueryFiles,
};
use project::ContextProviderWithTasks;
use std::{borrow::Cow, path::Path, sync::Arc};
use task::TaskTemplates;
use util::ResultExt;

pub(crate) async fn load_plugin_language(
    fs: Arc<dyn Fs>,
    language_path: &Path,
    query_files: Option<QueryFiles>,
) -> Result<LoadedLanguage> {
    let config = {
        let fs = fs.clone();
        let config_path = language_path.join(LanguageConfig::FILE_NAME);
        async move {
            let contents = fs.load(&config_path).await?;
            toml::from_str::<LanguageConfig>(&contents).map_err(anyhow::Error::from)
        }
    };
    let context_provider = {
        let fs = fs.clone();
        let tasks_path = language_path.join(TaskTemplates::FILE_NAME);
        async move {
            fs.load(&tasks_path).await.ok().and_then(|contents| {
                serde_json_lenient::from_str(&contents)
                    .log_err()
                    .map(|definitions| {
                        Arc::new(ContextProviderWithTasks::new(definitions)) as Arc<_>
                    })
            })
        }
    };
    let (config, queries, context_provider) = futures::try_join!(
        config,
        async move { Ok(load_plugin_queries(fs, &language_path, query_files).await) },
        async move { Ok(context_provider.await) }
    )?;

    Ok(LoadedLanguage {
        config,
        queries,
        context_provider,
        toolchain_provider: None,
        manifest_name: None,
    })
}

pub(crate) async fn discover_query_files(fs: Arc<dyn Fs>, root_path: &Path) -> Result<QueryFiles> {
    let mut paths = fs.read_dir(root_path).await?;
    let mut query_files = QueryFiles::empty();
    while let Some(path) = paths.next().await {
        let path = path?;
        let Some(query_file) = path
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(|file_name| file_name.parse::<QueryFile>().ok())
        else {
            continue;
        };
        query_files.insert(query_file.into());
    }
    Ok(query_files)
}

pub(crate) async fn load_plugin_queries(
    fs: Arc<dyn Fs>,
    root_path: &Path,
    query_files: Option<QueryFiles>,
) -> LanguageQueries {
    let query_files = query_files.unwrap_or_else(QueryFiles::all);
    let files = join_all(query_files.query_files().map(|query_file| {
        let fs = fs.clone();
        let path = root_path.join(query_file.file_name());
        async move {
            fs.load(&path)
                .await
                .ok()
                .map(|contents| QueryFileContents::new(query_file, Cow::Owned(contents)))
        }
    }))
    .await;
    LanguageQueries::from_files(files.into_iter().flatten())
}
