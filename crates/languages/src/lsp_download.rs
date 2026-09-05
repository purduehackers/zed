//! Installing the built-in language servers from GitHub releases.
//!
//! Natively this is `http_client::github` and `http_client::github_download` (release lookup and
//! asset download) plus the `util::fs` and `util::archive` helpers that unpack a download and
//! prune the download directory, re-exported unchanged.
//!
//! In the browser (wasm32-unknown-unknown) those modules are gated out: language servers run on
//! the workspace host, which installs them with the native code, and the client has neither a
//! filesystem to unpack into nor a reason to fetch a release. The adapters' `LspInstaller` impls
//! are one piece of code on both targets, so here the same names resolve to stand-ins — the types
//! the impls are written against and functions that report the gap instead of touching the
//! network or the filesystem.

#[cfg(not(target_family = "wasm"))]
pub(crate) use http_client::github::{
    AssetKind, GitHubLspBinaryVersion, build_asset_url, latest_github_release,
};
#[cfg(not(target_family = "wasm"))]
pub(crate) use http_client::github_download::{GithubBinaryMetadata, download_server_binary};
#[cfg(not(target_family = "wasm"))]
pub(crate) use util::archive::extract_zip;
#[cfg(not(target_family = "wasm"))]
pub(crate) use util::fs::{make_file_executable, remove_matching};

/// Unpacks the gzipped tarball read from `reader` into `destination`.
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn extract_tar_gz<R: futures::AsyncRead + Unpin>(
    destination: &std::path::Path,
    reader: R,
) -> anyhow::Result<()> {
    use async_compression::futures::bufread::GzipDecoder;
    use async_tar::Archive;
    use smol::io::BufReader;

    let decompressed_bytes = GzipDecoder::new(BufReader::new(reader));
    let archive = Archive::new(decompressed_bytes);
    archive.unpack(destination).await?;
    Ok(())
}

#[cfg(target_family = "wasm")]
pub(crate) use wasm::*;

#[cfg(target_family = "wasm")]
mod wasm {
    use anyhow::{Result, bail};
    use futures::AsyncRead;
    use http_client::HttpClient;
    use std::{io, path::Path, sync::Arc};

    /// Why every install step fails in the browser.
    const NOT_IN_BROWSER: &str =
        "language servers are installed on the workspace host, not in the browser";

    pub struct GitHubLspBinaryVersion {
        pub name: String,
        pub url: String,
        pub digest: Option<String>,
    }

    // The adapters match every kind; the browser's target constants only ever name some of them.
    #[allow(dead_code)]
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub enum AssetKind {
        TarGz,
        TarBz2,
        Gz,
        Zip,
    }

    pub struct GithubRelease {
        pub tag_name: String,
        pub assets: Vec<GithubReleaseAsset>,
    }

    pub struct GithubReleaseAsset {
        pub name: String,
        pub browser_download_url: String,
        pub digest: Option<String>,
    }

    pub async fn latest_github_release(
        repo_name_with_owner: &str,
        _require_assets: bool,
        _pre_release: bool,
        _http: Arc<dyn HttpClient>,
    ) -> Result<GithubRelease> {
        bail!("cannot look up the latest release of {repo_name_with_owner}: {NOT_IN_BROWSER}")
    }

    pub fn build_asset_url(
        repo_name_with_owner: &str,
        tag: &str,
        kind: AssetKind,
    ) -> Result<String> {
        bail!("no {kind:?} asset of {repo_name_with_owner} {tag} to download: {NOT_IN_BROWSER}")
    }

    pub struct GithubBinaryMetadata {
        pub metadata_version: u64,
        pub digest: Option<String>,
    }

    impl GithubBinaryMetadata {
        pub async fn read_from_file(metadata_path: &Path) -> Result<GithubBinaryMetadata> {
            bail!("cannot read {metadata_path:?}: {NOT_IN_BROWSER}")
        }

        pub async fn write_to_file(&self, metadata_path: &Path) -> Result<()> {
            bail!(
                "cannot write metadata (version {}) to {metadata_path:?}: {NOT_IN_BROWSER}",
                self.metadata_version
            )
        }
    }

    pub async fn download_server_binary(
        _http_client: &dyn HttpClient,
        url: &str,
        _digest: Option<&str>,
        destination_path: &Path,
        _asset_kind: AssetKind,
    ) -> Result<()> {
        bail!("cannot download {url} to {destination_path:?}: {NOT_IN_BROWSER}")
    }

    pub async fn extract_zip<R: AsyncRead + Unpin>(destination: &Path, _reader: R) -> Result<()> {
        bail!("cannot unpack a zip archive into {destination:?}: {NOT_IN_BROWSER}")
    }

    pub async fn extract_tar_gz<R: AsyncRead + Unpin>(
        destination: &Path,
        _reader: R,
    ) -> Result<()> {
        bail!("cannot unpack a tarball into {destination:?}: {NOT_IN_BROWSER}")
    }

    pub async fn make_file_executable(path: &Path) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("cannot make {path:?} executable: {NOT_IN_BROWSER}"),
        ))
    }

    /// Nothing to prune: the browser has no download directory.
    pub async fn remove_matching<F>(_dir: &Path, _predicate: F)
    where
        F: Fn(&Path) -> bool,
    {
    }
}
