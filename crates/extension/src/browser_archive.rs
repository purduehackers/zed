//! Bounded, link-free transfers between the browser and sandbox extension hosts.
use anyhow::{Context as _, Result, ensure};
use futures::{AsyncReadExt, StreamExt};
use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
};

pub const MAX_ENTRIES: usize = 10_000;

pub fn validate_path(path: &Path) -> Result<()> {
    ensure!(
        !path.as_os_str().is_empty()
            && path
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
            && path
                .to_str()
                .is_some_and(|path| !path.contains(['\\', '\0'])
                    && !path.split('/').any(|part| part == "..")),
        "Invalid extension archive path"
    );
    Ok(())
}

/// Validate the complete archive before callers write any files. `prefix` is the
/// installed extension id for downloads; uploads contain plain relative paths.
pub async fn read(
    bytes: &[u8],
    prefix: Option<&str>,
    limit: u64,
) -> Result<Vec<(PathBuf, Vec<u8>)>> {
    ensure!(
        bytes.len() as u64 <= limit,
        "Extension archive is too large"
    );
    let mut archive = async_tar::Archive::new(bytes).entries()?;
    let mut names = BTreeSet::new();
    let mut entries = Vec::new();
    let mut total = 0u64;
    while let Some(entry) = archive.next().await {
        let mut entry = entry?;
        let path = PathBuf::from(entry.path()?.as_os_str());
        validate_path(&path)?;
        ensure!(
            names.insert(path.clone()) && names.len() <= MAX_ENTRIES,
            "Duplicate or excessive extension entries"
        );
        let path = match prefix {
            Some(prefix) => path.strip_prefix(prefix)?.to_path_buf(),
            None => path,
        };
        let kind = entry.header().entry_type();
        ensure!(
            kind.is_dir() || kind.is_file(),
            "Links and special files are not extension assets"
        );
        if kind.is_dir() {
            continue;
        }
        validate_path(&path)?;
        let size = entry.header().size()?;
        total = total.checked_add(size).context("Extension size overflow")?;
        ensure!(
            size <= 64 * 1024 * 1024 && total <= limit,
            "Extension assets are too large"
        );
        let mut contents = Vec::new();
        (&mut entry)
            .take(size + 1)
            .read_to_end(&mut contents)
            .await?;
        ensure!(contents.len() as u64 == size, "Truncated extension asset");
        entries.push((path, contents));
    }
    Ok(entries)
}
