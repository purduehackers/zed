//! Browser filesystem and authenticated HTTP services for Zed's extension host.
use anyhow::{Context as _, Result, ensure};
use extension::browser_archive::{MAX_ENTRIES, read as read_archive, validate_path as relative};
use fs::{Fs, RemoveOptions};
use futures::{AsyncReadExt, StreamExt};
use gpui::{App, AppContext, AsyncApp};
use http_client::{AsyncBody, http};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use workspace::notifications::{
    NotificationId, show_app_notification, simple_message_notification::MessageNotification,
};

const MAX_ARCHIVE: u64 = 128 * 1024 * 1024;
const MAX_DEV_ARCHIVE: usize = 64 * 1024 * 1024;
const CACHE_ROOT: &str = "/extensions/browser";
struct ExtensionError;

pub fn init(fs: Arc<dyn Fs>, cx: &mut App) {
    extension_host::init(
        fs.clone(),
        extension_host::BrowserExtensionBackend {
            download: Arc::new({
                let fs = fs.clone();
                move |id, cx| {
                    let fs = fs.clone();
                    cx.spawn(async move |cx| {
                        let response =
                            request(&format!("/extensions/{id}/assets/"), None, cx).await?;
                        let fs_for_read = fs.clone();
                        let root = cx
                            .background_spawn(async move {
                                let entries = read_archive(&response, Some(&id), MAX_ARCHIVE).await?;
                                let incoming = entries.iter().map(|(_, bytes)| bytes.len() as u64).sum::<u64>();
                                ensure!(cache_bytes(&fs_for_read).await? + incoming <= 256 * 1024 * 1024,
                                    "This tab has reached its 256 MiB extension-asset limit; remove unused extensions");
                                let root = PathBuf::from(CACHE_ROOT)
                                    .join(uuid::Uuid::new_v4().to_string())
                                    .join(id.as_ref());
                                let written = async {
                                  fs_for_read.create_dir(&root).await?;
                                  for (relative, contents) in entries {
                                    // Extension code runs only in the sandbox. Keep client memory
                                    // for grammars and declarative assets, not a second WASI host.
                                    if relative == Path::new("extension.wasm") {
                                        continue;
                                    }
                                    let path = root.join(relative);
                                    fs_for_read
                                        .create_dir(path.parent().context("Invalid asset path")?)
                                        .await?;
                                    fs_for_read.write(&path, &contents).await?;
                                  }
                                  Ok::<_, anyhow::Error>(())
                                }.await;
                                if let Err(error) = written {
                                    fs_for_read.remove_dir(&root, RemoveOptions { recursive: true, ignore_if_not_exists: true }).await.ok();
                                    return Err(error);
                                }
                                Ok::<_, anyhow::Error>(root)
                            })
                            .await?;
                        Ok(root)
                    })
                }
            }),
            install_dev: Arc::new(move |path, cx| {
                let fs = fs.clone();
                cx.spawn(async move |cx| {
                    ensure!(
                        path.starts_with("/browser/imports"),
                        "Select a local extension source folder"
                    );
                    let body = cx
                        .background_spawn(async move { pack_source(fs, path).await })
                        .await?;
                    request("/extensions/dev", Some(body), cx).await?;
                    Ok(())
                })
            }),
            report_error: Arc::new(|message, cx| {
                log::error!("{message}");
                show_app_notification(NotificationId::unique::<ExtensionError>(), cx, move |cx| {
                    cx.new(|cx| MessageNotification::new(message.clone(), cx))
                });
            }),
        },
        cx,
    );
}

async fn cache_bytes(fs: &Arc<dyn Fs>) -> Result<u64> {
    let root = PathBuf::from(CACHE_ROOT);
    if !fs.is_dir(&root).await {
        return Ok(0);
    }
    let mut pending = vec![root];
    let mut total = 0u64;
    while let Some(directory) = pending.pop() {
        let mut entries = fs.read_dir(&directory).await?;
        while let Some(path) = entries.next().await {
            let path = path?;
            let Some(metadata) = fs.metadata(&path).await? else {
                continue;
            };
            if metadata.is_dir {
                pending.push(path);
            } else {
                total += metadata.len;
            }
        }
    }
    Ok(total)
}

async fn request(path: &str, body: Option<Vec<u8>>, cx: &mut AsyncApp) -> Result<Vec<u8>> {
    crate::bridge::ensure_fresh_token(cx).await?;
    let session = crate::bridge::current_session().context("The workspace is disconnected")?;
    let mut url = url::Url::parse(&session.ws_url)?;
    let scheme = if url.scheme() == "wss" {
        "https"
    } else {
        "http"
    };
    url.set_scheme(scheme)
        .map_err(|_| anyhow::anyhow!("Invalid sandbox URL"))?;
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    let client = cx.update(|cx| cx.http_client());
    let request = http::Request::builder()
        .uri(url.as_str())
        .method(if body.is_some() { "POST" } else { "GET" })
        .header("Authorization", format!("Bearer {}", session.token))
        .header("Content-Type", "application/x-tar")
        .body(body.map(AsyncBody::from).unwrap_or_else(AsyncBody::empty))?;
    let mut response = client.send(request).await?;
    let status = response.status();
    let mut bytes = Vec::new();
    response
        .body_mut()
        .take(if status.is_success() {
            MAX_ARCHIVE + 1
        } else {
            4097
        })
        .read_to_end(&mut bytes)
        .await?;
    ensure!(
        status.is_success(),
        "Extension transfer failed ({status}): {}",
        String::from_utf8_lossy(&bytes)
    );
    ensure!(
        bytes.len() as u64 <= MAX_ARCHIVE,
        "Extension assets exceed 128 MiB"
    );
    Ok(bytes)
}

async fn pack_source(fs: Arc<dyn Fs>, root: PathBuf) -> Result<Vec<u8>> {
    let mut pending = vec![root.clone()];
    let mut builder = async_tar::Builder::new(Vec::new());
    let mut count = 0usize;
    let mut total = 0usize;
    while let Some(directory) = pending.pop() {
        let mut entries = fs.read_dir(&directory).await?;
        while let Some(path) = entries.next().await {
            let path = path?;
            let rel = path.strip_prefix(&root)?;
            relative(rel)?;
            if rel.components().any(|part| {
                matches!(
                    part.as_os_str().to_str(),
                    Some(".git" | "target" | "node_modules")
                )
            }) {
                continue;
            }
            let metadata = fs
                .metadata(&path)
                .await?
                .context("Source file disappeared")?;
            ensure!(
                !metadata.is_symlink,
                "Development extensions cannot contain symlinks"
            );
            count += 1;
            ensure!(count <= MAX_ENTRIES, "Too many extension source files");
            if metadata.is_dir {
                pending.push(path);
                continue;
            }
            let contents = fs.load_bytes(&path).await?;
            total += contents.len() + 1024;
            ensure!(total <= MAX_DEV_ARCHIVE, "Extension sources exceed 64 MiB");
            let mut header = async_tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, rel, contents.as_slice())
                .await?;
        }
    }
    builder.finish().await?;
    Ok(builder.into_inner().await?)
}
