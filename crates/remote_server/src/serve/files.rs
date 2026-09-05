//! `/files` upload and download and the `/extensions/{id}/assets/{rel}` route
//! (BUILD-SPEC §3.6, §9). Every path is confined to the workspace root (or, for extension
//! assets, to whatever the gpui side resolves) before the filesystem is touched.

use std::{
    ffi::OsString,
    io,
    path::{Component, Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use bytes::Bytes;
use futures::{Stream, StreamExt as _, channel::mpsc};
use http_body_util::{BodyDataStream, BodyExt, StreamBody};
use hyper::{
    Request, Response, StatusCode,
    body::{Body, Frame},
    header,
};
use tokio::io::AsyncReadExt as _;
use util::paths::normalize_lexically;

use crate::serve::{
    GpuiCommand,
    auth::{Claims, query_pairs},
    http::{BoxBody, ServeState, error_response, json_response},
};

/// Largest accepted upload, checked on `Content-Length` and again while streaming.
pub const MAX_UPLOAD_BYTES: u64 = 512 * 1024 * 1024;
/// Largest number of file parts in one multipart upload.
pub const MAX_FILES_PER_UPLOAD: usize = 512;
/// A directory download with more entries than this aborts the tar stream.
pub const MAX_DOWNLOAD_ENTRIES: usize = 100_000;
/// Suffix of the temporary file an upload streams into before the rename.
pub const TEMP_SUFFIX: &str = ".zs-upload-";
/// Longest extension id; CONTRACTS §4 grammar `^[a-z0-9][a-z0-9_-]{0,63}$`.
pub const MAX_EXTENSION_ID_BYTES: usize = 64;
/// Chunk size of streamed file reads.
const READ_CHUNK_BYTES: usize = 64 * 1024;
/// Bound on the tar producer → response body channel (frames, not bytes).
const TAR_CHANNEL_FRAMES: usize = 16;
/// Bounds of the startup temp-file sweep.
const SWEEP_MAX_DEPTH: usize = 32;
const SWEEP_MAX_ENTRIES: usize = 200_000;

/// Why a `/files` or `/extensions/*` request failed, with its HTTP status.
#[derive(Debug, thiserror::Error)]
pub enum FilesError {
    /// `..` climbing above the root, or a symlink that resolves outside it.
    #[error("path is outside the workspace root")]
    Traversal,
    /// Empty, absolute, NUL-bearing, drive-prefixed or non-UTF-8 path.
    #[error("path contains an invalid component")]
    InvalidPath,
    /// The requested file, directory or asset does not exist.
    #[error("not found")]
    NotFound,
    /// The upload exceeds [`MAX_UPLOAD_BYTES`].
    #[error("upload too large")]
    TooLarge,
    /// The multipart upload has more than [`MAX_FILES_PER_UPLOAD`] file parts.
    #[error("too many files")]
    TooManyFiles,
    /// The `Content-Type` is neither multipart nor octet-stream.
    #[error("unsupported content type")]
    UnsupportedMedia,
    /// The request body could not be parsed (bad multipart framing, missing boundary).
    #[error("malformed request: {0}")]
    Malformed(String),
    /// Any other I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl FilesError {
    /// The HTTP status of this error.
    pub fn status(&self) -> StatusCode {
        match self {
            FilesError::Traversal | FilesError::InvalidPath | FilesError::Malformed(_) => {
                StatusCode::BAD_REQUEST
            }
            FilesError::NotFound => StatusCode::NOT_FOUND,
            FilesError::TooLarge | FilesError::TooManyFiles => StatusCode::PAYLOAD_TOO_LARGE,
            FilesError::UnsupportedMedia => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            FilesError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The `{"error": ...}` body value.
    pub fn code(&self) -> &'static str {
        match self {
            FilesError::Traversal => "path_outside_root",
            FilesError::InvalidPath => "invalid_path",
            FilesError::NotFound => "not_found",
            FilesError::TooLarge => "upload_too_large",
            FilesError::TooManyFiles => "too_many_files",
            FilesError::UnsupportedMedia => "unsupported_media_type",
            FilesError::Malformed(_) => "malformed_request",
            FilesError::Io(_) => "io_error",
        }
    }

    /// The error as an HTTP response.
    pub fn into_response(self) -> Response<BoxBody> {
        if let FilesError::Io(error) = &self {
            log::error!("files request failed: {error:#}");
        }
        error_response(self.status(), self.code())
    }
}

/// The `201 Created` body of an upload: paths relative to the root, `/`-separated.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct UploadResponse {
    /// Every file that was written, in upload order.
    pub written: Vec<String>,
}

/// Resolves `requested` (the percent-encoded `?path=` value, `/`-separated) under `root`.
///
/// Rejects, in order: invalid UTF-8 after percent-decoding, empty paths, NUL bytes,
/// backslashes, absolute paths and drive prefixes (`InvalidPath`); `..` climbing above the
/// root (`Traversal`); and, after canonicalizing the root and the deepest existing ancestor of
/// the joined path, anything that resolves outside the canonical root (`Traversal`, which
/// defeats symlink escapes). Returns the joined, non-canonical path so a not-yet-existing file
/// name is preserved.
pub fn resolve_under_root(root: &Path, requested: &str) -> Result<PathBuf, FilesError> {
    let decoded = percent_encoding::percent_decode_str(requested)
        .decode_utf8()
        .map_err(|_| FilesError::InvalidPath)?;
    resolve_decoded(root, &decoded)
}

/// [`resolve_under_root`] for a value that is already percent-decoded.
///
/// The confinement check canonicalizes the deepest existing ancestor and then returns the
/// raw joined path, which the caller opens afterwards; a process with the same privileges as
/// the server could swap a component for a symlink in between. That is accepted: `serve`
/// runs as the same user as every shell, task and language server in the sandbox, so nothing
/// is gained by racing it. If a privilege boundary is ever introduced, open the final
/// component with `O_NOFOLLOW` (or `openat2(RESOLVE_BENEATH)`) instead.
pub fn resolve_decoded(root: &Path, requested: &str) -> Result<PathBuf, FilesError> {
    if requested.is_empty()
        || requested.contains('\0')
        || requested.contains('\\')
        || requested.starts_with('/')
        || has_drive_prefix(requested)
    {
        return Err(FilesError::InvalidPath);
    }
    let requested_path = Path::new(requested);
    if requested_path.is_absolute()
        || requested_path
            .components()
            .any(|component| matches!(component, Component::Prefix(_) | Component::RootDir))
    {
        return Err(FilesError::InvalidPath);
    }
    let relative = normalize_lexically(requested_path).map_err(|_| FilesError::Traversal)?;
    let canonical_root = root.canonicalize()?;
    let joined = canonical_root.join(relative);

    let mut ancestor = joined.as_path();
    loop {
        match ancestor.canonicalize() {
            Ok(canonical) => {
                if !canonical.starts_with(&canonical_root) {
                    return Err(FilesError::Traversal);
                }
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => match ancestor.parent() {
                Some(parent) => ancestor = parent,
                None => return Err(FilesError::Traversal),
            },
            // A path component below a regular file (`README.md/notes.txt`): bad input, not
            // a server fault.
            Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
                return Err(FilesError::InvalidPath);
            }
            // An ancestor the server may not traverse is indistinguishable from a missing one
            // to the caller.
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                return Err(FilesError::NotFound);
            }
            Err(error) => return Err(FilesError::Io(error)),
        }
    }
    Ok(joined)
}

/// Whether `id` matches the extension-id grammar `^[a-z0-9][a-z0-9_-]{0,63}$` (CONTRACTS §4).
/// Rejecting `.`, `..` and separators here keeps `/extensions/{id}/assets/*` inside one
/// extension's directory. The one definition lives in `crate::extensions`, which the
/// install paths use too.
pub fn is_valid_extension_id(id: &str) -> bool {
    crate::extensions::is_valid_extension_id(id)
}

fn has_drive_prefix(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

/// The `/`-separated path of `path` relative to `root`, for [`UploadResponse::written`].
fn relative_display(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path);
    relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The `?path=` query value (percent-decoded), if present.
fn path_param<B>(req: &Request<B>) -> Option<String> {
    query_pairs(req.uri().query().unwrap_or(""))
        .find(|(key, _)| key == "path")
        .map(|(_, value)| value.into_owned())
}

fn content_length<B>(req: &Request<B>) -> Option<u64> {
    req.headers()
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Only `[A-Za-z0-9_-]` of the token id survive into the temp-file suffix.
fn temp_tag(claims: &Claims) -> String {
    let tag: String = claims
        .jti
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .take(64)
        .collect();
    if tag.is_empty() {
        format!("{}", std::process::id())
    } else {
        tag
    }
}

fn temp_path(target: &Path, tag: &str) -> PathBuf {
    let mut name: OsString = target.as_os_str().to_owned();
    name.push(TEMP_SUFFIX);
    name.push(tag);
    PathBuf::from(name)
}

/// Running byte budget shared by every part of one upload.
struct UploadBudget {
    remaining: u64,
}

impl UploadBudget {
    fn consume(&mut self, len: usize) -> Result<(), FilesError> {
        let len = len as u64;
        if len > self.remaining {
            self.remaining = 0;
            return Err(FilesError::TooLarge);
        }
        self.remaining -= len;
        Ok(())
    }
}

/// Why reading an upload body stopped early.
#[derive(Debug)]
enum BodyReadError {
    /// The body ran past the upload limit.
    TooLarge,
    /// The transport failed mid-body.
    Body(String),
}

impl std::fmt::Display for BodyReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyReadError::TooLarge => write!(f, "upload body over the limit"),
            BodyReadError::Body(error) => write!(f, "reading the upload body failed: {error}"),
        }
    }
}

impl std::error::Error for BodyReadError {}

impl From<BodyReadError> for FilesError {
    fn from(error: BodyReadError) -> Self {
        match error {
            BodyReadError::TooLarge => FilesError::TooLarge,
            BodyReadError::Body(message) => FilesError::Malformed(message),
        }
    }
}

/// The request body as a byte stream that fails with [`BodyReadError::TooLarge`] at the
/// first byte past `limit`, whatever the transfer framing. This is what bounds memory: the
/// multipart parser buffers part headers until it sees their terminator, so without a cap on
/// the raw body a chunked request could feed it a header run until the process is killed.
fn bounded_body_stream<B>(
    body: B,
    limit: u64,
) -> Pin<Box<dyn Stream<Item = Result<Bytes, BodyReadError>> + Send>>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send,
{
    Box::pin(futures::stream::unfold(
        (BodyDataStream::new(body), limit),
        |(mut stream, remaining)| async move {
            match stream.next().await {
                None => None,
                Some(Err(error)) => Some((
                    Err(BodyReadError::Body(error.to_string())),
                    (stream, remaining),
                )),
                Some(Ok(chunk)) => {
                    if chunk.len() as u64 > remaining {
                        Some((Err(BodyReadError::TooLarge), (stream, 0)))
                    } else {
                        let remaining = remaining - chunk.len() as u64;
                        Some((Ok(chunk), (stream, remaining)))
                    }
                }
            }
        },
    ))
}

/// Maps a multipart parser error to the response it deserves: a size cap (the parser's own
/// or the bounded body underneath it) is 413, anything else is a malformed request.
fn multer_error(error: multer::Error) -> FilesError {
    match error {
        multer::Error::StreamSizeExceeded { .. } | multer::Error::FieldSizeExceeded { .. } => {
            FilesError::TooLarge
        }
        multer::Error::StreamReadFailed(inner)
            if inner
                .downcast_ref::<BodyReadError>()
                .is_some_and(|error| matches!(error, BodyReadError::TooLarge)) =>
        {
            FilesError::TooLarge
        }
        other => FilesError::Malformed(other.to_string()),
    }
}

/// A multipart part's `filename` must name a single file below the upload directory: not
/// empty, not `.` or `..`, no drive prefix, no NUL, and ending in a normal component (so
/// `a/` cannot resolve to the directory itself).
fn validate_upload_file_name(file_name: &str) -> Result<(), FilesError> {
    if file_name.is_empty()
        || file_name.contains('\0')
        || file_name.contains('\\')
        || file_name.starts_with('/')
        || has_drive_prefix(file_name)
    {
        return Err(FilesError::InvalidPath);
    }
    match Path::new(file_name).components().next_back() {
        Some(Component::Normal(_)) => Ok(()),
        _ => Err(FilesError::InvalidPath),
    }
}

/// Streams `chunks` into `<target><TEMP_SUFFIX><tag>` and renames it over `target`. On any
/// error the temp file is removed and nothing is left at `target`.
async fn write_stream_to_target<S>(
    target: &Path,
    tag: &str,
    mut chunks: S,
    budget: &mut UploadBudget,
) -> Result<(), FilesError>
where
    S: Stream<Item = Result<Bytes, FilesError>> + Unpin,
{
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temp = temp_path(target, tag);
    let result = async {
        let mut file = tokio::fs::File::create(&temp).await?;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            budget.consume(chunk.len())?;
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        }
        tokio::io::AsyncWriteExt::flush(&mut file).await?;
        drop(file);
        tokio::fs::rename(&temp, target).await?;
        Ok::<(), FilesError>(())
    }
    .await;
    if result.is_err()
        && let Err(error) = tokio::fs::remove_file(&temp).await
        && error.kind() != io::ErrorKind::NotFound
    {
        log::warn!("failed to remove upload temp file {temp:?}: {error}");
    }
    result
}

/// `POST /files?path=<dir>` with `multipart/form-data` (each part's `filename` is joined
/// under `<dir>`; nested `a/b.txt` names are allowed and re-validated), or
/// `POST /files?path=<file>` with `application/octet-stream` (the body is the file).
///
/// `Content-Length > MAX_UPLOAD_BYTES` is refused before the body is read; while reading, a
/// running counter across all parts refuses the first byte past the limit and more than
/// [`MAX_FILES_PER_UPLOAD`] parts. Each file streams into a temp file and is renamed into place.
/// After the last rename the upload counts as input and `GpuiCommand::FilesUploaded` tells the
/// project to rescan the written paths; the 201 does not wait for that.
pub async fn handle_upload<B>(
    state: &ServeState,
    claims: &Claims,
    req: Request<B>,
) -> Result<Response<BoxBody>, FilesError>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send,
{
    handle_upload_with_limit(state, claims, req, MAX_UPLOAD_BYTES).await
}

/// [`handle_upload`] with an explicit byte limit (tests exercise the cap without streaming
/// half a gigabyte).
pub(crate) async fn handle_upload_with_limit<B>(
    state: &ServeState,
    claims: &Claims,
    req: Request<B>,
    max_upload_bytes: u64,
) -> Result<Response<BoxBody>, FilesError>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send,
{
    let path = path_param(&req).ok_or(FilesError::InvalidPath)?;
    if content_length(&req).is_some_and(|len| len > max_upload_bytes) {
        return Err(FilesError::TooLarge);
    }
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_owned();
    let root = state.workspace_root.clone();
    let tag = temp_tag(claims);
    let mut budget = UploadBudget {
        remaining: max_upload_bytes,
    };
    let mut written: Vec<PathBuf> = Vec::new();

    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if media_type == "multipart/form-data" {
        let boundary = multer::parse_boundary(&content_type)
            .map_err(|error| FilesError::Malformed(error.to_string()))?;
        let directory = resolve_decoded(&root, &path)?;
        let body_stream = bounded_body_stream(req.into_body(), max_upload_bytes);
        let constraints = multer::Constraints::new()
            .size_limit(multer::SizeLimit::new().whole_stream(max_upload_bytes));
        let mut multipart = multer::Multipart::with_constraints(body_stream, boundary, constraints);
        let mut file_count = 0usize;
        loop {
            let field = match multipart.next_field().await {
                Ok(Some(field)) => field,
                Ok(None) => break,
                Err(error) => return Err(multer_error(error)),
            };
            // Parts without a filename (plain form fields) are skipped; the bounded body
            // stream above caps what draining them can cost.
            let Some(file_name) = field.file_name().map(str::to_owned) else {
                continue;
            };
            file_count += 1;
            if file_count > MAX_FILES_PER_UPLOAD {
                return Err(FilesError::TooManyFiles);
            }
            validate_upload_file_name(&file_name)?;
            let target = resolve_decoded(&root, &format!("{path}/{file_name}"))?;
            if target == directory {
                return Err(FilesError::InvalidPath);
            }
            if !target.starts_with(&directory) {
                return Err(FilesError::Traversal);
            }
            if tokio::fs::metadata(&target)
                .await
                .is_ok_and(|metadata| metadata.is_dir())
            {
                return Err(FilesError::InvalidPath);
            }
            let chunks = futures::stream::unfold(field, |mut field| async move {
                match field.chunk().await {
                    Ok(Some(chunk)) => Some((Ok(chunk), field)),
                    Ok(None) => None,
                    Err(error) => Some((Err(multer_error(error)), field)),
                }
            });
            futures::pin_mut!(chunks);
            write_stream_to_target(&target, &tag, chunks, &mut budget).await?;
            written.push(target);
        }
    } else if media_type == "application/octet-stream" {
        let target = resolve_decoded(&root, &path)?;
        if tokio::fs::metadata(&target)
            .await
            .is_ok_and(|metadata| metadata.is_dir())
        {
            return Err(FilesError::InvalidPath);
        }
        let mut chunks = bounded_body_stream(req.into_body(), max_upload_bytes)
            .map(|chunk| chunk.map_err(FilesError::from));
        write_stream_to_target(&target, &tag, &mut chunks, &mut budget).await?;
        written.push(target);
    } else {
        return Err(FilesError::UnsupportedMedia);
    }

    state.touch_input();
    let canonical_root = root.canonicalize().unwrap_or(root);
    let relative: Vec<String> = written
        .iter()
        .map(|path| relative_display(&canonical_root, path))
        .collect();
    if let Err(error) = state
        .gpui_tx
        .unbounded_send(GpuiCommand::FilesUploaded(written))
    {
        log::warn!("gpui command loop is gone; upload rescan skipped: {error}");
    }
    Ok(json_response(
        StatusCode::CREATED,
        &UploadResponse { written: relative },
    ))
}

/// `GET /files?path=<rel>`: a file streams as `application/octet-stream` with
/// `Content-Length` and `Content-Disposition: attachment`; a directory streams as
/// `application/x-tar` (symlinks are archived as links, never followed; more than
/// [`MAX_DOWNLOAD_ENTRIES`] entries abort the stream).
pub async fn handle_download<B>(
    state: &ServeState,
    req: Request<B>,
) -> Result<Response<BoxBody>, FilesError> {
    let path = path_param(&req).ok_or(FilesError::InvalidPath)?;
    let target = resolve_decoded(&state.workspace_root, &path)?;
    let metadata = match tokio::fs::metadata(&target).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(FilesError::NotFound),
        Err(error) => return Err(FilesError::Io(error)),
    };
    let name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "workspace".to_owned());
    if metadata.is_file() {
        file_response(
            &target,
            metadata.len(),
            "application/octet-stream",
            Some(&name),
        )
        .await
    } else if metadata.is_dir() {
        Ok(tar_response(target, name))
    } else {
        Err(FilesError::NotFound)
    }
}

/// `GET /extensions/{id}/assets/{rel}`: `id` and `rel` are percent-decoded path segments.
/// Both are rejected with 400 before any gpui round trip when they contain `..`, NUL, a
/// backslash or an absolute component; resolution itself happens on the gpui side
/// (`GpuiCommand::ResolveExtensionAsset`), which answers `None` for unknown ids, uninstalled
/// extensions and any traversal. The file streams with a `Content-Type` derived from its
/// extension and no `Content-Disposition`; directories are 404.
pub async fn handle_extension_asset(
    state: &ServeState,
    id: String,
    rel: String,
) -> Result<Response<BoxBody>, FilesError> {
    if !is_valid_extension_id(&id) {
        return Err(FilesError::InvalidPath);
    }
    validate_asset_segment(&rel)?;
    let (reply_tx, reply_rx) = futures::channel::oneshot::channel();
    state
        .gpui_tx
        .unbounded_send(GpuiCommand::ResolveExtensionAsset {
            id,
            rel,
            reply: reply_tx,
        })
        .map_err(|_| io::Error::other("gpui command loop is gone"))?;
    let Some(path) = reply_rx.await.map_err(|_| FilesError::NotFound)? else {
        return Err(FilesError::NotFound);
    };
    let metadata = match tokio::fs::metadata(&path).await {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return Err(FilesError::NotFound),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(FilesError::NotFound),
        Err(error) => return Err(FilesError::Io(error)),
    };
    file_response(&path, metadata.len(), content_type_for(&path), None).await
}

fn validate_asset_segment(segment: &str) -> Result<(), FilesError> {
    if segment.is_empty()
        || segment.contains('\0')
        || segment.contains('\\')
        || segment.starts_with('/')
        || has_drive_prefix(segment)
        || segment.split('/').any(|part| part == "..")
    {
        return Err(FilesError::InvalidPath);
    }
    Ok(())
}

/// `Content-Type` for an extension asset, by file extension.
pub fn content_type_for(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("ttf") => "font/ttf",
        Some("woff2") => "font/woff2",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

/// Streams `path` in [`READ_CHUNK_BYTES`] reads.
async fn file_response(
    path: &Path,
    len: u64,
    content_type: &str,
    attachment_name: Option<&str>,
) -> Result<Response<BoxBody>, FilesError> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(FilesError::NotFound),
        Err(error) => return Err(FilesError::Io(error)),
    };
    let stream = futures::stream::unfold(Some(file), |file| async move {
        let mut file = file?;
        let mut buffer = vec![0u8; READ_CHUNK_BYTES];
        match file.read(&mut buffer).await {
            Ok(0) => None,
            Ok(read) => {
                buffer.truncate(read);
                Some((Ok(Frame::data(Bytes::from(buffer))), Some(file)))
            }
            Err(error) => Some((Err(error), None)),
        }
    });
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, len);
    if let Some(name) = attachment_name {
        builder = builder.header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", sanitize_filename(name)),
        );
    }
    builder
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .map_err(|error| FilesError::Io(io::Error::other(error)))
}

/// Keeps a `Content-Disposition` filename inside a quoted string.
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|character| match character {
            '"' | '\\' | '\r' | '\n' => '_',
            other if other.is_control() => '_',
            other => other,
        })
        .collect()
}

/// A directory as a streamed tar archive.
fn tar_response(directory: PathBuf, name: String) -> Response<BoxBody> {
    let (sender, receiver) = mpsc::channel::<Result<Frame<Bytes>, io::Error>>(TAR_CHANNEL_FRAMES);
    let archive_name = name.clone();
    tokio::spawn(async move {
        let mut builder = async_tar::Builder::new(ChannelWriter { sender });
        builder.follow_symlinks(false);
        let result = append_directory_bounded(&mut builder, &archive_name, &directory).await;
        let result = match result {
            Ok(()) => builder.finish().await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            log::warn!("tar download of {directory:?} aborted: {error}");
            let mut writer = builder.into_inner().await.ok();
            if let Some(writer) = writer.as_mut() {
                writer.abort(error);
            }
        }
    });
    let body = BodyExt::boxed(StreamBody::new(receiver));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-tar")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}.tar\"", sanitize_filename(&name)),
        )
        .body(body)
        .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "io_error"))
}

/// Walks `directory` iteratively, appending every entry under `archive_name` and counting
/// them against [`MAX_DOWNLOAD_ENTRIES`]. Symlinks are appended as link entries and never
/// descended. Sockets, FIFOs and device nodes are skipped, as are files that vanish or become
/// unreadable mid-walk: the 200 has already been sent, so aborting the stream would hand the
/// browser a truncated archive over one stray `.sock` in `node_modules`.
async fn append_directory_bounded(
    builder: &mut async_tar::Builder<ChannelWriter>,
    archive_name: &str,
    directory: &Path,
) -> io::Result<()> {
    let root_name = PathBuf::from(archive_name);
    builder.append_dir(&root_name, directory).await?;
    let mut entries = 1usize;
    let mut stack = vec![(directory.to_path_buf(), root_name)];
    while let Some((source_dir, archive_dir)) = stack.pop() {
        let mut read_dir = match tokio::fs::read_dir(&source_dir).await {
            Ok(read_dir) => read_dir,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                log::warn!("skipping unreadable directory {source_dir:?} in a download");
                continue;
            }
            Err(error) => return Err(error),
        };
        while let Some(entry) = read_dir.next_entry().await? {
            entries += 1;
            if entries > MAX_DOWNLOAD_ENTRIES {
                return Err(io::Error::other(format!(
                    "directory has more than {MAX_DOWNLOAD_ENTRIES} entries"
                )));
            }
            let source = entry.path();
            let archive_path = archive_dir.join(entry.file_name());
            let file_type = match entry.file_type().await {
                Ok(file_type) => file_type,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if file_type.is_dir() {
                builder.append_dir(&archive_path, &source).await?;
                stack.push((source, archive_path));
            } else if file_type.is_file() || file_type.is_symlink() {
                match builder.append_path_with_name(&source, &archive_path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                        log::warn!("skipping unreadable file {source:?} in a download");
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        log::debug!("skipping {source:?}: removed during the download");
                    }
                    Err(error) => return Err(error),
                }
            } else {
                log::debug!("skipping special file {source:?} in a download");
            }
        }
    }
    Ok(())
}

/// Removes leftover `*<TEMP_SUFFIX>*` files under `root` whose modification time predates
/// `older_than` (bounded walk, best effort). Called once at startup with the moment the
/// listeners were bound, so an upload that starts while the sweep is still walking a large
/// workspace never has its temp file unlinked from under it.
pub async fn sweep_temp_files(root: &Path, older_than: SystemTime) {
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    while let Some((directory, depth)) = stack.pop() {
        let Ok(mut read_dir) = tokio::fs::read_dir(&directory).await else {
            continue;
        };
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            visited += 1;
            if visited > SWEEP_MAX_ENTRIES {
                log::warn!("temp-file sweep stopped after {SWEEP_MAX_ENTRIES} entries");
                return;
            }
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                if depth < SWEEP_MAX_DEPTH {
                    stack.push((entry.path(), depth + 1));
                }
            } else if entry.file_name().to_string_lossy().contains(TEMP_SUFFIX) {
                let is_stale = match entry
                    .metadata()
                    .await
                    .and_then(|metadata| metadata.modified())
                {
                    Ok(modified) => modified < older_than,
                    Err(error) => {
                        log::debug!("cannot read the mtime of {:?}: {error}", entry.path());
                        false
                    }
                };
                if !is_stale {
                    continue;
                }
                match tokio::fs::remove_file(entry.path()).await {
                    Ok(()) => log::info!("removed stale upload temp file {:?}", entry.path()),
                    Err(error) => {
                        log::warn!("failed to remove {:?}: {error}", entry.path())
                    }
                }
            }
        }
    }
}

/// `futures::AsyncWrite` adapter that forwards each write as a body frame into a bounded
/// channel; the receiver is the response body.
struct ChannelWriter {
    sender: mpsc::Sender<Result<Frame<Bytes>, io::Error>>,
}

impl ChannelWriter {
    fn abort(&mut self, error: io::Error) {
        self.sender.try_send(Err(error)).ok();
        self.sender.close_channel();
    }
}

impl futures::AsyncWrite for ChannelWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let frame = Frame::data(Bytes::copy_from_slice(buf));
                match self.sender.start_send(Ok(frame)) {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(_) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
                }
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.sender.close_channel();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::{
        auth::test_support,
        http::{ServeConfig, ServeState},
    };
    use http_body_util::Full;
    use std::sync::Arc;

    fn test_state(root: &Path) -> (Arc<ServeState>, mpsc::UnboundedReceiver<GpuiCommand>) {
        let (broker_tx, _broker_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gpui_tx, gpui_rx) = mpsc::unbounded();
        let state = ServeState::new(
            ServeConfig {
                build: "test".into(),
                version: "test".into(),
                workspace_id: test_support::WORKSPACE.into(),
                workspace_root: root.canonicalize().unwrap(),
                auth: test_support::config(),
                allowed_origins: Vec::new(),
                client_build: None,
            },
            broker_tx,
            gpui_tx,
        );
        (Arc::new(state), gpui_rx)
    }

    fn claims() -> Claims {
        test_support::config()
            .verify(&test_support::token("sid_files"))
            .unwrap()
    }

    fn request(
        method: &str,
        target: &str,
        content_type: &str,
        body: Bytes,
    ) -> Request<Full<Bytes>> {
        Request::builder()
            .method(method)
            .uri(target)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, body.len())
            .body(Full::new(body))
            .unwrap()
    }

    async fn body_bytes(response: Response<BoxBody>) -> Bytes {
        response.into_body().collect().await.unwrap().to_bytes()
    }

    fn multipart_body(boundary: &str, parts: &[(&str, &[u8])]) -> Bytes {
        let mut body = Vec::new();
        for (name, content) in parts {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"file\"; filename=\"{name}\"\r\n")
                    .as_bytes(),
            );
            body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
            body.extend_from_slice(content);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        Bytes::from(body)
    }

    #[test]
    fn resolve_accepts_relative() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        assert_eq!(
            resolve_under_root(&root, "a/b.txt").unwrap(),
            root.join("a/b.txt")
        );
        assert_eq!(resolve_under_root(&root, "./a").unwrap(), root.join("a"));
        assert_eq!(
            resolve_under_root(&root, "a/./b").unwrap(),
            root.join("a/b")
        );
        assert_eq!(
            resolve_under_root(&root, "a%2Fb.txt").unwrap(),
            root.join("a/b.txt")
        );
    }

    #[test]
    fn resolve_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        assert!(matches!(
            resolve_under_root(root, "../x"),
            Err(FilesError::Traversal)
        ));
        assert!(matches!(
            resolve_under_root(root, "a/../../x"),
            Err(FilesError::Traversal)
        ));
        for invalid in ["/etc/passwd", "a\0b", "", "C:\\x", "a\\b"] {
            assert!(
                matches!(
                    resolve_under_root(root, invalid),
                    Err(FilesError::InvalidPath)
                ),
                "{invalid:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn resolve_rejects_symlink_escape() {
        let outside = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).unwrap();
        assert!(matches!(
            resolve_under_root(dir.path(), "link/file"),
            Err(FilesError::Traversal)
        ));
        std::fs::write(outside.path().join("file"), b"x").unwrap();
        assert!(matches!(
            resolve_under_root(dir.path(), "link/file"),
            Err(FilesError::Traversal)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn octet_stream_upload_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let (state, mut gpui_rx) = test_state(dir.path());
        let response = handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=dir%2Ff.bin",
                "application/octet-stream",
                Bytes::from_static(b"hello"),
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: UploadResponse = serde_json::from_slice(&body_bytes(response).await).unwrap();
        assert_eq!(body.written, vec!["dir/f.bin".to_owned()]);
        assert_eq!(
            std::fs::read(dir.path().join("dir/f.bin")).unwrap(),
            b"hello"
        );
        assert!(
            !std::fs::read_dir(dir.path().join("dir"))
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(TEMP_SUFFIX))
        );
        assert!(state.health(true).last_input_at.is_some());
        match gpui_rx.next().await {
            Some(GpuiCommand::FilesUploaded(paths)) => {
                assert_eq!(paths, vec![state.workspace_root.join("dir/f.bin")])
            }
            other => panic!("unexpected command {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_upload_writes_all_parts() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let boundary = "zsboundary";
        let body = multipart_body(boundary, &[("a.txt", b"aaa"), ("sub/b.txt", b"bbb")]);
        let response = handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=up",
                &format!("multipart/form-data; boundary={boundary}"),
                body,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: UploadResponse = serde_json::from_slice(&body_bytes(response).await).unwrap();
        assert_eq!(
            body.written,
            vec!["up/a.txt".to_owned(), "up/sub/b.txt".to_owned()]
        );
        assert_eq!(std::fs::read(dir.path().join("up/a.txt")).unwrap(), b"aaa");
        assert_eq!(
            std::fs::read(dir.path().join("up/sub/b.txt")).unwrap(),
            b"bbb"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upload_sends_files_uploaded_command() {
        let dir = tempfile::tempdir().unwrap();
        let (state, mut gpui_rx) = test_state(dir.path());
        let boundary = "zsboundary";
        let body = multipart_body(boundary, &[("a.txt", b"aaa"), ("sub/b.txt", b"bbb")]);
        handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=.",
                &format!("multipart/form-data; boundary={boundary}"),
                body,
            ),
        )
        .await
        .unwrap();
        match gpui_rx.next().await {
            Some(GpuiCommand::FilesUploaded(paths)) => {
                assert_eq!(
                    paths,
                    vec![
                        state.workspace_root.join("a.txt"),
                        state.workspace_root.join("sub/b.txt")
                    ]
                );
                for path in paths {
                    assert!(path.exists());
                }
            }
            other => panic!("unexpected command {other:?}"),
        }
        assert!(
            gpui_rx.try_recv().is_err(),
            "exactly one FilesUploaded command"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_upload_rejects_traversal_in_filename() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let boundary = "zsboundary";
        let body = multipart_body(boundary, &[("../escape.txt", b"x")]);
        let error = handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=up",
                &format!("multipart/form-data; boundary={boundary}"),
                body,
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, FilesError::Traversal), "{error:?}");
        assert!(!dir.path().join("escape.txt").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upload_too_large_is_413_early() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let req = Request::builder()
            .method("POST")
            .uri("/files?path=big.bin")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, MAX_UPLOAD_BYTES + 1)
            .body(Full::new(Bytes::from_static(b"tiny")))
            .unwrap();
        let error = handle_upload(&state, &claims(), req).await.unwrap_err();
        assert!(matches!(error, FilesError::TooLarge));
        assert!(!dir.path().join("big.bin").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upload_too_large_is_413_streaming() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let chunk = Bytes::from(vec![0u8; 1024 * 1024]);
        let chunks = (MAX_UPLOAD_BYTES / chunk.len() as u64) + 1;
        let stream = futures::stream::iter((0..chunks).map({
            let chunk = chunk.clone();
            move |_| Ok::<_, io::Error>(Frame::data(chunk.clone()))
        }));
        let req = Request::builder()
            .method("POST")
            .uri("/files?path=big.bin")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(StreamBody::new(stream))
            .unwrap();
        let error = handle_upload(&state, &claims(), req).await.unwrap_err();
        assert!(matches!(error, FilesError::TooLarge));
        assert!(!dir.path().join("big.bin").exists());
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upload_too_many_parts_is_413() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let boundary = "zsboundary";
        let names: Vec<String> = (0..=MAX_FILES_PER_UPLOAD)
            .map(|index| format!("f{index}.txt"))
            .collect();
        let parts: Vec<(&str, &[u8])> = names
            .iter()
            .map(|name| (name.as_str(), &b"x"[..]))
            .collect();
        let body = multipart_body(boundary, &parts);
        let error = handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=many",
                &format!("multipart/form-data; boundary={boundary}"),
                body,
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, FilesError::TooManyFiles), "{error:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unsupported_media_is_415() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let error = handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=x",
                "text/plain",
                Bytes::from_static(b"x"),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, FilesError::UnsupportedMedia));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_file_streams_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let content: Vec<u8> = (0..(READ_CHUNK_BYTES * 3 + 17)).map(|i| i as u8).collect();
        std::fs::write(dir.path().join("data.bin"), &content).unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let response = handle_download(
            &state,
            request("GET", "/files?path=data.bin", "", Bytes::new()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            content.len().to_string()
        );
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"data.bin\""
        );
        assert_eq!(body_bytes(response).await, content);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_dir_is_tar() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("d/sub")).unwrap();
        std::fs::write(dir.path().join("d/a.txt"), b"a").unwrap();
        std::fs::write(dir.path().join("d/sub/b.txt"), b"bb").unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let response = handle_download(&state, request("GET", "/files?path=d", "", Bytes::new()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/x-tar"
        );
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"d.tar\""
        );
        let bytes = body_bytes(response).await;
        let names = tar_entry_names(&bytes).await;
        assert_eq!(names, vec!["d", "d/a.txt", "d/sub", "d/sub/b.txt"]);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn download_dir_does_not_follow_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("d")).unwrap();
        std::os::unix::fs::symlink("/", dir.path().join("d/link")).unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let response = handle_download(&state, request("GET", "/files?path=d", "", Bytes::new()))
            .await
            .unwrap();
        let bytes = body_bytes(response).await;
        let names = tar_entry_names(&bytes).await;
        assert_eq!(names, vec!["d", "d/link"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn download_missing_is_404() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let error = handle_download(
            &state,
            request("GET", "/files?path=nope.txt", "", Bytes::new()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, FilesError::NotFound));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sweep_removes_stale_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("nested")).unwrap();
        std::fs::write(dir.path().join(format!("x{TEMP_SUFFIX}old")), b"").unwrap();
        std::fs::write(dir.path().join(format!("nested/y{TEMP_SUFFIX}old")), b"").unwrap();
        std::fs::write(dir.path().join("x"), b"keep").unwrap();
        sweep_temp_files(
            dir.path(),
            SystemTime::now() + std::time::Duration::from_secs(60),
        )
        .await;
        assert!(!dir.path().join(format!("x{TEMP_SUFFIX}old")).exists());
        assert!(
            !dir.path()
                .join(format!("nested/y{TEMP_SUFFIX}old"))
                .exists()
        );
        assert_eq!(std::fs::read(dir.path().join("x")).unwrap(), b"keep");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sweep_keeps_temp_files_newer_than_the_cutoff() {
        let dir = tempfile::tempdir().unwrap();
        let in_progress = dir.path().join(format!("upload.bin{TEMP_SUFFIX}live"));
        std::fs::write(&in_progress, b"partial").unwrap();
        sweep_temp_files(
            dir.path(),
            SystemTime::now() - std::time::Duration::from_secs(3600),
        )
        .await;
        assert!(in_progress.exists(), "an upload in progress is never swept");
    }

    #[test]
    fn resolve_rejects_a_path_below_a_file_as_bad_input() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("README.md"), b"x").unwrap();
        assert!(matches!(
            resolve_under_root(dir.path(), "README.md/notes.txt"),
            Err(FilesError::InvalidPath)
        ));
    }

    #[test]
    fn extension_id_grammar() {
        for valid in [
            "toml",
            "zed-lua",
            "a",
            "x_1",
            &"a".repeat(MAX_EXTENSION_ID_BYTES),
        ] {
            assert!(is_valid_extension_id(valid), "{valid:?}");
        }
        for invalid in [
            "",
            ".",
            "..",
            "-lead",
            "_lead",
            "Upper",
            "a/b",
            "a b",
            &"a".repeat(MAX_EXTENSION_ID_BYTES + 1),
        ] {
            assert!(!is_valid_extension_id(invalid), "{invalid:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_asset_rejects_dot_id_before_gpui() {
        let dir = tempfile::tempdir().unwrap();
        let (state, mut gpui_rx) = test_state(dir.path());
        for id in [".", "..", "Theme", "a/b"] {
            let error = handle_extension_asset(&state, id.into(), "x.json".into())
                .await
                .unwrap_err();
            assert!(
                matches!(error, FilesError::InvalidPath),
                "{id:?}: {error:?}"
            );
        }
        drop(state);
        assert!(gpui_rx.next().await.is_none(), "no gpui round trip");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_upload_rejects_filenames_that_name_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let boundary = "zsboundary";
        // (`sub/` and `sub/.` normalize to the file `sub`, which is a valid target.)
        for file_name in ["", ".", "..", "./", "../"] {
            let body = multipart_body(boundary, &[(file_name, b"x"), ("a.txt", b"a")]);
            let error = handle_upload(
                &state,
                &claims(),
                request(
                    "POST",
                    "/files?path=newdir",
                    &format!("multipart/form-data; boundary={boundary}"),
                    body,
                ),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(error, FilesError::InvalidPath | FilesError::Traversal),
                "{file_name:?}: {error:?}"
            );
            assert!(
                !dir.path().join("newdir").is_file(),
                "{file_name:?} must not turn the upload directory into a file"
            );
        }

        // A filename that resolves to an existing directory is refused too.
        std::fs::create_dir_all(dir.path().join("up/sub")).unwrap();
        let body = multipart_body(boundary, &[("sub", b"x")]);
        let error = handle_upload(
            &state,
            &claims(),
            request(
                "POST",
                "/files?path=up",
                &format!("multipart/form-data; boundary={boundary}"),
                body,
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, FilesError::InvalidPath), "{error:?}");
        assert!(dir.path().join("up/sub").is_dir());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chunked_multipart_header_run_is_413_with_bounded_memory() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

        let dir = tempfile::tempdir().unwrap();
        let (state, _gpui_rx) = test_state(dir.path());
        let limit = 1024 * 1024u64;
        let chunk_len = 64 * 1024usize;
        let pulled = Arc::new(AtomicU64::new(0));
        // A part header that never terminates, streamed with no Content-Length.
        let stream = futures::stream::iter((0usize..).map({
            let pulled = pulled.clone();
            move |index| {
                let data = if index == 0 {
                    Bytes::from_static(b"--zsboundary\r\nX-Pad: ")
                } else {
                    Bytes::from(vec![b'a'; chunk_len])
                };
                pulled.fetch_add(data.len() as u64, SeqCst);
                Ok::<_, io::Error>(Frame::data(data))
            }
        }));
        let req = Request::builder()
            .method("POST")
            .uri("/files?path=up")
            .header(
                header::CONTENT_TYPE,
                "multipart/form-data; boundary=zsboundary",
            )
            .body(StreamBody::new(stream))
            .unwrap();
        let error = handle_upload_with_limit(&state, &claims(), req, limit)
            .await
            .unwrap_err();
        assert!(matches!(error, FilesError::TooLarge), "{error:?}");
        assert!(
            pulled.load(SeqCst) <= limit + 2 * chunk_len as u64,
            "pulled {} bytes past a {limit} byte limit",
            pulled.load(SeqCst)
        );
        assert!(!dir.path().join("up").exists());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn download_dir_skips_special_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("d")).unwrap();
        std::fs::write(dir.path().join("d/a.txt"), b"a").unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(dir.path().join("d/x.sock")).unwrap();
        let fifo = std::ffi::CString::new(
            dir.path()
                .join("d/pipe")
                .to_str()
                .unwrap()
                .as_bytes()
                .to_vec(),
        )
        .unwrap();
        // Safe: a valid NUL-terminated path; the result is checked.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o644) }, 0);
        let (state, _gpui_rx) = test_state(dir.path());
        let response = handle_download(&state, request("GET", "/files?path=d", "", Bytes::new()))
            .await
            .unwrap();
        let bytes = body_bytes(response).await;
        let names = tar_entry_names(&bytes).await;
        assert_eq!(names, vec!["d", "d/a.txt"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_asset_streams_file() {
        let dir = tempfile::tempdir().unwrap();
        let asset = dir.path().join("x.json");
        std::fs::write(&asset, b"{\"theme\":1}").unwrap();
        let (state, mut gpui_rx) = test_state(dir.path());
        let asset_path = asset.clone();
        let stub = tokio::spawn(async move {
            match gpui_rx.next().await {
                Some(GpuiCommand::ResolveExtensionAsset { id, rel, reply }) => {
                    assert_eq!(id, "theme-x");
                    assert_eq!(rel, "themes/x.json");
                    reply.send(Some(asset_path)).ok();
                }
                other => panic!("unexpected command {other:?}"),
            }
        });
        let response = handle_extension_asset(&state, "theme-x".into(), "themes/x.json".into())
            .await
            .unwrap();
        stub.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert!(
            response
                .headers()
                .get(header::CONTENT_DISPOSITION)
                .is_none()
        );
        assert_eq!(body_bytes(response).await, &b"{\"theme\":1}"[..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_asset_unknown_is_404() {
        let dir = tempfile::tempdir().unwrap();
        let (state, mut gpui_rx) = test_state(dir.path());
        let stub = tokio::spawn(async move {
            match gpui_rx.next().await {
                Some(GpuiCommand::ResolveExtensionAsset { reply, .. }) => {
                    reply.send(None).ok();
                }
                other => panic!("unexpected command {other:?}"),
            }
            assert!(
                gpui_rx.next().await.is_none(),
                "traversal must not reach gpui"
            );
        });
        let error = handle_extension_asset(&state, "nope".into(), "x.json".into())
            .await
            .unwrap_err();
        assert!(matches!(error, FilesError::NotFound));
        let error = handle_extension_asset(&state, "theme".into(), "../x".into())
            .await
            .unwrap_err();
        assert!(matches!(error, FilesError::InvalidPath));
        let error = handle_extension_asset(&state, "../x".into(), "x.json".into())
            .await
            .unwrap_err();
        assert!(matches!(error, FilesError::InvalidPath));
        drop(state);
        stub.await.unwrap();
    }

    async fn tar_entry_names(bytes: &[u8]) -> Vec<String> {
        use futures::StreamExt as _;
        let archive = async_tar::Archive::new(bytes);
        let mut entries = archive.entries().unwrap();
        let mut names = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            names.push(path.trim_end_matches('/').to_owned());
        }
        // Directory iteration order is filesystem-defined; the archive content is what matters.
        names.sort();
        names
    }
}
