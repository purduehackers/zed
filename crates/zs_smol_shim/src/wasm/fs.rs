//! `smol::fs` on wasm32-unknown-unknown: async-fs 2's surface (sized to the calls in the browser
//! crate set), every operation returning `io::ErrorKind::Unsupported` immediately. Browser code
//! reaches files through the `Fs` trait or the remote protocol.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::{AsyncRead, AsyncSeek, AsyncWrite, Stream};

use super::unsupported;

/// Unsupported in the browser.
pub async fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let _ = path;
    Err(unsupported("smol::fs::canonicalize"))
}

/// Unsupported in the browser.
pub async fn copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<u64> {
    let _ = (src, dst);
    Err(unsupported("smol::fs::copy"))
}

/// Unsupported in the browser.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let _ = path;
    Err(unsupported("smol::fs::create_dir"))
}

/// Unsupported in the browser.
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let _ = path;
    Err(unsupported("smol::fs::create_dir_all"))
}

/// Unsupported in the browser.
pub async fn hard_link(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let _ = (src, dst);
    Err(unsupported("smol::fs::hard_link"))
}

/// Unsupported in the browser.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<std::fs::Metadata> {
    let _ = path;
    Err(unsupported("smol::fs::metadata"))
}

/// Unsupported in the browser.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let _ = path;
    Err(unsupported("smol::fs::read"))
}

/// Unsupported in the browser.
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let _ = path;
    Err(unsupported("smol::fs::read_dir"))
}

/// Unsupported in the browser.
pub async fn read_link(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let _ = path;
    Err(unsupported("smol::fs::read_link"))
}

/// Unsupported in the browser.
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let _ = path;
    Err(unsupported("smol::fs::read_to_string"))
}

/// Unsupported in the browser.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let _ = path;
    Err(unsupported("smol::fs::remove_dir"))
}

/// Unsupported in the browser.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let _ = path;
    Err(unsupported("smol::fs::remove_dir_all"))
}

/// Unsupported in the browser.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let _ = path;
    Err(unsupported("smol::fs::remove_file"))
}

/// Unsupported in the browser.
pub async fn rename(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let _ = (src, dst);
    Err(unsupported("smol::fs::rename"))
}

/// Unsupported in the browser.
pub async fn set_permissions(path: impl AsRef<Path>, perm: std::fs::Permissions) -> io::Result<()> {
    let _ = (path, perm);
    Err(unsupported("smol::fs::set_permissions"))
}

/// Unsupported in the browser.
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<std::fs::Metadata> {
    let _ = path;
    Err(unsupported("smol::fs::symlink_metadata"))
}

/// Unsupported in the browser.
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let _ = (path, contents);
    Err(unsupported("smol::fs::write"))
}

/// A builder for creating directories; `create` is unsupported in the browser.
#[derive(Debug, Default, Clone)]
pub struct DirBuilder {
    recursive: bool,
}

impl DirBuilder {
    /// Creates a builder with `recursive` off.
    pub fn new() -> DirBuilder {
        DirBuilder { recursive: false }
    }

    /// Sets whether parent directories are created too.
    pub fn recursive(&mut self, recursive: bool) -> &mut Self {
        self.recursive = recursive;
        self
    }

    /// Unsupported in the browser.
    pub async fn create(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let _ = (path, self.recursive);
        Err(unsupported("smol::fs::DirBuilder::create"))
    }
}

/// A stream of directory entries; never constructed in the browser (`read_dir` fails), and
/// empty if it ever were.
#[derive(Debug)]
pub struct ReadDir {
    _private: (),
}

impl Stream for ReadDir {
    type Item = io::Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// An entry yielded by `ReadDir`; only its path is known.
#[derive(Debug, Clone)]
pub struct DirEntry {
    path: PathBuf,
}

impl DirEntry {
    /// The full path of the entry.
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// The final component of the entry's path.
    pub fn file_name(&self) -> OsString {
        self.path
            .file_name()
            .map(OsString::from)
            .unwrap_or_default()
    }

    /// Unsupported in the browser.
    pub async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        Err(unsupported("smol::fs::DirEntry::metadata"))
    }

    /// Unsupported in the browser.
    pub async fn file_type(&self) -> io::Result<std::fs::FileType> {
        Err(unsupported("smol::fs::DirEntry::file_type"))
    }
}

/// An open file; never constructed in the browser (`open`/`create` fail). Its I/O trait
/// implementations exist so `BufReader<File>`/`BufWriter<File>` type-check, and they fail
/// with `Unsupported` if ever polled.
#[derive(Debug)]
pub struct File {
    path: PathBuf,
}

impl File {
    /// Unsupported in the browser.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        let _ = path;
        Err(unsupported("smol::fs::File::open"))
    }

    /// Unsupported in the browser.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        let _ = path;
        Err(unsupported("smol::fs::File::create"))
    }

    /// Unsupported in the browser.
    pub async fn sync_all(&self) -> io::Result<()> {
        Err(unsupported("smol::fs::File::sync_all"))
    }

    /// Unsupported in the browser.
    pub async fn sync_data(&self) -> io::Result<()> {
        Err(unsupported("smol::fs::File::sync_data"))
    }

    /// Unsupported in the browser.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        let _ = size;
        Err(unsupported("smol::fs::File::set_len"))
    }

    /// Unsupported in the browser.
    pub async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        let _ = &self.path;
        Err(unsupported("smol::fs::File::metadata"))
    }

    /// Unsupported in the browser.
    pub async fn set_permissions(&self, perm: std::fs::Permissions) -> io::Result<()> {
        let _ = perm;
        Err(unsupported("smol::fs::File::set_permissions"))
    }
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::fs::File")))
    }
}

impl AsyncWrite for File {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::fs::File")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported("smol::fs::File")))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported("smol::fs::File")))
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        Poll::Ready(Err(unsupported("smol::fs::File")))
    }
}

/// Options for opening a file; `open` is unsupported in the browser.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
}

impl OpenOptions {
    /// Creates a blank set of options.
    pub fn new() -> OpenOptions {
        OpenOptions::default()
    }

    /// Sets the read flag.
    pub fn read(&mut self, v: bool) -> &mut Self {
        self.read = v;
        self
    }

    /// Sets the write flag.
    pub fn write(&mut self, v: bool) -> &mut Self {
        self.write = v;
        self
    }

    /// Sets the append flag.
    pub fn append(&mut self, v: bool) -> &mut Self {
        self.append = v;
        self
    }

    /// Sets the truncate flag.
    pub fn truncate(&mut self, v: bool) -> &mut Self {
        self.truncate = v;
        self
    }

    /// Sets the create flag.
    pub fn create(&mut self, v: bool) -> &mut Self {
        self.create = v;
        self
    }

    /// Sets the create-new flag.
    pub fn create_new(&mut self, v: bool) -> &mut Self {
        self.create_new = v;
        self
    }

    /// Unsupported in the browser.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let _ = (
            path,
            self.read,
            self.write,
            self.append,
            self.truncate,
            self.create,
            self.create_new,
        );
        Err(unsupported("smol::fs::OpenOptions::open"))
    }
}
