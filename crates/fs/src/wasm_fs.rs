//! In-memory filesystem for the browser client. Holds the few files Zed reads
//! and writes locally (settings, keymap, snippets, prompt overrides) and emits
//! change events so `settings::watch_config_file` and friends work unchanged.
//!
//! Paths must be absolute POSIX paths; `path::normalize_path` strips `.` and
//! `..` (`/..` stays `/`), and every entry point rejects a relative path with an
//! error rather than guessing a root.
//!
//! Compiled for the browser and, so it is unit-tested natively, under the
//! `test-support` feature (the crate's only test target is `tests/integration`,
//! which enables it).

use crate::{
    CopyOptions, CreateOptions, FileHandle, Fs, JobEventReceiver, JobEventSender, MTime, Metadata,
    PathEvent, PathEventKind, RemoveOptions, RenameOptions, TrashId, TrashRestoreError, Watcher,
};
use anyhow::{Context as _, Result, anyhow, bail};
use futures::{AsyncRead, AsyncReadExt as _, Stream, StreamExt as _};
use gpui::BackgroundExecutor;
use path::normalize_path;
use rope::Rope;
use slotmap::SlotMap;
use std::{
    cell::UnsafeCell,
    collections::BTreeMap,
    ffi::OsString,
    io,
    ops::{Deref, DerefMut},
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use text::LineEnding;

/// `WasmFs` is written from the main thread (`SettingsStore`'s foreground update
/// task) and read from workers (`settings::watch_config_file`). A contended
/// `parking_lot` mutex parks with `Atomics.wait`, which traps on the browser main
/// thread, so on wasm the state lock is a spin lock: every critical section here
/// is a tree walk or a `Vec` push and never awaits. Natively (`test-support`
/// builds) it is `parking_lot`.
#[cfg(not(target_family = "wasm"))]
type FsMutex<T> = parking_lot::Mutex<T>;
#[cfg(target_family = "wasm")]
type FsMutex<T> = SpinMutex<T>;

/// Maximum number of symlinks followed while resolving one path (POSIX `ELOOP`).
const MAX_SYMLINK_HOPS: usize = 40;

/// Minimal test-and-test-and-set spin lock. It is the state lock of [`WasmFs`]
/// in the browser, where a parked thread would trap; public so the crate's
/// integration tests can exercise it natively.
pub struct SpinMutex<T> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

// SAFETY: the lock hands out at most one guard at a time, so `T` is only ever
// accessed by one thread; sending the value requires `T: Send`, as for any mutex.
unsafe impl<T: Send> Send for SpinMutex<T> {}
unsafe impl<T: Send> Sync for SpinMutex<T> {}

impl<T> SpinMutex<T> {
    /// Wraps `value` in an unlocked mutex.
    pub const fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    /// Acquires the lock, spinning until it is free.
    pub fn lock(&self) -> SpinGuard<'_, T> {
        loop {
            if !self.locked.swap(true, Ordering::Acquire) {
                return SpinGuard { mutex: self };
            }
            while self.locked.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }
        }
    }
}

impl<T: Default> Default for SpinMutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Exclusive access to the value behind a [`SpinMutex`]; releases the lock on drop.
pub struct SpinGuard<'a, T> {
    mutex: &'a SpinMutex<T>,
}

impl<T> Deref for SpinGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds the lock.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T> DerefMut for SpinGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: the guard holds the lock and is the only reference.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T> Drop for SpinGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.locked.store(false, Ordering::Release);
    }
}

/// In-memory [`Fs`] for the browser; see the module documentation.
pub struct WasmFs {
    this: Weak<WasmFs>,
    state: FsMutex<WasmFsState>,
    executor: BackgroundExecutor,
    job_event_subscribers: FsMutex<Vec<JobEventSender>>,
    write_through: FsMutex<Option<(PathBuf, async_channel::Sender<ExternalFileWrite>)>>,
}

/// A virtual file write that must finish in a browser-owned destination first.
/// Bytes and the completion channel can cross workers; browser handles cannot.
pub struct ExternalFileWrite {
    pub path: PathBuf,
    pub bytes: Vec<u8>,
    pub completion: futures::channel::oneshot::Sender<Result<()>>,
}

struct WasmFsState {
    /// Always a `Dir`.
    root: WasmEntry,
    next_inode: u64,
    /// The last mtime handed out; `next_mtime` keeps them strictly increasing.
    last_mtime: MTime,
    watchers: Vec<Weak<WasmWatcher>>,
    /// Trashed entries by id, with the absolute path they were removed from.
    trash: SlotMap<TrashId, (PathBuf, WasmEntry)>,
    /// Files written, moved or removed since the last `take_dirty`; `None` is removed.
    dirty: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

#[derive(Clone, Debug)]
enum WasmEntry {
    File {
        inode: u64,
        mtime: MTime,
        content: Vec<u8>,
    },
    Dir {
        inode: u64,
        mtime: MTime,
        entries: BTreeMap<OsString, WasmEntry>,
    },
    Symlink {
        target: PathBuf,
    },
}

/// A file written, renamed into place or removed since the previous
/// [`WasmFs::take_dirty`]; `contents == None` means removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirtyFile {
    /// The absolute path of the file.
    pub path: PathBuf,
    /// Its latest contents, or `None` when it was removed.
    pub contents: Option<Vec<u8>>,
}

/// The [`Watcher`] returned by [`WasmFs::watch`]: a set of path prefixes and the
/// events that matched them since the stream last drained.
pub struct WasmWatcher {
    prefixes: FsMutex<Vec<PathBuf>>,
    pending: FsMutex<Vec<PathEvent>>,
    /// One `()` per emitted event; the stream side debounces them.
    wake: async_channel::Sender<()>,
}

#[derive(Debug)]
struct WasmHandle {
    fs: Weak<WasmFs>,
    inode: u64,
}

impl WasmEntry {
    fn inode(&self) -> Option<u64> {
        match self {
            WasmEntry::File { inode, .. } | WasmEntry::Dir { inode, .. } => Some(*inode),
            WasmEntry::Symlink { .. } => None,
        }
    }

    fn is_dir(&self) -> bool {
        matches!(self, WasmEntry::Dir { .. })
    }

    /// Every path below `path` (inclusive), deepest first, with whether it is a file.
    /// Symlinks count as files (they are removed and restored as entries).
    fn paths_deepest_first(&self, path: &Path, output: &mut Vec<(PathBuf, bool)>) {
        if let WasmEntry::Dir { entries, .. } = self {
            for (name, entry) in entries {
                entry.paths_deepest_first(&path.join(name), output);
            }
        }
        output.push((path.to_path_buf(), !self.is_dir()));
    }

    /// Every file path below `path`, in path order (symlinks excluded, as in
    /// `files_with_contents`).
    fn file_paths(&self, path: &Path, output: &mut Vec<PathBuf>) {
        match self {
            WasmEntry::File { .. } => output.push(path.to_path_buf()),
            WasmEntry::Dir { entries, .. } => {
                for (name, entry) in entries {
                    entry.file_paths(&path.join(name), output);
                }
            }
            WasmEntry::Symlink { .. } => {}
        }
    }

    /// Every file path and its contents below `path`, in path order.
    fn files_with_contents(&self, path: &Path, output: &mut Vec<(PathBuf, Vec<u8>)>) {
        match self {
            WasmEntry::File { content, .. } => output.push((path.to_path_buf(), content.clone())),
            WasmEntry::Dir { entries, .. } => {
                for (name, entry) in entries {
                    entry.files_with_contents(&path.join(name), output);
                }
            }
            WasmEntry::Symlink { .. } => {}
        }
    }

    fn find_path_by_inode(&self, wanted: u64, path: &Path) -> Option<PathBuf> {
        if self.inode() == Some(wanted) {
            return Some(path.to_path_buf());
        }
        if let WasmEntry::Dir { entries, .. } = self {
            for (name, entry) in entries {
                if let Some(found) = entry.find_path_by_inode(wanted, &path.join(name)) {
                    return Some(found);
                }
            }
        }
        None
    }
}

impl WasmFsState {
    fn next_inode(&mut self) -> u64 {
        let inode = self.next_inode;
        self.next_inode += 1;
        inode
    }

    /// The current wall-clock time as an mtime, bumped by a nanosecond whenever it
    /// would not exceed the previous one, so successive writes always differ
    /// (buffers compare `MTime`s for equality to detect external changes).
    fn next_mtime(&mut self) -> MTime {
        let now = web_time::SystemTime::now()
            .duration_since(web_time::UNIX_EPOCH)
            .unwrap_or_default();
        let (mut secs, mut nanos) = (now.as_secs(), now.subsec_nanos());
        if let Some((last_secs, last_nanos)) =
            self.last_mtime.to_seconds_and_nanos_for_persistence()
            && (secs, nanos) <= (last_secs, last_nanos)
        {
            secs = last_secs;
            nanos = last_nanos + 1;
            if nanos >= 1_000_000_000 {
                secs += 1;
                nanos = 0;
            }
        }
        let mtime = MTime::from_seconds_and_nanos(secs, nanos);
        self.last_mtime = mtime;
        mtime
    }

    /// Resolves `path` (normalized, absolute), following symlinks in intermediate
    /// components always and in the final component when `follow_final`. Returns
    /// the entry and its canonical path.
    fn resolve(&self, path: &Path, follow_final: bool) -> Option<(&WasmEntry, PathBuf)> {
        let mut pending: Vec<OsString> = path
            .components()
            .filter_map(|component| match component {
                Component::Normal(name) => Some(name.to_os_string()),
                _ => None,
            })
            .rev()
            .collect();
        let mut current = &self.root;
        let mut canonical = PathBuf::from("/");
        let mut hops = 0;

        while let Some(name) = pending.pop() {
            let WasmEntry::Dir { entries, .. } = current else {
                return None;
            };
            let entry = entries.get(&name)?;
            let is_final = pending.is_empty();
            match entry {
                WasmEntry::Symlink { target } if !is_final || follow_final => {
                    hops += 1;
                    if hops > MAX_SYMLINK_HOPS {
                        return None;
                    }
                    // `Path::is_absolute` is always false on wasm32-unknown-unknown (the
                    // non-Unix implementation requires a prefix); `has_root` is the Unix
                    // notion this in-memory tree uses.
                    let target = if target.has_root() {
                        normalize_path(target)
                    } else {
                        normalize_path(&canonical.join(target))
                    };
                    let mut restarted: Vec<OsString> = target
                        .components()
                        .filter_map(|component| match component {
                            Component::Normal(name) => Some(name.to_os_string()),
                            _ => None,
                        })
                        .rev()
                        .collect();
                    // The still-unvisited components follow the link target's.
                    restarted.splice(0..0, pending.drain(..));
                    pending = restarted;
                    current = &self.root;
                    canonical = PathBuf::from("/");
                }
                _ => {
                    canonical.push(&name);
                    current = entry;
                }
            }
        }
        Some((current, canonical))
    }

    /// Mutable access to the entry at `path`, following every symlink.
    fn resolve_mut(&mut self, path: &Path) -> Option<&mut WasmEntry> {
        let (_, canonical) = self.resolve(path, true)?;
        let mut current = &mut self.root;
        for component in canonical.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            let WasmEntry::Dir { entries, .. } = current else {
                return None;
            };
            current = entries.get_mut(name)?;
        }
        Some(current)
    }

    /// The entries of `path`'s parent directory (following symlinks) and `path`'s
    /// own file name, for inserting, replacing or detaching the entry itself
    /// without following a symlink at `path`.
    fn parent_dir_mut(
        &mut self,
        path: &Path,
    ) -> Result<(&mut BTreeMap<OsString, WasmEntry>, OsString)> {
        let name = path
            .file_name()
            .with_context(|| format!("path has no file name: {path:?}"))?
            .to_os_string();
        let parent = path
            .parent()
            .with_context(|| format!("path has no parent: {path:?}"))?;
        match self.resolve_mut(parent) {
            Some(WasmEntry::Dir { entries, .. }) => Ok((entries, name)),
            Some(_) => bail!("not a directory: {parent:?}"),
            None => bail!("path does not exist: {parent:?}"),
        }
    }

    /// `mkdir -p`; returns the directories it created, shallowest first.
    fn mkdir_p(&mut self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut created = Vec::new();
        let mut prefix = PathBuf::from("/");
        for component in path.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            prefix.push(name);
            match self.resolve(&prefix, true) {
                Some((WasmEntry::Dir { .. }, _)) => continue,
                Some(_) => bail!("not a directory: {prefix:?}"),
                None => {
                    let inode = self.next_inode();
                    let mtime = self.next_mtime();
                    let (entries, name) = self.parent_dir_mut(&prefix)?;
                    entries.insert(
                        name,
                        WasmEntry::Dir {
                            inode,
                            mtime,
                            entries: BTreeMap::new(),
                        },
                    );
                    created.push(prefix.clone());
                }
            }
        }
        Ok(created)
    }

    fn emit(&mut self, path: &Path, kind: PathEventKind) {
        self.watchers.retain(|watcher| watcher.strong_count() > 0);
        for watcher in &self.watchers {
            let Some(watcher) = watcher.upgrade() else {
                continue;
            };
            if watcher
                .prefixes
                .lock()
                .iter()
                .any(|prefix| path.starts_with(prefix))
            {
                watcher.pending.lock().push(PathEvent {
                    path: path.to_path_buf(),
                    kind: Some(kind),
                });
                // A closed receiver means the stream was dropped; the watcher is
                // pruned once its last `Arc` goes.
                watcher.wake.try_send(()).ok();
            }
        }
    }

    fn mark_dirty(&mut self, path: &Path, contents: Option<Vec<u8>>) {
        self.dirty.insert(path.to_path_buf(), contents);
    }

    /// Writes `content` at `path` (parents must exist), keeping the inode of an
    /// existing file. Returns the event kind to emit.
    fn write_file(&mut self, path: &Path, content: Vec<u8>) -> Result<PathEventKind> {
        // Writing through a link writes its target, as the OS does.
        if let Some((WasmEntry::Symlink { .. }, _)) = self.resolve(path, false) {
            let (_, canonical) = self
                .resolve(path, true)
                .with_context(|| format!("dangling symlink: {path:?}"))?;
            return self.write_file(&canonical, content);
        }
        let mtime = self.next_mtime();
        let new_inode = self.next_inode();
        let kind = {
            let (entries, name) = self.parent_dir_mut(path)?;
            match entries.get_mut(&name) {
                Some(WasmEntry::File {
                    content: existing,
                    mtime: existing_mtime,
                    ..
                }) => {
                    *existing = content.clone();
                    *existing_mtime = mtime;
                    PathEventKind::Changed
                }
                Some(WasmEntry::Dir { .. }) => bail!("is a directory: {path:?}"),
                Some(WasmEntry::Symlink { .. }) => bail!("dangling symlink: {path:?}"),
                None => {
                    entries.insert(
                        name,
                        WasmEntry::File {
                            inode: new_inode,
                            mtime,
                            content: content.clone(),
                        },
                    );
                    PathEventKind::Created
                }
            }
        };
        self.mark_dirty(path, Some(content));
        Ok(kind)
    }

    /// Detaches the entry at `path` (not following a final symlink), emitting
    /// `Removed` for it and every descendant and marking their files removed.
    fn detach(&mut self, path: &Path) -> Result<Option<WasmEntry>> {
        let (entries, name) = self.parent_dir_mut(path)?;
        let Some(entry) = entries.remove(&name) else {
            return Ok(None);
        };
        let mut removed = Vec::new();
        entry.paths_deepest_first(path, &mut removed);
        for (removed_path, is_file) in removed {
            if is_file {
                self.mark_dirty(&removed_path, None);
            }
            self.emit(&removed_path, PathEventKind::Removed);
        }
        Ok(Some(entry))
    }

    /// Inserts `entry` at `path` (parents must exist, nothing may be there),
    /// emitting `Created` for it and every descendant and marking its files dirty.
    fn attach(&mut self, path: &Path, entry: WasmEntry) -> Result<()> {
        let mut created = Vec::new();
        entry.paths_deepest_first(path, &mut created);
        let mut files = Vec::new();
        entry.files_with_contents(path, &mut files);
        let (entries, name) = self.parent_dir_mut(path)?;
        anyhow::ensure!(
            !entries.contains_key(&name),
            "path already exists: {path:?}"
        );
        entries.insert(name, entry);
        for (file, contents) in files {
            self.mark_dirty(&file, Some(contents));
        }
        for (created_path, _) in created.into_iter().rev() {
            self.emit(&created_path, PathEventKind::Created);
        }
        Ok(())
    }
}

impl WasmFs {
    /// The executor is only used for the watch-debounce timer.
    pub fn new(executor: BackgroundExecutor) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            this: this.clone(),
            state: FsMutex::new(WasmFsState {
                root: WasmEntry::Dir {
                    inode: 1,
                    mtime: MTime::from_seconds_and_nanos(0, 0),
                    entries: BTreeMap::new(),
                },
                next_inode: 2,
                last_mtime: MTime::from_seconds_and_nanos(0, 0),
                watchers: Vec::new(),
                trash: SlotMap::with_key(),
                dirty: BTreeMap::new(),
            }),
            executor,
            job_event_subscribers: FsMutex::new(Vec::new()),
            write_through: FsMutex::new(None),
        })
    }

    /// Connect a browser-owned subtree to the main thread. The caller must keep
    /// serving requests; dropped receivers make Save fail rather than lose data.
    pub fn set_write_through(
        &self,
        prefix: PathBuf,
        sender: async_channel::Sender<ExternalFileWrite>,
    ) {
        *self.write_through.lock() = Some((prefix, sender));
    }

    /// Boot helper: `write` without the async wrapper, for seeding settings and the
    /// keymap before the app loop runs (`settings::seed_config_files`). Creates
    /// parent directories and emits a change event. Panics on a non-absolute path
    /// or a file in the way of a parent directory (a boot bug).
    pub fn insert_file(&self, path: &Path, contents: Vec<u8>) {
        let path = Self::abs(path).expect("WasmFs::insert_file needs an absolute path");
        let mut state = self.state.lock();
        state
            .write_file_with_parents(&path, contents)
            .expect("WasmFs::insert_file could not create the file");
    }

    /// Boot helper: `create_dir` without the async wrapper, for the empty snippets,
    /// prompts and tasks directories seeded so their watchers start cleanly.
    /// `mkdir -p` semantics; emits `Created` for each new directory; an existing
    /// directory is a no-op; a file in the way or a non-absolute path panics (a
    /// boot bug).
    pub fn insert_dir(&self, path: &Path) {
        let path = Self::abs(path).expect("WasmFs::insert_dir needs an absolute path");
        let mut state = self.state.lock();
        let created = state
            .mkdir_p(&path)
            .expect("WasmFs::insert_dir could not create the directory");
        for created_path in created {
            state.emit(&created_path, PathEventKind::Created);
        }
    }

    /// All file paths, sorted (tests and the control-plane save hook).
    pub fn files(&self) -> Vec<PathBuf> {
        let state = self.state.lock();
        let mut paths = Vec::new();
        state.root.file_paths(Path::new("/"), &mut paths);
        paths.sort();
        paths
    }

    /// Files written, renamed into place or removed since the previous call
    /// (`contents == None` means removed), in path order; clears the dirty set.
    /// Synchronous so the `visibilitychange` → hidden / `STOPPING` flush can drain
    /// it without awaiting (D32).
    pub fn take_dirty(&self) -> Vec<DirtyFile> {
        let mut state = self.state.lock();
        std::mem::take(&mut state.dirty)
            .into_iter()
            .map(|(path, contents)| DirtyFile { path, contents })
            .collect()
    }

    /// Normalizes `path`, rejecting anything that is not absolute.
    fn abs(path: &Path) -> Result<PathBuf> {
        // See the note on symlink targets above: `has_root`, not `is_absolute`, on wasm.
        anyhow::ensure!(path.has_root(), "path is not absolute: {path:?}");
        Ok(normalize_path(path))
    }

    /// Whether `path` names a file (following symlinks); `None` when it is missing.
    fn entry_kind(&self, path: &Path) -> Result<Option<bool>> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        Ok(state.resolve(&path, true).map(|(entry, _)| entry.is_dir()))
    }
}

impl WasmFsState {
    /// `write_file` after `mkdir_p` on the parent, emitting the events for both.
    fn write_file_with_parents(&mut self, path: &Path, content: Vec<u8>) -> Result<()> {
        if let Some(parent) = path.parent() {
            for created in self.mkdir_p(parent)? {
                self.emit(&created, PathEventKind::Created);
            }
        }
        let kind = self.write_file(path, content)?;
        self.emit(path, kind);
        Ok(())
    }
}

impl FileHandle for WasmHandle {
    fn current_path(&self, _: &Arc<dyn Fs>) -> Result<PathBuf> {
        let fs = self
            .fs
            .upgrade()
            .context("the filesystem behind this handle is gone")?;
        let state = fs.state.lock();
        state
            .root
            .find_path_by_inode(self.inode, Path::new("/"))
            .with_context(|| format!("no entry with inode {} exists any more", self.inode))
    }
}

impl Watcher for WasmWatcher {
    fn add(&self, path: &Path) -> Result<()> {
        let path = WasmFs::abs(path)?;
        let mut prefixes = self.prefixes.lock();
        if !prefixes.contains(&path) {
            prefixes.push(path);
        }
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let path = WasmFs::abs(path)?;
        self.prefixes.lock().retain(|prefix| prefix != &path);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Fs for WasmFs {
    async fn create_dir(&self, path: &Path) -> Result<()> {
        let path = Self::abs(path)?;
        let mut state = self.state.lock();
        for created in state.mkdir_p(&path)? {
            state.emit(&created, PathEventKind::Created);
        }
        Ok(())
    }

    async fn create_symlink(&self, path: &Path, target: PathBuf) -> Result<()> {
        let path = Self::abs(path)?;
        let mut state = self.state.lock();
        let (entries, name) = state.parent_dir_mut(&path)?;
        anyhow::ensure!(
            !entries.contains_key(&name),
            "path already exists: {path:?}"
        );
        entries.insert(name, WasmEntry::Symlink { target });
        state.emit(&path, PathEventKind::Created);
        Ok(())
    }

    async fn create_file(&self, path: &Path, options: CreateOptions) -> Result<()> {
        let path = Self::abs(path)?;
        let mut state = self.state.lock();
        let exists = state.resolve(&path, false).is_some();
        if exists {
            if options.overwrite {
                let kind = state.write_file(&path, Vec::new())?;
                state.emit(&path, kind);
                return Ok(());
            }
            if options.ignore_if_exists {
                return Ok(());
            }
            // Downcastable like `RealFs`'s error, which `is_case_sensitive` probes.
            return Err(io::Error::from(io::ErrorKind::AlreadyExists).into());
        }
        // `OpenOptions::create` does not create parents.
        state.parent_dir_mut(&path)?;
        let kind = state.write_file(&path, Vec::new())?;
        state.emit(&path, kind);
        Ok(())
    }

    async fn create_file_with(
        &self,
        path: &Path,
        mut content: Pin<&mut (dyn AsyncRead + Send)>,
    ) -> Result<()> {
        let mut bytes = Vec::new();
        content.read_to_end(&mut bytes).await?;
        self.write(path, &bytes).await
    }

    #[cfg(not(target_family = "wasm"))]
    async fn extract_tar_file(
        &self,
        path: &Path,
        _content: async_tar::Archive<Pin<&mut (dyn AsyncRead + Send)>>,
    ) -> Result<()> {
        bail!("archives cannot be extracted into the in-memory filesystem ({path:?})")
    }

    async fn copy_file(&self, source: &Path, target: &Path, options: CopyOptions) -> Result<()> {
        let source = Self::abs(source)?;
        let target = Self::abs(target)?;
        let mut state = self.state.lock();
        let content = match state.resolve(&source, true) {
            Some((WasmEntry::File { content, .. }, _)) => content.clone(),
            Some(_) => bail!("not a file: {source:?}"),
            None => bail!("path does not exist: {source:?}"),
        };
        if state.resolve(&target, false).is_some() {
            if options.ignore_if_exists {
                return Ok(());
            }
            anyhow::ensure!(options.overwrite, "path already exists: {target:?}");
        }
        state.parent_dir_mut(&target)?;
        let kind = state.write_file(&target, content)?;
        state.emit(&target, kind);
        Ok(())
    }

    async fn rename(&self, source: &Path, target: &Path, options: RenameOptions) -> Result<()> {
        let source = Self::abs(source)?;
        let target = Self::abs(target)?;
        let mut state = self.state.lock();
        anyhow::ensure!(
            state.resolve(&source, false).is_some(),
            "path does not exist: {source:?}"
        );
        // POSIX `rename` succeeds without doing anything when both names resolve
        // to the same file.
        if source == target {
            return Ok(());
        }
        anyhow::ensure!(
            !target.starts_with(&source),
            "cannot move {source:?} into itself ({target:?})"
        );
        if options.create_parents
            && let Some(parent) = target.parent()
        {
            for created in state.mkdir_p(parent)? {
                state.emit(&created, PathEventKind::Created);
            }
        }
        if state.resolve(&target, false).is_some() {
            if options.overwrite {
                state.detach(&target)?;
            } else if options.ignore_if_exists {
                // `RealFs` reports success without moving anything here.
                return Ok(());
            } else {
                bail!("path already exists: {target:?}");
            }
        }
        // The target's parent must exist before the source is detached, or a
        // failure would lose the entry.
        state.parent_dir_mut(&target)?;
        let entry = state
            .detach(&source)?
            .with_context(|| format!("path does not exist: {source:?}"))?;
        state.attach(&target, entry)
    }

    async fn remove_dir(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        let path = Self::abs(path)?;
        let mut state = self.state.lock();
        match state.resolve(&path, false) {
            Some((WasmEntry::Dir { entries, .. }, _)) => {
                anyhow::ensure!(
                    options.recursive || entries.is_empty(),
                    "directory is not empty: {path:?}"
                );
            }
            Some(_) => bail!("not a directory: {path:?}"),
            None => {
                if options.ignore_if_not_exists {
                    return Ok(());
                }
                bail!("path does not exist: {path:?}");
            }
        }
        state.detach(&path)?;
        Ok(())
    }

    async fn trash(&self, path: &Path, options: RemoveOptions) -> Result<TrashId> {
        let path = Self::abs(path)?;
        let mut state = self.state.lock();
        match state.resolve(&path, false) {
            Some((WasmEntry::Dir { entries, .. }, _)) => {
                anyhow::ensure!(
                    options.recursive || entries.is_empty(),
                    "directory is not empty: {path:?}"
                );
            }
            Some(_) => {}
            None => {
                if options.ignore_if_not_exists {
                    bail!("nothing to trash at {path:?}");
                }
                bail!("path does not exist: {path:?}");
            }
        }
        let entry = state
            .detach(&path)?
            .with_context(|| format!("path does not exist: {path:?}"))?;
        Ok(state.trash.insert((path, entry)))
    }

    async fn remove_file(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        let path = Self::abs(path)?;
        let mut state = self.state.lock();
        match state.resolve(&path, false) {
            Some((WasmEntry::Dir { .. }, _)) => bail!("is a directory: {path:?}"),
            Some(_) => {}
            None => {
                if options.ignore_if_not_exists {
                    return Ok(());
                }
                bail!("path does not exist: {path:?}");
            }
        }
        state.detach(&path)?;
        Ok(())
    }

    async fn open_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        let inode = match state.resolve(&path, true) {
            Some((entry, _)) => entry
                .inode()
                .with_context(|| format!("not a file or directory: {path:?}"))?,
            None => bail!("path does not exist: {path:?}"),
        };
        Ok(Arc::new(WasmHandle {
            fs: self.this.clone(),
            inode,
        }))
    }

    async fn open_sync(&self, path: &Path) -> Result<Box<dyn io::Read + Send + Sync>> {
        let bytes = self.load_bytes(path).await?;
        Ok(Box::new(io::Cursor::new(bytes)))
    }

    async fn load_bytes(&self, path: &Path) -> Result<Vec<u8>> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        match state.resolve(&path, true) {
            Some((WasmEntry::File { content, .. }, _)) => Ok(content.clone()),
            Some(_) => bail!("is a directory: {path:?}"),
            None => bail!("path does not exist: {path:?}"),
        }
    }

    async fn atomic_write(&self, path: PathBuf, text: String) -> Result<()> {
        self.write(&path, text.as_bytes()).await
    }

    async fn save(&self, path: &Path, text: &Rope, line_ending: LineEnding) -> Result<()> {
        let content = text::chunks_with_line_ending(text, line_ending).collect::<String>();
        self.write(path, content.as_bytes()).await
    }

    async fn write(&self, path: &Path, content: &[u8]) -> Result<()> {
        let path = Self::abs(path)?;
        let sender = self
            .write_through
            .lock()
            .as_ref()
            .and_then(|(prefix, sender)| path.starts_with(prefix).then(|| sender.clone()));
        if let Some(sender) = sender {
            let (completion, result) = futures::channel::oneshot::channel();
            sender
                .send(ExternalFileWrite {
                    path: path.clone(),
                    bytes: content.to_vec(),
                    completion,
                })
                .await
                .map_err(|_| anyhow!("Browser file writer is unavailable"))?;
            result
                .await
                .context("Browser file write was interrupted")??;
        }
        let mut state = self.state.lock();
        state.write_file_with_parents(&path, content.to_vec())
    }

    async fn canonicalize(&self, path: &Path) -> Result<PathBuf> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        state
            .resolve(&path, true)
            .map(|(_, canonical)| canonical)
            .with_context(|| format!("path does not exist: {path:?}"))
    }

    async fn is_file(&self, path: &Path) -> bool {
        matches!(self.entry_kind(path), Ok(Some(false)))
    }

    async fn is_dir(&self, path: &Path) -> bool {
        matches!(self.entry_kind(path), Ok(Some(true)))
    }

    async fn metadata(&self, path: &Path) -> Result<Option<Metadata>> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        let Some((entry, _)) = state.resolve(&path, false) else {
            return Ok(None);
        };
        let is_symlink = matches!(entry, WasmEntry::Symlink { .. });
        let entry = if is_symlink {
            match state.resolve(&path, true) {
                Some((entry, _)) => entry,
                None => return Ok(None),
            }
        } else {
            entry
        };
        let (inode, mtime, is_dir, len) = match entry {
            WasmEntry::File {
                inode,
                mtime,
                content,
            } => (*inode, *mtime, false, content.len() as u64),
            WasmEntry::Dir { inode, mtime, .. } => (*inode, *mtime, true, 0),
            WasmEntry::Symlink { .. } => return Ok(None),
        };
        Ok(Some(Metadata {
            inode,
            mtime,
            is_symlink,
            is_dir,
            len,
            is_fifo: false,
            is_executable: false,
            is_writable: true,
        }))
    }

    async fn read_link(&self, path: &Path) -> Result<PathBuf> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        match state.resolve(&path, false) {
            Some((WasmEntry::Symlink { target }, _)) => Ok(target.clone()),
            Some(_) => bail!("not a symlink: {path:?}"),
            None => bail!("path does not exist: {path:?}"),
        }
    }

    async fn read_dir(
        &self,
        path: &Path,
    ) -> Result<Pin<Box<dyn Send + Stream<Item = Result<PathBuf>>>>> {
        let path = Self::abs(path)?;
        let state = self.state.lock();
        let children: Vec<Result<PathBuf>> = match state.resolve(&path, true) {
            Some((WasmEntry::Dir { entries, .. }, _)) => {
                entries.keys().map(|name| Ok(path.join(name))).collect()
            }
            Some(_) => bail!("not a directory: {path:?}"),
            None => bail!("path does not exist: {path:?}"),
        };
        Ok(Box::pin(futures::stream::iter(children)))
    }

    async fn watch(
        &self,
        path: &Path,
        latency: Duration,
    ) -> (
        Pin<Box<dyn Send + Stream<Item = Vec<PathEvent>>>>,
        Arc<dyn Watcher>,
    ) {
        let (wake, woken) = async_channel::unbounded();
        let watcher = Arc::new(WasmWatcher {
            prefixes: FsMutex::new(Vec::new()),
            pending: FsMutex::new(Vec::new()),
            wake,
        });
        // The path need not exist yet: `watch_config_file` watches settings.json
        // before it is written.
        if let Err(error) = watcher.add(path) {
            log::warn!("Failed to watch {}:\n{error}", path.display());
        }
        {
            let mut state = self.state.lock();
            // A settings file behind a link reloads when the target changes, as
            // with `RealFs`.
            if let Ok(path) = Self::abs(path)
                && let Some((WasmEntry::Symlink { .. }, _)) = state.resolve(&path, false)
                && let Some((_, target)) = state.resolve(&path, true)
            {
                watcher.add(&target).ok();
            }
            state.watchers.push(Arc::downgrade(&watcher));
        }

        let executor = self.executor.clone();
        let stream = woken.filter_map({
            let watcher = watcher.clone();
            move |()| {
                let watcher = watcher.clone();
                let executor = executor.clone();
                async move {
                    executor.timer(latency).await;
                    let events = std::mem::take(&mut *watcher.pending.lock());
                    (!events.is_empty()).then_some(events)
                }
            }
        });
        (Box::pin(stream), watcher)
    }

    fn open_repo(
        &self,
        abs_dot_git: &Path,
        _system_git_binary_path: Option<&Path>,
    ) -> Result<Arc<dyn git::repository::GitRepository>> {
        Err(anyhow!(
            "git repositories live on the remote host ({abs_dot_git:?})"
        ))
    }

    async fn git_init(
        &self,
        abs_work_directory: &Path,
        _fallback_branch_name: String,
    ) -> Result<()> {
        Err(anyhow!(
            "git repositories live on the remote host ({abs_work_directory:?})"
        ))
    }

    async fn git_clone(&self, abs_work_directory: &Path, _repo_url: &str) -> Result<()> {
        Err(anyhow!(
            "git repositories live on the remote host ({abs_work_directory:?})"
        ))
    }

    async fn git_config(&self, abs_work_directory: &Path, _args: Vec<String>) -> Result<String> {
        Err(anyhow!(
            "git repositories live on the remote host ({abs_work_directory:?})"
        ))
    }

    fn is_fake(&self) -> bool {
        // The settings-import and "fake fs" branches in `project`/`workspace`
        // must not trigger for the browser filesystem.
        false
    }

    async fn is_case_sensitive(&self) -> bool {
        true
    }

    fn subscribe_to_jobs(&self) -> JobEventReceiver {
        let (sender, receiver) = futures::channel::mpsc::unbounded();
        self.job_event_subscribers.lock().push(sender);
        receiver
    }

    fn original_path_for_trash_id(&self, trash_id: TrashId) -> Option<PathBuf> {
        self.state
            .lock()
            .trash
            .get(trash_id)
            .map(|(path, _)| path.clone())
    }

    async fn restore(&self, trash_id: TrashId) -> std::result::Result<PathBuf, TrashRestoreError> {
        let mut state = self.state.lock();
        let Some((path, _)) = state.trash.get(trash_id) else {
            return Err(TrashRestoreError::AlreadyRestored);
        };
        let path = path.clone();
        if state.resolve(&path, false).is_some() {
            // The entry stays in the trash so a retry can succeed once the
            // blocker is gone.
            return Err(TrashRestoreError::Collision { path });
        }
        if let Err(error) = state.parent_dir_mut(&path) {
            return Err(TrashRestoreError::Unknown {
                description: format!("{error:#}"),
            });
        }
        let (_, entry) = state
            .trash
            .remove(trash_id)
            .ok_or(TrashRestoreError::AlreadyRestored)?;
        state
            .attach(&path, entry)
            .map_err(|error| TrashRestoreError::Unknown {
                description: format!("{error:#}"),
            })?;
        Ok(path)
    }
}
