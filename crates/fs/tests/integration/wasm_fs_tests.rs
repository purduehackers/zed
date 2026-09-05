//! Native coverage of `fs::WasmFs`, the in-memory filesystem of the browser client.
//! `WasmFs` only speaks absolute POSIX paths, so these tests are skipped on Windows.
#![cfg(not(target_os = "windows"))]

use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use fs::{
    CreateOptions, Fs, PathEvent, PathEventKind, RemoveOptions, RenameOptions, TrashRestoreError,
    WasmFs, copy_recursive, wasm_fs::SpinMutex,
};
use futures::StreamExt;
use gpui::BackgroundExecutor;
use serde_json::json;

const CONFIG_DIR: &str = "/home/web/.config/zed";
const SETTINGS: &str = "/home/web/.config/zed/settings.json";
const KEYMAP: &str = "/home/web/.config/zed/keymap.json";

fn kinds(events: &[PathEvent]) -> Vec<(PathBuf, Option<PathEventKind>)> {
    events
        .iter()
        .map(|event| (event.path.clone(), event.kind))
        .collect()
}

/// Seeds `fs` with a `json!` tree the way `FakeFs::insert_tree` does, using the
/// boot helpers.
fn insert_tree(fs: &WasmFs, root: &Path, tree: serde_json::Value) {
    match tree {
        serde_json::Value::Object(entries) => {
            fs.insert_dir(root);
            for (name, value) in entries {
                insert_tree(fs, &root.join(name), value);
            }
        }
        serde_json::Value::String(contents) => fs.insert_file(root, contents.into_bytes()),
        other => panic!("unsupported tree node {other:?}"),
    }
}

#[gpui::test]
async fn test_wasm_fs_write_load_metadata_read_dir(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    fs.write(Path::new(SETTINGS), b"{}").await.unwrap();

    assert_eq!(fs.load(Path::new(SETTINGS)).await.unwrap(), "{}");
    let metadata = fs.metadata(Path::new(SETTINGS)).await.unwrap().unwrap();
    assert!(!metadata.is_dir);
    assert!(!metadata.is_symlink);
    assert_eq!(metadata.len, 2);
    assert!(metadata.is_writable);
    assert!(fs.is_dir(Path::new(CONFIG_DIR)).await);
    assert!(fs.is_dir(Path::new("/home")).await);
    assert!(fs.is_file(Path::new(SETTINGS)).await);
    assert!(!fs.is_file(Path::new(CONFIG_DIR)).await);

    let children: Vec<PathBuf> = fs
        .read_dir(Path::new(CONFIG_DIR))
        .await
        .unwrap()
        .map(|child| child.unwrap())
        .collect()
        .await;
    assert_eq!(children, vec![PathBuf::from(SETTINGS)]);

    fs.write(Path::new(SETTINGS), b"{\"a\": 1}").await.unwrap();
    let rewritten = fs.metadata(Path::new(SETTINGS)).await.unwrap().unwrap();
    assert_eq!(
        rewritten.inode, metadata.inode,
        "inodes are stable across writes"
    );
    assert_ne!(
        rewritten.mtime, metadata.mtime,
        "every write gets a new mtime"
    );
    assert_eq!(rewritten.len, 8);
    assert_eq!(fs.files(), vec![PathBuf::from(SETTINGS)]);
}

#[gpui::test]
async fn test_wasm_fs_rejects_relative_paths(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    let relative = Path::new("relative/x");
    assert!(fs.write(relative, b"x").await.is_err());
    assert!(fs.load(relative).await.is_err());
    assert!(fs.metadata(relative).await.is_err());
    assert!(fs.create_dir(relative).await.is_err());
    assert!(fs.canonicalize(relative).await.is_err());
    let (_stream, watcher) = fs.watch(relative, Duration::from_millis(10)).await;
    assert!(watcher.add(relative).is_err());
    assert!(fs.files().is_empty());

    fs.write(Path::new("/a/../b"), b"b").await.unwrap();
    assert_eq!(fs.files(), vec![PathBuf::from("/b")]);
    assert_eq!(
        fs.canonicalize(Path::new("/..")).await.unwrap(),
        PathBuf::from("/")
    );
    assert_eq!(
        fs.canonicalize(Path::new("/./b")).await.unwrap(),
        PathBuf::from("/b")
    );
}

#[gpui::test]
async fn test_wasm_fs_watch_file_debounces_and_batches(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor.clone());
    let settings = Path::new(SETTINGS);
    let (mut events, _watcher) = fs.watch(settings, Duration::from_millis(100)).await;

    fs.write(settings, b"1").await.unwrap();
    fs.write(settings, b"2").await.unwrap();
    fs.write(settings, b"3").await.unwrap();
    executor.advance_clock(Duration::from_millis(100));

    let batch = events.next().await.unwrap();
    assert_eq!(
        kinds(&batch),
        vec![
            (settings.to_path_buf(), Some(PathEventKind::Created)),
            (settings.to_path_buf(), Some(PathEventKind::Changed)),
            (settings.to_path_buf(), Some(PathEventKind::Changed)),
        ]
    );

    // The two wake-ups left over from the batched writes find nothing pending.
    executor.advance_clock(Duration::from_millis(300));
    executor.run_until_parked();
    assert!(
        futures::poll!(events.next()).is_pending(),
        "a debounce window without events yields no batch"
    );

    drop(events);
    fs.write(settings, b"4").await.unwrap();
    assert_eq!(fs.load(settings).await.unwrap(), "4");
}

#[gpui::test]
async fn test_wasm_fs_watch_dir_reports_child_paths(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor.clone());
    fs.insert_dir(Path::new(CONFIG_DIR));
    let (mut events, _watcher) = fs
        .watch(Path::new(CONFIG_DIR), Duration::from_millis(100))
        .await;

    fs.write(Path::new(KEYMAP), b"[]").await.unwrap();
    executor.advance_clock(Duration::from_millis(100));
    let batch = events.next().await.unwrap();
    assert_eq!(
        kinds(&batch),
        vec![(PathBuf::from(KEYMAP), Some(PathEventKind::Created))]
    );

    fs.remove_file(Path::new(KEYMAP), RemoveOptions::default())
        .await
        .unwrap();
    executor.advance_clock(Duration::from_millis(100));
    let batch = events.next().await.unwrap();
    assert_eq!(
        kinds(&batch),
        vec![(PathBuf::from(KEYMAP), Some(PathEventKind::Removed))]
    );

    // A sibling directory is outside the watched prefix.
    fs.write(Path::new("/home/web/other.txt"), b"x")
        .await
        .unwrap();
    executor.advance_clock(Duration::from_millis(300));
    executor.run_until_parked();
    assert!(futures::poll!(events.next()).is_pending());
}

/// `settings::watch_config_file`'s loop, inlined (`settings` depends on `fs`, so
/// it cannot be a dev-dependency): canonicalize, watch, load, then reload on
/// every batch.
#[gpui::test]
async fn test_wasm_fs_watch_config_file_end_to_end(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor.clone());
    fs.insert_file(Path::new(SETTINGS), b"{\"theme\": \"one\"}".to_vec());
    let (tx, mut rx) = futures::channel::mpsc::unbounded::<String>();

    let watch_task = executor.spawn({
        let fs = fs.clone();
        async move {
            let path = fs
                .canonicalize(Path::new(SETTINGS))
                .await
                .unwrap_or_else(|_| PathBuf::from(SETTINGS));
            let (mut events, _watcher) = fs.watch(&path, Duration::from_millis(100)).await;
            if let Ok(contents) = fs.load(&path).await {
                tx.unbounded_send(contents).ok();
            }
            while events.next().await.is_some() {
                if let Ok(contents) = fs.load(&path).await {
                    tx.unbounded_send(contents).ok();
                }
            }
        }
    });

    executor.run_until_parked();
    assert_eq!(rx.next().await.unwrap(), "{\"theme\": \"one\"}");

    fs.atomic_write(PathBuf::from(SETTINGS), "{\"theme\": \"two\"}".into())
        .await
        .unwrap();
    executor.advance_clock(Duration::from_millis(100));
    executor.run_until_parked();
    assert_eq!(rx.next().await.unwrap(), "{\"theme\": \"two\"}");
    drop(watch_task);
}

#[gpui::test]
async fn test_wasm_fs_rename_semantics(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    let fs_dyn: Arc<dyn Fs> = fs.clone();
    let a = Path::new("/dir/a.txt");
    let b = Path::new("/dir/b.txt");
    fs.write(a, b"A").await.unwrap();
    fs.write(b, b"B").await.unwrap();

    fs.rename(a, a, RenameOptions::default()).await.unwrap();
    assert_eq!(fs.load(a).await.unwrap(), "A");

    fs.rename(
        a,
        b,
        RenameOptions {
            ignore_if_exists: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.load(a).await.unwrap(), "A");
    assert_eq!(fs.load(b).await.unwrap(), "B");

    assert!(
        fs.rename(a, b, RenameOptions::default()).await.is_err(),
        "renaming onto an existing file without overwrite fails"
    );

    let handle = fs.open_handle(a).await.unwrap();
    let inode = fs.metadata(a).await.unwrap().unwrap().inode;
    fs.rename(
        a,
        b,
        RenameOptions {
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.metadata(a).await.unwrap().map(|_| ()), None);
    assert_eq!(fs.load(b).await.unwrap(), "A");
    assert_eq!(fs.metadata(b).await.unwrap().unwrap().inode, inode);
    assert_eq!(handle.current_path(&fs_dyn).unwrap(), b);

    let nested = Path::new("/dir/new/deeper/c.txt");
    assert!(
        fs.rename(b, nested, RenameOptions::default())
            .await
            .is_err()
    );
    fs.rename(
        b,
        nested,
        RenameOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(fs.is_dir(Path::new("/dir/new/deeper")).await);
    assert_eq!(fs.load(nested).await.unwrap(), "A");
    assert_eq!(handle.current_path(&fs_dyn).unwrap(), nested);

    assert!(
        fs.rename(Path::new("/missing"), a, RenameOptions::default())
            .await
            .is_err()
    );
}

#[gpui::test]
async fn test_wasm_fs_symlinks(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    fs.write(Path::new("/a/dir/file"), b"F").await.unwrap();
    fs.create_symlink(Path::new("/a/link"), "./dir".into())
        .await
        .unwrap();

    assert_eq!(
        fs.canonicalize(Path::new("/a/link/file")).await.unwrap(),
        PathBuf::from("/a/dir/file")
    );
    assert_eq!(fs.load(Path::new("/a/link/file")).await.unwrap(), "F");
    let metadata = fs.metadata(Path::new("/a/link")).await.unwrap().unwrap();
    assert!(metadata.is_symlink);
    assert!(metadata.is_dir);
    assert_eq!(
        fs.read_link(Path::new("/a/link")).await.unwrap(),
        PathBuf::from("./dir")
    );
    assert!(fs.read_link(Path::new("/a/dir")).await.is_err());

    fs.create_symlink(Path::new("/a/dangling"), "/nowhere".into())
        .await
        .unwrap();
    assert!(
        fs.metadata(Path::new("/a/dangling"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(fs.canonicalize(Path::new("/a/dangling")).await.is_err());

    fs.create_symlink(Path::new("/a/loop1"), "/a/loop2".into())
        .await
        .unwrap();
    fs.create_symlink(Path::new("/a/loop2"), "/a/loop1".into())
        .await
        .unwrap();
    assert!(fs.metadata(Path::new("/a/loop1")).await.unwrap().is_none());

    // Watching a link also watches its target.
    let (mut events, _watcher) = fs
        .watch(Path::new("/a/link"), Duration::from_millis(10))
        .await;
    fs.write(Path::new("/a/dir/file"), b"G").await.unwrap();
    let batch = events.next().await.unwrap();
    assert_eq!(
        kinds(&batch),
        vec![(PathBuf::from("/a/dir/file"), Some(PathEventKind::Changed))]
    );
}

#[gpui::test]
async fn test_wasm_fs_trash_and_restore(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    let path = Path::new("/root/a.txt");
    fs.write(path, b"original").await.unwrap();
    let (mut events, _watcher) = fs
        .watch(Path::new("/root"), Duration::from_millis(10))
        .await;

    let trash_id = fs.trash(path, RemoveOptions::default()).await.unwrap();
    assert!(fs.files().is_empty());
    assert_eq!(
        fs.original_path_for_trash_id(trash_id),
        Some(path.to_path_buf())
    );
    assert_eq!(
        kinds(&events.next().await.unwrap()),
        vec![(path.to_path_buf(), Some(PathEventKind::Removed))]
    );

    fs.write(path, b"conflicting").await.unwrap();
    events.next().await.unwrap();
    let error = fs.restore(trash_id).await.unwrap_err();
    assert!(matches!(error, TrashRestoreError::Collision { .. }));
    assert_eq!(fs.load(path).await.unwrap(), "conflicting");

    fs.remove_file(path, RemoveOptions::default())
        .await
        .unwrap();
    events.next().await.unwrap();
    assert_eq!(fs.restore(trash_id).await.unwrap(), path);
    assert_eq!(fs.load(path).await.unwrap(), "original");
    assert_eq!(fs.original_path_for_trash_id(trash_id), None);
    assert_eq!(
        kinds(&events.next().await.unwrap()),
        vec![(path.to_path_buf(), Some(PathEventKind::Created))]
    );
    assert!(matches!(
        fs.restore(trash_id).await.unwrap_err(),
        TrashRestoreError::AlreadyRestored
    ));

    // A directory is trashed and restored as a whole.
    fs.write(Path::new("/root/src/b.txt"), b"B").await.unwrap();
    assert!(
        fs.trash(Path::new("/root/src"), RemoveOptions::default())
            .await
            .is_err(),
        "a non-empty directory needs `recursive`"
    );
    let trash_id = fs
        .trash(
            Path::new("/root/src"),
            RemoveOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(fs.files(), vec![path.to_path_buf()]);
    fs.restore(trash_id).await.unwrap();
    assert_eq!(
        fs.files(),
        vec![path.to_path_buf(), PathBuf::from("/root/src/b.txt")]
    );
}

#[gpui::test]
async fn test_wasm_fs_copy_recursive(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    insert_tree(
        &fs,
        Path::new("/outer"),
        json!({
            "inner1": {
                "a": "A",
                "b": "B",
                "inner3": {
                    "d": "D",
                },
                "inner4": {}
            },
            "inner2": {
                "c": "C",
            }
        }),
    );
    assert_eq!(
        fs.files(),
        vec![
            PathBuf::from("/outer/inner1/a"),
            PathBuf::from("/outer/inner1/b"),
            PathBuf::from("/outer/inner1/inner3/d"),
            PathBuf::from("/outer/inner2/c"),
        ]
    );
    assert!(fs.is_dir(Path::new("/outer/inner1/inner4")).await);

    let source = Path::new("/outer");
    let target = Path::new("/copy");
    copy_recursive(fs.as_ref(), source, target, Default::default())
        .await
        .unwrap();

    assert_eq!(
        fs.files(),
        vec![
            PathBuf::from("/copy/inner1/a"),
            PathBuf::from("/copy/inner1/b"),
            PathBuf::from("/copy/inner1/inner3/d"),
            PathBuf::from("/copy/inner2/c"),
            PathBuf::from("/outer/inner1/a"),
            PathBuf::from("/outer/inner1/b"),
            PathBuf::from("/outer/inner1/inner3/d"),
            PathBuf::from("/outer/inner2/c"),
        ]
    );
    assert!(fs.is_dir(Path::new("/copy/inner1/inner4")).await);
    assert_eq!(
        fs.load(Path::new("/copy/inner1/inner3/d")).await.unwrap(),
        "D"
    );
}

#[gpui::test]
async fn test_wasm_fs_take_dirty(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    let a = Path::new("/state/a");
    let b = Path::new("/state/b");
    let c = Path::new("/state/c");
    fs.write(a, b"1").await.unwrap();
    fs.write(a, b"2").await.unwrap();
    fs.write(b, b"x").await.unwrap();
    fs.remove_file(b, RemoveOptions::default()).await.unwrap();

    let dirty = fs.take_dirty();
    assert_eq!(
        dirty
            .iter()
            .map(|file| (file.path.clone(), file.contents.clone()))
            .collect::<Vec<_>>(),
        vec![
            (a.to_path_buf(), Some(b"2".to_vec())),
            (b.to_path_buf(), None),
        ]
    );
    assert!(fs.take_dirty().is_empty(), "the dirty set is drained");

    fs.rename(a, c, RenameOptions::default()).await.unwrap();
    let dirty = fs.take_dirty();
    assert_eq!(
        dirty
            .iter()
            .map(|file| (file.path.clone(), file.contents.clone()))
            .collect::<Vec<_>>(),
        vec![
            (a.to_path_buf(), None),
            (c.to_path_buf(), Some(b"2".to_vec())),
        ]
    );
}

#[gpui::test]
async fn test_wasm_fs_insert_dir(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor.clone());
    fs.insert_dir(Path::new(CONFIG_DIR));
    let (mut events, _watcher) = fs
        .watch(Path::new(CONFIG_DIR), Duration::from_millis(10))
        .await;

    let snippets = Path::new("/home/web/.config/zed/snippets");
    fs.insert_dir(snippets);
    assert!(fs.is_dir(snippets).await);
    let children: Vec<_> = fs.read_dir(snippets).await.unwrap().collect().await;
    assert!(children.is_empty());
    assert_eq!(
        kinds(&events.next().await.unwrap()),
        vec![(snippets.to_path_buf(), Some(PathEventKind::Created))]
    );

    fs.insert_dir(snippets);
    executor.advance_clock(Duration::from_millis(50));
    executor.run_until_parked();
    assert!(
        futures::poll!(events.next()).is_pending(),
        "an existing directory emits nothing"
    );
    assert!(fs.take_dirty().is_empty(), "directories are never dirty");
}

#[gpui::test]
#[should_panic(expected = "could not create the directory")]
async fn test_wasm_fs_insert_dir_under_a_file_panics(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    fs.insert_file(Path::new("/file"), b"x".to_vec());
    fs.insert_dir(Path::new("/file/dir"));
}

#[gpui::test]
async fn test_wasm_fs_create_file_options(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    let path = Path::new("/dir/file");
    assert!(
        fs.create_file(path, CreateOptions::default())
            .await
            .is_err(),
        "create_file does not create parents"
    );
    fs.create_dir(Path::new("/dir")).await.unwrap();
    fs.create_file(path, CreateOptions::default())
        .await
        .unwrap();
    fs.write(path, b"content").await.unwrap();

    let error = fs
        .create_file(path, CreateOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<std::io::Error>()
            .map(|error| error.kind()),
        Some(std::io::ErrorKind::AlreadyExists)
    );
    fs.create_file(
        path,
        CreateOptions {
            ignore_if_exists: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.load(path).await.unwrap(), "content");
    fs.create_file(
        path,
        CreateOptions {
            overwrite: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(fs.load(path).await.unwrap(), "");

    assert!(
        fs.remove_dir(Path::new("/dir"), RemoveOptions::default())
            .await
            .is_err()
    );
    fs.remove_dir(
        Path::new("/dir"),
        RemoveOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(fs.files().is_empty());
    assert!(!fs.is_dir(Path::new("/dir")).await);
}

#[gpui::test]
async fn test_wasm_fs_unsupported_ops(executor: BackgroundExecutor) {
    let fs = WasmFs::new(executor);
    assert!(fs.open_repo(Path::new("/repo/.git"), None).is_err());
    assert!(
        fs.git_init(Path::new("/repo"), "main".to_string())
            .await
            .is_err()
    );
    assert!(
        fs.git_clone(Path::new("/repo"), "https://example.invalid/repo.git")
            .await
            .is_err()
    );
    let mut empty = futures::io::empty();
    let content: Pin<&mut (dyn futures::AsyncRead + Send)> = Pin::new(&mut empty) as _;
    let tar = async_tar::Archive::new(content);
    assert!(fs.extract_tar_file(Path::new("/tar"), tar).await.is_err());
    assert!(!fs.is_fake());
    assert!(fs.is_case_sensitive().await);
    let _jobs = fs.subscribe_to_jobs();
}

#[test]
fn test_wasm_fs_spin_mutex_serializes() {
    const THREADS: usize = 8;
    const INCREMENTS: u64 = 10_000;
    let counter = Arc::new(SpinMutex::new(0u64));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let counter = counter.clone();
            std::thread::spawn(move || {
                for _ in 0..INCREMENTS {
                    *counter.lock() += 1;
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(*counter.lock(), THREADS as u64 * INCREMENTS);
}
