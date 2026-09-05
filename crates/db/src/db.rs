pub mod client_state;
pub mod kvp;
pub mod query;

// Re-export
pub use anyhow;
#[cfg(not(target_family = "wasm"))]
use anyhow::Context as _;
pub use gpui;
use gpui::{App, AppContext, Global};
pub use indoc::indoc;
pub use inventory;
pub use paths::database_dir;
pub use sqlez;
pub use sqlez_macros;
pub use uuid;

pub use release_channel::RELEASE_CHANNEL;
use release_channel::ReleaseChannel;
use sqlez::domain::Migrator;
pub use sqlez::thread_safe_connection::RestoreOutcome;
use sqlez::thread_safe_connection::ThreadSafeConnection;
use sqlez_macros::sql;
#[cfg(not(target_family = "wasm"))]
use std::fs::create_dir_all;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::AtomicBool;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::Ordering;
use util::ResultExt;
#[cfg(not(target_family = "wasm"))]
use util::maybe;
#[cfg(not(target_family = "wasm"))]
use zed_env_vars::ZED_STATELESS;

/// A migration registered via `static_connection!` and collected at link time.
pub struct DomainMigration {
    pub name: &'static str,
    pub migrations: &'static [&'static str],
    pub dependencies: &'static [&'static str],
    pub should_allow_migration_change: fn(usize, &str, &str) -> bool,
}

inventory::collect!(DomainMigration);

/// How many domain migrations the link-time registry holds. On wasm the registry is
/// filled by `__wasm_call_ctors`, which the loader must run before `open_with_image`;
/// a count of zero there means the constructors did not run and the database would
/// silently open with an empty schema, so the browser boot asserts on this first.
pub fn registered_migration_count() -> usize {
    inventory::iter::<DomainMigration>().count()
}

/// The shared database connection backing all domain-specific DB wrappers.
/// Set as a GPUI global per-App. Falls back to a shared LazyLock if not set.
pub struct AppDatabase(pub ThreadSafeConnection);

impl Global for AppDatabase {}

/// Migrator that runs all inventory-registered domain migrations.
pub struct AppMigrator;

impl Migrator for AppMigrator {
    fn migrate(connection: &sqlez::connection::Connection) -> anyhow::Result<()> {
        let registrations: Vec<&DomainMigration> = inventory::iter::<DomainMigration>().collect();
        let sorted = topological_sort(&registrations);
        for reg in &sorted {
            let mut should_allow = reg.should_allow_migration_change;
            connection.migrate(reg.name, reg.migrations, &mut should_allow)?;
        }
        Ok(())
    }
}

impl AppDatabase {
    /// Opens the production database and runs all inventory-registered
    /// migrations in dependency order.
    #[cfg(not(target_family = "wasm"))]
    pub fn new() -> Self {
        let db_dir = database_dir();
        let connection = gpui::block_on(open_db::<AppMigrator>(db_dir, *RELEASE_CHANNEL));
        Self(connection)
    }

    /// Native: like `new`, but restores `image` (a `ThreadSafeConnection::serialize` image
    /// of an `AppDatabase`, when `Some`) before migrating. An unusable image is reported as
    /// `RestoreOutcome::Skipped` and the database opens empty.
    #[cfg(not(target_family = "wasm"))]
    pub fn new_with_image(image: Option<Vec<u8>>) -> (Self, RestoreOutcome) {
        gpui::block_on(Self::open_with_image(image))
    }

    /// Async constructor for targets without `gpui::block_on` (the browser entry point);
    /// otherwise identical to `new_with_image`. Await it from a background task: the restore
    /// copies the image on the awaiting thread where the write queue runs inline.
    pub async fn open_with_image(image: Option<Vec<u8>>) -> (Self, RestoreOutcome) {
        let db_dir = database_dir();
        let (connection, outcome) =
            open_db_with_image::<AppMigrator>(db_dir, *RELEASE_CHANNEL, image).await;
        (Self(connection), outcome)
    }

    /// Creates a new in-memory database with a unique name and runs all
    /// inventory-registered migrations in dependency order.
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_new() -> Self {
        Self::test_new_with_image(None).0
    }

    /// Like `test_new`, but restores `image` first (the in-memory counterpart of
    /// `new_with_image`, so tests never touch the real database directory).
    #[cfg(any(test, feature = "test-support"))]
    pub fn test_new_with_image(image: Option<Vec<u8>>) -> (Self, RestoreOutcome) {
        let name = format!("test-db-{}", uuid::Uuid::new_v4());
        let (connection, outcome) =
            gpui::block_on(open_test_db_with_image::<AppMigrator>(&name, image));
        (Self(connection), outcome)
    }

    /// Returns the per-App connection if set, otherwise falls back to
    /// the shared LazyLock.
    pub fn global(cx: &App) -> &ThreadSafeConnection {
        #[allow(unreachable_code)]
        if let Some(db) = cx.try_global::<Self>() {
            return &db.0;
        } else {
            #[cfg(any(feature = "test-support", test))]
            return &TEST_APP_DATABASE.0;

            panic!("database not initialized")
        }
    }
}

fn topological_sort<'a>(registrations: &[&'a DomainMigration]) -> Vec<&'a DomainMigration> {
    let mut sorted: Vec<&DomainMigration> = Vec::new();
    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();

    fn visit<'a>(
        name: &str,
        registrations: &[&'a DomainMigration],
        sorted: &mut Vec<&'a DomainMigration>,
        visited: &mut std::collections::HashSet<&'a str>,
    ) {
        if visited.contains(name) {
            return;
        }
        if let Some(reg) = registrations.iter().find(|r| r.name == name) {
            for dep in reg.dependencies {
                visit(dep, registrations, sorted, visited);
            }
            visited.insert(reg.name);
            sorted.push(reg);
        }
    }

    for reg in registrations {
        visit(reg.name, registrations, &mut sorted, &mut visited);
    }
    sorted
}

/// Shared fallback `AppDatabase` used when no per-App global is set.
#[cfg(any(test, feature = "test-support"))]
static TEST_APP_DATABASE: LazyLock<AppDatabase> = LazyLock::new(AppDatabase::test_new);

const CONNECTION_INITIALIZE_QUERY: &str = sql!(
    PRAGMA foreign_keys=TRUE;
);

#[cfg(not(target_family = "wasm"))]
const DB_INITIALIZE_QUERY: &str = sql!(
    PRAGMA journal_mode=WAL;
    PRAGMA busy_timeout=500;
    PRAGMA case_sensitive_like=TRUE;
    PRAGMA synchronous=NORMAL;
);

/// No busy handler in the browser: SQLite's busy wait goes through the VFS `xSleep`,
/// which is `memory.atomic.wait32` in sqlite-wasm-rs and traps on the main thread.
/// With a single connection SQLITE_BUSY cannot occur anyway.
#[cfg(target_family = "wasm")]
const DB_INITIALIZE_QUERY: &str = sql!(
    PRAGMA journal_mode=MEMORY;
    PRAGMA case_sensitive_like=TRUE;
    PRAGMA synchronous=OFF;
);

const FALLBACK_DB_NAME: &str = "FALLBACK_MEMORY_DB";

const DB_FILE_NAME: &str = "db.sqlite";

pub static ALL_FILE_DB_FAILED: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

/// A type that can be used as a database scope for path construction.
pub trait DbScope {
    fn scope_name(&self) -> &str;
}

impl DbScope for ReleaseChannel {
    fn scope_name(&self) -> &str {
        self.dev_name()
    }
}

/// A database scope shared across all release channels.
pub struct GlobalDbScope;

impl DbScope for GlobalDbScope {
    fn scope_name(&self) -> &str {
        "global"
    }
}

/// Returns the path to the `AppDatabase` SQLite file for the given scope
/// under `db_dir`.
pub fn db_path(db_dir: &Path, scope: impl DbScope) -> PathBuf {
    db_dir
        .join(format!("0-{}", scope.scope_name()))
        .join(DB_FILE_NAME)
}

/// Open or create a database at the given directory path.
/// This will retry a couple times if there are failures. If opening fails once, the db directory
/// is moved to a backup folder and a new one is created. If that fails, a shared in memory db is created.
/// In either case, static variables are set so that the user can be notified.
pub async fn open_db<M: Migrator + 'static>(
    db_dir: &Path,
    scope: impl DbScope,
) -> ThreadSafeConnection {
    open_db_with_image::<M>(db_dir, scope, None).await.0
}

/// Like [`open_db`], but restores `image` (when `Some`) into whichever database is opened
/// (the file database, or the in-memory fallback) before its migrations run, and reports
/// what happened to it. The image is validated on a scratch connection first, so it can
/// neither make the file database fail to open nor make the fallback panic: an unusable
/// image yields `RestoreOutcome::Skipped` and an empty database.
pub async fn open_db_with_image<M: Migrator + 'static>(
    db_dir: &Path,
    scope: impl DbScope,
    image: Option<Vec<u8>>,
) -> (ThreadSafeConnection, RestoreOutcome) {
    // The browser has no database directory: the memory VFS would accept a fake path,
    // but there is nothing to gain, and `ALL_FILE_DB_FAILED` must not be set on a normal
    // boot. Persistence is the client-state image (`client_state`).
    #[cfg(target_family = "wasm")]
    {
        log::debug!(
            "browser build: opening the in-memory database in place of {}",
            db_path(db_dir, scope).display()
        );
        return open_fallback_db::<M>(image).await;
    }

    #[cfg(not(target_family = "wasm"))]
    if *ZED_STATELESS {
        return open_fallback_db::<M>(image).await;
    }

    #[cfg(not(target_family = "wasm"))]
    let db_path = db_path(db_dir, scope);

    // The main attempt gets a copy: an image never makes `open_main_db` fail (it is
    // validated on a scratch connection first), so a failure here is unrelated to it and the
    // fallback must still receive it, or the client would run an empty database read-write
    // and overwrite the stored image on its first save.
    #[cfg(not(target_family = "wasm"))]
    let connection = maybe!(async {
        if let Some(parent) = db_path.parent() {
            create_dir_all(parent)
                .context("Could not create db directory")
                .log_err()?;
        }
        open_main_db::<M>(&db_path, image.clone()).await
    })
    .await;

    #[cfg(not(target_family = "wasm"))]
    if let Some(connection) = connection {
        return connection;
    }

    // Set another static ref so that we can escalate the notification
    #[cfg(not(target_family = "wasm"))]
    ALL_FILE_DB_FAILED.store(true, Ordering::Release);

    // If still failed, create an in memory db with a known name
    #[cfg(not(target_family = "wasm"))]
    open_fallback_db::<M>(image).await
}

#[cfg(not(target_family = "wasm"))]
async fn open_main_db<M: Migrator>(
    db_path: &Path,
    image: Option<Vec<u8>>,
) -> Option<(ThreadSafeConnection, RestoreOutcome)> {
    log::trace!("Opening database {}", db_path.display());
    let mut builder = ThreadSafeConnection::builder::<M>(db_path.to_string_lossy().as_ref(), true)
        .with_db_initialization_query(DB_INITIALIZE_QUERY)
        .with_connection_initialize_query(CONNECTION_INITIALIZE_QUERY);
    if let Some(image) = image {
        builder = builder.with_restore_image(image);
    }
    builder.build_with_outcome().await.log_err()
}

async fn open_fallback_db<M: Migrator>(
    image: Option<Vec<u8>>,
) -> (ThreadSafeConnection, RestoreOutcome) {
    open_fallback_db_named::<M>(FALLBACK_DB_NAME, image).await
}

async fn open_fallback_db_named<M: Migrator>(
    name: &str,
    image: Option<Vec<u8>>,
) -> (ThreadSafeConnection, RestoreOutcome) {
    open_fallback_db_with_queue::<M>(name, image, None).await
}

/// `open_fallback_db` with an explicit write queue. The builder's default queue is an
/// OS thread natively and `sqlez`'s `wasm_lock_queue` in the browser; tests pass the
/// wasm queue to run the browser's path on the host without spawning a thread.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn open_fallback_db_with_queue<M: Migrator>(
    name: &str,
    image: Option<Vec<u8>>,
    write_queue_constructor: Option<sqlez::thread_safe_connection::WriteQueueConstructor>,
) -> (ThreadSafeConnection, RestoreOutcome) {
    // The in-memory database is the browser's normal path, not a fallback.
    #[cfg(target_family = "wasm")]
    log::debug!("Opening in-memory database {name}");
    #[cfg(not(target_family = "wasm"))]
    log::warn!("Opening fallback in-memory database");
    let mut builder = ThreadSafeConnection::builder::<M>(name, false)
        .with_db_initialization_query(DB_INITIALIZE_QUERY)
        .with_connection_initialize_query(CONNECTION_INITIALIZE_QUERY);
    if let Some(image) = image {
        builder = builder.with_restore_image(image);
    }
    if let Some(write_queue_constructor) = write_queue_constructor {
        builder = builder.with_write_queue_constructor(write_queue_constructor);
    }
    builder
        .build_with_outcome()
        .await
        .expect(
            "Fallback in memory database failed. Likely initialization queries or migrations have fundamental errors",
        )
}

#[cfg(any(test, feature = "test-support"))]
pub async fn open_test_db<M: Migrator>(db_name: &str) -> ThreadSafeConnection {
    open_test_db_with_image::<M>(db_name, None).await.0
}

/// Test counterpart of [`open_db_with_image`]: a uniquely named in-memory database whose
/// writes run inline on a mutex.
#[cfg(any(test, feature = "test-support"))]
pub async fn open_test_db_with_image<M: Migrator>(
    db_name: &str,
    image: Option<Vec<u8>>,
) -> (ThreadSafeConnection, RestoreOutcome) {
    use sqlez::thread_safe_connection::locking_queue;

    let mut builder = ThreadSafeConnection::builder::<M>(db_name, false)
        .with_db_initialization_query(DB_INITIALIZE_QUERY)
        .with_connection_initialize_query(CONNECTION_INITIALIZE_QUERY)
        // Serialize queued writes via a mutex and run them synchronously
        .with_write_queue_constructor(locking_queue());
    if let Some(image) = image {
        builder = builder.with_restore_image(image);
    }
    builder.build_with_outcome().await.unwrap()
}

/// Implements a basic DB wrapper for a given domain
///
/// Arguments:
/// - type of connection wrapper
/// - dependencies, whose migrations should be run prior to this domain's migrations
#[macro_export]
macro_rules! static_connection {
    ($t:ident, [ $($d:ty),* ]) => {
        impl ::std::ops::Deref for $t {
            type Target = $crate::sqlez::thread_safe_connection::ThreadSafeConnection;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::clone::Clone for $t {
            fn clone(&self) -> Self {
                $t(self.0.clone())
            }
        }

        impl $t {
            /// Returns an instance backed by the per-App database if set,
            /// or the shared fallback connection otherwise.
            pub fn global(cx: &$crate::gpui::App) -> Self {
                $t($crate::AppDatabase::global(cx).clone())
            }

            #[cfg(any(test, feature = "test-support"))]
            pub async fn open_test_db(name: &'static str) -> Self {
                $t($crate::open_test_db::<$t>(name).await)
            }
        }

        $crate::inventory::submit! {
            $crate::DomainMigration {
                name: <$t as $crate::sqlez::domain::Domain>::NAME,
                migrations: <$t as $crate::sqlez::domain::Domain>::MIGRATIONS,
                dependencies: &[$(<$d as $crate::sqlez::domain::Domain>::NAME),*],
                should_allow_migration_change: <$t as $crate::sqlez::domain::Domain>::should_allow_migration_change,
            }
        }
    }
}

pub fn write_and_log<F>(cx: &App, db_write: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = anyhow::Result<()>> + Send,
{
    cx.background_spawn(async move { db_write().await.log_err() })
        .detach()
}

#[cfg(test)]
mod tests {
    use std::thread;

    use sqlez::domain::Domain;
    use sqlez_macros::sql;

    use crate::{
        AppMigrator, RestoreOutcome, open_db, open_db_with_image, open_fallback_db_named,
        open_fallback_db_with_queue, registered_migration_count,
    };

    /// The browser boot awaits `open_with_image` on the background executor, which needs
    /// the future to be `Send`; checked here, natively, so the contract cannot silently
    /// break in `sqlez`.
    #[test]
    fn open_with_image_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let future = crate::AppDatabase::open_with_image(None);
        assert_send(&future);
        // Never polled: it would open the real database directory.
        drop(future);
    }

    /// The browser's path: no OS thread for the write queue (the test dispatcher would
    /// panic on parking), the migrations run inline under `wasm_lock`, and the
    /// link-time migration registry is populated.
    #[gpui::test]
    async fn open_fallback_db_with_wasm_lock_queue_needs_no_parking(cx: &mut gpui::TestAppContext) {
        assert!(registered_migration_count() > 0);

        let (connection, outcome) = open_fallback_db_with_queue::<AppMigrator>(
            "open_fallback_db_with_wasm_lock_queue_needs_no_parking",
            None,
            Some(sqlez::thread_safe_connection::wasm_lock_queue()),
        )
        .await;
        cx.run_until_parked();
        assert_eq!(outcome, RestoreOutcome::NoImage);
        connection
            .write(|connection| {
                connection
                    .exec("INSERT INTO kv_store(key, value) VALUES ('k', 'v')")
                    .unwrap()()
                .unwrap();
            })
            .await;
        assert_eq!(
            connection
                .select_row::<String>("SELECT value FROM kv_store WHERE key = 'k'")
                .unwrap()()
            .unwrap(),
            Some("v".to_string())
        );
        let image = connection.serialize().await.unwrap();
        let (restored, outcome) = open_fallback_db_with_queue::<AppMigrator>(
            "open_fallback_db_with_wasm_lock_queue_needs_no_parking_restored",
            Some(image),
            Some(sqlez::thread_safe_connection::wasm_lock_queue()),
        )
        .await;
        assert_eq!(outcome, RestoreOutcome::Restored);
        assert_eq!(
            restored
                .select_row::<String>("SELECT value FROM kv_store WHERE key = 'k'")
                .unwrap()()
            .unwrap(),
            Some("v".to_string())
        );
    }

    /// Test that an image produced under an older migration list is upgraded on restore.
    #[gpui::test]
    async fn open_db_with_image_runs_missing_migrations(cx: &mut gpui::TestAppContext) {
        cx.executor().allow_parking();

        enum OlderDB {}
        impl Domain for OlderDB {
            const NAME: &str = "image_skew";
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE a(value INTEGER);)];
        }

        enum NewerDB {}
        impl Domain for NewerDB {
            const NAME: &str = "image_skew";
            const MIGRATIONS: &[&str] = &[
                sql!(CREATE TABLE a(value INTEGER);),
                sql!(CREATE TABLE b(value INTEGER);),
            ];
        }

        let source_dir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        let older =
            open_db::<OlderDB>(source_dir.path(), release_channel::ReleaseChannel::Dev).await;
        older
            .write(|connection| {
                connection.exec("INSERT INTO a(value) VALUES (5)").unwrap()().unwrap();
            })
            .await;
        let image = older.serialize().await.unwrap();

        let target_dir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        let (newer, outcome) = open_db_with_image::<NewerDB>(
            target_dir.path(),
            release_channel::ReleaseChannel::Dev,
            Some(image),
        )
        .await;
        assert_eq!(outcome, RestoreOutcome::Restored);
        assert_eq!(
            newer.select_row::<i64>("SELECT value FROM a").unwrap()().unwrap(),
            Some(5),
            "rows from the image survive"
        );
        assert_eq!(
            newer.select_row::<i64>("SELECT count(*) FROM b").unwrap()().unwrap(),
            Some(0),
            "the missing migration was applied"
        );
    }

    /// An image with a completed step beyond the code's list opens; a differing step text
    /// is skipped (the client must never run an image the code cannot migrate).
    #[gpui::test]
    async fn open_db_with_image_from_newer_build(cx: &mut gpui::TestAppContext) {
        cx.executor().allow_parking();

        enum NewerDB {}
        impl Domain for NewerDB {
            const NAME: &str = "image_newer";
            const MIGRATIONS: &[&str] = &[
                sql!(CREATE TABLE a(value INTEGER);),
                sql!(CREATE TABLE c(value INTEGER);),
            ];
        }
        enum OlderDB {}
        impl Domain for OlderDB {
            const NAME: &str = "image_newer";
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE a(value INTEGER);)];
        }
        enum DifferentDB {}
        impl Domain for DifferentDB {
            const NAME: &str = "image_newer";
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE a(other INTEGER);)];
        }

        let source_dir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        let newer =
            open_db::<NewerDB>(source_dir.path(), release_channel::ReleaseChannel::Dev).await;
        let image = newer.serialize().await.unwrap();

        let older_dir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        let (older, outcome) = open_db_with_image::<OlderDB>(
            older_dir.path(),
            release_channel::ReleaseChannel::Dev,
            Some(image.clone()),
        )
        .await;
        assert_eq!(outcome, RestoreOutcome::Restored);
        assert_eq!(
            older.select_row::<i64>("SELECT count(*) FROM c").unwrap()().unwrap(),
            Some(0),
            "extra completed steps are kept as-is"
        );

        let different_dir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        let (different, outcome) = open_db_with_image::<DifferentDB>(
            different_dir.path(),
            release_channel::ReleaseChannel::Dev,
            Some(image),
        )
        .await;
        assert!(
            matches!(outcome, RestoreOutcome::Skipped(_)),
            "a changed migration text must skip the image, got {outcome:?}"
        );
        assert_eq!(
            different
                .select_row::<i64>("SELECT count(*) FROM a")
                .unwrap()()
            .unwrap(),
            Some(0),
            "the database opened empty and migrated"
        );
    }

    /// The in-memory fallback also applies the image, and a bad image does not panic it.
    #[gpui::test]
    async fn open_fallback_db_applies_image(cx: &mut gpui::TestAppContext) {
        cx.executor().allow_parking();

        enum FallbackDB {}
        impl Domain for FallbackDB {
            const NAME: &str = "image_fallback";
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE a(value INTEGER);)];
        }

        let (source, _) = open_fallback_db_named::<FallbackDB>("fallback_image_source", None).await;
        source
            .write(|connection| {
                connection.exec("INSERT INTO a(value) VALUES (9)").unwrap()().unwrap();
            })
            .await;
        let image = source.serialize().await.unwrap();

        let (restored, outcome) =
            open_fallback_db_named::<FallbackDB>("fallback_image_target", Some(image)).await;
        assert_eq!(outcome, RestoreOutcome::Restored);
        assert_eq!(
            restored.select_row::<i64>("SELECT value FROM a").unwrap()().unwrap(),
            Some(9)
        );

        let (empty, outcome) =
            open_fallback_db_named::<FallbackDB>("fallback_image_bad", Some(b"garbage".to_vec()))
                .await;
        assert!(matches!(outcome, RestoreOutcome::Skipped(_)));
        assert_eq!(
            empty.select_row::<i64>("SELECT count(*) FROM a").unwrap()().unwrap(),
            Some(0)
        );
    }

    // Test bad migration panics
    #[gpui::test]
    #[should_panic]
    async fn test_bad_migration_panics() {
        enum BadDB {}

        impl Domain for BadDB {
            const NAME: &str = "db_tests";
            const MIGRATIONS: &[&str] = &[
                sql!(CREATE TABLE test(value);),
                // failure because test already exists
                sql!(CREATE TABLE test(value);),
            ];
        }

        let tempdir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        let _bad_db = open_db::<BadDB>(tempdir.path(), release_channel::ReleaseChannel::Dev).await;
    }

    /// Test that DB exists but corrupted (causing recreate)
    #[gpui::test]
    async fn test_db_corruption(cx: &mut gpui::TestAppContext) {
        cx.executor().allow_parking();

        enum CorruptedDB {}

        impl Domain for CorruptedDB {
            const NAME: &str = "db_tests";
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE test(value);)];
        }

        enum GoodDB {}

        impl Domain for GoodDB {
            const NAME: &str = "db_tests"; //Notice same name
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE test2(value);)];
        }

        let tempdir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        {
            let corrupt_db =
                open_db::<CorruptedDB>(tempdir.path(), release_channel::ReleaseChannel::Dev).await;
            assert!(corrupt_db.persistent());
        }

        let good_db = open_db::<GoodDB>(tempdir.path(), release_channel::ReleaseChannel::Dev).await;
        assert!(
            good_db.select_row::<usize>("SELECT * FROM test2").unwrap()()
                .unwrap()
                .is_none()
        );
    }

    /// Test that DB exists but corrupted (causing recreate)
    #[gpui::test(iterations = 30)]
    async fn test_simultaneous_db_corruption(cx: &mut gpui::TestAppContext) {
        cx.executor().allow_parking();

        enum CorruptedDB {}

        impl Domain for CorruptedDB {
            const NAME: &str = "db_tests";

            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE test(value);)];
        }

        enum GoodDB {}

        impl Domain for GoodDB {
            const NAME: &str = "db_tests"; //Notice same name
            const MIGRATIONS: &[&str] = &[sql!(CREATE TABLE test2(value);)]; // But different migration
        }

        let tempdir = tempfile::Builder::new()
            .prefix("DbTests")
            .tempdir()
            .unwrap();
        {
            // Setup the bad database
            let corrupt_db =
                open_db::<CorruptedDB>(tempdir.path(), release_channel::ReleaseChannel::Dev).await;
            assert!(corrupt_db.persistent());
        }

        // Try to connect to it a bunch of times at once
        let mut guards = vec![];
        for _ in 0..10 {
            let tmp_path = tempdir.path().to_path_buf();
            let guard = thread::spawn(move || {
                let good_db = gpui::block_on(open_db::<GoodDB>(
                    tmp_path.as_path(),
                    release_channel::ReleaseChannel::Dev,
                ));
                assert!(
                    good_db.select_row::<usize>("SELECT * FROM test2").unwrap()()
                        .unwrap()
                        .is_none()
                );
            });

            guards.push(guard);
        }

        for guard in guards.into_iter() {
            assert!(guard.join().is_ok());
        }
    }
}
