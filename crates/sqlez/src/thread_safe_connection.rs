use anyhow::Context as _;
use collections::HashMap;
use futures::{Future, FutureExt, channel::oneshot};
use parking_lot::{Mutex, RwLock};
use std::{
    marker::PhantomData,
    ops::Deref,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};
#[cfg(not(target_family = "wasm"))]
use std::{thread, time::Duration};
#[cfg(not(target_family = "wasm"))]
use thread_local::ThreadLocal;

#[cfg(not(target_family = "wasm"))]
use crate::util::UnboundedSyncSender;
use crate::{connection::Connection, domain::Migrator};

const MIGRATION_RETRIES: usize = 10;
#[cfg(not(target_family = "wasm"))]
const CONNECTION_INITIALIZE_RETRIES: usize = 50;
#[cfg(not(target_family = "wasm"))]
const CONNECTION_INITIALIZE_RETRY_DELAY: Duration = Duration::from_millis(1);

/// One queued `write` callback, already bound to its connection.
pub type QueuedWrite = Box<dyn 'static + Send + FnOnce()>;
/// Runs (or forwards) one queued write; every write to a database goes through its queue.
pub type WriteQueue = Box<dyn 'static + Send + Sync + Fn(QueuedWrite)>;
/// Builds the [`WriteQueue`] of a database the first time it is opened in this process.
pub type WriteQueueConstructor = Box<dyn 'static + Send + FnMut() -> WriteQueue>;

/// List of queues of tasks by database uri. This lets us serialize writes to the database
/// and have a single worker thread per db file. This means many thread safe connections
/// (possibly with different migrations) could all be communicating with the same background
/// thread. Queues are `Arc`s so `enqueue` can clone one out and release the map's lock
/// before running it: `wasm_lock_queue` runs the write inline, and a nested `write` from
/// inside it would otherwise re-enter this `RwLock` for reading while another thread's
/// `initialize_queues` waits to write it, which parking_lot does not allow (deadlock).
static QUEUES: LazyLock<RwLock<HashMap<Arc<str>, Arc<WriteQueue>>>> =
    LazyLock::new(Default::default);

/// Thread safe connection to a given database file or in memory db. This can be cloned, shared, static,
/// whatever. It derefs to a synchronous connection by thread that is read only. A write capable connection
/// may be accessed by passing a callback to the `write` function which will queue the callback
#[derive(Clone)]
pub struct ThreadSafeConnection {
    uri: Arc<str>,
    persistent: bool,
    connection_initialize_query: Option<&'static str>,
    #[cfg(not(target_family = "wasm"))]
    connections: Arc<ThreadLocal<Connection>>,
    /// One connection shared by every thread; every FFI call is serialized by `wasm_lock`.
    #[cfg(target_family = "wasm")]
    connections: Arc<std::sync::OnceLock<Connection>>,
    /// Bumped once per completed `write` callback; `write_generation()` lets a caller tell
    /// whether anything was written since it last looked (dirty tracking for image saves).
    write_generation: Arc<AtomicU64>,
}

unsafe impl Send for ThreadSafeConnection {}
unsafe impl Sync for ThreadSafeConnection {}

/// What `ThreadSafeConnectionBuilder::build_with_outcome` did with its restore image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreOutcome {
    /// No image was given.
    NoImage,
    /// The image was validated, migrated and copied into the database before anything else
    /// touched it.
    Restored,
    /// The image was unusable (bad header, corrupt pages, or a migration the running code
    /// cannot apply); the database was left empty and the build continued without it. The
    /// payload is the error text.
    Skipped(String),
}

pub struct ThreadSafeConnectionBuilder<M: Migrator + 'static = ()> {
    db_initialize_query: Option<&'static str>,
    write_queue_constructor: Option<WriteQueueConstructor>,
    restore_image: Option<Vec<u8>>,
    connection: ThreadSafeConnection,
    /// `fn() -> M` rather than `*mut M`: the marker must not make the builder (and so the
    /// `build_with_outcome` future, which holds `self` across its await) `!Send`; the
    /// browser boot awaits that future on the background executor.
    _migrator: PhantomData<fn() -> M>,
}

impl<M: Migrator> ThreadSafeConnectionBuilder<M> {
    /// Sets the query to run every time a connection is opened. This must
    /// be infallible (EG only use pragma statements) and not cause writes.
    /// to the db or it will panic.
    pub fn with_connection_initialize_query(mut self, initialize_query: &'static str) -> Self {
        self.connection.connection_initialize_query = Some(initialize_query);
        self
    }

    /// Queues an initialization query for the database file. This must be infallible
    /// but may cause changes to the database file such as with `PRAGMA journal_mode`
    pub fn with_db_initialization_query(mut self, initialize_query: &'static str) -> Self {
        self.db_initialize_query = Some(initialize_query);
        self
    }

    /// Specifies how the thread safe connection should serialize writes. If provided
    /// the connection will call the write_queue_constructor for each database file in
    /// this process. The constructor is responsible for setting up a background thread or
    /// async task which handles queued writes with the provided connection.
    pub fn with_write_queue_constructor(
        mut self,
        write_queue_constructor: WriteQueueConstructor,
    ) -> Self {
        self.write_queue_constructor = Some(write_queue_constructor);
        self
    }

    /// Restores `image` (a `Connection::serialize_main` image) into the still-empty
    /// database before the initialization query and the migrations run. The image is
    /// validated on a scratch connection first (header check, `M::migrate` dry-run) so an
    /// unusable image never touches the database; see [`RestoreOutcome`].
    pub fn with_restore_image(mut self, image: Vec<u8>) -> Self {
        self.restore_image = Some(image);
        self
    }

    pub async fn build(self) -> anyhow::Result<ThreadSafeConnection> {
        Ok(self.build_with_outcome().await?.0)
    }

    /// Like `build`, but also reports what happened to the restore image.
    pub async fn build_with_outcome(
        self,
    ) -> anyhow::Result<(ThreadSafeConnection, RestoreOutcome)> {
        self.connection
            .initialize_queues(self.write_queue_constructor);

        let db_initialize_query = self.db_initialize_query;
        let restore_image = self.restore_image;

        let outcome = self
            .connection
            .write(move |connection| {
                let outcome = match restore_image {
                    None => RestoreOutcome::NoImage,
                    Some(image) => match restore_image_into::<M>(connection, &image) {
                        Ok(()) => RestoreOutcome::Restored,
                        Err(error) => {
                            log::warn!("skipping client-state image: {error:#}");
                            RestoreOutcome::Skipped(format!("{error:#}"))
                        }
                    },
                };

                if let Some(db_initialize_query) = db_initialize_query {
                    connection.exec(db_initialize_query).with_context(|| {
                        format!(
                            "Db initialize query failed to execute: {}",
                            db_initialize_query
                        )
                    })?()?;
                }

                // Retry failed migrations in case they were run in parallel from different
                // processes. This gives a best attempt at migrating before bailing
                let mut migration_result =
                    anyhow::Result::<()>::Err(anyhow::anyhow!("Migration never run"));

                let foreign_keys_enabled: bool =
                    connection.select_row::<i32>("PRAGMA foreign_keys")?()
                        .unwrap_or(None)
                        .map(|enabled| enabled != 0)
                        .unwrap_or(false);

                connection.exec("PRAGMA foreign_keys = OFF;")?()?;

                for _ in 0..MIGRATION_RETRIES {
                    migration_result = connection
                        .with_savepoint("thread_safe_multi_migration", || M::migrate(connection));

                    if migration_result.is_ok() {
                        break;
                    }
                }

                if foreign_keys_enabled {
                    connection.exec("PRAGMA foreign_keys = ON;")?()?;
                }
                migration_result.map(|()| outcome)
            })
            .await?;

        Ok((self.connection, outcome))
    }
}

/// Validates `image` on a private scratch connection (header check, then `M::migrate` as a
/// dry run, which is where a corrupt page or a changed migration text surfaces) and copies
/// it into `connection` through the backup API. `connection` is untouched unless every step
/// succeeds: the backup either completes or rolls back.
fn restore_image_into<M: Migrator>(connection: &Connection, image: &[u8]) -> anyhow::Result<()> {
    let scratch = Connection::open_scratch_from_image(image)?;
    scratch
        .with_savepoint("restore_image_dry_run", || M::migrate(&scratch))
        .context("migrating the restored image")?;
    connection
        .restore_main_from(&scratch)
        .context("copying the restored image into the database")?;
    drop(scratch);
    Ok(())
}

impl ThreadSafeConnection {
    fn initialize_queues(&self, write_queue_constructor: Option<WriteQueueConstructor>) -> bool {
        if !QUEUES.read().contains_key(&self.uri) {
            let mut queues = QUEUES.write();
            if !queues.contains_key(&self.uri) {
                let mut write_queue_constructor =
                    write_queue_constructor.unwrap_or_else(default_write_queue);
                queues.insert(self.uri.clone(), Arc::new(write_queue_constructor()));
                return true;
            }
        }
        false
    }

    pub fn builder<M: Migrator>(uri: &str, persistent: bool) -> ThreadSafeConnectionBuilder<M> {
        ThreadSafeConnectionBuilder::<M> {
            db_initialize_query: None,
            write_queue_constructor: None,
            restore_image: None,
            connection: Self {
                uri: Arc::from(uri),
                persistent,
                connection_initialize_query: None,
                connections: Default::default(),
                write_generation: Arc::new(AtomicU64::new(0)),
            },
            _migrator: PhantomData,
        }
    }

    /// Number of `write` callbacks that have completed on this database so far (shared by
    /// every clone). Reads never change it; every successful mutation goes through `write`
    /// because read connections refuse non-readonly statements.
    pub fn write_generation(&self) -> u64 {
        self.write_generation.load(Ordering::Acquire)
    }

    /// Serializes the whole database into a standalone SQLite image on the write queue
    /// (`Connection::serialize_main`), so it never overlaps a write. The copy is submitted
    /// to the queue when this is *called*, not when the future is polled (see [`write`]):
    /// on targets whose write queue runs inline (the browser) it runs on the calling
    /// thread, so call this from inside a background task rather than handing the future
    /// to one.
    ///
    /// [`write`]: ThreadSafeConnection::write
    pub fn serialize(&self) -> impl Future<Output = anyhow::Result<Vec<u8>>> {
        self.enqueue(|connection| connection.serialize_main(), false)
    }

    /// Opens a new db connection with the initialized file path. This is internal and only
    /// called from the deref function.
    fn open_file(uri: &str) -> Connection {
        Connection::open_file(uri)
    }

    /// Opens a shared memory connection using the file path as the identifier. This is internal
    /// and only called from the deref function.
    fn open_shared_memory(uri: &str) -> Connection {
        Connection::open_memory(Some(uri))
    }

    /// Queues `callback` on the database's write queue and returns its result. The
    /// callback is handed to the queue when `write` is called (the returned future only
    /// waits for the result), so it runs even if the future is dropped, and on a queue
    /// that executes inline (`wasm_lock_queue`, the browser default) it runs on the
    /// calling thread before `write` returns: callers that must keep SQLite off the main
    /// thread call `write` from inside the background task, not before spawning it.
    pub fn write<T: 'static + Send + Sync>(
        &self,
        callback: impl 'static + Send + FnOnce(&Connection) -> T,
    ) -> impl Future<Output = T> {
        self.enqueue(callback, true)
    }

    /// Runs `callback` on the write queue; `counts_as_write` decides whether it bumps the
    /// write generation (a serialization pass must not, or the database would never look
    /// clean after a save).
    fn enqueue<T: 'static + Send + Sync>(
        &self,
        callback: impl 'static + Send + FnOnce(&Connection) -> T,
        counts_as_write: bool,
    ) -> impl Future<Output = T> {
        // Clone the queue out and drop the read guard before invoking it (see `QUEUES`).
        let write_channel = QUEUES
            .read()
            .get(&self.uri)
            .cloned()
            .expect("Queues are inserted when build is called. This should always succeed");

        // Create a one shot channel for the result of the queued write
        // so we can await on the result
        let (sender, receiver) = oneshot::channel();

        let thread_safe_connection = (*self).clone();
        write_channel(Box::new(move || {
            let connection = thread_safe_connection.deref();
            let result = connection.with_write(|connection| callback(connection));
            if counts_as_write {
                thread_safe_connection
                    .write_generation
                    .fetch_add(1, Ordering::Release);
            }
            sender.send(result).ok();
        }));
        receiver.map(|response| response.expect("Write queue unexpectedly closed"))
    }

    pub(crate) fn create_connection(
        persistent: bool,
        uri: &str,
        connection_initialize_query: Option<&'static str>,
    ) -> Connection {
        let connection = if persistent {
            Self::open_file(uri)
        } else {
            Self::open_shared_memory(uri)
        };

        #[cfg(target_family = "wasm")]
        if let Some(initialize_query) = connection_initialize_query {
            // A schema lock cannot occur with a single connection, and `thread::sleep`
            // is unavailable, so there is no retry loop.
            if let Err(err) = connection
                .exec(initialize_query)
                .and_then(|mut statement| statement())
            {
                panic!(
                    "Initialize query failed to execute: {}\n\nCaused by:\n{err:#}",
                    initialize_query
                )
            }
        }

        #[cfg(not(target_family = "wasm"))]
        if let Some(initialize_query) = connection_initialize_query {
            let mut last_error = None;
            let initialized = (0..CONNECTION_INITIALIZE_RETRIES).any(|attempt| {
                match connection
                    .exec(initialize_query)
                    .and_then(|mut statement| statement())
                {
                    Ok(()) => true,
                    Err(err)
                        if is_schema_lock_error(&err)
                            && attempt + 1 < CONNECTION_INITIALIZE_RETRIES =>
                    {
                        last_error = Some(err);
                        thread::sleep(CONNECTION_INITIALIZE_RETRY_DELAY);
                        false
                    }
                    Err(err) => {
                        panic!(
                            "Initialize query failed to execute: {}\n\nCaused by:\n{err:#}",
                            initialize_query
                        )
                    }
                }
            });

            if !initialized {
                let err = last_error
                    .expect("connection initialization retries should record the last error");
                panic!(
                    "Initialize query failed to execute after retries: {}\n\nCaused by:\n{err:#}",
                    initialize_query
                );
            }
        }

        // Disallow writes on the connection. The only writes allowed for thread safe connections
        // are from the background thread that can serialize them.
        connection.write.store(false, Ordering::Release);

        connection
    }
}

#[cfg(not(target_family = "wasm"))]
fn is_schema_lock_error(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}");
    message.contains("database schema is locked") || message.contains("database is locked")
}

impl ThreadSafeConnection {
    /// Special constructor for ThreadSafeConnection which disallows db initialization and migrations.
    /// This allows construction to be infallible and not write to the db.
    pub fn new(
        uri: &str,
        persistent: bool,
        connection_initialize_query: Option<&'static str>,
        write_queue_constructor: Option<WriteQueueConstructor>,
    ) -> Self {
        let connection = Self {
            uri: Arc::from(uri),
            persistent,
            connection_initialize_query,
            connections: Default::default(),
            write_generation: Arc::new(AtomicU64::new(0)),
        };

        connection.initialize_queues(write_queue_constructor);
        connection
    }
}

impl Deref for ThreadSafeConnection {
    type Target = Connection;

    #[cfg(not(target_family = "wasm"))]
    fn deref(&self) -> &Self::Target {
        self.connections.get_or(|| {
            Self::create_connection(self.persistent, &self.uri, self.connection_initialize_query)
        })
    }

    #[cfg(target_family = "wasm")]
    fn deref(&self) -> &Self::Target {
        self.connections.get_or_init(|| {
            Self::create_connection(self.persistent, &self.uri, self.connection_initialize_query)
        })
    }
}

/// The queue used when the builder passes none: an OS thread natively, the
/// process-wide reentrant spin lock in the browser (std threads cannot be spawned on
/// wasm32-unknown-unknown, and `locking_queue`'s parking_lot mutex would park the main
/// thread when a worker holds it).
fn default_write_queue() -> WriteQueueConstructor {
    #[cfg(not(target_family = "wasm"))]
    {
        background_thread_queue()
    }
    #[cfg(target_family = "wasm")]
    {
        wasm_lock_queue()
    }
}

/// Runs each queued write inline on the caller's thread under `wasm_lock`, the same
/// lock every statement takes, so a `write` issued from inside another write's closure
/// (or while a statement is alive) simply re-enters it.
#[cfg(any(target_family = "wasm", test, feature = "test-support"))]
pub fn wasm_lock_queue() -> WriteQueueConstructor {
    Box::new(|| {
        Box::new(|queued_write| {
            let _guard = crate::wasm_lock::lock();
            queued_write();
        })
    })
}

#[cfg(not(target_family = "wasm"))]
pub fn background_thread_queue() -> WriteQueueConstructor {
    use std::sync::mpsc::channel;

    Box::new(|| {
        let (sender, receiver) = channel::<QueuedWrite>();

        thread::Builder::new()
            .name("sqlezWorker".to_string())
            .spawn(move || {
                while let Ok(write) = receiver.recv() {
                    write()
                }
            })
            .unwrap();

        let sender = UnboundedSyncSender::new(sender);
        Box::new(move |queued_write| {
            sender
                .send(queued_write)
                .expect("Could not send write action to background thread");
        })
    })
}

pub fn locking_queue() -> WriteQueueConstructor {
    Box::new(|| {
        let write_mutex = Mutex::new(());
        Box::new(move |queued_write| {
            let _lock = write_mutex.lock();
            queued_write();
        })
    })
}

#[cfg(test)]
mod test {
    use indoc::indoc;
    use std::ops::Deref;

    use std::{thread, time::Duration};

    use crate::{
        domain::Domain,
        thread_safe_connection::{
            RestoreOutcome, ThreadSafeConnection, locking_queue, wasm_lock_queue,
        },
    };

    #[test]
    fn wasm_lock_queue_runs_writes_inline_and_reentrantly() {
        enum QueueDomain {}
        impl Domain for QueueDomain {
            const NAME: &str = "wasm_lock_queue_runs_writes_inline_and_reentrantly";
            const MIGRATIONS: &[&str] = &["CREATE TABLE rows(value INTEGER) STRICT;"];
        }

        let connection = pollster::block_on(
            ThreadSafeConnection::builder::<QueueDomain>(
                "wasm_lock_queue_runs_writes_inline_and_reentrantly",
                false,
            )
            .with_write_queue_constructor(wasm_lock_queue())
            .build(),
        )
        .unwrap();

        // A write issued from inside another write's closure completes: the queue's
        // lock is reentrant and the inner write runs inline.
        let outer = connection.clone();
        pollster::block_on(connection.write(move |inner_connection| {
            inner_connection
                .exec("INSERT INTO rows(value) VALUES (1)")
                .unwrap()()
            .unwrap();
            pollster::block_on(outer.write(|inner_connection| {
                inner_connection
                    .exec("INSERT INTO rows(value) VALUES (2)")
                    .unwrap()()
                .unwrap();
            }));
            // The outer closure is still the writer after the nested write returned.
            assert!(inner_connection.can_write());
            inner_connection
                .exec("INSERT INTO rows(value) VALUES (4)")
                .unwrap()()
            .unwrap();
        }));
        assert!(!connection.can_write());
        assert_eq!(
            connection
                .select_row::<i64>("SELECT count(*) FROM rows")
                .unwrap()()
            .unwrap(),
            Some(3)
        );

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let connection = connection.clone();
                thread::spawn(move || {
                    for _ in 0..1_000 {
                        pollster::block_on(connection.write(|connection| {
                            connection
                                .exec("INSERT INTO rows(value) VALUES (3)")
                                .unwrap()()
                            .unwrap();
                        }));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(
            connection
                .select_row::<i64>("SELECT count(*) FROM rows")
                .unwrap()()
            .unwrap(),
            Some(3 + 2 * 1_000)
        );
    }

    /// The `build_with_outcome` future must be `Send`: the browser boot awaits it on the
    /// background executor (`db::AppDatabase::open_with_image`).
    #[test]
    fn build_future_is_send() {
        enum SendDomain {}
        impl Domain for SendDomain {
            const NAME: &str = "build_future_is_send";
            const MIGRATIONS: &[&str] = &[];
        }
        fn assert_send<T: Send>(_: &T) {}
        let future = ThreadSafeConnection::builder::<SendDomain>("build_future_is_send", false)
            .with_write_queue_constructor(locking_queue())
            .build_with_outcome();
        assert_send(&future);
        pollster::block_on(future).unwrap();
    }

    /// `Statement::prepare` takes the queue's lock before reading the write flag, so a
    /// reader cannot observe a write in progress: its statement completes only after
    /// the write's closure has returned.
    #[test]
    fn write_flag_is_not_visible_across_threads() {
        use std::sync::{
            Barrier,
            atomic::{AtomicBool, Ordering},
        };

        enum FlagDomain {}
        impl Domain for FlagDomain {
            const NAME: &str = "write_flag_is_not_visible_across_threads";
            const MIGRATIONS: &[&str] = &["CREATE TABLE flag(value INTEGER) STRICT;"];
        }

        let connection = pollster::block_on(
            ThreadSafeConnection::builder::<FlagDomain>(
                "write_flag_is_not_visible_across_threads",
                false,
            )
            .with_write_queue_constructor(wasm_lock_queue())
            .build(),
        )
        .unwrap();

        let entered = std::sync::Arc::new(Barrier::new(2));
        let released = std::sync::Arc::new(AtomicBool::new(false));
        let writer = {
            let connection = connection.clone();
            let entered = entered.clone();
            let released = released.clone();
            thread::spawn(move || {
                pollster::block_on(connection.write(move |connection| {
                    assert!(connection.can_write());
                    entered.wait();
                    thread::sleep(Duration::from_millis(50));
                    released.store(true, Ordering::SeqCst);
                }));
            })
        };
        entered.wait();

        let reader = {
            let connection = connection.clone();
            let released = released.clone();
            thread::spawn(move || {
                let count = connection
                    .select_row::<i64>("SELECT count(*) FROM flag")
                    .unwrap()()
                .unwrap();
                assert!(
                    released.load(Ordering::SeqCst),
                    "the reader's statement ran while the write closure was still active"
                );
                assert!(!connection.can_write());
                count
            })
        };
        assert_eq!(reader.join().unwrap(), Some(0));
        writer.join().unwrap();
    }

    #[test]
    fn write_bumps_generation() {
        enum GenerationDomain {}
        impl Domain for GenerationDomain {
            const NAME: &str = "write_bumps_generation";
            const MIGRATIONS: &[&str] = &["CREATE TABLE gen(value INTEGER) STRICT;"];
        }

        let connection = pollster::block_on(
            ThreadSafeConnection::builder::<GenerationDomain>("write_bumps_generation", false)
                .with_write_queue_constructor(locking_queue())
                .build(),
        )
        .unwrap();
        let after_build = connection.write_generation();
        assert!(after_build >= 1, "migrations are a write");

        connection.select::<i64>("SELECT value FROM gen").unwrap()().unwrap();
        assert_eq!(
            connection.write_generation(),
            after_build,
            "reads do not count"
        );

        pollster::block_on(connection.write(|connection| {
            connection
                .exec("INSERT INTO gen(value) VALUES (1)")
                .unwrap()()
            .unwrap();
        }));
        assert_eq!(connection.write_generation(), after_build + 1);

        pollster::block_on(connection.write(|_| ()));
        assert_eq!(connection.write_generation(), after_build + 2);
    }

    #[test]
    fn restore_visible_across_thread_local_connections() {
        enum RestoreDomain {}
        impl Domain for RestoreDomain {
            const NAME: &str = "restore_visible_across_thread_local_connections";
            const MIGRATIONS: &[&str] = &["CREATE TABLE restored(value INTEGER) STRICT;"];
        }

        let source = pollster::block_on(
            ThreadSafeConnection::builder::<RestoreDomain>(
                "restore_visible_across_thread_local_connections_source",
                false,
            )
            .with_write_queue_constructor(locking_queue())
            .build(),
        )
        .unwrap();
        pollster::block_on(source.write(|connection| {
            connection
                .exec("INSERT INTO restored(value) VALUES (42)")
                .unwrap()()
            .unwrap();
        }));
        let generation_before_serialize = source.write_generation();
        let image = pollster::block_on(source.serialize()).unwrap();
        assert_eq!(
            source.write_generation(),
            generation_before_serialize,
            "serializing is not a write"
        );

        let (restored, outcome) = pollster::block_on(
            ThreadSafeConnection::builder::<RestoreDomain>(
                "restore_visible_across_thread_local_connections_target",
                false,
            )
            .with_write_queue_constructor(locking_queue())
            .with_restore_image(image)
            .build_with_outcome(),
        )
        .unwrap();
        assert_eq!(outcome, RestoreOutcome::Restored);
        assert_eq!(
            restored
                .select_row::<i64>("SELECT value FROM restored")
                .unwrap()()
            .unwrap(),
            Some(42)
        );

        let restored_for_thread = restored.clone();
        let seen_on_other_thread = thread::spawn(move || {
            restored_for_thread
                .select_row::<i64>("SELECT value FROM restored")
                .unwrap()()
            .unwrap()
        })
        .join()
        .unwrap();
        assert_eq!(seen_on_other_thread, Some(42));
    }

    #[test]
    fn build_with_outcome_skips_unmigratable_image() {
        enum OriginalDomain {}
        impl Domain for OriginalDomain {
            const NAME: &str = "build_with_outcome_skips_unmigratable_image";
            const MIGRATIONS: &[&str] = &["CREATE TABLE original(value INTEGER) STRICT;"];
        }
        enum ChangedDomain {}
        impl Domain for ChangedDomain {
            const NAME: &str = "build_with_outcome_skips_unmigratable_image";
            const MIGRATIONS: &[&str] = &["CREATE TABLE changed(value INTEGER) STRICT;"];
        }

        let source = pollster::block_on(
            ThreadSafeConnection::builder::<OriginalDomain>(
                "build_with_outcome_skips_unmigratable_image_source",
                false,
            )
            .with_write_queue_constructor(locking_queue())
            .build(),
        )
        .unwrap();
        let image = pollster::block_on(source.serialize()).unwrap();

        let (target, outcome) = pollster::block_on(
            ThreadSafeConnection::builder::<ChangedDomain>(
                "build_with_outcome_skips_unmigratable_image_target",
                false,
            )
            .with_write_queue_constructor(locking_queue())
            .with_restore_image(image.clone())
            .build_with_outcome(),
        )
        .unwrap();
        match outcome {
            RestoreOutcome::Skipped(reason) => assert!(
                reason.contains("Migration changed"),
                "unexpected skip reason: {reason}"
            ),
            other => panic!("expected Skipped, got {other:?}"),
        }
        assert_eq!(
            target
                .select_row::<i64>("SELECT count(*) FROM changed")
                .unwrap()()
            .unwrap(),
            Some(0),
            "the empty database was migrated with the current code"
        );
        assert!(
            target
                .select_row::<i64>("SELECT count(*) FROM original")
                .is_err(),
            "nothing from the skipped image reached the database"
        );

        // `build` (which discards the outcome) must not panic either.
        pollster::block_on(
            ThreadSafeConnection::builder::<ChangedDomain>(
                "build_with_outcome_skips_unmigratable_image_target_2",
                false,
            )
            .with_write_queue_constructor(locking_queue())
            .with_restore_image(image)
            .build(),
        )
        .unwrap();
    }

    /// An image whose `migrations` is a view (the hook a hostile image would use to run
    /// its own SQL during the restore dry-run) is skipped at validation, before any of
    /// its SQL runs.
    #[test]
    fn build_with_outcome_skips_image_with_view_backed_migrations() {
        enum ViewDomain {}
        impl Domain for ViewDomain {
            const NAME: &str = "build_with_outcome_skips_image_with_view_backed_migrations";
            const MIGRATIONS: &[&str] = &["CREATE TABLE rows(value INTEGER) STRICT;"];
        }

        let source = crate::connection::Connection::open_memory(None);
        source
            .exec(
                "CREATE VIEW migrations AS WITH RECURSIVE c(x) AS \
                 (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT 'D', x, 'm' FROM c",
            )
            .unwrap()()
        .unwrap();
        let image = source.serialize_main().unwrap();

        let started = std::time::Instant::now();
        let (target, outcome) = pollster::block_on(
            ThreadSafeConnection::builder::<ViewDomain>(
                "build_with_outcome_skips_image_with_view_backed_migrations_target",
                false,
            )
            .with_write_queue_constructor(locking_queue())
            .with_restore_image(image)
            .build_with_outcome(),
        )
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "validation took {:?}",
            started.elapsed()
        );
        match outcome {
            RestoreOutcome::Skipped(reason) => assert!(
                reason.contains("other than tables and indexes"),
                "unexpected skip reason: {reason}"
            ),
            other => panic!("expected Skipped, got {other:?}"),
        }
        assert_eq!(
            target
                .select_row::<i64>("SELECT count(*) FROM rows")
                .unwrap()()
            .unwrap(),
            Some(0),
            "the empty database was migrated with the current code"
        );
    }

    #[test]
    fn many_initialize_and_migrate_queries_at_once() {
        let mut handles = vec![];

        enum TestDomain {}
        impl Domain for TestDomain {
            const NAME: &str = "test";
            const MIGRATIONS: &[&str] = &["CREATE TABLE test(col1 TEXT, col2 TEXT) STRICT;"];
        }

        for _ in 0..100 {
            handles.push(thread::spawn(|| {
                let builder =
                    ThreadSafeConnection::builder::<TestDomain>("annoying-test.db", false)
                        .with_db_initialization_query("PRAGMA journal_mode=WAL")
                        .with_connection_initialize_query(indoc! {"
                                PRAGMA synchronous=NORMAL;
                                PRAGMA busy_timeout=1;
                                PRAGMA foreign_keys=TRUE;
                                PRAGMA case_sensitive_like=TRUE;
                            "});

                let _ = pollster::block_on(builder.build()).unwrap().deref();
            }));
        }

        for handle in handles {
            let _ = handle.join();
        }
    }

    #[test]
    fn connection_initialize_query_retries_transient_schema_lock() {
        let name = "connection_initialize_query_retries_transient_schema_lock";
        let locking_connection = crate::connection::Connection::open_memory(Some(name));
        locking_connection.exec("BEGIN IMMEDIATE").unwrap()().unwrap();
        locking_connection
            .exec("CREATE TABLE test(col TEXT)")
            .unwrap()()
        .unwrap();

        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            locking_connection.exec("ROLLBACK").unwrap()().unwrap();
        });

        ThreadSafeConnection::create_connection(false, name, Some("PRAGMA FOREIGN_KEYS=true"));
        releaser.join().unwrap();
    }
}
