use std::{
    ffi::{CStr, CString, c_int, c_void},
    marker::PhantomData,
    path::Path,
    ptr,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::Result;
use libsqlite3_sys::*;

/// Largest database image `open_scratch_from_image` accepts, in bytes: the limit the
/// remote server puts on a stored client-state image and `db::client_state` puts on a
/// save. Re-checked here because the bytes arrive from the sandbox's storage, which the
/// code that saved them does not control.
pub const MAX_IMAGE_BYTES: usize = 12 * 1024 * 1024;

/// Virtual-machine steps a scratch connection may spend validating an image and
/// dry-running its migrations before SQLite interrupts the statement: generous for a real
/// migration over a `MAX_IMAGE_BYTES` database (on the order of a hundred million row
/// visits), finite for a schema built to run forever.
const SCRATCH_STEP_BUDGET: u64 = 200_000_000;

/// Virtual-machine steps between two `sqlite3_progress_handler` callbacks.
const PROGRESS_STRIDE: c_int = 10_000;

pub struct Connection {
    pub(crate) sqlite3: *mut sqlite3,
    persistent: bool,
    /// Whether write statements may be prepared right now. Only ever `true` while the
    /// write queue's closure runs (`with_write`); an atomic rather than a `RefCell` so
    /// the single shared connection of the browser build is `Sync`.
    pub(crate) write: AtomicBool,
    /// Progress-handler callbacks left before statements on this connection are
    /// interrupted; `Some` only after `limit_steps`. Boxed so the pointer handed to SQLite
    /// stays valid while the `Connection` moves, and declared after `sqlite3` so it is
    /// freed after `Drop` has closed the handle.
    step_budget: Option<Box<AtomicU64>>,
    _sqlite: PhantomData<sqlite3>,
}
unsafe impl Send for Connection {}
// SAFETY (wasm): SQLite is built with SQLITE_THREADSAFE=0 there, and every FFI call in
// this crate happens under `crate::wasm_lock`, so one connection may be shared between
// the main thread and workers. `write` is only observed under the same lock: it is
// `true` only while the write queue's closure runs (which holds the lock for the whole
// closure), and `Statement::prepare` takes the lock before reading it.
#[cfg(target_family = "wasm")]
unsafe impl Sync for Connection {}

impl Connection {
    fn open_with_flags(uri: &str, persistent: bool, flags: i32) -> Result<Self> {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        let mut connection = Self {
            sqlite3: ptr::null_mut(),
            persistent,
            write: AtomicBool::new(true),
            step_budget: None,
            _sqlite: PhantomData,
        };

        unsafe {
            sqlite3_open_v2(
                CString::new(uri)?.as_ptr(),
                &mut connection.sqlite3,
                flags,
                ptr::null(),
            );

            // Turn on extended error codes
            sqlite3_extended_result_codes(connection.sqlite3, 1);

            connection.last_error()?;
        }

        Ok(connection)
    }

    pub(crate) fn open(uri: &str, persistent: bool) -> Result<Self> {
        Self::open_with_flags(
            uri,
            persistent,
            SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE,
        )
    }

    /// Attempts to open the database at uri. If it fails, a shared memory db will be opened
    /// instead.
    pub fn open_file(uri: &str) -> Self {
        Self::open(uri, true).unwrap_or_else(|_| Self::open_memory(Some(uri)))
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn open_memory(uri: Option<&str>) -> Self {
        if let Some(uri) = uri {
            let in_memory_path = format!("file:{}?mode=memory&cache=shared", uri);
            return Self::open_with_flags(
                &in_memory_path,
                false,
                SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI,
            )
            .expect("Could not create fallback in memory db");
        } else {
            Self::open(":memory:", false).expect("Could not create fallback in memory db")
        }
    }

    /// SQLite is built with SQLITE_OMIT_SHARED_CACHE in the browser and there is one
    /// connection per `ThreadSafeConnection`, so a private `:memory:` database is
    /// equivalent to the shared-cache named database used natively; the URI is ignored.
    #[cfg(target_family = "wasm")]
    pub fn open_memory(_uri: Option<&str>) -> Self {
        Self::open(":memory:", false).expect("Could not create in memory db")
    }

    pub fn persistent(&self) -> bool {
        self.persistent
    }

    pub fn can_write(&self) -> bool {
        self.write.load(Ordering::Acquire)
    }

    pub fn backup_main(&self, destination: &Connection) -> Result<()> {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        unsafe {
            let backup = sqlite3_backup_init(
                destination.sqlite3,
                CString::new("main")?.as_ptr(),
                self.sqlite3,
                CString::new("main")?.as_ptr(),
            );
            sqlite3_backup_step(backup, -1);
            sqlite3_backup_finish(backup);
            destination.last_error()
        }
    }

    pub fn backup_main_to(&self, destination: impl AsRef<Path>) -> Result<()> {
        let destination = Self::open_file(destination.as_ref().to_string_lossy().as_ref());
        self.backup_main(&destination)
    }

    /// Copies the `main` database into a standalone SQLite image (header + pages), the
    /// format `sqlite3_serialize` produces. Pages are read through the pager, so a WAL-mode
    /// database's unflushed frames are included.
    ///
    /// A database without a schema is not an error: `sqlite3_serialize` materializes
    /// page 1 when the page count is still zero, so a fresh connection yields a one-page
    /// image holding only the file header. Should SQLite ever report no pages at all (a
    /// null pointer with size 0, where `last_error` reports `Ok`), the size is checked
    /// explicitly and `Ok(Vec::new())` is returned. A null pointer with a positive size is
    /// an allocation failure and is reported as an error.
    pub fn serialize_main(&self) -> Result<Vec<u8>> {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        let schema = CString::new("main")?;
        let mut size: sqlite3_int64 = 0;
        unsafe {
            let data = sqlite3_serialize(self.sqlite3, schema.as_ptr(), &mut size, 0);
            if data.is_null() {
                if size <= 0 {
                    self.last_error()?;
                    return Ok(Vec::new());
                }
                anyhow::bail!("sqlite3_serialize could not allocate {size} bytes");
            }
            let bytes = std::slice::from_raw_parts(data as *const u8, size as usize).to_vec();
            sqlite3_free(data as *mut c_void);
            Ok(bytes)
        }
    }

    /// Opens a private `:memory:` scratch connection and `sqlite3_deserialize`s `image`
    /// into it. The bytes are copied into a buffer from `sqlite3_malloc64`, handed over
    /// with `FREEONCLOSE | RESIZEABLE`, and header bytes 18-19 (the write/read file-format
    /// versions) are set to 1 on that copy first: images produced from a WAL-mode file
    /// database carry 2 there, and SQLite refuses to use a deserialized WAL database
    /// (`SQLITE_CANTOPEN`).
    ///
    /// The image is untrusted (it is whatever the sandbox handed back), so before touching
    /// SQLite it is rejected when longer than [`MAX_IMAGE_BYTES`], shorter than the 100-byte
    /// header or without the `SQLite format 3\0` magic; the scratch runs with SQLite's
    /// defenses for files of unknown origin (`DEFENSIVE`, `trusted_schema` off, views and
    /// triggers disabled) and a step budget that interrupts runaway statements; and the
    /// deserialized database must pass `PRAGMA quick_check` and carry nothing but tables
    /// and indexes (no views, triggers or virtual tables, which Zed's schema never contains
    /// and which are the vehicles for hostile SQL), so a corrupt or crafted image fails
    /// here rather than on first use.
    pub fn open_scratch_from_image(image: &[u8]) -> Result<Connection> {
        const HEADER_LEN: usize = 100;
        const MAGIC: &[u8; 16] = b"SQLite format 3\0";
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        anyhow::ensure!(
            image.len() <= MAX_IMAGE_BYTES,
            "database image is {} bytes, over the {MAX_IMAGE_BYTES} byte limit",
            image.len()
        );
        anyhow::ensure!(
            image.len() >= HEADER_LEN,
            "database image is too short ({} bytes)",
            image.len()
        );
        anyhow::ensure!(
            &image[..MAGIC.len()] == MAGIC,
            "database image does not start with the SQLite header"
        );

        let mut scratch = Self::open(":memory:", false)?;
        scratch.harden_for_untrusted_schema()?;
        scratch.db_config(SQLITE_DBCONFIG_ENABLE_VIEW, 0)?;
        scratch.db_config(SQLITE_DBCONFIG_ENABLE_TRIGGER, 0)?;
        scratch.limit_steps(SCRATCH_STEP_BUDGET);
        let schema = CString::new("main")?;
        let size = image.len();
        unsafe {
            let buffer = sqlite3_malloc64(size as sqlite3_uint64) as *mut u8;
            anyhow::ensure!(
                !buffer.is_null(),
                "sqlite3_malloc64 could not allocate {size} bytes"
            );
            ptr::copy_nonoverlapping(image.as_ptr(), buffer, size);
            *buffer.add(18) = 1;
            *buffer.add(19) = 1;
            // On failure SQLite frees a FREEONCLOSE buffer itself (sqlite3_deserialize's
            // end_deserialize path), so nothing leaks on either branch.
            let rc = sqlite3_deserialize(
                scratch.sqlite3,
                schema.as_ptr(),
                buffer,
                size as sqlite3_int64,
                size as sqlite3_int64,
                SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE,
            );
            if rc != SQLITE_OK {
                scratch.last_error()?;
                anyhow::bail!("sqlite3_deserialize failed with code {rc}");
            }
        }
        // A deserialized image is only validated lazily; force the header and schema to be
        // read now so a truncated or corrupt image surfaces as an error here.
        scratch.select_row::<i64>("SELECT count(*) FROM sqlite_master")?()
            .map_err(|error| anyhow::anyhow!("database image is unreadable: {error:#}"))?;
        let check = scratch.select_row::<String>("PRAGMA quick_check")?()
            .map_err(|error| anyhow::anyhow!("database image could not be checked: {error:#}"))?;
        anyhow::ensure!(
            check.as_deref() == Some("ok"),
            "database image failed quick_check: {}",
            check.unwrap_or_default()
        );
        let foreign_objects = scratch.select_row::<i64>(
            "SELECT count(*) FROM sqlite_master \
             WHERE type NOT IN ('table', 'index') OR sql LIKE 'CREATE VIRTUAL%'",
        )?()?
        .unwrap_or_default();
        anyhow::ensure!(
            foreign_objects == 0,
            "database image carries {foreign_objects} schema object(s) other than tables and indexes"
        );
        Ok(scratch)
    }

    /// `sqlite3_db_config(op, value)` for the boolean options.
    fn db_config(&self, op: c_int, value: c_int) -> Result<()> {
        let rc = unsafe { sqlite3_db_config(self.sqlite3, op, value, ptr::null_mut::<c_int>()) };
        anyhow::ensure!(
            rc == SQLITE_OK,
            "sqlite3_db_config({op}, {value}) failed with code {rc}"
        );
        Ok(())
    }

    /// SQLite's defenses for a database whose contents are not trusted: `DEFENSIVE` (no
    /// SQL can rewrite the schema or shadow tables) and `trusted_schema` off (functions
    /// named in the schema are limited to the innocuous ones). Zed's own SQL is unaffected.
    fn harden_for_untrusted_schema(&self) -> Result<()> {
        self.db_config(SQLITE_DBCONFIG_DEFENSIVE, 1)?;
        self.db_config(SQLITE_DBCONFIG_TRUSTED_SCHEMA, 0)
    }

    /// Interrupts whatever statement is running on this connection once about `steps`
    /// virtual-machine steps have run in total (the statement fails with
    /// `SQLITE_INTERRUPT`); every later statement fails immediately.
    pub(crate) fn limit_steps(&mut self, steps: u64) {
        let budget = Box::new(AtomicU64::new(steps.div_ceil(PROGRESS_STRIDE as u64)));
        unsafe {
            sqlite3_progress_handler(
                self.sqlite3,
                PROGRESS_STRIDE,
                Some(step_budget_progress_handler),
                &*budget as *const AtomicU64 as *mut c_void,
            );
        }
        self.step_budget = Some(budget);
    }

    /// Replaces the contents of `main` with the contents of `source` through the online
    /// backup API (`source.backup_main(self)` with explicit result checks). `self` should be
    /// empty and not yet in WAL mode: the backup then adopts the source page size. Backing
    /// up into a non-empty WAL database with a different page size fails with
    /// `SQLITE_READONLY` and is not supported here.
    pub fn restore_main_from(&self, source: &Connection) -> Result<()> {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        // The pages about to be copied in came from an untrusted image (already validated
        // on `source`); keep the same defenses on the connection that will serve them.
        self.harden_for_untrusted_schema()?;
        let main = CString::new("main")?;
        unsafe {
            let backup =
                sqlite3_backup_init(self.sqlite3, main.as_ptr(), source.sqlite3, main.as_ptr());
            if backup.is_null() {
                self.last_error()?;
                anyhow::bail!("sqlite3_backup_init failed");
            }
            let step = sqlite3_backup_step(backup, -1);
            let finish = sqlite3_backup_finish(backup);
            if step != SQLITE_DONE {
                self.last_error()?;
                anyhow::bail!("sqlite3_backup_step failed with code {step}");
            }
            if finish != SQLITE_OK {
                self.last_error()?;
                anyhow::bail!("sqlite3_backup_finish failed with code {finish}");
            }
        }
        Ok(())
    }

    pub fn sql_has_syntax_error(&self, sql: &str) -> Option<(String, usize)> {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        let sql = CString::new(sql).unwrap();
        let mut remaining_sql = sql.as_c_str();
        let sql_start = remaining_sql.as_ptr();

        let mut alter_table = None;
        while {
            let remaining_sql_str = remaining_sql.to_str().unwrap().trim();
            let any_remaining_sql = remaining_sql_str != ";" && !remaining_sql_str.is_empty();
            if any_remaining_sql {
                alter_table = parse_alter_table(remaining_sql_str);
            }
            any_remaining_sql
        } {
            let mut raw_statement = ptr::null_mut::<sqlite3_stmt>();
            let mut remaining_sql_ptr = ptr::null();

            let (res, offset, message, _conn) = if let Some((table_to_alter, column)) = alter_table
            {
                // ALTER TABLE is a weird statement. When preparing the statement the table's
                // existence is checked *before* syntax checking any other part of the statement.
                // Therefore, we need to make sure that the table has been created before calling
                // prepare. As we don't want to trash whatever database this is connected to, we
                // create a new in-memory DB to test.

                let temp_connection = Connection::open_memory(None);
                //This should always succeed, if it doesn't then you really should know about it
                temp_connection
                    .exec(&format!("CREATE TABLE {table_to_alter}({column})"))
                    .unwrap()()
                .unwrap();

                unsafe {
                    sqlite3_prepare_v2(
                        temp_connection.sqlite3,
                        remaining_sql.as_ptr(),
                        -1,
                        &mut raw_statement,
                        &mut remaining_sql_ptr,
                    )
                };

                #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
                let offset = unsafe { sqlite3_error_offset(temp_connection.sqlite3) };

                #[cfg(any(target_os = "linux", target_os = "freebsd"))]
                let offset = 0;

                unsafe {
                    (
                        sqlite3_errcode(temp_connection.sqlite3),
                        offset,
                        sqlite3_errmsg(temp_connection.sqlite3),
                        Some(temp_connection),
                    )
                }
            } else {
                unsafe {
                    sqlite3_prepare_v2(
                        self.sqlite3,
                        remaining_sql.as_ptr(),
                        -1,
                        &mut raw_statement,
                        &mut remaining_sql_ptr,
                    )
                };

                #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
                let offset = unsafe { sqlite3_error_offset(self.sqlite3) };

                #[cfg(any(target_os = "linux", target_os = "freebsd"))]
                let offset = 0;

                unsafe {
                    (
                        sqlite3_errcode(self.sqlite3),
                        offset,
                        sqlite3_errmsg(self.sqlite3),
                        None,
                    )
                }
            };

            unsafe { sqlite3_finalize(raw_statement) };

            if res == 1 && offset >= 0 {
                let sub_statement_correction = remaining_sql.as_ptr() as usize - sql_start as usize;
                let err_msg = String::from_utf8_lossy(unsafe {
                    CStr::from_ptr(message as *const _).to_bytes()
                })
                .into_owned();

                return Some((err_msg, offset as usize + sub_statement_correction));
            }
            remaining_sql = unsafe { CStr::from_ptr(remaining_sql_ptr) };
            alter_table = None;
        }
        None
    }

    pub(crate) fn last_error(&self) -> Result<()> {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        unsafe {
            let code = sqlite3_errcode(self.sqlite3);
            const NON_ERROR_CODES: &[i32] = &[SQLITE_OK, SQLITE_ROW];
            if NON_ERROR_CODES.contains(&code) {
                return Ok(());
            }

            let message = sqlite3_errmsg(self.sqlite3);
            let message = if message.is_null() {
                None
            } else {
                Some(
                    String::from_utf8_lossy(CStr::from_ptr(message as *const _).to_bytes())
                        .into_owned(),
                )
            };

            anyhow::bail!("Sqlite call failed with code {code} and message: {message:?}")
        }
    }

    pub(crate) fn with_write<T>(&self, callback: impl FnOnce(&Connection) -> T) -> T {
        #[cfg(target_family = "wasm")]
        debug_assert!(
            crate::wasm_lock::is_held_by_current_thread(),
            "with_write outside wasm_lock"
        );
        // Restore rather than clear: a `write` issued from inside another write's closure
        // runs inline on the wasm queue, and the outer closure's remaining statements must
        // still be accepted after the inner one returns.
        let was_writable = self.write.swap(true, Ordering::AcqRel);
        let result = callback(self);
        self.write.store(was_writable, Ordering::Release);
        result
    }
}

/// `sqlite3_progress_handler` callback of `Connection::limit_steps`: counts down the
/// remaining callbacks and asks SQLite to interrupt once they are spent.
unsafe extern "C" fn step_budget_progress_handler(budget: *mut c_void) -> c_int {
    // SAFETY: `budget` is the `Box<AtomicU64>` installed by `limit_steps`, which the
    // `Connection` keeps alive until after its SQLite handle is closed.
    let budget = unsafe { &*(budget as *const AtomicU64) };
    let remaining = budget.load(Ordering::Relaxed);
    if remaining == 0 {
        return 1;
    }
    budget.store(remaining - 1, Ordering::Relaxed);
    0
}

fn parse_alter_table(remaining_sql_str: &str) -> Option<(String, String)> {
    let remaining_sql_str = remaining_sql_str.to_lowercase();
    if remaining_sql_str.starts_with("alter")
        && let Some(table_offset) = remaining_sql_str.find("table")
    {
        let after_table_offset = table_offset + "table".len();
        let table_to_alter = remaining_sql_str
            .chars()
            .skip(after_table_offset)
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| !c.is_whitespace())
            .collect::<String>();
        if !table_to_alter.is_empty() {
            let column_name = if let Some(rename_offset) = remaining_sql_str.find("rename column") {
                let after_rename_offset = rename_offset + "rename column".len();
                remaining_sql_str
                    .chars()
                    .skip(after_rename_offset)
                    .skip_while(|c| c.is_whitespace())
                    .take_while(|c| !c.is_whitespace())
                    .collect::<String>()
            } else if let Some(drop_offset) = remaining_sql_str.find("drop column") {
                let after_drop_offset = drop_offset + "drop column".len();
                remaining_sql_str
                    .chars()
                    .skip(after_drop_offset)
                    .skip_while(|c| c.is_whitespace())
                    .take_while(|c| !c.is_whitespace())
                    .collect::<String>()
            } else {
                "__place_holder_column_for_syntax_checking".to_string()
            };
            return Some((table_to_alter, column_name));
        }
    }
    None
}

impl Drop for Connection {
    fn drop(&mut self) {
        #[cfg(target_family = "wasm")]
        let _guard = crate::wasm_lock::lock();
        unsafe { sqlite3_close(self.sqlite3) };
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use indoc::indoc;
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::connection::{Connection, MAX_IMAGE_BYTES};

    static NEXT_NAMED_MEMORY_DB_ID: AtomicUsize = AtomicUsize::new(0);

    fn image_with(statements: &[&str]) -> Vec<u8> {
        let source = Connection::open_memory(None);
        for statement in statements {
            source.exec(statement).unwrap()().unwrap();
        }
        source.serialize_main().unwrap()
    }

    /// The error `open_scratch_from_image` rejects `image` with (`Connection` is not
    /// `Debug`, so `unwrap_err` cannot be used).
    fn scratch_error(image: &[u8]) -> anyhow::Error {
        match Connection::open_scratch_from_image(image) {
            Ok(_) => panic!("the image was accepted"),
            Err(error) => error,
        }
    }

    #[test]
    fn open_scratch_from_image_accepts_tables_and_indexes() {
        let image = image_with(&[
            "CREATE TABLE rows(id INTEGER PRIMARY KEY, value TEXT)",
            "CREATE INDEX rows_value ON rows(value)",
            "INSERT INTO rows(value) VALUES ('a'), ('b')",
        ]);
        let scratch = Connection::open_scratch_from_image(&image).unwrap();
        assert_eq!(
            scratch
                .select_row::<i64>("SELECT count(*) FROM rows")
                .unwrap()()
            .unwrap(),
            Some(2)
        );
    }

    #[test]
    fn open_scratch_from_image_rejects_views_triggers_and_oversized_images() {
        let view = image_with(&[
            "CREATE TABLE rows(value TEXT)",
            "CREATE VIEW migrations AS WITH RECURSIVE c(x) AS \
             (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT 'D', x, 'm' FROM c",
        ]);
        let error = format!("{:#}", scratch_error(&view));
        assert!(
            error.contains("other than tables and indexes"),
            "unexpected error: {error}"
        );

        let trigger = image_with(&[
            "CREATE TABLE rows(value TEXT)",
            "CREATE TRIGGER rows_insert AFTER INSERT ON rows BEGIN \
             INSERT INTO rows(value) VALUES ('again'); END",
        ]);
        let error = format!("{:#}", scratch_error(&trigger));
        assert!(
            error.contains("other than tables and indexes"),
            "unexpected error: {error}"
        );

        let mut oversized = image_with(&["CREATE TABLE rows(value TEXT)"]);
        oversized.resize(MAX_IMAGE_BYTES + 1, 0);
        let error = format!("{:#}", scratch_error(&oversized));
        assert!(error.contains("over the"), "unexpected error: {error}");
    }

    #[test]
    fn open_scratch_from_image_rejects_corrupt_pages() {
        let mut image = image_with(&[
            "CREATE TABLE rows(value TEXT)",
            "INSERT INTO rows(value) VALUES ('a')",
        ]);
        let page_size = u16::from_be_bytes([image[16], image[17]]) as usize;
        assert!(
            image.len() >= 2 * page_size,
            "the table needs a page of its own"
        );
        // Page 2's first byte is its b-tree page type; 0xff is not one.
        image[page_size] = 0xff;
        // Either `quick_check` reports the page or reading it already fails (SQLITE_CORRUPT
        // out of the check itself); both reject the image before anything runs on it.
        let error = format!("{:#}", scratch_error(&image));
        assert!(
            error.contains("could not be checked")
                || error.contains("quick_check")
                || error.contains("unreadable"),
            "unexpected error: {error}"
        );
        assert!(error.contains("malformed") || error.contains("quick_check"));
    }

    #[test]
    fn limited_scratch_interrupts_runaway_statements() {
        let image = image_with(&["CREATE TABLE rows(value TEXT)"]);
        let mut scratch = Connection::open_scratch_from_image(&image).unwrap();
        scratch.limit_steps(100_000);
        let error = scratch
            .select_row::<i64>(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) \
                 SELECT count(*) FROM c",
            )
            .unwrap()()
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("interrupted"), "unexpected error: {error}");
    }

    fn unique_named_memory_db(prefix: &str) -> String {
        format!(
            "{prefix}_{}_{}",
            std::process::id(),
            NEXT_NAMED_MEMORY_DB_ID.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn literal_named_memory_paths(name: &str) -> [String; 3] {
        let main = format!("file:{name}?mode=memory&cache=shared");
        [main.clone(), format!("{main}-wal"), format!("{main}-shm")]
    }

    struct NamedMemoryPathGuard {
        paths: [String; 3],
    }

    impl NamedMemoryPathGuard {
        fn new(name: &str) -> Self {
            let paths = literal_named_memory_paths(name);
            for path in &paths {
                let _ = fs::remove_file(path);
            }
            Self { paths }
        }
    }

    impl Drop for NamedMemoryPathGuard {
        fn drop(&mut self) {
            for path in &self.paths {
                let _ = fs::remove_file(path);
            }
        }
    }

    #[test]
    fn string_round_trips() -> Result<()> {
        let connection = Connection::open_memory(Some("string_round_trips"));
        connection
            .exec(indoc! {"
            CREATE TABLE text (
                text TEXT
            );"})
            .unwrap()()
        .unwrap();

        let text = "Some test text";

        connection
            .exec_bound("INSERT INTO text (text) VALUES (?);")
            .unwrap()(text)
        .unwrap();

        assert_eq!(
            connection.select_row("SELECT text FROM text;").unwrap()().unwrap(),
            Some(text.to_string())
        );

        Ok(())
    }

    #[test]
    fn tuple_round_trips() {
        let connection = Connection::open_memory(Some("tuple_round_trips"));
        connection
            .exec(indoc! {"
                CREATE TABLE test (
                    text TEXT,
                    integer INTEGER,
                    blob BLOB
                );"})
            .unwrap()()
        .unwrap();

        let tuple1 = ("test".to_string(), 64, vec![0, 1, 2, 4, 8, 16, 32, 64]);
        let tuple2 = ("test2".to_string(), 32, vec![64, 32, 16, 8, 4, 2, 1, 0]);

        let mut insert = connection
            .exec_bound::<(String, usize, Vec<u8>)>(
                "INSERT INTO test (text, integer, blob) VALUES (?, ?, ?)",
            )
            .unwrap();

        insert(tuple1.clone()).unwrap();
        insert(tuple2.clone()).unwrap();

        assert_eq!(
            connection
                .select::<(String, usize, Vec<u8>)>("SELECT * FROM test")
                .unwrap()()
            .unwrap(),
            vec![tuple1, tuple2]
        );
    }

    #[test]
    fn bool_round_trips() {
        let connection = Connection::open_memory(Some("bool_round_trips"));
        connection
            .exec(indoc! {"
                CREATE TABLE bools (
                    t INTEGER,
                    f INTEGER
                );"})
            .unwrap()()
        .unwrap();

        connection
            .exec_bound("INSERT INTO bools(t, f) VALUES (?, ?)")
            .unwrap()((true, false))
        .unwrap();

        assert_eq!(
            connection
                .select_row::<(bool, bool)>("SELECT * FROM bools;")
                .unwrap()()
            .unwrap(),
            Some((true, false))
        );
    }

    #[test]
    fn backup_works() {
        let connection1 = Connection::open_memory(Some("backup_works"));
        connection1
            .exec(indoc! {"
                CREATE TABLE blobs (
                    data BLOB
                );"})
            .unwrap()()
        .unwrap();
        let blob = vec![0, 1, 2, 4, 8, 16, 32, 64];
        connection1
            .exec_bound::<Vec<u8>>("INSERT INTO blobs (data) VALUES (?);")
            .unwrap()(blob.clone())
        .unwrap();

        // Backup connection1 to connection2
        let connection2 = Connection::open_memory(Some("backup_works_other"));
        connection1.backup_main(&connection2).unwrap();

        // Delete the added blob and verify its deleted on the other side
        let read_blobs = connection1
            .select::<Vec<u8>>("SELECT * FROM blobs;")
            .unwrap()()
        .unwrap();
        assert_eq!(read_blobs, vec![blob]);
    }

    #[test]
    fn named_memory_connections_do_not_create_literal_backing_files() {
        let name = unique_named_memory_db("named_memory_connections_do_not_create_backing_files");
        let guard = NamedMemoryPathGuard::new(&name);

        let connection1 = Connection::open_memory(Some(&name));
        connection1
            .exec(indoc! {"
                CREATE TABLE shared (
                    value INTEGER
                )"})
            .unwrap()()
        .unwrap();
        connection1
            .exec("INSERT INTO shared (value) VALUES (7)")
            .unwrap()()
        .unwrap();

        let connection2 = Connection::open_memory(Some(&name));
        assert_eq!(
            connection2
                .select_row::<i64>("SELECT value FROM shared")
                .unwrap()()
            .unwrap(),
            Some(7)
        );

        for path in &guard.paths {
            assert!(
                fs::metadata(path).is_err(),
                "named in-memory database unexpectedly created backing file {path}"
            );
        }
    }

    #[test]
    fn serialize_then_restore_preserves_rows() {
        let source = Connection::open_memory(Some("serialize_then_restore_preserves_rows"));
        source
            .exec(indoc! {"
                CREATE TABLE rows (
                    id INTEGER,
                    name TEXT
                );"})
            .unwrap()()
        .unwrap();
        let mut insert = source
            .exec_bound::<(i64, String)>("INSERT INTO rows (id, name) VALUES (?, ?)")
            .unwrap();
        insert((1, "one".to_string())).unwrap();
        insert((2, "two".to_string())).unwrap();

        let image = source.serialize_main().unwrap();
        assert!(image.len() >= 100);
        assert_eq!(&image[..16], b"SQLite format 3\0");

        let scratch = Connection::open_scratch_from_image(&image).unwrap();
        let restored = Connection::open_memory(Some("serialize_then_restore_preserves_rows_2"));
        restored.restore_main_from(&scratch).unwrap();
        drop(scratch);

        assert_eq!(
            restored
                .select::<(i64, String)>("SELECT id, name FROM rows ORDER BY id")
                .unwrap()()
            .unwrap(),
            vec![(1, "one".to_string()), (2, "two".to_string())]
        );
    }

    #[test]
    fn serialize_empty_database_is_one_page() {
        // A database without a schema is not an error. `sqlite3_serialize` materializes
        // page 1 (`BEGIN IMMEDIATE; COMMIT`) when the page count is still zero, so the image
        // of a fresh connection is exactly one page carrying the file header, and it
        // restores as an empty database.
        let connection = Connection::open_memory(None);
        let page_size = connection.select_row::<i64>("PRAGMA page_size").unwrap()()
            .unwrap()
            .unwrap() as usize;
        let image = connection.serialize_main().unwrap();
        assert!(image.starts_with(b"SQLite format 3\0"));
        assert_eq!(image.len(), page_size, "one page: the header");

        let scratch = Connection::open_scratch_from_image(&image).unwrap();
        let tables = scratch
            .select::<String>("SELECT name FROM sqlite_master")
            .unwrap()()
        .unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn restore_from_wal_image() {
        let dir = std::env::temp_dir().join(format!(
            "sqlez_restore_from_wal_image_{}_{}",
            std::process::id(),
            NEXT_NAMED_MEMORY_DB_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("db.sqlite");
        {
            let file_db = Connection::open(path.to_str().unwrap(), true).unwrap();
            let mode = file_db
                .select_row::<String>("PRAGMA journal_mode=WAL")
                .unwrap()()
            .unwrap();
            assert_eq!(mode.as_deref(), Some("wal"));
            file_db
                .exec("CREATE TABLE wal_rows (value INTEGER)")
                .unwrap()()
            .unwrap();
            file_db
                .exec("INSERT INTO wal_rows (value) VALUES (7)")
                .unwrap()()
            .unwrap();

            let image = file_db.serialize_main().unwrap();
            assert_eq!(
                (image[18], image[19]),
                (2, 2),
                "a WAL-mode database serializes with file-format version 2"
            );

            let scratch = Connection::open_scratch_from_image(&image).unwrap();
            let restored = Connection::open_memory(None);
            restored.restore_main_from(&scratch).unwrap();
            assert_eq!(
                restored
                    .select_row::<i64>("SELECT value FROM wal_rows")
                    .unwrap()()
                .unwrap(),
                Some(7)
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_rejects_garbage() {
        assert!(Connection::open_scratch_from_image(b"nope").is_err());
        let mut fake_header = vec![0u8; 200];
        fake_header[..16].copy_from_slice(b"SQLite format 3\0");
        assert!(
            Connection::open_scratch_from_image(&fake_header).is_err(),
            "a header without a valid page must not open"
        );
    }

    #[test]
    fn multi_step_statement_works() {
        let connection = Connection::open_memory(Some("multi_step_statement_works"));

        connection
            .exec(indoc! {"
                CREATE TABLE test (
                    col INTEGER
                )"})
            .unwrap()()
        .unwrap();

        connection
            .exec(indoc! {"
            INSERT INTO test(col) VALUES (2)"})
            .unwrap()()
        .unwrap();

        assert_eq!(
            connection
                .select_row::<usize>("SELECT * FROM test")
                .unwrap()()
            .unwrap(),
            Some(2)
        );
    }

    #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
    #[test]
    fn test_sql_has_syntax_errors() {
        let connection = Connection::open_memory(Some("test_sql_has_syntax_errors"));
        let first_stmt =
            "CREATE TABLE kv_store(key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT ;";
        let second_stmt = "SELECT FROM";

        let second_offset = connection.sql_has_syntax_error(second_stmt).unwrap().1;

        let res = connection
            .sql_has_syntax_error(&format!("{}\n{}", first_stmt, second_stmt))
            .map(|(_, offset)| offset);

        assert_eq!(res, Some(first_stmt.len() + second_offset + 1));
    }

    #[test]
    fn test_alter_table_syntax() {
        let connection = Connection::open_memory(Some("test_alter_table_syntax"));

        assert!(
            connection
                .sql_has_syntax_error("ALTER TABLE test ADD x TEXT")
                .is_none()
        );

        assert!(
            connection
                .sql_has_syntax_error("ALTER TABLE test AAD x TEXT")
                .is_some()
        );
    }
}
