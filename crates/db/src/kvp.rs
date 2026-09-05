use anyhow::Context as _;
use gpui::App;
use sqlez_macros::sql;
use util::ResultExt as _;

use crate::{
    query,
    sqlez::{domain::Domain, thread_safe_connection::ThreadSafeConnection},
    write_and_log,
};

/// The `scoped_kv_store` namespace reserved for the folded [`GlobalKeyValueStore`]
/// (`GlobalKeyValueStore::from_app_db`). `KeyValueStore::scoped` refuses it (debug
/// assertion) so no app-side code aliases the global rows by accident. In the browser those
/// rows arrive inside the client-state image, which the sandbox stores and can rewrite, so
/// a consumer of a global key (the rules-to-skills migration marker, for one) treats the
/// value as a hint it can recover from, never as something only Zed could have written.
pub const GLOBAL_KVP_NAMESPACE: &str = "global";

pub struct KeyValueStore(crate::sqlez::thread_safe_connection::ThreadSafeConnection);

impl KeyValueStore {
    pub fn from_app_db(db: &crate::AppDatabase) -> Self {
        Self(db.0.clone())
    }
}

impl Domain for KeyValueStore {
    const NAME: &str = stringify!(KeyValueStore);

    const MIGRATIONS: &[&str] = &[
        sql!(
            CREATE TABLE IF NOT EXISTS kv_store(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;
        ),
        sql!(
            CREATE TABLE IF NOT EXISTS scoped_kv_store(
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY(namespace, key)
            ) STRICT;
        ),
    ];
}

crate::static_connection!(KeyValueStore, []);

pub trait Dismissable {
    const KEY: &'static str;

    fn dismissed(cx: &App) -> bool {
        KeyValueStore::global(cx)
            .read_kvp(Self::KEY)
            .log_err()
            .is_some_and(|s| s.is_some())
    }

    fn set_dismissed(is_dismissed: bool, cx: &mut App) {
        let db = KeyValueStore::global(cx);
        write_and_log(cx, move || async move {
            if is_dismissed {
                db.write_kvp(Self::KEY.into(), "1".into()).await
            } else {
                db.delete_kvp(Self::KEY.into()).await
            }
        })
    }
}

impl KeyValueStore {
    query! {
        pub fn read_kvp(key: &str) -> Result<Option<String>> {
            SELECT value FROM kv_store WHERE key = (?)
        }
    }

    pub async fn write_kvp(&self, key: String, value: String) -> anyhow::Result<()> {
        log::debug!("Writing key-value pair for key {key}");
        self.write_kvp_inner(key, value).await
    }

    query! {
        async fn write_kvp_inner(key: String, value: String) -> Result<()> {
            INSERT OR REPLACE INTO kv_store(key, value) VALUES ((?), (?))
        }
    }

    query! {
        pub async fn delete_kvp(key: String) -> Result<()> {
            DELETE FROM kv_store WHERE key = (?)
        }
    }

    pub fn scoped<'a>(&'a self, namespace: &'a str) -> ScopedKeyValueStore<'a> {
        debug_assert_ne!(
            namespace, GLOBAL_KVP_NAMESPACE,
            "the {GLOBAL_KVP_NAMESPACE:?} namespace is reserved for GlobalKeyValueStore"
        );
        ScopedKeyValueStore {
            store: self,
            namespace,
        }
    }
}

/// A namespaced view of `scoped_kv_store`. The namespace [`GLOBAL_KVP_NAMESPACE`] is
/// reserved for the folded global store (`KeyValueStore::scoped` refuses it) and travels
/// with the application database image.
pub struct ScopedKeyValueStore<'a> {
    store: &'a KeyValueStore,
    namespace: &'a str,
}

impl ScopedKeyValueStore<'_> {
    pub fn read(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.store.select_row_bound::<(&str, &str), String>(
            "SELECT value FROM scoped_kv_store WHERE namespace = (?) AND key = (?)",
        )?((self.namespace, key))
        .context("Failed to read from scoped_kv_store")
    }

    pub async fn write(&self, key: String, value: String) -> anyhow::Result<()> {
        let namespace = self.namespace.to_owned();
        self.store
            .write(move |connection| {
                connection.exec_bound::<(&str, &str, &str)>(
                    "INSERT OR REPLACE INTO scoped_kv_store(namespace, key, value) VALUES ((?), (?), (?))",
                )?((&namespace, &key, &value))
                .context("Failed to write to scoped_kv_store")
            })
            .await
    }

    pub async fn delete(&self, key: String) -> anyhow::Result<()> {
        let namespace = self.namespace.to_owned();
        self.store
            .write(move |connection| {
                connection.exec_bound::<(&str, &str)>(
                    "DELETE FROM scoped_kv_store WHERE namespace = (?) AND key = (?)",
                )?((&namespace, &key))
                .context("Failed to delete from scoped_kv_store")
            })
            .await
    }

    pub async fn delete_all(&self) -> anyhow::Result<()> {
        let namespace = self.namespace.to_owned();
        self.store
            .write(move |connection| {
                connection
                    .exec_bound::<&str>("DELETE FROM scoped_kv_store WHERE namespace = (?)")?(
                    &namespace,
                )
                .context("Failed to delete_all from scoped_kv_store")
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::kvp::KeyValueStore;

    #[gpui::test]
    async fn test_kvp() {
        let db = KeyValueStore::open_test_db("test_kvp").await;

        assert_eq!(db.read_kvp("key-1").unwrap(), None);

        db.write_kvp("key-1".to_string(), "one".to_string())
            .await
            .unwrap();
        assert_eq!(db.read_kvp("key-1").unwrap(), Some("one".to_string()));

        db.write_kvp("key-1".to_string(), "one-2".to_string())
            .await
            .unwrap();
        assert_eq!(db.read_kvp("key-1").unwrap(), Some("one-2".to_string()));

        db.write_kvp("key-2".to_string(), "two".to_string())
            .await
            .unwrap();
        assert_eq!(db.read_kvp("key-2").unwrap(), Some("two".to_string()));

        db.delete_kvp("key-1".to_string()).await.unwrap();
        assert_eq!(db.read_kvp("key-1").unwrap(), None);
    }

    #[gpui::test]
    async fn test_scoped_kvp() {
        let db = KeyValueStore::open_test_db("test_scoped_kvp").await;

        let scope_a = db.scoped("namespace-a");
        let scope_b = db.scoped("namespace-b");

        // Reading a missing key returns None
        assert_eq!(scope_a.read("key-1").unwrap(), None);

        // Writing and reading back a key works
        scope_a
            .write("key-1".to_string(), "value-a1".to_string())
            .await
            .unwrap();
        assert_eq!(scope_a.read("key-1").unwrap(), Some("value-a1".to_string()));

        // Two namespaces with the same key don't collide
        scope_b
            .write("key-1".to_string(), "value-b1".to_string())
            .await
            .unwrap();
        assert_eq!(scope_a.read("key-1").unwrap(), Some("value-a1".to_string()));
        assert_eq!(scope_b.read("key-1").unwrap(), Some("value-b1".to_string()));

        // delete removes a single key without affecting others in the namespace
        scope_a
            .write("key-2".to_string(), "value-a2".to_string())
            .await
            .unwrap();
        scope_a.delete("key-1".to_string()).await.unwrap();
        assert_eq!(scope_a.read("key-1").unwrap(), None);
        assert_eq!(scope_a.read("key-2").unwrap(), Some("value-a2".to_string()));
        assert_eq!(scope_b.read("key-1").unwrap(), Some("value-b1".to_string()));

        // delete_all removes all keys in a namespace without affecting other namespaces
        scope_a
            .write("key-3".to_string(), "value-a3".to_string())
            .await
            .unwrap();
        scope_a.delete_all().await.unwrap();
        assert_eq!(scope_a.read("key-2").unwrap(), None);
        assert_eq!(scope_a.read("key-3").unwrap(), None);
        assert_eq!(scope_b.read("key-1").unwrap(), Some("value-b1".to_string()));
    }
}

/// Where a [`GlobalKeyValueStore`]'s rows live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backing {
    /// Its own database file (`database_dir()/0-global/db.sqlite`), shared by every release
    /// channel; the native default.
    #[cfg_attr(target_family = "wasm", allow(dead_code))]
    OwnFile,
    /// The application database's `scoped_kv_store` under [`GLOBAL_KVP_NAMESPACE`], so the
    /// rows travel inside the client-state image (D7).
    AppDb,
}

/// A key-value store whose rows are shared by every release channel. Natively it is a
/// separate database file; [`GlobalKeyValueStore::from_app_db`] folds it into the
/// application database instead (the only form on wasm).
pub struct GlobalKeyValueStore {
    connection: ThreadSafeConnection,
    backing: Backing,
}

impl Domain for GlobalKeyValueStore {
    const NAME: &str = stringify!(GlobalKeyValueStore);
    const MIGRATIONS: &[&str] = &[sql!(
        CREATE TABLE IF NOT EXISTS kv_store(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) STRICT;
    )];
}

impl std::ops::Deref for GlobalKeyValueStore {
    type Target = ThreadSafeConnection;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

#[cfg(not(target_family = "wasm"))]
static GLOBAL_KEY_VALUE_STORE: std::sync::LazyLock<GlobalKeyValueStore> =
    std::sync::LazyLock::new(|| {
        let db_dir = crate::database_dir();
        GlobalKeyValueStore {
            connection: gpui::block_on(crate::open_db::<GlobalKeyValueStore>(
                db_dir,
                crate::GlobalDbScope,
            )),
            backing: Backing::OwnFile,
        }
    });

#[cfg(target_family = "wasm")]
static GLOBAL_KEY_VALUE_STORE: std::sync::OnceLock<GlobalKeyValueStore> =
    std::sync::OnceLock::new();

impl GlobalKeyValueStore {
    /// The process-wide store backed by its own database file (native).
    #[cfg(not(target_family = "wasm"))]
    pub fn global() -> &'static Self {
        &GLOBAL_KEY_VALUE_STORE
    }

    /// The process-wide store installed by [`GlobalKeyValueStore::init`] (wasm).
    #[cfg(target_family = "wasm")]
    pub fn global() -> &'static Self {
        GLOBAL_KEY_VALUE_STORE
            .get()
            .expect("GlobalKeyValueStore::init must run before global()")
    }

    /// wasm: installs the process-wide instance that `global()` returns, backed by the
    /// application database. Called once by the entry crate right after
    /// `AppDatabase::open_with_image`; idempotent.
    #[cfg(target_family = "wasm")]
    pub fn init(db: &crate::AppDatabase) {
        GLOBAL_KEY_VALUE_STORE.set(Self::from_app_db(db)).ok();
    }

    /// A global store backed by the application database's `scoped_kv_store` under
    /// [`GLOBAL_KVP_NAMESPACE`] (no new table or migration), so its rows are part of the
    /// client-state image. Available on every target; the only constructor on wasm.
    pub fn from_app_db(db: &crate::AppDatabase) -> Self {
        Self {
            connection: db.0.clone(),
            backing: Backing::AppDb,
        }
    }

    pub fn read_kvp(&self, key: &str) -> anyhow::Result<Option<String>> {
        match self.backing {
            Backing::OwnFile => self.read_kvp_own_file(key),
            Backing::AppDb => self.select_row_bound::<(&str, &str), String>(
                "SELECT value FROM scoped_kv_store WHERE namespace = (?) AND key = (?)",
            )?((GLOBAL_KVP_NAMESPACE, key))
            .context("Failed to read global key from scoped_kv_store"),
        }
    }

    query! {
        fn read_kvp_own_file(key: &str) -> Result<Option<String>> {
            SELECT value FROM kv_store WHERE key = (?)
        }
    }

    pub async fn write_kvp(&self, key: String, value: String) -> anyhow::Result<()> {
        log::debug!("Writing global key-value pair for key {key}");
        match self.backing {
            Backing::OwnFile => self.write_kvp_own_file(key, value).await,
            Backing::AppDb => {
                self.connection
                    .write(move |connection| {
                        connection.exec_bound::<(&str, &str, &str)>(
                            "INSERT OR REPLACE INTO scoped_kv_store(namespace, key, value) VALUES ((?), (?), (?))",
                        )?((GLOBAL_KVP_NAMESPACE, &key, &value))
                        .context("Failed to write global key to scoped_kv_store")
                    })
                    .await
            }
        }
    }

    query! {
        async fn write_kvp_own_file(key: String, value: String) -> Result<()> {
            INSERT OR REPLACE INTO kv_store(key, value) VALUES ((?), (?))
        }
    }

    pub async fn delete_kvp(&self, key: String) -> anyhow::Result<()> {
        match self.backing {
            Backing::OwnFile => self.delete_kvp_own_file(key).await,
            Backing::AppDb => {
                self.connection
                    .write(move |connection| {
                        connection.exec_bound::<(&str, &str)>(
                            "DELETE FROM scoped_kv_store WHERE namespace = (?) AND key = (?)",
                        )?((GLOBAL_KVP_NAMESPACE, &key))
                        .context("Failed to delete global key from scoped_kv_store")
                    })
                    .await
            }
        }
    }

    query! {
        async fn delete_kvp_own_file(key: String) -> Result<()> {
            DELETE FROM kv_store WHERE key = (?)
        }
    }
}

#[cfg(test)]
mod global_tests {
    use crate::{
        AppDatabase, RestoreOutcome,
        kvp::{Backing, GLOBAL_KVP_NAMESPACE, GlobalKeyValueStore, KeyValueStore},
    };

    /// D7: the folded global store's rows are part of the application database image
    /// (one table, one image, next to the app-side `kv_store` rows), stored under the
    /// reserved `scoped_kv_store` namespace.
    #[gpui::test]
    async fn global_kvp_rows_travel_in_image(cx: &mut gpui::TestAppContext) {
        // The native file store below runs its writes on a real background thread.
        cx.executor().allow_parking();
        let db = AppDatabase::test_new();
        let global = GlobalKeyValueStore::from_app_db(&db);
        global
            .write_kvp("k".to_string(), "v".to_string())
            .await
            .unwrap();
        assert_eq!(global.read_kvp("k").unwrap(), Some("v".to_string()));
        let app_store = KeyValueStore::from_app_db(&db);
        assert_eq!(
            app_store.read_kvp("k").unwrap(),
            None,
            "the global namespace never leaks into kv_store"
        );
        app_store
            .write_kvp("dismissed-image-test".to_string(), "1".to_string())
            .await
            .unwrap();

        let image = db.0.serialize().await.unwrap();
        let (db2, outcome) = AppDatabase::test_new_with_image(Some(image));
        assert_eq!(outcome, RestoreOutcome::Restored);
        assert_eq!(
            GlobalKeyValueStore::from_app_db(&db2)
                .read_kvp("k")
                .unwrap(),
            Some("v".to_string())
        );
        assert_eq!(
            KeyValueStore::from_app_db(&db2)
                .read_kvp("dismissed-image-test")
                .unwrap(),
            Some("1".to_string()),
            "an app-side key written before the serialize survives the same image"
        );
        assert_eq!(
            db2.0
                .select_row_bound::<(&str, &str), String>(
                    "SELECT value FROM scoped_kv_store WHERE namespace = (?) AND key = (?)"
                )
                .unwrap()((GLOBAL_KVP_NAMESPACE, "k"))
            .unwrap(),
            Some("v".to_string()),
            "the rows live under the reserved namespace"
        );

        let folded = GlobalKeyValueStore::from_app_db(&db2);
        folded.delete_kvp("k".to_string()).await.unwrap();
        assert_eq!(folded.read_kvp("k").unwrap(), None);

        // A native store over its own file keeps its `kv_store` rows apart from the fold.
        let dir = tempfile::tempdir().unwrap();
        let own_file = GlobalKeyValueStore {
            connection: crate::open_db::<GlobalKeyValueStore>(dir.path(), crate::GlobalDbScope)
                .await,
            backing: Backing::OwnFile,
        };
        assert_eq!(own_file.read_kvp("k").unwrap(), None);
        own_file
            .write_kvp("k".to_string(), "own".to_string())
            .await
            .unwrap();
        assert_eq!(own_file.read_kvp("k").unwrap(), Some("own".to_string()));
        assert_eq!(
            GlobalKeyValueStore::from_app_db(&db).read_kvp("k").unwrap(),
            Some("v".to_string()),
            "the folded store is unaffected by the file store"
        );
        assert_eq!(folded.read_kvp("k").unwrap(), None);
    }
}
