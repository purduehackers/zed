#![cfg(target_family = "wasm")]

//! The client-state SQLite image round trip in the browser (b6 §6, b4 §6): the only
//! coverage of `sqlite-wasm-rs` being driven from a worker and from the main thread through
//! one `sqlez` connection (its README calls the library not thread-safe; `sqlez::wasm_lock`
//! is what makes the sharing sound) and of the D7 fold of the global key-value store into
//! the application database image.
//!
//! Runs under `wasm-bindgen-test` in a browser (`run_in_browser`), which is b7's browser
//! suite: `wasm-bindgen-test-runner` over a `cargo test --target wasm32-unknown-unknown
//! -Zbuild-std=std,panic_abort -p zed_web --test sqlite_image_round_trip` build with the
//! flags `script/check-wasm` uses. Natively the file is empty.

use futures::channel::oneshot;
use gpui::AppContext as _;
use gpui_platform::WebBackendPreference;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

const APP_KEY: &str = "dismissed-sqlite-round-trip";
const GLOBAL_KEY: &str = "rules_to_skills_migration_done";

#[wasm_bindgen_test]
async fn sqlite_image_round_trip_across_workers() {
    gpui_platform::web_init();
    assert!(
        db::registered_migration_count() > 0,
        "static constructors did not run: the migration registry is empty"
    );

    // Main thread: open without an image and install the folded global store, in the
    // order the boot uses.
    let (app_db, outcome) = db::AppDatabase::open_with_image(None).await;
    assert_eq!(outcome, db::RestoreOutcome::NoImage);
    db::kvp::GlobalKeyValueStore::init(&app_db);

    // A worker writes an app-side key: the `write_and_log` shape, with the write future
    // created inside the background task so the SQLite work runs on the worker.
    let (written_tx, written_rx) = oneshot::channel::<anyhow::Result<()>>();
    let app = gpui_platform::application_with_web_backend(WebBackendPreference::Auto);
    let _app = app.run_embedded({
        let store = db::kvp::KeyValueStore::from_app_db(&app_db);
        move |cx| {
            let write = cx
                .background_spawn(async move { store.write_kvp(APP_KEY.into(), "1".into()).await });
            cx.spawn(async move |_cx| {
                written_tx.send(write.await).ok();
            })
            .detach();
        }
    });
    written_rx
        .await
        .expect("the worker task was dropped before it wrote")
        .expect("the worker's write failed");

    // Main thread, same connection: a global key.
    db::kvp::GlobalKeyValueStore::global()
        .write_kvp(GLOBAL_KEY.into(), "1".into())
        .await
        .expect("the global write failed");

    let store = db::kvp::KeyValueStore::from_app_db(&app_db);
    assert_eq!(store.read_kvp(APP_KEY).unwrap(), Some("1".to_string()));
    assert_eq!(
        db::kvp::GlobalKeyValueStore::global()
            .read_kvp(GLOBAL_KEY)
            .unwrap(),
        Some("1".to_string())
    );

    // Serialize, reopen from the image (a fresh in-memory database), read both back.
    let image = app_db.0.serialize().await.expect("serialize failed");
    let (reopened, outcome) = db::AppDatabase::open_with_image(Some(image)).await;
    assert_eq!(outcome, db::RestoreOutcome::Restored);
    let store = db::kvp::KeyValueStore::from_app_db(&reopened);
    assert_eq!(store.read_kvp(APP_KEY).unwrap(), Some("1".to_string()));
    let global = db::kvp::GlobalKeyValueStore::from_app_db(&reopened);
    assert_eq!(global.read_kvp(GLOBAL_KEY).unwrap(), Some("1".to_string()));
}
