//! The client side of client-state persistence (BUILD-SPEC 5.4, D7): a periodic saver that
//! serializes the whole `AppDatabase` into one SQLite image and hands it to a
//! transport-agnostic [`ClientStateSink`]. The sink (proto encoding, gzip) lives in
//! `workspace::client_state`; this module knows nothing about the wire.
//!
//! Flush triggers are exactly three: the [`SAVE_INTERVAL`] dirty timer, `visibilitychange`
//! to hidden ([`set_hidden`]) and `LifecycleNotice STOPPING` ([`flush_client_state_for_stop`],
//! whose save is tagged `stopping` on the wire so the server's stopping wait ends only on it).

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use futures::{
    FutureExt as _,
    future::{BoxFuture, Shared},
};
use gpui::{App, AppContext as _, Context, Entity, EventEmitter, Global, Task};
use sqlez::thread_safe_connection::ThreadSafeConnection;

use crate::{AppDatabase, RestoreOutcome};

/// How often a dirty database is saved while the tab is visible.
pub const SAVE_INTERVAL: Duration = Duration::from_secs(15);
/// The save interval while the tab is hidden (a hidden tab may be discarded at any time).
pub const HIDDEN_SAVE_INTERVAL: Duration = Duration::from_secs(5);
/// Largest uncompressed image the client sends; keeps even a 1:1 gzip under the 16 MiB
/// WebSocket frame ceiling. The same limit `sqlez` enforces when an image is restored.
pub const MAX_IMAGE_BYTES: usize = sqlez::connection::MAX_IMAGE_BYTES;

/// The result of one [`ClientStateSink::save`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SaveOutcome {
    /// Whether the server stored the image.
    pub accepted: bool,
    /// The server's current version after the call.
    pub version: u64,
}

/// Where images go. Implemented over the remote-server session by `workspace`.
pub trait ClientStateSink: Send + Sync + 'static {
    /// Persist `image` as `version`. `accepted == false` means the server already holds
    /// `outcome.version >= version` (a replayed request, or a superseded tab). `stopping`
    /// marks the flush that answers a `Stopping` notice (D6): the server's stopping wait
    /// ends only on an accepted save carrying it. Called off the foreground thread.
    fn save(
        &self,
        image: Vec<u8>,
        version: u64,
        stopping: bool,
    ) -> BoxFuture<'static, Result<SaveOutcome>>;
    /// The server's current version without the image (used after a reconnect).
    fn current_version(&self) -> BoxFuture<'static, Result<u64>>;
}

/// Events a [`ClientStateStore`] emits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientStateEvent {
    /// A save was accepted as `version`.
    Saved {
        /// The version the server now holds.
        version: u64,
    },
    /// A save failed (serialization, size cap, or the sink); the store stays dirty.
    SaveFailed(String),
    /// The server held a newer version; the store adopted it and stays dirty.
    Stale {
        /// The server's version.
        server_version: u64,
    },
    /// The restore was skipped at open; nothing is sent until `allow_overwrite`.
    ReadOnly(String),
}

type SaveTask = Shared<Task<Result<(), Arc<anyhow::Error>>>>;

/// Periodic saver of the application database image.
pub struct ClientStateStore {
    db: ThreadSafeConnection,
    sink: Arc<dyn ClientStateSink>,
    /// Last version known to be held by the sink.
    version: u64,
    /// `db.write_generation()` at the last accepted save.
    saved_generation: u64,
    /// Set by `resync` so the next tick saves even if nothing was written meanwhile.
    force_dirty: bool,
    interval: Duration,
    /// `Some` after `RestoreOutcome::Skipped` until `allow_overwrite`: the skip reason.
    read_only: Option<String>,
    /// True after any `Disconnected` (takeover, server stopped, reconnect exhausted);
    /// cleared by `rebind`.
    stopped: bool,
    hidden: bool,
    in_flight: Option<InFlight>,
    _ticker: Task<()>,
}

#[derive(Clone)]
struct InFlight {
    task: SaveTask,
    /// The write generation the in-flight image was taken at.
    generation: u64,
}

impl EventEmitter<ClientStateEvent> for ClientStateStore {}

impl ClientStateStore {
    /// `loaded_version` is the version returned by `LoadClientState` (0 when none);
    /// `outcome` is what `AppDatabase::{new,open}_with_image` reported.
    pub fn new(
        db: &AppDatabase,
        sink: Arc<dyn ClientStateSink>,
        loaded_version: u64,
        outcome: RestoreOutcome,
        cx: &mut Context<Self>,
    ) -> Self {
        let read_only = match outcome {
            RestoreOutcome::Skipped(reason) => {
                log::warn!("client state is read-only: restore skipped ({reason})");
                // Emitted from a task so subscribers registered right after construction
                // still observe it.
                cx.spawn({
                    let reason = reason.clone();
                    async move |this, cx| {
                        this.update(cx, |_, cx| cx.emit(ClientStateEvent::ReadOnly(reason)))
                            .ok();
                    }
                })
                .detach();
                Some(reason)
            }
            RestoreOutcome::NoImage | RestoreOutcome::Restored => None,
        };
        let ticker = cx.spawn(async move |this, cx| {
            loop {
                let Ok(interval) = this.read_with(cx, |this, _| this.interval) else {
                    break;
                };
                cx.background_executor().timer(interval).await;
                if this.update(cx, |this, cx| this.tick(cx)).is_err() {
                    break;
                }
            }
        });
        Self {
            db: db.0.clone(),
            sink,
            version: loaded_version,
            saved_generation: db.0.write_generation(),
            force_dirty: false,
            interval: SAVE_INTERVAL,
            read_only,
            stopped: false,
            hidden: false,
            in_flight: None,
            _ticker: ticker,
        }
    }

    /// Whether anything was written since the last accepted save.
    pub fn is_dirty(&self) -> bool {
        self.force_dirty || self.db.write_generation() != self.saved_generation
    }

    /// Whether the store refuses to send because the restore was skipped.
    pub fn is_read_only(&self) -> bool {
        self.read_only.is_some()
    }

    /// Why the store is read-only, when it is.
    pub fn read_only_reason(&self) -> Option<&str> {
        self.read_only.as_deref()
    }

    /// Whether the store was stopped by a disconnect and not yet rebound.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// The last version known to be held by the sink.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The current save interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Whether the tab is currently reported hidden.
    pub fn is_hidden(&self) -> bool {
        self.hidden
    }

    /// Lifts `read_only` (the entry crate decides when a skipped restore may be overwritten).
    pub fn allow_overwrite(&mut self) {
        self.read_only = None;
    }

    /// Changes how often a dirty database is saved; takes effect after the current tick.
    pub fn set_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    /// `visibilitychange`: while hidden the interval drops to [`HIDDEN_SAVE_INTERVAL`] and a
    /// flush starts immediately (the returned task); becoming visible restores
    /// [`SAVE_INTERVAL`] and resolves at once.
    pub fn set_hidden(&mut self, hidden: bool, cx: &mut Context<Self>) -> Task<Result<()>> {
        self.hidden = hidden;
        if hidden {
            self.interval = HIDDEN_SAVE_INTERVAL;
            self.flush_now(cx)
        } else {
            self.interval = SAVE_INTERVAL;
            Task::ready(Ok(()))
        }
    }

    /// Serializes and sends now, dirty or not: a flush is the client saying "this is my
    /// final state". A save already in flight that carries the current write generation is
    /// awaited instead of duplicated; one taken before newer writes is awaited and then
    /// followed by a fresh save. Returns `Err` at once while read-only or stopped. The
    /// hidden-tab and best-effort `pagehide` trigger; the `Stopping` notice uses
    /// [`ClientStateStore::flush_for_stop`].
    pub fn flush_now(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        if let Err(error) = self.check_sendable() {
            return Task::ready(Err(error));
        }
        let generation = self.db.write_generation();
        match self.in_flight.clone() {
            Some(in_flight) if in_flight.generation == generation => {
                let task = in_flight.task;
                cx.spawn(async move |_, _| task.await.map_err(shared_error))
            }
            Some(in_flight) => cx.spawn(async move |this, cx| {
                if let Err(error) = in_flight.task.await {
                    // Already reported as `SaveFailed` by the save itself; the fresh save
                    // below is what this flush answers with.
                    log::debug!("in-flight save failed before the flush: {error:#}");
                }
                let task = this.update(cx, |this, cx| {
                    this.check_sendable()?;
                    anyhow::Ok(this.start_save(cx))
                })??;
                task.await.map_err(shared_error)
            }),
            None => {
                let task = self.start_save(cx);
                cx.spawn(async move |_, _| task.await.map_err(shared_error))
            }
        }
    }

    /// The `Stopping` flush (D6, after the unsaved-buffer snapshot): always starts its own
    /// save tagged `stopping`, never joining one already in flight, because the server ends
    /// its stopping wait only on an accepted save carrying the tag and a ticker save that
    /// began before the snapshot does not hold the `unsaved_buffers` rows. An in-flight save
    /// is awaited first so the two cannot interleave. Returns `Err` at once while read-only
    /// or stopped.
    pub fn flush_for_stop(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        if let Err(error) = self.check_sendable() {
            return Task::ready(Err(error));
        }
        match self.in_flight.clone() {
            Some(in_flight) => cx.spawn(async move |this, cx| {
                if let Err(error) = in_flight.task.await {
                    log::debug!("in-flight save failed before the stopping flush: {error:#}");
                }
                let task = this.update(cx, |this, cx| {
                    this.check_sendable()?;
                    anyhow::Ok(this.spawn_save(cx, true).task)
                })??;
                task.await.map_err(shared_error)
            }),
            None => {
                let task = self.spawn_save(cx, true).task;
                cx.spawn(async move |_, _| task.await.map_err(shared_error))
            }
        }
    }

    /// Called on every `RemoteClientEvent::Disconnected { .. }` (both terminal states):
    /// stops sending; `flush_now` returns `Err` until `rebind`.
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    /// Called on `RemoteClientEvent::Reconnected` (transport-level reconnect, same client):
    /// re-queries the sink's version, adopts the max, and marks the store dirty so the next
    /// tick sends a fresh image above it.
    pub fn resync(&mut self, cx: &mut Context<Self>) -> Task<Result<()>> {
        let sink = self.sink.clone();
        cx.spawn(async move |this, cx| {
            let server_version = sink.current_version().await?;
            this.update(cx, |this, _| {
                this.version = this.version.max(server_version);
                this.force_dirty = true;
            })?;
            Ok(())
        })
    }

    /// Called when the host's `reconnect()` builds a new connection from scratch in the
    /// same page (a new sink): replaces the sink, clears `stopped`, then `resync`.
    pub fn rebind(
        &mut self,
        sink: Arc<dyn ClientStateSink>,
        cx: &mut Context<Self>,
    ) -> Task<Result<()>> {
        self.sink = sink;
        self.stopped = false;
        self.resync(cx)
    }

    fn check_sendable(&self) -> Result<()> {
        if let Some(reason) = &self.read_only {
            anyhow::bail!(
                "client state is read-only: the stored image was not restored ({reason})"
            );
        }
        if self.stopped {
            anyhow::bail!("client state store is stopped (disconnected)");
        }
        Ok(())
    }

    fn tick(&mut self, cx: &mut Context<Self>) {
        if self.read_only.is_some() || self.stopped || self.in_flight.is_some() || !self.is_dirty()
        {
            return;
        }
        self.spawn_save(cx, false);
    }

    /// The in-flight save, or a new one.
    fn start_save(&mut self, cx: &mut Context<Self>) -> SaveTask {
        match &self.in_flight {
            Some(in_flight) => in_flight.task.clone(),
            None => self.spawn_save(cx, false).task,
        }
    }

    /// Serializes, size-checks and sends `version + 1` entirely on the background executor
    /// (D7: the image work, gzip included, stays off the main thread); the shared task is
    /// remembered as `in_flight` until it resolves.
    fn spawn_save(&mut self, cx: &mut Context<Self>, stopping: bool) -> InFlight {
        let generation = self.db.write_generation();
        // Saturating: a store whose version somehow reached the maximum keeps being
        // rejected as stale by the server rather than wrapping to 0.
        let version = self.version.saturating_add(1);
        let db = self.db.clone();
        let sink = self.sink.clone();
        let task = cx.spawn(async move |this, cx| {
            let result: Result<()> = async {
                let outcome = cx
                    .background_spawn(async move {
                        let image = db.serialize().await?;
                        anyhow::ensure!(
                            image.len() <= MAX_IMAGE_BYTES,
                            "client-state image is {} bytes, over the {} byte limit",
                            image.len(),
                            MAX_IMAGE_BYTES
                        );
                        sink.save(image, version, stopping).await
                    })
                    .await?;
                this.update(cx, |this, cx| {
                    if outcome.accepted {
                        this.version = outcome.version;
                        this.saved_generation = generation;
                        this.force_dirty = false;
                        cx.emit(ClientStateEvent::Saved {
                            version: outcome.version,
                        });
                    } else {
                        this.version = this.version.max(outcome.version);
                        cx.emit(ClientStateEvent::Stale {
                            server_version: outcome.version,
                        });
                    }
                })?;
                Ok(())
            }
            .await;
            this.update(cx, |this, cx| {
                this.in_flight = None;
                if let Err(error) = &result {
                    log::warn!("client state save failed: {error:#}");
                    cx.emit(ClientStateEvent::SaveFailed(format!("{error:#}")));
                }
            })
            .ok();
            result.map_err(Arc::new)
        });
        let in_flight = InFlight {
            task: task.shared(),
            generation,
        };
        self.in_flight = Some(in_flight.clone());
        in_flight
    }
}

fn shared_error(error: Arc<anyhow::Error>) -> anyhow::Error {
    anyhow!("{error:#}")
}

struct GlobalClientStateStore(Entity<ClientStateStore>);

impl Global for GlobalClientStateStore {}

/// Installs the store that [`flush_client_state`] and [`set_hidden`] address. The entry
/// crate calls it once after constructing the store.
pub fn set_global_store(store: Entity<ClientStateStore>, cx: &mut App) {
    cx.set_global(GlobalClientStateStore(store));
}

/// The installed store, if any.
pub fn global_store(cx: &App) -> Option<Entity<ClientStateStore>> {
    cx.try_global::<GlobalClientStateStore>()
        .map(|store| store.0.clone())
}

/// Best-effort flush (the hidden-tab path and the shell's `pagehide` call): serializes and
/// sends the current image now, joining a save already in flight. `Err` when no store is
/// installed, or when the store is read-only or stopped. Not the `Stopping` trigger: see
/// [`flush_client_state_for_stop`].
pub fn flush_client_state(cx: &mut App) -> Task<Result<()>> {
    match global_store(cx) {
        Some(store) => store.update(cx, |store, cx| store.flush_now(cx)),
        None => Task::ready(Err(anyhow!("no client-state store is installed"))),
    }
}

/// Flush trigger for `LifecycleNotice STOPPING`, run after the unsaved-buffer snapshot
/// (D6): always a fresh save tagged `stopping`, which is the only save that ends the
/// server's stopping wait. `Err` when no store is installed, or when the store is read-only
/// or stopped.
pub fn flush_client_state_for_stop(cx: &mut App) -> Task<Result<()>> {
    match global_store(cx) {
        Some(store) => store.update(cx, |store, cx| store.flush_for_stop(cx)),
        None => Task::ready(Err(anyhow!("no client-state store is installed"))),
    }
}

/// Flush trigger for `visibilitychange`: see [`ClientStateStore::set_hidden`]. `Err` when
/// no store is installed.
pub fn set_hidden(hidden: bool, cx: &mut App) -> Task<Result<()>> {
    match global_store(cx) {
        Some(store) => store.update(cx, |store, cx| store.set_hidden(hidden, cx)),
        None => Task::ready(Err(anyhow!("no client-state store is installed"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvp::KeyValueStore;
    use futures::channel::oneshot;
    use gpui::TestAppContext;
    use std::sync::Mutex;

    /// A sink that records every call and answers with a configurable outcome.
    struct FakeSink {
        saves: Mutex<Vec<(usize, u64)>>,
        /// Versions of the saves that carried the `stopping` tag.
        stopping_saves: Mutex<Vec<u64>>,
        version_queries: Mutex<usize>,
        response: Mutex<Response>,
        /// When set, the next save waits for this gate before answering.
        gate: Mutex<Option<oneshot::Receiver<()>>>,
        server_version: Mutex<u64>,
    }

    #[derive(Clone, Copy)]
    enum Response {
        Accept,
        Stale(u64),
        Fail,
    }

    impl FakeSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                saves: Mutex::new(Vec::new()),
                stopping_saves: Mutex::new(Vec::new()),
                version_queries: Mutex::new(0),
                response: Mutex::new(Response::Accept),
                gate: Mutex::new(None),
                server_version: Mutex::new(0),
            })
        }

        fn saves(&self) -> Vec<(usize, u64)> {
            self.saves.lock().unwrap().clone()
        }
    }

    impl ClientStateSink for FakeSink {
        fn save(
            &self,
            image: Vec<u8>,
            version: u64,
            stopping: bool,
        ) -> BoxFuture<'static, Result<SaveOutcome>> {
            self.saves.lock().unwrap().push((image.len(), version));
            if stopping {
                self.stopping_saves.lock().unwrap().push(version);
            }
            let response = *self.response.lock().unwrap();
            let gate = self.gate.lock().unwrap().take();
            let server_version = match response {
                Response::Accept => {
                    *self.server_version.lock().unwrap() = version;
                    version
                }
                Response::Stale(server) => server,
                Response::Fail => 0,
            };
            async move {
                if let Some(gate) = gate {
                    gate.await.ok();
                }
                match response {
                    Response::Accept => Ok(SaveOutcome {
                        accepted: true,
                        version: server_version,
                    }),
                    Response::Stale(server) => Ok(SaveOutcome {
                        accepted: false,
                        version: server,
                    }),
                    Response::Fail => Err(anyhow!("sink exploded")),
                }
            }
            .boxed()
        }

        fn current_version(&self) -> BoxFuture<'static, Result<u64>> {
            *self.version_queries.lock().unwrap() += 1;
            let version = *self.server_version.lock().unwrap();
            async move { Ok(version) }.boxed()
        }
    }

    fn store(
        db: &AppDatabase,
        sink: Arc<FakeSink>,
        loaded_version: u64,
        outcome: RestoreOutcome,
        cx: &mut TestAppContext,
    ) -> (Entity<ClientStateStore>, Arc<Mutex<Vec<ClientStateEvent>>>) {
        let store = cx.new(|cx| ClientStateStore::new(db, sink, loaded_version, outcome, cx));
        let events = Arc::new(Mutex::new(Vec::new()));
        cx.update({
            let events = events.clone();
            let store = store.clone();
            move |cx| {
                cx.subscribe(&store, move |_, event, _| {
                    events.lock().unwrap().push(event.clone())
                })
                .detach();
            }
        });
        (store, events)
    }

    async fn dirty(db: &AppDatabase, key: &str) {
        KeyValueStore::from_app_db(db)
            .write_kvp(key.to_string(), "x".to_string())
            .await
            .unwrap();
    }

    #[gpui::test]
    async fn client_state_store_saves_only_when_dirty(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        let (store, events) = store(&db, sink.clone(), 3, RestoreOutcome::NoImage, cx);

        cx.executor().advance_clock(SAVE_INTERVAL * 2);
        cx.run_until_parked();
        assert!(sink.saves().is_empty(), "nothing written, nothing saved");

        dirty(&db, "a").await;
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        let saves = sink.saves();
        assert_eq!(saves.len(), 1);
        assert_eq!(saves[0].1, 4, "version == loaded + 1");
        assert!(saves[0].0 > 100, "a real image was sent");
        assert_eq!(store.read_with(cx, |store, _| store.version()), 4);
        assert!(
            events
                .lock()
                .unwrap()
                .contains(&ClientStateEvent::Saved { version: 4 })
        );
        assert!(!store.read_with(cx, |store, _| store.is_dirty()));

        // A flush while a save is in flight coalesces onto it.
        let (release, gate) = oneshot::channel();
        *sink.gate.lock().unwrap() = Some(gate);
        dirty(&db, "b").await;
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 2, "the tick started a gated save");
        let flush = store.update(cx, |store, cx| store.flush_now(cx));
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 2, "the flush joined the in-flight save");
        release.send(()).unwrap();
        flush.await.unwrap();
        assert_eq!(sink.saves().len(), 2);
        assert_eq!(store.read_with(cx, |store, _| store.version()), 5);

        // A flush on a clean store still sends.
        store
            .update(cx, |store, cx| store.flush_now(cx))
            .await
            .unwrap();
        assert_eq!(sink.saves().len(), 3);
        assert_eq!(sink.saves()[2].1, 6);

        // An image over the cap is refused without bumping the version.
        let oversized = "x".repeat(MAX_IMAGE_BYTES + 1);
        KeyValueStore::from_app_db(&db)
            .write_kvp("big".to_string(), oversized)
            .await
            .unwrap();
        let result = store.update(cx, |store, cx| store.flush_now(cx)).await;
        assert!(result.is_err());
        assert_eq!(
            sink.saves().len(),
            3,
            "the oversized image never reached the sink"
        );
        assert_eq!(store.read_with(cx, |store, _| store.version()), 6);
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, ClientStateEvent::SaveFailed(_)))
        );
    }

    #[gpui::test]
    async fn client_state_store_stop_flush_never_joins_in_flight(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        let (store, _) = store(&db, sink.clone(), 0, RestoreOutcome::NoImage, cx);
        cx.update(|cx| set_global_store(store.clone(), cx));

        // A ticker save is in flight (gated) when the stopping notice arrives.
        let (release, gate) = oneshot::channel();
        *sink.gate.lock().unwrap() = Some(gate);
        dirty(&db, "a").await;
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves(), vec![(sink.saves()[0].0, 1)]);
        assert!(sink.stopping_saves.lock().unwrap().is_empty());

        // The same write generation: `flush_now` would join the in-flight save, but the
        // stopping flush must produce its own tagged save after it.
        let flush = cx.update(|cx| flush_client_state_for_stop(cx));
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 1, "waits for the in-flight save first");
        release.send(()).unwrap();
        flush.await.unwrap();
        assert_eq!(sink.saves().len(), 2);
        assert_eq!(sink.saves()[1].1, 2);
        assert_eq!(*sink.stopping_saves.lock().unwrap(), vec![2]);
        assert_eq!(store.read_with(cx, |store, _| store.version()), 2);

        // Nothing in flight: one fresh tagged save, and the plain flush stays untagged.
        cx.update(|cx| flush_client_state_for_stop(cx))
            .await
            .unwrap();
        cx.update(|cx| flush_client_state(cx)).await.unwrap();
        assert_eq!(sink.saves().len(), 4);
        assert_eq!(*sink.stopping_saves.lock().unwrap(), vec![2, 3]);

        store.update(cx, |store, _| store.stop());
        assert!(
            cx.update(|cx| flush_client_state_for_stop(cx))
                .await
                .is_err(),
            "a stopped store refuses the stopping flush too"
        );
    }

    #[gpui::test]
    async fn client_state_store_converges_after_stale(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        *sink.response.lock().unwrap() = Response::Stale(7);
        let (store, events) = store(&db, sink.clone(), 2, RestoreOutcome::NoImage, cx);

        dirty(&db, "a").await;
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves(), vec![(sink.saves()[0].0, 3)]);
        assert_eq!(store.read_with(cx, |store, _| store.version()), 7);
        assert!(store.read_with(cx, |store, _| store.is_dirty()));
        assert!(
            events
                .lock()
                .unwrap()
                .contains(&ClientStateEvent::Stale { server_version: 7 })
        );

        *sink.response.lock().unwrap() = Response::Accept;
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 2);
        assert_eq!(sink.saves()[1].1, 8);
        assert_eq!(store.read_with(cx, |store, _| store.version()), 8);
        assert!(
            events
                .lock()
                .unwrap()
                .contains(&ClientStateEvent::Saved { version: 8 })
        );
        assert!(!store.read_with(cx, |store, _| store.is_dirty()));
    }

    #[gpui::test]
    async fn client_state_store_read_only_after_skipped_restore(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        let (store, events) = store(
            &db,
            sink.clone(),
            0,
            RestoreOutcome::Skipped("bad image".into()),
            cx,
        );
        cx.run_until_parked();
        assert!(
            events
                .lock()
                .unwrap()
                .contains(&ClientStateEvent::ReadOnly("bad image".into()))
        );
        assert!(store.read_with(cx, |store, _| store.is_read_only()));

        dirty(&db, "a").await;
        cx.executor().advance_clock(SAVE_INTERVAL * 3);
        cx.run_until_parked();
        assert!(sink.saves().is_empty(), "read-only stores never send");
        assert!(
            store
                .update(cx, |store, cx| store.flush_now(cx))
                .await
                .is_err()
        );

        store.update(cx, |store, _| store.allow_overwrite());
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 1);
        assert_eq!(sink.saves()[0].1, 1);
    }

    #[gpui::test]
    async fn client_state_store_sink_error_surfaces(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        *sink.response.lock().unwrap() = Response::Fail;
        let (store, events) = store(&db, sink.clone(), 0, RestoreOutcome::NoImage, cx);

        dirty(&db, "a").await;
        let result = store.update(cx, |store, cx| store.flush_now(cx)).await;
        let error = result.expect_err("the sink error propagates");
        assert!(error.to_string().contains("sink exploded"), "{error:#}");
        assert!(
            events
                .lock().unwrap()
                .iter()
                .any(|event| matches!(event, ClientStateEvent::SaveFailed(message) if message.contains("sink exploded")))
        );
        assert!(store.read_with(cx, |store, _| store.is_dirty()));
        assert_eq!(store.read_with(cx, |store, _| store.version()), 0);
    }

    #[gpui::test]
    async fn client_state_store_stop_and_resync(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        let (store, _) = store(&db, sink.clone(), 0, RestoreOutcome::NoImage, cx);

        store.update(cx, |store, _| store.stop());
        dirty(&db, "a").await;
        cx.executor().advance_clock(SAVE_INTERVAL * 2);
        cx.run_until_parked();
        assert!(sink.saves().is_empty(), "stopped stores never send");
        assert!(
            store
                .update(cx, |store, cx| store.flush_now(cx))
                .await
                .is_err()
        );

        *sink.server_version.lock().unwrap() = 9;
        store.update(cx, |store, _| store.stopped = false);
        store
            .update(cx, |store, cx| store.resync(cx))
            .await
            .unwrap();
        assert_eq!(*sink.version_queries.lock().unwrap(), 1);
        assert_eq!(store.read_with(cx, |store, _| store.version()), 9);
        assert!(store.read_with(cx, |store, _| store.is_dirty()));
        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 1);
        assert_eq!(sink.saves()[0].1, 10);
    }

    #[gpui::test]
    async fn client_state_store_rebind(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let old_sink = FakeSink::new();
        let (store, _) = store(&db, old_sink.clone(), 0, RestoreOutcome::NoImage, cx);

        store.update(cx, |store, _| store.stop());
        let new_sink = FakeSink::new();
        *new_sink.server_version.lock().unwrap() = 4;
        store
            .update(cx, |store, cx| store.rebind(new_sink.clone(), cx))
            .await
            .unwrap();
        assert_eq!(*new_sink.version_queries.lock().unwrap(), 1);
        assert_eq!(*old_sink.version_queries.lock().unwrap(), 0);
        assert!(!store.read_with(cx, |store, _| store.is_stopped()));

        cx.executor().advance_clock(SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(
            new_sink.saves().len(),
            1,
            "the ticker resumed on the new sink"
        );
        assert_eq!(new_sink.saves()[0].1, 5);
        assert!(
            old_sink.saves().is_empty(),
            "the old sink receives nothing more"
        );
    }

    #[gpui::test]
    async fn client_state_store_hidden_flushes_and_shortens_interval(cx: &mut TestAppContext) {
        let db = AppDatabase::test_new();
        let sink = FakeSink::new();
        let (store, _) = store(&db, sink.clone(), 0, RestoreOutcome::NoImage, cx);
        cx.update(|cx| set_global_store(store.clone(), cx));

        cx.update(|cx| set_hidden(true, cx)).await.unwrap();
        assert_eq!(sink.saves().len(), 1, "hiding flushes at once");
        assert_eq!(
            store.read_with(cx, |store, _| store.interval()),
            HIDDEN_SAVE_INTERVAL
        );

        dirty(&db, "a").await;
        cx.executor().advance_clock(HIDDEN_SAVE_INTERVAL);
        cx.run_until_parked();
        assert_eq!(sink.saves().len(), 2, "the hidden interval applies");

        cx.update(|cx| set_hidden(false, cx)).await.unwrap();
        assert_eq!(
            store.read_with(cx, |store, _| store.interval()),
            SAVE_INTERVAL
        );
        assert_eq!(sink.saves().len(), 2, "becoming visible does not flush");

        cx.update(|cx| flush_client_state(cx)).await.unwrap();
        assert_eq!(sink.saves().len(), 3);
    }
}
