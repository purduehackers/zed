//! Server-side PTY hosting for remote terminals (b3-terminals, BUILD-SPEC §5.1).
//!
//! A [`PtyManager`] owns every PTY the remote server has spawned on behalf of a
//! client. It lives at process level — installed as an `App` global by the first
//! [`crate::HeadlessProject`] (or explicitly by `serve`) and never reset with the
//! project — so terminals survive client disconnects *and* fresh sessions (D3,
//! D24). Each terminal keeps a bounded scrollback ring, streams output to the
//! client with ack-based flow control, and is torn down only by an explicit
//! `CloseTerminal`, the child's own exit, or process shutdown.
//!
//! Threading: three OS threads per terminal (reader, writer, waiter) only
//! enqueue into an unbounded channel; one foreground drain task hands the
//! messages to the [`TerminalOutputSink`] (the session's `AnyProtoClient`) so the
//! `ChannelClient`'s single-producer id/ack bookkeeping is never fed from two
//! threads at once.

use anyhow::{Context as _, Result, anyhow, bail};
use collections::HashMap;
use futures::{
    StreamExt as _,
    channel::mpsc::{UnboundedSender, unbounded},
};
use gpui::{App, Global};
use parking_lot::{Condvar, Mutex};
#[cfg(not(unix))]
use portable_pty::ChildKiller as _;
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use rpc::{AnyProtoClient, proto};
use std::{
    collections::VecDeque,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use util::ResultExt as _;

/// Bytes of output retained per terminal for replay after a reconnect or a
/// fresh-session restore.
pub const SCROLLBACK_CAPACITY: usize = 2 * 1024 * 1024;
/// Maximum unacknowledged bytes in flight to an attached client before the
/// reader thread stalls (and the kernel PTY buffer back-pressures the child).
pub const OUTPUT_WINDOW: u64 = 512 * 1024;
/// Maximum payload of one `TerminalOutput` message.
pub const OUTPUT_CHUNK: usize = 64 * 1024;
/// How long the wait thread gives the reader to see EOF after the child exited
/// before it reports `TerminalExited` anyway (a grandchild may still hold the
/// slave open).
pub const EXIT_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
/// Delay between SIGTERM and SIGKILL when closing a terminal.
pub const KILL_ESCALATION_DELAY: Duration = Duration::from_millis(100);
/// Upper bound on concurrent terminals per server process (three OS threads
/// each); `spawn` returns an error beyond this.
pub const MAX_TERMINALS: usize = 64;
/// Largest single `TerminalInput` payload the manager accepts (the wire
/// contract; the client chunks at this size). Larger frames are rejected.
pub const INPUT_CHUNK: usize = 64 * 1024;
/// Upper bound on input bytes queued for a child that is not reading its PTY.
/// Further input is dropped rather than growing the queue without limit.
pub const INPUT_QUEUE_CAP: usize = 4 * 1024 * 1024;

/// Everything the manager puts on the wire, in stream order. Drained by one
/// foreground task so that `ChannelClient` never sees interleaved producers;
/// OS threads only enqueue.
#[derive(Debug)]
pub enum OutboundMessage {
    /// A chunk of terminal output.
    Output(proto::TerminalOutput),
    /// The child exited; sent after the remaining output was flushed.
    Exited(proto::TerminalExited),
}

impl OutboundMessage {
    /// The terminal the message belongs to.
    pub fn terminal_id(&self) -> u64 {
        match self {
            OutboundMessage::Output(message) => message.terminal_id,
            OutboundMessage::Exited(message) => message.terminal_id,
        }
    }
}

/// Where terminal output goes. `AnyProtoClient` implements it; tests use a
/// channel. Called only from the foreground drain task.
pub trait TerminalOutputSink: 'static {
    /// Deliver one message. Failures are the sink's to log; the manager never
    /// retries.
    fn send(&self, message: OutboundMessage);
}

impl TerminalOutputSink for AnyProtoClient {
    fn send(&self, message: OutboundMessage) {
        match message {
            OutboundMessage::Output(message) => {
                AnyProtoClient::send(self, message).log_err();
            }
            OutboundMessage::Exited(message) => {
                AnyProtoClient::send(self, message).log_err();
            }
        }
    }
}

/// What `SpawnTerminal` asks for.
#[derive(Debug, Clone)]
pub struct SpawnOptions {
    /// `Shell::System` runs the server's login shell (`$SHELL -l`).
    pub shell: task::Shell,
    /// Initial working directory; the server process's cwd when `None`.
    pub working_directory: Option<PathBuf>,
    /// Environment overlaid on the server process environment (minus `SHLVL`).
    pub env: HashMap<String, String>,
    /// Initial window size in cells.
    pub cols: u16,
    /// Initial window size in cells.
    pub rows: u16,
    /// The task this terminal runs, if any (reported by `ListTerminals`).
    pub task_id: Option<String>,
    /// Display title (task label or `"<host> — Terminal"`); defaults to the
    /// program name.
    pub title: Option<String>,
}

/// Result of [`PtyManager::attach`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachOutcome {
    /// First offset actually replayed (`>= from_offset` when scrollback was evicted).
    pub replayed_from: u64,
    /// Offset after the last byte the terminal has produced so far.
    pub end_offset: u64,
    /// Present when the child has already exited (informational; the
    /// stream-ordered `TerminalExited` is authoritative).
    pub exit: Option<proto::TerminalExit>,
}

/// One per process (D3): installed as an `App` global by the first
/// `HeadlessProject::new` (or explicitly by `serve` / tests simulating a server
/// restart) and never reset with the project, so PTYs outlive fresh sessions.
/// `HeadlessProject` only holds an `Arc` handle to it.
///
/// `Send + Sync`, as `serve`'s `PtyHooks` requires: everything mutable lives
/// behind the locks in [`PtyManagerState`].
pub struct PtyManager {
    state: Arc<PtyManagerState>,
}

/// The `App` global holding the process-level [`PtyManager`].
pub struct GlobalPtyManager(Arc<PtyManager>);

impl Global for GlobalPtyManager {}

/// Shared with the reader/wait threads (via `Weak`) and the `on_app_quit`
/// closure.
struct PtyManagerState {
    project_id: u64,
    outbound_tx: UnboundedSender<OutboundMessage>,
    terminals: Mutex<HashMap<u64, Arc<TerminalEntry>>>,
}

impl PtyManager {
    /// Creates a manager whose output is delivered to `sink` by a foreground
    /// drain task, and registers an `on_app_quit` observer that SIGTERMs every
    /// process group synchronously and SIGKILLs the survivors after
    /// [`KILL_ESCALATION_DELAY`] — well inside gpui's 200 ms shutdown budget.
    ///
    /// The drain task is detached rather than owned so that messages still
    /// queued by a terminal's threads (its final `TerminalExited`, say) reach
    /// the sink even after the manager itself has been dropped; it ends on its
    /// own once the last sender is gone.
    pub fn new(project_id: u64, sink: Arc<dyn TerminalOutputSink>, cx: &mut App) -> Self {
        let (outbound_tx, mut outbound_rx) = unbounded::<OutboundMessage>();
        cx.spawn(async move |_| {
            while let Some(message) = outbound_rx.next().await {
                sink.send(message);
            }
        })
        .detach();

        let state = Arc::new(PtyManagerState {
            project_id,
            outbound_tx,
            terminals: Mutex::new(HashMap::default()),
        });

        // Detached rather than stored: `Subscription` is neither `Send` nor
        // `Sync`, and `serve`'s `PtyHooks` needs both. The closure holds only a
        // `Weak`, so once this manager is dropped (a replacing `install`, app
        // teardown) the observer becomes a no-op instead of signalling stale
        // process groups.
        let quit_state = Arc::downgrade(&state);
        cx.on_app_quit(move |cx| {
            let state = quit_state.upgrade();
            let terminated = state.as_ref().map_or(0, |state| state.terminate_all());
            let escalation = cx.background_executor().timer(KILL_ESCALATION_DELAY);
            async move {
                if terminated > 0
                    && let Some(state) = state
                {
                    escalation.await;
                    state.kill_unexited();
                }
            }
        })
        .detach();

        Self { state }
    }

    /// The process-level manager, if one has been installed.
    pub fn global(cx: &App) -> Option<Arc<PtyManager>> {
        cx.try_global::<GlobalPtyManager>()
            .map(|global| global.0.clone())
    }

    /// [`Self::new`] + `set_global`, replacing any previous global (which
    /// drops — and SIGKILLs its children — once its last handle is gone).
    /// Production reaches it once, lazily, from `HeadlessProject::new`, or
    /// explicitly from `serve` before the project is built; tests call it to
    /// simulate a restarted server.
    pub fn install(
        project_id: u64,
        sink: Arc<dyn TerminalOutputSink>,
        cx: &mut App,
    ) -> Arc<PtyManager> {
        let manager = Arc::new(Self::new(project_id, sink, cx));
        cx.set_global(GlobalPtyManager(manager.clone()));
        manager
    }

    /// Spawns a new PTY. The terminal starts *detached*: the reader fills the
    /// scrollback ring until the client sends `AttachTerminal { from_offset: 0 }`.
    /// Returns the random, non-zero terminal id.
    pub fn spawn(&self, options: SpawnOptions) -> Result<u64> {
        let mut terminals = self.state.terminals.lock();
        if terminals.len() >= MAX_TERMINALS {
            bail!("too many terminals ({MAX_TERMINALS}); close one first");
        }
        let id = loop {
            let id = uuid::Uuid::new_v4().as_u128() as u64;
            if id != 0 && !terminals.contains_key(&id) {
                break id;
            }
        };
        let entry = TerminalEntry::spawn(self.state.project_id, id, &options, &self.state)?;
        terminals.insert(id, entry);
        Ok(id)
    }

    /// Queues input for the child. Never blocks the caller: the writer thread
    /// applies the PTY's own back-pressure. Rejects oversized frames and drops
    /// input once more than [`INPUT_QUEUE_CAP`] bytes are already queued for a
    /// child that is not reading, so a flood cannot grow the queue without
    /// bound.
    pub fn write(&self, terminal_id: u64, data: Vec<u8>) -> Result<()> {
        anyhow::ensure!(
            data.len() <= INPUT_CHUNK,
            "terminal input frame of {} bytes exceeds the {INPUT_CHUNK}-byte limit",
            data.len()
        );
        let entry = self.entry(terminal_id)?;
        let queued = entry.input_queued.load(std::sync::atomic::Ordering::Acquire);
        if queued.saturating_add(data.len()) > INPUT_QUEUE_CAP {
            log::warn!(
                "dropping {} bytes of input for terminal {terminal_id}: {queued} bytes already queued",
                data.len()
            );
            return Ok(());
        }
        entry
            .input_queued
            .fetch_add(data.len(), std::sync::atomic::Ordering::AcqRel);
        entry.input_tx.send(data).map_err(|error| {
            entry
                .input_queued
                .fetch_sub(error.0.len(), std::sync::atomic::Ordering::AcqRel);
            anyhow!("terminal {terminal_id} no longer accepts input")
        })
    }

    /// Changes the window size (`TIOCSWINSZ`; the child gets `SIGWINCH`).
    pub fn resize(&self, terminal_id: u64, cols: u16, rows: u16) -> Result<()> {
        let entry = self.entry(terminal_id)?;
        let exited = {
            let mut stream = entry.stream.lock();
            stream.cols = cols.max(1);
            stream.rows = rows.max(1);
            stream.exited
        };
        entry.resize_master(cols.max(1), rows.max(1), exited)
    }

    /// The client has parsed everything below `offset`; re-opens the send
    /// window. Unknown ids are ignored.
    pub fn ack(&self, terminal_id: u64, offset: u64) {
        let Some(entry) = self.state.terminals.lock().get(&terminal_id).cloned() else {
            return;
        };
        let mut stream = entry.stream.lock();
        let acked = stream.acked.max(offset.min(stream.sent));
        stream.acked = acked;
        entry.credit.notify_all();
    }

    /// Replays `[max(from_offset, scrollback_start), end)` as `TerminalOutput`
    /// messages (the first carries `reset = true` if `from_offset` was evicted;
    /// an empty reset chunk if there is nothing to replay but the client's
    /// offset is stale), re-sends `TerminalExited` when the child has already
    /// exited, then resumes live streaming from `end`. Replay ignores the ack
    /// window on purpose (at most [`SCROLLBACK_CAPACITY`] bytes once).
    pub fn attach(
        &self,
        terminal_id: u64,
        from_offset: u64,
        cols: u16,
        rows: u16,
    ) -> Result<AttachOutcome> {
        let entry = self.entry(terminal_id)?;
        let (outcome, exited) = {
            let mut stream = entry.stream.lock();
            let (from, reset) = stream.replay_range(from_offset);
            let end = stream.end;
            let mut offset = from;
            let mut first = true;
            while offset < end {
                let to = end.min(offset + OUTPUT_CHUNK as u64);
                entry.enqueue(OutboundMessage::Output(proto::TerminalOutput {
                    project_id: entry.project_id,
                    terminal_id,
                    offset,
                    data: stream.slice(offset, to),
                    reset: first && reset,
                }));
                first = false;
                offset = to;
            }
            if first && reset {
                entry.enqueue(OutboundMessage::Output(proto::TerminalOutput {
                    project_id: entry.project_id,
                    terminal_id,
                    offset: from,
                    data: Vec::new(),
                    reset: true,
                }));
            }
            if stream.exited {
                entry.enqueue(OutboundMessage::Exited(proto::TerminalExited {
                    project_id: entry.project_id,
                    terminal_id,
                    exit: stream.exit.clone(),
                    end_offset: end,
                }));
            }
            stream.sent = end;
            stream.acked = from;
            stream.attached = true;
            stream.cols = cols.max(1);
            stream.rows = rows.max(1);
            entry.credit.notify_all();
            (
                AttachOutcome {
                    replayed_from: from,
                    end_offset: end,
                    exit: stream.exit.clone(),
                },
                stream.exited,
            )
        };
        entry.resize_master(cols.max(1), rows.max(1), exited)?;
        Ok(outcome)
    }

    /// Stops streaming to the client; the reader keeps filling the ring and
    /// the child is never back-pressured while detached. Unknown ids are
    /// ignored.
    pub fn detach(&self, terminal_id: u64) {
        let Some(entry) = self.state.terminals.lock().get(&terminal_id).cloned() else {
            return;
        };
        entry.detach();
    }

    /// Session-detach hook (D20, D24): detaches every terminal so readers stop
    /// enqueueing into a channel nobody drains. Never kills anything.
    pub fn detach_all(&self) {
        let entries: Vec<_> = self.state.terminals.lock().values().cloned().collect();
        for entry in entries {
            entry.detach();
        }
    }

    /// Inventory for `ListTerminals`, including exited terminals that have not
    /// been closed yet (their scrollback is still replayable).
    pub fn list(&self) -> Vec<proto::TerminalInfo> {
        let entries: Vec<_> = self.state.terminals.lock().values().cloned().collect();
        entries
            .iter()
            .map(|entry| {
                let cwd = entry.current_working_directory();
                let stream = entry.stream.lock();
                proto::TerminalInfo {
                    terminal_id: entry.id,
                    title: entry.title.clone(),
                    cwd,
                    task_id: entry.task_id.clone(),
                    end_offset: stream.end,
                    scrollback_start: stream.ring_start,
                    exit: stream.exit.clone(),
                    cols: stream.cols as u32,
                    rows: stream.rows as u32,
                }
            })
            .collect()
    }

    /// `CloseTerminal`: SIGTERMs the process group (SIGKILL after
    /// [`KILL_ESCALATION_DELAY`]) and keeps streaming until the child exits,
    /// after which the wait thread removes the entry. An already-exited
    /// terminal is removed immediately.
    pub fn close(&self, terminal_id: u64) -> Result<()> {
        let entry = self.entry(terminal_id)?;
        let already_exited = {
            let mut stream = entry.stream.lock();
            if stream.exited {
                // Removing the entry now: stop the reader in case a descendant
                // still holds the slave open (see `on_exited`).
                stream.attached = false;
                entry.credit.notify_all();
            } else {
                stream.close_requested = true;
            }
            stream.exited
        };
        if already_exited {
            self.state.terminals.lock().remove(&terminal_id);
            return Ok(());
        }
        entry.terminate();
        spawn_kill_escalation(entry);
        Ok(())
    }

    /// Process shutdown (D3): SIGTERMs every live process group, marks every
    /// entry for removal once it exits, and SIGKILLs whatever is still alive
    /// after [`KILL_ESCALATION_DELAY`]. Never called for a fresh session.
    pub fn kill_all(&self) {
        if self.state.terminate_all() > 0 {
            let state = self.state.clone();
            std::thread::Builder::new()
                .name("pty-kill-all".into())
                .spawn(move || {
                    std::thread::sleep(KILL_ESCALATION_DELAY);
                    state.kill_unexited();
                })
                .log_err();
        }
    }

    /// The child process id of a terminal, for liveness checks.
    pub fn child_pid(&self, terminal_id: u64) -> Option<u32> {
        self.state
            .terminals
            .lock()
            .get(&terminal_id)
            .map(|entry| entry.child_pid)
    }

    fn entry(&self, terminal_id: u64) -> Result<Arc<TerminalEntry>> {
        self.state
            .terminals
            .lock()
            .get(&terminal_id)
            .cloned()
            .ok_or_else(|| anyhow!("unknown terminal {terminal_id}"))
    }
}

impl Drop for PtyManager {
    /// Last handle gone (`TestAppContext` teardown, or `install` replaced the
    /// global): SIGKILL every child immediately. Their threads still hold
    /// sender clones, so the final `TerminalExited`s reach the sink.
    fn drop(&mut self) {
        self.state.kill_unexited();
    }
}

impl PtyManagerState {
    /// SIGTERM every live process group and mark it for removal after exit;
    /// exited-but-unclosed entries are dropped right away. Returns how many
    /// were signalled.
    fn terminate_all(&self) -> usize {
        let mut signalled = 0;
        let mut terminals = self.terminals.lock();
        terminals.retain(|_, entry| {
            let mut stream = entry.stream.lock();
            if stream.exited {
                return false;
            }
            stream.close_requested = true;
            drop(stream);
            entry.terminate();
            signalled += 1;
            true
        });
        signalled
    }

    /// SIGKILL every process group whose child has not been reaped yet.
    fn kill_unexited(&self) {
        let entries: Vec<_> = self.terminals.lock().values().cloned().collect();
        for entry in entries {
            if !entry.stream.lock().exited {
                entry.kill();
            }
        }
    }
}

/// Session-detach hook for `serve` (`ServeHooks::session_detached`, D27):
/// detaches every terminal of the process-level manager. A no-op when no
/// manager has been installed. Nothing is ever killed here (D3, D20).
pub fn detach_all_terminals(cx: &App) {
    if let Some(manager) = PtyManager::global(cx) {
        manager.detach_all();
    }
}

fn spawn_kill_escalation(entry: Arc<TerminalEntry>) {
    std::thread::Builder::new()
        .name(format!("pty-kill-{}", entry.id))
        .spawn(move || {
            std::thread::sleep(KILL_ESCALATION_DELAY);
            if !entry.stream.lock().exited {
                entry.kill();
            }
        })
        .log_err();
}

struct TerminalEntry {
    project_id: u64,
    id: u64,
    task_id: Option<String>,
    title: String,
    working_directory: Option<PathBuf>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    child_pid: u32,
    #[cfg(not(unix))]
    killer: Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>,
    input_tx: std::sync::mpsc::Sender<Vec<u8>>,
    /// Bytes handed to `input_tx` but not yet written by the writer thread,
    /// so [`PtyManager::write`] can bound a stuck child's input queue.
    input_queued: Arc<std::sync::atomic::AtomicUsize>,
    outbound_tx: UnboundedSender<OutboundMessage>,
    stream: Mutex<OutputStream>,
    credit: Condvar,
}

impl TerminalEntry {
    fn spawn(
        project_id: u64,
        id: u64,
        options: &SpawnOptions,
        state: &Arc<PtyManagerState>,
    ) -> Result<Arc<Self>> {
        let cols = options.cols.max(1);
        let rows = options.rows.max(1);
        let pair = native_pty_system()
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("failed to open a pty")?;

        let (program, args) = match &options.shell {
            task::Shell::System => {
                let program = util::shell::get_system_shell();
                #[cfg(unix)]
                let args = vec!["-l".to_string()];
                #[cfg(not(unix))]
                let args = Vec::new();
                (program, args)
            }
            task::Shell::Program(program) => (program.clone(), Vec::new()),
            task::Shell::WithArguments { program, args, .. } => (program.clone(), args.clone()),
        };

        let mut command = CommandBuilder::new(&program);
        command.args(&args);
        if let Some(cwd) = &options.working_directory {
            // portable-pty silently falls back to $HOME for a non-existent cwd,
            // so a task asked to run in a deleted directory would run in the
            // home directory instead. Fail the spawn instead, as the brief and
            // the client's `FailedToSpawnTerminal` mapping expect.
            anyhow::ensure!(
                std::fs::metadata(cwd).map(|meta| meta.is_dir()).unwrap_or(false),
                "working directory {cwd:?} does not exist"
            );
            command.cwd(cwd);
        }
        for (key, value) in &options.env {
            command.env(key, value);
        }
        // The directory env captured by `GetDirectoryEnvironment` carries the
        // server shell's `SHLVL`; strip it so the child starts at 1, as
        // `TerminalBuilder::new` does locally.
        command.env_remove("SHLVL");
        if command.get_env("LANG").is_none() {
            command.env("LANG", "en_US.UTF-8");
        }

        let child = pair
            .slave
            .spawn_command(command)
            .with_context(|| format!("failed to spawn {program}"))?;
        // The parent must not keep the slave open, or the reader never sees
        // EOF and every exit waits the full `EXIT_DRAIN_TIMEOUT`.
        drop(pair.slave);

        let child_pid = child
            .process_id()
            .context("spawned child has no process id")?;
        let reader = pair
            .master
            .try_clone_reader()
            .context("failed to clone the pty reader")?;
        let mut writer = pair
            .master
            .take_writer()
            .context("failed to take the pty writer")?;
        #[cfg(not(unix))]
        let killer = child.clone_killer();

        let title = options.title.clone().unwrap_or_else(|| {
            Path::new(&program)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| program.clone())
        });

        let (input_tx, input_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let entry = Arc::new(TerminalEntry {
            project_id,
            id,
            task_id: options.task_id.clone(),
            title,
            working_directory: options.working_directory.clone(),
            master: Mutex::new(pair.master),
            child_pid,
            #[cfg(not(unix))]
            killer: Mutex::new(killer),
            input_tx,
            input_queued: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            outbound_tx: state.outbound_tx.clone(),
            stream: Mutex::new(OutputStream::new(cols, rows)),
            credit: Condvar::new(),
        });

        // The writer must not hold the entry: it owns the receiving end of
        // `input_tx`, and ends when the last entry handle (and so the sender)
        // is gone.
        std::thread::Builder::new()
            .name(format!("pty-writer-{id}"))
            .spawn({
                let input_queued = entry.input_queued.clone();
                move || {
                    use std::io::Write as _;
                    for bytes in input_rx {
                        let len = bytes.len();
                        let result = writer.write_all(&bytes).and_then(|()| writer.flush());
                        input_queued.fetch_sub(len, std::sync::atomic::Ordering::AcqRel);
                        if result.is_err() {
                            break;
                        }
                    }
                }
            })
            .context("failed to spawn the pty writer thread")?;

        std::thread::Builder::new()
            .name(format!("pty-reader-{id}"))
            .spawn({
                let entry = entry.clone();
                move || entry.reader_loop(reader)
            })
            .context("failed to spawn the pty reader thread")?;

        std::thread::Builder::new()
            .name(format!("pty-wait-{id}"))
            .spawn({
                let entry = entry.clone();
                let state = Arc::downgrade(state);
                move || {
                    let exit = wait_for_exit(child, child_pid);
                    entry.on_exited(exit, state);
                }
            })
            .context("failed to spawn the pty wait thread")?;

        Ok(entry)
    }

    fn enqueue(&self, message: OutboundMessage) {
        // The receiver only goes away with the drain task at app teardown, so a
        // failed send has nowhere left to report to.
        self.outbound_tx.unbounded_send(message).ok();
    }

    /// Reads from the master until EOF, filling the ring and, while attached,
    /// streaming chunks within the ack window. Runs on `pty-reader-{id}`.
    fn reader_loop(&self, mut reader: Box<dyn Read + Send>) {
        let mut buffer = vec![0u8; OUTPUT_CHUNK];
        loop {
            let read = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                // EIO on Linux once the slave is closed; anything else is fatal too.
                Err(_) => break,
            };
            let mut stream = self.stream.lock();
            stream.push(&buffer[..read]);
            while stream.attached && stream.sent < stream.end {
                if !stream.window_open() {
                    self.credit.wait(&mut stream);
                    continue;
                }
                let message = stream.take_next_chunk(self.project_id, self.id);
                self.enqueue(OutboundMessage::Output(message));
            }
        }
        let mut stream = self.stream.lock();
        stream.eof = true;
        self.credit.notify_all();
    }

    /// Records the exit, flushes what the reader has left, reports
    /// `TerminalExited` and removes the entry when a close was requested.
    /// Runs on `pty-wait-{id}`.
    fn on_exited(&self, exit: proto::TerminalExit, state: Weak<PtyManagerState>) {
        let mut stream = self.stream.lock();
        stream.exited = true;
        stream.exit = Some(exit.clone());
        // `window_open` is unconditionally true now, so a reader stalled on acks
        // drains what is left before the client stops acking altogether.
        self.credit.notify_all();

        let deadline = Instant::now() + EXIT_DRAIN_TIMEOUT;
        while !stream.eof {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            self.credit.wait_for(&mut stream, deadline - now);
        }

        if stream.attached {
            while stream.sent < stream.end {
                let message = stream.take_next_chunk(self.project_id, self.id);
                self.enqueue(OutboundMessage::Output(message));
            }
        }
        let end_offset = stream.end;
        let close_requested = stream.close_requested;
        if close_requested {
            // The entry is about to leave the map, so a descendant that still
            // holds the slave open (a backgrounded job under job control) must
            // not keep the reader thread enqueueing output for an id nobody can
            // reach through `detach_all`/`kill_all` any more.
            stream.attached = false;
            self.credit.notify_all();
        }
        drop(stream);

        // Remove before reporting, so `list()` is already consistent when the
        // client (or a test) observes `TerminalExited`.
        if close_requested && let Some(state) = state.upgrade() {
            state.terminals.lock().remove(&self.id);
        }
        self.enqueue(OutboundMessage::Exited(proto::TerminalExited {
            project_id: self.project_id,
            terminal_id: self.id,
            exit: Some(exit),
            end_offset,
        }));
    }

    fn detach(&self) {
        let mut stream = self.stream.lock();
        stream.attached = false;
        self.credit.notify_all();
    }

    fn resize_master(&self, cols: u16, rows: u16, exited: bool) -> Result<()> {
        let result = self.master.lock().resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        });
        match result {
            Ok(()) => Ok(()),
            // Nobody is left to notice the size of a finished terminal.
            Err(error) if exited => {
                log::debug!("ignoring resize of exited terminal {}: {error:#}", self.id);
                Ok(())
            }
            Err(error) => Err(error).with_context(|| format!("resizing terminal {}", self.id)),
        }
    }

    /// Best-effort current working directory of the foreground process group
    /// (Linux `/proc`), else the directory the terminal was spawned in.
    fn current_working_directory(&self) -> Option<String> {
        #[cfg(target_os = "linux")]
        {
            if let Some(pgrp) = self.master.lock().process_group_leader()
                && let Ok(cwd) = std::fs::read_link(format!("/proc/{pgrp}/cwd"))
            {
                return Some(cwd.to_string_lossy().into_owned());
            }
        }
        self.working_directory
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
    }

    /// Asks the process group to exit (SIGTERM on unix).
    fn terminate(&self) {
        #[cfg(unix)]
        signal_process_group(self.child_pid, libc::SIGTERM);
        #[cfg(not(unix))]
        self.killer.lock().kill().log_err();
    }

    /// Forces the process group to exit (SIGKILL on unix).
    fn kill(&self) {
        #[cfg(unix)]
        signal_process_group(self.child_pid, libc::SIGKILL);
        #[cfg(not(unix))]
        self.killer.lock().kill().log_err();
    }
}

/// The child is a session leader (`setsid` in portable-pty's `pre_exec`), so
/// its pid is its process group id.
#[cfg(unix)]
fn signal_process_group(pid: u32, signal: libc::c_int) {
    // SAFETY: plain syscall; an ESRCH for an already-gone group is harmless.
    unsafe {
        libc::killpg(pid as libc::pid_t, signal);
    }
}

/// Blocks until the child exits and decodes the status. On unix this uses
/// `waitpid` directly: portable-pty's `Child::wait` forces `exit_code()` to 1
/// for signalled children and reports the signal as a `strsignal` string.
#[cfg(unix)]
fn wait_for_exit(
    child: Box<dyn portable_pty::Child + Send + Sync>,
    pid: u32,
) -> proto::TerminalExit {
    // `std::process::Child` neither kills nor reaps on drop; `waitpid` below
    // is the only reaper.
    drop(child);
    let mut status: libc::c_int = 0;
    loop {
        // SAFETY: `status` outlives the call; `pid` is our own child.
        let rc = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, 0) };
        if rc == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            log::error!("waitpid({pid}) failed: {error}");
            return proto::TerminalExit {
                code: Some(1),
                signal: None,
            };
        }
        if libc::WIFEXITED(status) {
            return proto::TerminalExit {
                code: Some(libc::WEXITSTATUS(status)),
                signal: None,
            };
        }
        if libc::WIFSIGNALED(status) {
            return proto::TerminalExit {
                code: None,
                signal: Some(libc::WTERMSIG(status)),
            };
        }
    }
}

#[cfg(not(unix))]
fn wait_for_exit(
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    pid: u32,
) -> proto::TerminalExit {
    match child.wait() {
        Ok(status) => proto::TerminalExit {
            code: Some(status.exit_code() as i32),
            signal: None,
        },
        Err(error) => {
            log::error!("waiting for child {pid} failed: {error}");
            proto::TerminalExit {
                code: Some(1),
                signal: None,
            }
        }
    }
}

/// Pure, testable state shared by the reader thread and the handlers: the
/// scrollback ring plus the stream/ack bookkeeping.
pub(crate) struct OutputStream {
    /// The last [`SCROLLBACK_CAPACITY`] bytes produced.
    ring: VecDeque<u8>,
    /// Absolute offset of `ring[0]`.
    pub(crate) ring_start: u64,
    /// Absolute offset after the last byte produced.
    pub(crate) end: u64,
    /// Absolute offset after the last byte handed to the sink.
    pub(crate) sent: u64,
    /// Highest offset the client has acknowledged.
    pub(crate) acked: u64,
    /// `false` until the first `AttachTerminal`, and after a detach.
    pub(crate) attached: bool,
    /// The reader saw EOF.
    pub(crate) eof: bool,
    /// `waitpid` returned.
    pub(crate) exited: bool,
    /// `CloseTerminal` (or shutdown) asked for removal after exit.
    pub(crate) close_requested: bool,
    /// The exit status, once `exited`.
    pub(crate) exit: Option<proto::TerminalExit>,
    /// Last window size applied.
    pub(crate) cols: u16,
    /// Last window size applied.
    pub(crate) rows: u16,
}

impl OutputStream {
    pub(crate) fn new(cols: u16, rows: u16) -> Self {
        Self {
            ring: VecDeque::with_capacity(OUTPUT_CHUNK),
            ring_start: 0,
            end: 0,
            sent: 0,
            acked: 0,
            attached: false,
            eof: false,
            exited: false,
            close_requested: false,
            exit: None,
            cols,
            rows,
        }
    }

    /// Appends output, evicting from the front once the ring is full.
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        if len >= SCROLLBACK_CAPACITY {
            self.ring.clear();
            self.ring.extend(&bytes[len - SCROLLBACK_CAPACITY..]);
            self.ring_start = self.end + (len - SCROLLBACK_CAPACITY) as u64;
        } else {
            let overflow = (self.ring.len() + len).saturating_sub(SCROLLBACK_CAPACITY);
            if overflow > 0 {
                self.ring.drain(..overflow);
                self.ring_start += overflow as u64;
            }
            self.ring.extend(bytes);
        }
        self.end += len as u64;
    }

    /// Whether the reader may hand out another chunk: always after the child
    /// exited (the post-exit drain ignores the window), otherwise while fewer
    /// than [`OUTPUT_WINDOW`] bytes are unacknowledged.
    pub(crate) fn window_open(&self) -> bool {
        self.exited || self.sent.saturating_sub(self.acked) < OUTPUT_WINDOW
    }

    /// Where a replay asked to start at `from` actually starts, and whether
    /// the client must clear its grid first because `from` was evicted.
    pub(crate) fn replay_range(&self, from: u64) -> (u64, bool) {
        let clamped = from.clamp(self.ring_start, self.end);
        (clamped, clamped > from)
    }

    /// Copies `[from, to)` out of the ring (clamped to what is retained).
    pub(crate) fn slice(&self, from: u64, to: u64) -> Vec<u8> {
        let from = from.clamp(self.ring_start, self.end);
        let to = to.clamp(from, self.end);
        let start = (from - self.ring_start) as usize;
        let end = (to - self.ring_start) as usize;
        self.ring.range(start..end).copied().collect()
    }

    /// Builds the next `TerminalOutput` from `sent` and advances it.
    fn take_next_chunk(&mut self, project_id: u64, terminal_id: u64) -> proto::TerminalOutput {
        let from = self.sent.max(self.ring_start);
        let to = self.end.min(from + OUTPUT_CHUNK as u64);
        let message = proto::TerminalOutput {
            project_id,
            terminal_id,
            offset: from,
            data: self.slice(from, to),
            reset: false,
        };
        self.sent = to;
        message
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use gpui::TestAppContext;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use task::Shell;

    struct ChannelSink(Sender<OutboundMessage>);

    impl TerminalOutputSink for ChannelSink {
        fn send(&self, message: OutboundMessage) {
            self.0.send(message).ok();
        }
    }

    const MIB: usize = 1024 * 1024;

    fn new_manager(cx: &mut TestAppContext) -> (PtyManager, Receiver<OutboundMessage>) {
        cx.executor().allow_parking();
        let (tx, rx) = channel();
        let manager = cx.update(|cx| PtyManager::new(0, Arc::new(ChannelSink(tx)), cx));
        (manager, rx)
    }

    fn sh(script: &str) -> Shell {
        Shell::WithArguments {
            program: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            title_override: None,
        }
    }

    fn program(program: &str, args: &[&str]) -> Shell {
        Shell::WithArguments {
            program: program.to_string(),
            args: args.iter().map(|arg| arg.to_string()).collect(),
            title_override: None,
        }
    }

    fn options(shell: Shell) -> SpawnOptions {
        SpawnOptions {
            shell,
            working_directory: None,
            env: HashMap::default(),
            cols: 80,
            rows: 24,
            task_id: None,
            title: None,
        }
    }

    fn spawn_attached(manager: &PtyManager, shell: Shell) -> u64 {
        let id = manager.spawn(options(shell)).unwrap();
        manager.attach(id, 0, 80, 24).unwrap();
        id
    }

    fn pump(
        cx: &mut TestAppContext,
        rx: &Receiver<OutboundMessage>,
        into: &mut Vec<OutboundMessage>,
    ) {
        cx.run_until_parked();
        while let Ok(message) = rx.try_recv() {
            into.push(message);
        }
    }

    fn collect_until(
        cx: &mut TestAppContext,
        rx: &Receiver<OutboundMessage>,
        into: &mut Vec<OutboundMessage>,
        timeout: Duration,
        mut condition: impl FnMut(&[OutboundMessage]) -> bool,
    ) {
        let deadline = Instant::now() + timeout;
        loop {
            pump(cx, rx, into);
            if condition(into) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out after {timeout:?} with {} messages: {:?}",
                into.len(),
                into.iter()
                    .map(|message| match message {
                        OutboundMessage::Output(output) => format!(
                            "Output({}, {} bytes, reset={})",
                            output.offset,
                            output.data.len(),
                            output.reset
                        ),
                        OutboundMessage::Exited(exited) =>
                            format!("Exited({:?}, end={})", exited.exit, exited.end_offset),
                    })
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn output_bytes(messages: &[OutboundMessage]) -> Vec<u8> {
        messages
            .iter()
            .filter_map(|message| match message {
                OutboundMessage::Output(output) => Some(output.data.as_slice()),
                OutboundMessage::Exited(_) => None,
            })
            .flatten()
            .copied()
            .collect()
    }

    fn output_text(messages: &[OutboundMessage]) -> String {
        String::from_utf8_lossy(&output_bytes(messages)).into_owned()
    }

    fn last_exited(messages: &[OutboundMessage]) -> Option<&proto::TerminalExited> {
        messages.iter().rev().find_map(|message| match message {
            OutboundMessage::Exited(exited) => Some(exited),
            OutboundMessage::Output(_) => None,
        })
    }

    fn exited_count(messages: &[OutboundMessage]) -> usize {
        messages
            .iter()
            .filter(|message| matches!(message, OutboundMessage::Exited(_)))
            .count()
    }

    fn has_exited(messages: &[OutboundMessage]) -> bool {
        exited_count(messages) > 0
    }

    fn assert_contiguous(messages: &[OutboundMessage]) {
        let mut expected = 0;
        for message in messages {
            if let OutboundMessage::Output(output) = message {
                assert_eq!(output.offset, expected, "offsets must be contiguous");
                expected += output.data.len() as u64;
            }
        }
    }

    fn process_alive(pid: u32) -> bool {
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    fn wait_for_process_gone(pid: u32, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while process_alive(pid) {
            assert!(
                Instant::now() < deadline,
                "process {pid} still alive after {timeout:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn ack_all(manager: &PtyManager, id: u64, messages: &[OutboundMessage]) {
        let end = messages
            .iter()
            .filter_map(|message| match message {
                OutboundMessage::Output(output) => Some(output.offset + output.data.len() as u64),
                OutboundMessage::Exited(_) => None,
            })
            .max()
            .unwrap_or(0);
        manager.ack(id, end);
    }

    #[test]
    fn output_stream_ring_evicts_and_replays() {
        let mut stream = OutputStream::new(80, 24);
        let mut byte = 0u8;
        for _ in 0..3 {
            let chunk: Vec<u8> = (0..MIB)
                .map(|_| {
                    byte = byte.wrapping_add(1);
                    byte
                })
                .collect();
            stream.push(&chunk);
        }
        assert_eq!(stream.ring_start, MIB as u64);
        assert_eq!(stream.end, 3 * MIB as u64);
        assert_eq!(stream.replay_range(0), (MIB as u64, true));
        assert_eq!(
            stream.replay_range(stream.end - 10),
            (stream.end - 10, false)
        );
        let tail = stream.slice(stream.end - 10, stream.end);
        assert_eq!(tail.len(), 10);
        let expected: Vec<u8> = (0..10).map(|i| (3 * MIB - 10 + i + 1) as u8).collect();
        assert_eq!(tail, expected);

        // A single push larger than the ring keeps only its tail.
        let mut stream = OutputStream::new(80, 24);
        stream.push(&vec![7u8; SCROLLBACK_CAPACITY + 5]);
        assert_eq!(stream.ring_start, 5);
        assert_eq!(stream.end, SCROLLBACK_CAPACITY as u64 + 5);
        assert_eq!(stream.slice(0, 5), Vec::<u8>::new());
        assert_eq!(stream.slice(5, 8), vec![7, 7, 7]);
    }

    #[gpui::test]
    fn spawn_echoes_input(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, program("cat", &[]));
        manager.write(id, b"hello\r".to_vec()).unwrap();
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), |messages| {
            output_text(messages).contains("hello")
        });
        assert_contiguous(&messages);
        assert!(matches!(messages[0], OutboundMessage::Output(ref output) if output.offset == 0));
        manager.close(id).unwrap();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
    }

    #[gpui::test]
    fn input_order_is_preserved(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, program("cat", &[]));
        let written: Vec<u8> = (0..200u32).map(|i| (i % 26) as u8 + b'a').collect();
        for byte in &written {
            manager.write(id, vec![*byte]).unwrap();
        }
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), |messages| {
            output_bytes(messages).len() >= written.len()
        });
        // No newline was sent, so the only output is the line discipline's
        // echo of every byte, in order.
        assert_eq!(&output_bytes(&messages)[..written.len()], &written[..]);
        assert_contiguous(&messages);
        manager.close(id).unwrap();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
    }

    #[gpui::test]
    fn resize_reaches_child(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, sh("sleep 0.3; stty size"));
        manager.resize(id, 120, 40).unwrap();
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
        assert!(
            output_text(&messages).contains("40 120"),
            "got {:?}",
            output_text(&messages)
        );
        let info = manager
            .list()
            .into_iter()
            .find(|info| info.terminal_id == id)
            .unwrap();
        assert_eq!((info.cols, info.rows), (120, 40));
    }

    #[gpui::test]
    fn exit_code_and_signal_are_reported(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, sh("exit 3"));
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
        let exited = last_exited(&messages).unwrap();
        assert_eq!(exited.terminal_id, id);
        assert_eq!(
            exited.exit,
            Some(proto::TerminalExit {
                code: Some(3),
                signal: None
            })
        );
        assert_eq!(exited.end_offset, output_bytes(&messages).len() as u64);

        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, sh("kill -TERM $$"));
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
        let exited = last_exited(&messages).unwrap();
        assert_eq!(exited.terminal_id, id);
        assert_eq!(
            exited.exit,
            Some(proto::TerminalExit {
                code: None,
                signal: Some(libc::SIGTERM)
            })
        );
        assert_eq!(exited.end_offset, output_bytes(&messages).len() as u64);
    }

    #[gpui::test]
    fn flow_control_stalls_without_acks(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        // `/dev/zero` through `tr` rather than `yes`: the line discipline
        // would otherwise turn every `\n` into `\r\n` and inflate the count.
        let id = spawn_attached(&manager, sh("head -c 4000000 /dev/zero | tr '\\0' x"));
        let mut messages = Vec::new();
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(500) {
            pump(cx, &rx, &mut messages);
            std::thread::sleep(Duration::from_millis(10));
        }
        let received = output_bytes(&messages).len() as u64;
        assert!(
            received <= OUTPUT_WINDOW + OUTPUT_CHUNK as u64,
            "received {received} bytes without acks"
        );
        assert!(received > 0);
        assert!(!has_exited(&messages));

        collect_until(
            cx,
            &rx,
            &mut messages,
            Duration::from_secs(20),
            |messages| {
                ack_all(&manager, id, messages);
                has_exited(messages)
            },
        );
        assert_contiguous(&messages);
        assert_eq!(output_bytes(&messages).len(), 4_000_000);
        assert_eq!(last_exited(&messages).unwrap().end_offset, 4_000_000);
    }

    #[gpui::test]
    fn attach_replays_from_offset_with_reset(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(
            &manager,
            sh("head -c 3000000 /dev/zero | tr '\\0' x; exit 0"),
        );
        let mut messages = Vec::new();
        collect_until(
            cx,
            &rx,
            &mut messages,
            Duration::from_secs(20),
            |messages| {
                ack_all(&manager, id, messages);
                has_exited(messages)
            },
        );
        let end = last_exited(&messages).unwrap().end_offset;
        assert_eq!(end, 3_000_000);
        assert_eq!(output_bytes(&messages).len(), 3_000_000);

        let outcome = manager.attach(id, 0, 80, 24).unwrap();
        assert_eq!(outcome.replayed_from, end - SCROLLBACK_CAPACITY as u64);
        assert_eq!(outcome.end_offset, end);
        assert_eq!(
            outcome.exit,
            Some(proto::TerminalExit {
                code: Some(0),
                signal: None
            })
        );
        let mut replay = Vec::new();
        collect_until(cx, &rx, &mut replay, Duration::from_secs(10), has_exited);
        let OutboundMessage::Output(first) = &replay[0] else {
            panic!("expected output first, got {:?}", replay[0]);
        };
        assert!(first.reset);
        assert_eq!(first.offset, outcome.replayed_from);
        assert!(matches!(replay.last(), Some(OutboundMessage::Exited(_))));
        assert_eq!(output_bytes(&replay).len(), SCROLLBACK_CAPACITY);
        assert!(
            replay
                .iter()
                .filter_map(|message| match message {
                    OutboundMessage::Output(output) => Some(output.reset),
                    _ => None,
                })
                .skip(1)
                .all(|reset| !reset)
        );

        let outcome = manager.attach(id, end - 5, 80, 24).unwrap();
        assert_eq!(outcome.replayed_from, end - 5);
        let mut replay = Vec::new();
        collect_until(cx, &rx, &mut replay, Duration::from_secs(10), has_exited);
        assert_eq!(replay.len(), 2);
        let OutboundMessage::Output(only) = &replay[0] else {
            panic!("expected output first");
        };
        assert!(!only.reset);
        assert_eq!(only.offset, end - 5);
        assert_eq!(only.data.as_slice(), b"xxxxx".as_slice());
        assert_eq!(
            outcome.exit,
            Some(proto::TerminalExit {
                code: Some(0),
                signal: None
            })
        );
    }

    #[gpui::test]
    fn exit_drain_flushes_before_exited(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let _id = spawn_attached(&manager, sh("printf done; exit 0"));
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), |messages| {
            has_exited(messages) && output_text(messages).contains("done")
        });
        // Let a possible trailing replay settle, then check order.
        std::thread::sleep(Duration::from_millis(50));
        pump(cx, &rx, &mut messages);
        let last_output_ix = messages
            .iter()
            .rposition(|message| matches!(message, OutboundMessage::Output(_)))
            .unwrap();
        let last_exited_ix = messages
            .iter()
            .rposition(|message| matches!(message, OutboundMessage::Exited(_)))
            .unwrap();
        assert!(last_output_ix < last_exited_ix);
        let OutboundMessage::Output(output) = &messages[last_output_ix] else {
            unreachable!()
        };
        assert!(String::from_utf8_lossy(&output.data).contains("done"));
        assert_eq!(
            output.offset + output.data.len() as u64,
            last_exited(&messages).unwrap().end_offset
        );
    }

    #[gpui::test]
    fn close_streams_until_exit_then_removes(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(
            &manager,
            sh("trap 'echo bye; exit 0' TERM; sleep 30 & echo pid=$!; wait"),
        );
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), |messages| {
            output_text(messages).contains("pid=")
        });
        let text = output_text(&messages);
        let sleep_pid: u32 = text
            .split("pid=")
            .nth(1)
            .unwrap()
            .trim()
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(process_alive(sleep_pid));

        manager.close(id).unwrap();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(1), has_exited);
        assert!(output_text(&messages).contains("bye"));
        assert!(manager.list().is_empty());
        assert!(manager.child_pid(id).is_none());
        wait_for_process_gone(sleep_pid, Duration::from_secs(2));

        // A child that ignores SIGTERM is SIGKILLed after the escalation delay.
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, sh("trap '' TERM; sleep 30"));
        std::thread::sleep(Duration::from_millis(100));
        manager.close(id).unwrap();
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(2), has_exited);
        assert_eq!(
            last_exited(&messages).unwrap().exit,
            Some(proto::TerminalExit {
                code: None,
                signal: Some(libc::SIGKILL)
            })
        );
        assert!(manager.list().is_empty());
    }

    #[gpui::test]
    fn kill_all_on_quit(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let first = spawn_attached(&manager, program("sleep", &["30"]));
        let second = spawn_attached(&manager, program("sleep", &["30"]));
        let pids = [
            manager.child_pid(first).unwrap(),
            manager.child_pid(second).unwrap(),
        ];
        manager.kill_all();
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(2), |messages| {
            exited_count(messages) == 2
        });
        let mut exited_ids: Vec<u64> = messages
            .iter()
            .filter(|message| matches!(message, OutboundMessage::Exited(_)))
            .map(|message| message.terminal_id())
            .collect();
        exited_ids.sort();
        let mut expected = vec![first, second];
        expected.sort();
        assert_eq!(exited_ids, expected);
        assert!(manager.list().is_empty());
        for pid in pids {
            wait_for_process_gone(pid, Duration::from_secs(2));
        }

        // Dropping the manager kills its children outright.
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, program("sleep", &["30"]));
        let pid = manager.child_pid(id).unwrap();
        drop(manager);
        wait_for_process_gone(pid, Duration::from_secs(2));
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(2), has_exited);
        assert_eq!(
            last_exited(&messages).unwrap().exit,
            Some(proto::TerminalExit {
                code: None,
                signal: Some(libc::SIGKILL)
            })
        );
    }

    #[gpui::test]
    fn spawn_is_detached_until_attach(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = manager.spawn(options(sh("printf hi; exit 0"))).unwrap();
        let mut messages = Vec::new();
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(200) {
            pump(cx, &rx, &mut messages);
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            messages
                .iter()
                .all(|message| matches!(message, OutboundMessage::Exited(_))),
            "no output may be streamed before the first attach: {messages:?}"
        );

        let outcome = manager.attach(id, 0, 80, 24).unwrap();
        assert_eq!(outcome.replayed_from, 0);
        let mut replay = Vec::new();
        collect_until(cx, &rx, &mut replay, Duration::from_secs(5), |messages| {
            has_exited(messages) && !output_bytes(messages).is_empty()
        });
        assert_eq!(
            replay
                .iter()
                .filter_map(|message| match message {
                    OutboundMessage::Output(output) => Some(output),
                    _ => None,
                })
                .map(|output| (output.offset, output.data.clone(), output.reset))
                .collect::<Vec<_>>(),
            vec![(0, b"hi".to_vec(), false)]
        );
        assert!(matches!(replay.last(), Some(OutboundMessage::Exited(_))));
    }

    #[gpui::test]
    fn detached_child_is_not_blocked(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, sh("head -c 4000000 /dev/zero | tr '\\0' x"));
        manager.detach(id);
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(20), has_exited);
        let end = last_exited(&messages).unwrap().end_offset;
        assert_eq!(end, 4_000_000);
        assert!(output_bytes(&messages).len() as u64 <= OUTPUT_WINDOW + OUTPUT_CHUNK as u64);

        let outcome = manager.attach(id, 0, 80, 24).unwrap();
        assert_eq!(outcome.replayed_from, end - SCROLLBACK_CAPACITY as u64);
        let mut replay = Vec::new();
        collect_until(cx, &rx, &mut replay, Duration::from_secs(10), has_exited);
        let OutboundMessage::Output(first) = &replay[0] else {
            panic!("expected output first");
        };
        assert!(first.reset);
        assert_eq!(first.offset, outcome.replayed_from);
        assert_eq!(output_bytes(&replay).len(), SCROLLBACK_CAPACITY);
    }

    #[gpui::test]
    fn unknown_id_errors(cx: &mut TestAppContext) {
        let (manager, _rx) = new_manager(cx);
        let unknown = 0xdead;
        for error in [
            manager.write(unknown, b"x".to_vec()).unwrap_err(),
            manager.resize(unknown, 80, 24).unwrap_err(),
            manager.attach(unknown, 0, 80, 24).unwrap_err(),
            manager.close(unknown).unwrap_err(),
        ] {
            assert!(
                error.to_string().contains("unknown terminal"),
                "unexpected error: {error:#}"
            );
        }
        manager.ack(unknown, 10);
        manager.detach(unknown);
        assert!(manager.list().is_empty());
    }

    #[gpui::test]
    fn spawn_cap(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let mut ids = Vec::new();
        for _ in 0..MAX_TERMINALS {
            ids.push(manager.spawn(options(program("sleep", &["30"]))).unwrap());
        }
        let error = manager
            .spawn(options(program("sleep", &["30"])))
            .unwrap_err();
        assert!(error.to_string().contains("too many terminals"));
        assert_eq!(manager.list().len(), MAX_TERMINALS);
        manager.kill_all();
        let mut messages = Vec::new();
        collect_until(
            cx,
            &rx,
            &mut messages,
            Duration::from_secs(10),
            |messages| exited_count(messages) == MAX_TERMINALS,
        );
        assert!(manager.list().is_empty());
    }

    #[gpui::test]
    fn shlvl_is_reset_and_lang_defaults(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let mut options = options(sh("echo SHLVL=${SHLVL:-unset} LANG=${LANG:-unset}"));
        options.env.insert("SHLVL".to_string(), "7".to_string());
        let id = manager.spawn(options).unwrap();
        manager.attach(id, 0, 80, 24).unwrap();
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
        let text = output_text(&messages);
        // bash/zsh count themselves to 1 from an empty SHLVL; dash leaves it unset.
        assert!(
            text.contains("SHLVL=1") || text.contains("SHLVL=unset"),
            "got {text:?}"
        );
        assert!(!text.contains("SHLVL=7"));
        assert!(
            text.contains("LANG=") && !text.contains("LANG=unset"),
            "got {text:?}"
        );
    }

    #[gpui::test]
    fn close_after_exit_removes_immediately(cx: &mut TestAppContext) {
        let (manager, rx) = new_manager(cx);
        let id = spawn_attached(&manager, sh("exit 0"));
        let mut messages = Vec::new();
        collect_until(cx, &rx, &mut messages, Duration::from_secs(5), has_exited);
        assert_eq!(manager.list().len(), 1);
        manager.close(id).unwrap();
        assert!(manager.list().is_empty());
        std::thread::sleep(Duration::from_millis(100));
        let before = messages.len();
        pump(cx, &rx, &mut messages);
        assert_eq!(messages.len(), before, "no further messages after close");
        assert!(manager.close(id).is_err());
    }

    #[gpui::test]
    fn install_replaces_global(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let (first_tx, first_rx) = channel();
        let (second_tx, second_rx) = channel();
        let first = cx.update(|cx| PtyManager::install(0, Arc::new(ChannelSink(first_tx)), cx));
        assert!(cx.update(|cx| PtyManager::global(cx).is_some_and(|m| Arc::ptr_eq(&m, &first))));
        let id = first.spawn(options(program("sleep", &["30"]))).unwrap();
        first.attach(id, 0, 80, 24).unwrap();
        let pid = first.child_pid(id).unwrap();

        let second = cx.update(|cx| PtyManager::install(0, Arc::new(ChannelSink(second_tx)), cx));
        assert!(cx.update(|cx| PtyManager::global(cx).is_some_and(|m| Arc::ptr_eq(&m, &second))));
        assert!(second.list().is_empty());
        assert_eq!(first.list().len(), 1);
        assert!(process_alive(pid));

        drop(first);
        wait_for_process_gone(pid, Duration::from_secs(2));
        let mut messages = Vec::new();
        collect_until(
            cx,
            &first_rx,
            &mut messages,
            Duration::from_secs(2),
            has_exited,
        );
        assert_eq!(last_exited(&messages).unwrap().terminal_id, id);
        assert!(second_rx.try_recv().is_err());

        second.detach_all();
        cx.update(|cx| detach_all_terminals(cx));
        assert!(second.list().is_empty());
    }
}
