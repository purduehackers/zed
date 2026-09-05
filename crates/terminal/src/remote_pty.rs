//! Client half of the remote terminal protocol (b3).
//!
//! A terminal in a project whose connection reports
//! [`remote::RemoteClient::supports_remote_pty`] has no local PTY: the process
//! runs inside `remote_server`, which streams its output as `TerminalOutput`
//! messages. Those bytes are handed to [`RemotePtyHandle::push_output`] and
//! parsed into the same alacritty `Term` a local terminal uses, on the GPUI
//! foreground executor (no OS thread is spawned, which is what the browser
//! build needs). Everything `terminal_view` observes — `Wakeup`, `Bell`,
//! `TitleChanged`, `CloseTerminal` — is produced exactly as it is for a local
//! PTY, so no view code changes.
//!
//! Ownership of the wire is inverted from the local path: the client tells the
//! transport what it wants (`input`, `resize`, `ack`, `resync`, `close`) and
//! the server pushes output back through the handle.

use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
// `std::time::Instant` natively; on wasm `Instant::now()` panics.
use web_time::Instant;

use collections::{HashMap, VecDeque};
use futures::channel::mpsc::{UnboundedSender, unbounded};
use gpui::{BackgroundExecutor, Context, Task, px};
use task::Shell;
use util::paths::PathStyle;
use vte::ansi::Processor;

use crate::{
    Content, CopyTemplate, DEFAULT_SCROLL_HISTORY_LINES, Event, ExitStatus,
    MAX_SCROLL_HISTORY_LINES, PtyEvent, SelectionPhase, SyncHandler, Terminal,
    TerminalBackendEvent, TerminalBounds, TerminalBuilder, TerminalMode, TerminalModeKind,
    TerminalType,
    alacritty::{RegexSearches, new_term, pty_term_config, reset_term},
    normalize_terminal_bounds,
    terminal_settings::{AlternateScroll, CursorShape as SettingsCursorShape},
};

/// Bytes the client will let accumulate before it tells the server how far it
/// has parsed. The server stalls a live stream once `sent - acked` reaches its
/// own window (512 KiB), so this must stay well below it.
pub(crate) const REMOTE_ACK_THRESHOLD: u64 = 64 * 1024;

/// Maximum number of bytes of remote output parsed in a single foreground turn.
/// `subscribe`'s batch loop stops collecting once this much output is queued and
/// yields, so a terminal firehosing output cannot stall the UI thread for an
/// unbounded time (chunks are at most 64 KiB, so a batch parses at most
/// `REMOTE_PARSE_BUDGET + 64 KiB`).
pub(crate) const REMOTE_PARSE_BUDGET: usize = 256 * 1024;

/// The input side of a PTY hosted by the remote server.
///
/// Implemented by `project::terminals::ProtoPtyTransport` in production and by
/// fakes in tests. Only ever called from the foreground thread, but `Terminal`
/// itself has to stay `Send` (`TerminalBuilder::new` builds one on a background
/// task), so implementations must be too — `AnyProtoClient` already is.
pub trait RemotePtyTransport: Send + Sync + 'static {
    /// The server-assigned id of the terminal this transport speaks for.
    fn terminal_id(&self) -> u64;
    /// Sends keyboard (or programmatic) input to the remote PTY.
    fn input(&self, data: Cow<'static, [u8]>);
    /// Resizes the remote PTY's window.
    fn resize(&self, cols: u16, rows: u16);
    /// Reports that everything below `offset` has been parsed into the grid,
    /// which opens the server's send window.
    fn ack(&self, offset: u64);
    /// Asks the server to replay the stream from `from_offset`, after a gap was
    /// detected in the received offsets.
    fn resync(&self, from_offset: u64, cols: u16, rows: u16);
    /// Kills the remote process group. The server keeps streaming (and keeps the
    /// terminal) until the child actually exits.
    fn close(&self);
}

/// The output side of a remote PTY: the sink `project` pushes `TerminalOutput`
/// and `TerminalExited` payloads into.
///
/// Cheap to clone, and every method returns `false` once the terminal entity it
/// feeds has been dropped.
#[derive(Clone)]
pub struct RemotePtyHandle {
    events_tx: UnboundedSender<PtyEvent>,
}

impl RemotePtyHandle {
    /// Queues a chunk of remote output. `offset` is the absolute position of
    /// `data[0]` in the terminal's output stream; `reset` asks for the grid to be
    /// cleared first (the server replayed from after a scrollback eviction).
    pub fn push_output(&self, offset: u64, data: Vec<u8>, reset: bool) -> bool {
        self.events_tx
            .unbounded_send(PtyEvent::Output {
                offset,
                data,
                reset,
            })
            .is_ok()
    }

    /// Queues the remote child's exit. Exactly one of `code` and `signal` is
    /// expected to be set; both being `None` is reported as a plain failure.
    pub fn push_exit(&self, code: Option<i32>, signal: Option<i32>) -> bool {
        self.events_tx
            .unbounded_send(PtyEvent::Event(TerminalBackendEvent::ChildExit(
                exit_status_from_remote(code, signal),
            )))
            .is_ok()
    }

    /// Queues "the server no longer has this terminal", which detaches the
    /// terminal, prints a notice and completes any task with no exit status.
    pub fn push_lost(&self) -> bool {
        self.events_tx.unbounded_send(PtyEvent::RemoteLost).is_ok()
    }
}

/// Everything [`TerminalBuilder::new`] derives from settings, minus the process:
/// the remote server spawns that.
pub struct RemoteTerminalOptions {
    /// The directory the remote shell was spawned in (or restored into). Kept for
    /// workspace persistence; it is a path on the server, never resolved locally.
    pub working_directory: Option<PathBuf>,
    /// Interactive shell or tracked task.
    pub mode: TerminalMode,
    /// Recorded for `clone_builder`-style copies; the server already spawned this.
    pub shell: Shell,
    /// Recorded for copies; the server already applied this.
    pub env: HashMap<String, String>,
    pub cursor_shape: SettingsCursorShape,
    pub alternate_scroll: AlternateScroll,
    pub max_scroll_history_lines: Option<usize>,
    pub path_hyperlink_regexes: Vec<String>,
    pub path_hyperlink_timeout: Duration,
    pub window_id: u64,
    pub path_style: PathStyle,
    pub title_override: Option<String>,
    /// Shell commands written into the PTY right after it opens (venv
    /// activation). Empty when a terminal is reattached: they already ran.
    pub activation_script: Vec<String>,
}

/// Client-side bookkeeping for a terminal whose PTY lives on the server.
pub(crate) struct RemotePtyState {
    pub(crate) transport: Arc<dyn RemotePtyTransport>,
    /// The server-side working directory, persisted by `terminal_view` so the tab
    /// can be recreated after a sandbox stop (D28).
    pub(crate) working_directory: Option<PathBuf>,
    /// The next byte offset this terminal expects; everything below it is in the
    /// grid already.
    pub(crate) next_offset: u64,
    /// The highest offset the server has been told about.
    pub(crate) acked_offset: u64,
    /// False once the terminal was closed, exited or lost: nothing more is sent.
    pub(crate) attached: bool,
    /// Whether the server still holds an entry for this terminal that this
    /// client is responsible for closing. The server keeps an exited terminal
    /// (with its scrollback) until `CloseTerminal`, so this outlives `attached`;
    /// it is cleared once a close was sent, once the server reported the
    /// terminal lost, or when the client deliberately forgets the terminal.
    pub(crate) owns_server_entry: bool,
    /// An `AttachTerminal` was requested to fill a gap; further gap chunks are
    /// dropped without asking again.
    pub(crate) resync_pending: bool,
}

/// Builds the process-exit status reported by a remote PTY.
///
/// The wire carries a code or a signal number; both native representations of
/// `ExitStatus` are opaque, so they are rebuilt from their raw form. A signalled
/// child must report `code() == None` for `task_summary` to say "terminated by
/// signal". The server always sets exactly one field, so the both-`None` case
/// (treated as a clean exit, per the brief formula) is unreachable in practice.
pub(crate) fn exit_status_from_remote(code: Option<i32>, signal: Option<i32>) -> ExitStatus {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        let raw = match signal {
            Some(signal) => signal & 0x7f,
            None => (code.unwrap_or(0) & 0xff) << 8,
        };
        ExitStatus::from_raw(raw)
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::ExitStatusExt as _;
        // Windows has no signals; report the shell convention (128 + signal) so a
        // signalled child still reads as a failure.
        let raw = code
            .map(|code| code as u32)
            .or_else(|| signal.map(|signal| 128 + signal as u32))
            .unwrap_or(1);
        ExitStatus::from_raw(raw)
    }
    #[cfg(not(any(unix, windows)))]
    {
        ExitStatus::from_parts(code, signal)
    }
}

impl TerminalBuilder {
    /// Builds a terminal whose PTY lives on the remote server.
    ///
    /// Unlike [`TerminalBuilder::new`] this is synchronous: there is no local
    /// process to spawn. The returned [`RemotePtyHandle`] is how `project` feeds
    /// `TerminalOutput`/`TerminalExited` payloads back in; output pushed before
    /// [`TerminalBuilder::subscribe`] runs simply queues in the channel.
    pub fn new_remote(
        options: RemoteTerminalOptions,
        transport: Arc<dyn RemotePtyTransport>,
        background_executor: &BackgroundExecutor,
    ) -> (TerminalBuilder, RemotePtyHandle) {
        let RemoteTerminalOptions {
            working_directory,
            mode,
            shell,
            env,
            cursor_shape,
            alternate_scroll,
            max_scroll_history_lines,
            path_hyperlink_regexes,
            path_hyperlink_timeout,
            window_id,
            path_style,
            title_override,
            activation_script,
        } = options;

        let (task, completion_tx) = match mode.0 {
            TerminalModeKind::Interactive => (None, None),
            TerminalModeKind::InteractiveWithCompletion(completion_tx) => {
                (None, Some(completion_tx))
            }
            TerminalModeKind::Task {
                state,
                completion_tx,
            } => (Some(state), Some(completion_tx)),
        };

        let scrolling_history = if task.is_some() {
            // Tasks may produce a lot of output, and nothing is appended after they
            // finish, so allow the maximum scrollback.
            MAX_SCROLL_HISTORY_LINES
        } else {
            max_scroll_history_lines
                .unwrap_or(DEFAULT_SCROLL_HISTORY_LINES)
                .min(MAX_SCROLL_HISTORY_LINES)
        };
        // The PTY config (not the display-only one): a remote terminal is a real
        // terminal and must keep OSC 52 clipboard writes enabled.
        let config = pty_term_config(scrolling_history, cursor_shape);
        let terminal_bounds = normalize_terminal_bounds(TerminalBounds::default());

        let (events_tx, events_rx) = unbounded();
        let term = new_term(
            &config,
            terminal_bounds,
            events_tx.clone(),
            alternate_scroll,
        );

        let no_task = task.is_none();
        let terminal = Terminal {
            task,
            terminal_type: TerminalType::Remote(RemotePtyState {
                transport,
                working_directory,
                next_offset: 0,
                acked_offset: 0,
                attached: true,
                owns_server_entry: true,
                resync_pending: false,
            }),
            subprocess: None,
            completion_tx,
            term,
            term_config: config,
            output_processor: Processor::<SyncHandler>::new(),
            title_override,
            events: VecDeque::with_capacity(10),
            last_content: Content {
                terminal_bounds,
                ..Default::default()
            },
            last_mouse: None,
            mouse_down_position: None,
            matches: Vec::new(),
            selection_head: None,
            breadcrumb_text: String::new(),
            scroll_px: px(0.),
            next_link_id: 0,
            selection_phase: SelectionPhase::Ended,
            hyperlink_regex_searches: RegexSearches::new(
                &path_hyperlink_regexes,
                path_hyperlink_timeout,
            ),
            vi_mode_enabled: false,
            is_remote_terminal: true,
            last_mouse_move_time: Instant::now(),
            last_hyperlink_search_position: None,
            mouse_down_hyperlink: None,
            #[cfg(windows)]
            shell_program: None,
            activation_script: activation_script.clone(),
            template: CopyTemplate {
                shell,
                env,
                cursor_shape,
                alternate_scroll,
                max_scroll_history_lines,
                path_hyperlink_regexes,
                path_hyperlink_timeout,
                window_id,
            },
            child_exited: None,
            keyboard_input_sent: false,
            init_command_startup_marker: None,
            init_command_startup_tx: None,
            event_loop_task: Task::ready(Ok(())),
            sync_update_expiry: None,
            background_executor: background_executor.clone(),
            path_style,
            // The remote working directory is tracked in `RemotePtyState`; the
            // client cannot follow a server shell's `cd`, exactly as over ssh.
            cwd_history: Vec::new(),
            pending_cwd_boundary: None,
            #[cfg(any(test, feature = "test-support"))]
            input_log: Vec::new(),
            #[cfg(test)]
            suppress_hyperlink_throttle_once: false,
            #[cfg(any(test, feature = "test-support"))]
            pty_write_log: Default::default(),
        };

        if !activation_script.is_empty() && no_task {
            let shell_kind = terminal
                .template
                .shell
                .shell_kind(terminal.path_style.is_windows());
            for activation_script in activation_script {
                terminal.write_to_pty(activation_script.into_bytes());
                terminal.write_to_pty(b"\x0d");
            }
            terminal.write_to_pty(shell_kind.clear_screen_command().as_bytes());
            terminal.write_to_pty(b"\x0d");
        }

        (
            TerminalBuilder {
                terminal,
                events_rx,
            },
            RemotePtyHandle { events_tx },
        )
    }
}

impl Terminal {
    /// Whether this terminal's PTY lives on the remote server.
    pub fn is_remote_pty(&self) -> bool {
        matches!(self.terminal_type, TerminalType::Remote(_))
    }

    /// The server-assigned id of this terminal, if it is a remote PTY.
    pub fn remote_terminal_id(&self) -> Option<u64> {
        match &self.terminal_type {
            TerminalType::Remote(state) => Some(state.transport.terminal_id()),
            _ => None,
        }
    }

    /// The next output offset this terminal expects, i.e. how much of the remote
    /// stream it has parsed. Used to resume the stream after a reconnect.
    pub fn remote_next_offset(&self) -> Option<u64> {
        match &self.terminal_type {
            TerminalType::Remote(state) => Some(state.next_offset),
            _ => None,
        }
    }

    /// The working directory this remote terminal was spawned in, on the server.
    /// Persisted by `terminal_view` so the tab can be restored (D4/D28).
    pub fn remote_working_directory(&self) -> Option<&Path> {
        match &self.terminal_type {
            TerminalType::Remote(state) => state.working_directory.as_deref(),
            _ => None,
        }
    }

    /// The title `project` set for this terminal, if any. Persisted alongside the
    /// remote terminal id so a restored tab keeps its name.
    pub fn title_override(&self) -> Option<&str> {
        self.title_override.as_deref()
    }

    /// Feeds a chunk of remote output into the grid.
    ///
    /// Chunks that were already parsed are dropped, overlapping prefixes trimmed,
    /// and a gap answered with a single replay request; the grid is only ever
    /// cleared when the server says so with `reset`.
    pub(crate) fn process_remote_output(
        &mut self,
        offset: u64,
        data: Vec<u8>,
        reset: bool,
        cx: &mut Context<Self>,
    ) {
        if !self.is_remote_pty() {
            return;
        }
        let end = offset.saturating_add(data.len() as u64);

        if reset {
            {
                let mut term = self.term.lock();
                reset_term(&mut term);
            }
            self.reset_cwd_history();
            if let TerminalType::Remote(state) = &mut self.terminal_type {
                state.next_offset = offset;
                state.acked_offset = offset;
                state.resync_pending = false;
            }
            if data.is_empty() {
                // The server had nothing left to replay, but the grid was just
                // cleared and has to be repainted.
                cx.emit(Event::Wakeup);
                return;
            }
        }

        let next_offset = self.remote_next_offset().unwrap_or_default();
        if end <= next_offset {
            // A replay of bytes already in the grid.
            return;
        }
        if offset > next_offset {
            let cols = self.last_content.terminal_bounds.num_columns() as u16;
            let rows = self.last_content.terminal_bounds.num_lines() as u16;
            if let TerminalType::Remote(state) = &mut self.terminal_type
                && !state.resync_pending
            {
                log::warn!(
                    "remote terminal {}: output gap at offset {} (expected {}), requesting replay",
                    state.transport.terminal_id(),
                    offset,
                    state.next_offset
                );
                state.resync_pending = true;
                state.transport.resync(state.next_offset, cols, rows);
            }
            return;
        }

        let already_parsed = (next_offset - offset) as usize;
        {
            let mut term = self.term.lock();
            // No CRLF conversion: PTY output already went through a line
            // discipline, so inserting carriage returns would corrupt it.
            self.output_processor
                .advance(&mut *term, &data[already_parsed..]);
        }

        if let TerminalType::Remote(state) = &mut self.terminal_type {
            state.resync_pending = false;
            state.next_offset = end;
            if state.next_offset - state.acked_offset >= REMOTE_ACK_THRESHOLD {
                state.acked_offset = state.next_offset;
                state.transport.ack(state.next_offset);
            }
        }

        // `subscribe` coalesces `Wakeup`s and processes them *before* the events
        // of a batch, so the marker scan and the repaint have to happen here.
        self.detect_init_command_startup_marker();
        cx.emit(Event::Wakeup);
        self.expire_sync_update(cx);
    }

    /// Reports how far the grid has been fed, if the server has not been told yet.
    /// Called at the end of every processing batch so acks are never delayed by
    /// less than `REMOTE_ACK_THRESHOLD` bytes of trailing output.
    pub(crate) fn flush_remote_ack(&mut self) {
        if let TerminalType::Remote(state) = &mut self.terminal_type
            && state.next_offset > state.acked_offset
        {
            state.acked_offset = state.next_offset;
            state.transport.ack(state.next_offset);
        }
    }

    /// Handles "the server no longer has this terminal": prints a notice and
    /// finishes the terminal as if its process had gone away.
    ///
    /// A task completes with no exit status. An interactive shell keeps its
    /// tab (with the notice) unless the user had typed into it, in which case
    /// it closes like a shell that exited.
    pub(crate) fn process_remote_lost(&mut self, cx: &mut Context<Self>) {
        let TerminalType::Remote(state) = &mut self.terminal_type else {
            return;
        };
        if !state.attached {
            return;
        }
        state.attached = false;
        // Nothing is left on the server to close.
        state.owns_server_entry = false;
        self.append_notice("\r\n[remote terminal is no longer available]\r\n");
        if self.task.is_some() {
            self.process_event(TerminalBackendEvent::Exit, cx);
            return;
        }
        self.complete_init_command_startup_handshake();
        if self.keyboard_input_sent {
            cx.emit(Event::CloseTerminal);
        }
        cx.emit(Event::Wakeup);
    }

    /// Writes a client-generated line into the grid (not to the PTY).
    fn append_notice(&mut self, text: &str) {
        let mut term = self.term.lock();
        self.output_processor.advance(&mut *term, text.as_bytes());
    }

    /// Ends a synchronized update (`CSI ? 2026 h`) whose end sequence never
    /// arrived.
    ///
    /// Alacritty's event loop does this for local PTYs; nothing does it for
    /// output parsed straight into `output_processor`, so a TUI that starts a
    /// synchronized update and dies would otherwise freeze the grid until the
    /// next chunk (D17).
    pub(crate) fn expire_sync_update(&mut self, cx: &mut Context<Self>) {
        let Some(deadline) = self.output_processor.sync_timeout().sync_timeout() else {
            self.sync_update_expiry = None;
            return;
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        self.sync_update_expiry = Some(cx.spawn(async move |terminal, cx| {
            cx.background_executor().timer(timeout).await;
            terminal
                .update(cx, |terminal, cx| {
                    if terminal
                        .output_processor
                        .sync_timeout()
                        .sync_timeout()
                        .is_none()
                    {
                        return;
                    }
                    let mut term = terminal.term.lock();
                    terminal.output_processor.stop_sync(&mut *term);
                    drop(term);
                    cx.emit(Event::Wakeup);
                })
                .ok();
        }));
    }

    /// Detaches from the remote PTY without telling the server, as a client that
    /// vanished (page reload, crashed tab) would. The terminal stays alive on the
    /// server for the next session to reattach or reap.
    #[cfg(any(test, feature = "test-support"))]
    pub fn forget_remote_transport(&mut self) {
        if let TerminalType::Remote(state) = &mut self.terminal_type {
            state.attached = false;
            state.owns_server_entry = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TerminalBounds, terminal_settings::AlternateScroll};
    use gpui::{AppContext as _, Entity, TestAppContext, px};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use task::SpawnInTerminal;

    #[derive(Default)]
    struct FakeTransport {
        inputs: Mutex<Vec<Vec<u8>>>,
        resizes: Mutex<Vec<(u16, u16)>>,
        acks: Mutex<Vec<u64>>,
        resyncs: Mutex<Vec<u64>>,
        closes: AtomicUsize,
    }

    impl RemotePtyTransport for Arc<FakeTransport> {
        fn terminal_id(&self) -> u64 {
            7
        }

        fn input(&self, data: Cow<'static, [u8]>) {
            self.inputs.lock().push(data.into_owned());
        }

        fn resize(&self, cols: u16, rows: u16) {
            self.resizes.lock().push((cols, rows));
        }

        fn ack(&self, offset: u64) {
            self.acks.lock().push(offset);
        }

        fn resync(&self, from_offset: u64, _cols: u16, _rows: u16) {
            self.resyncs.lock().push(from_offset);
        }

        fn close(&self) {
            self.closes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn init_test(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
        });
    }

    fn remote_options(mode: TerminalMode) -> RemoteTerminalOptions {
        RemoteTerminalOptions {
            working_directory: Some(PathBuf::from("/workspaces/project")),
            mode,
            shell: Shell::System,
            env: HashMap::default(),
            cursor_shape: SettingsCursorShape::default(),
            alternate_scroll: AlternateScroll::On,
            max_scroll_history_lines: None,
            path_hyperlink_regexes: Vec::new(),
            path_hyperlink_timeout: Duration::from_millis(100),
            window_id: 0,
            path_style: PathStyle::Unix,
            title_override: Some("host — Terminal".to_string()),
            activation_script: Vec::new(),
        }
    }

    fn build_remote_terminal(
        cx: &mut TestAppContext,
        mode: TerminalMode,
    ) -> (Entity<Terminal>, Arc<FakeTransport>, RemotePtyHandle) {
        init_test(cx);
        let transport = Arc::new(FakeTransport::default());
        let (builder, handle) = cx.update(|cx| {
            TerminalBuilder::new_remote(
                remote_options(mode),
                Arc::new(transport.clone()),
                cx.background_executor(),
            )
        });
        let terminal = cx.new(|cx| builder.subscribe(cx));
        (terminal, transport, handle)
    }

    fn content(terminal: &Entity<Terminal>, cx: &mut TestAppContext) -> String {
        terminal.update(cx, |terminal, _| terminal.get_content())
    }

    #[gpui::test]
    async fn lone_chunk_repaints(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());
        let wakeups = Arc::new(AtomicUsize::new(0));
        let _subscription = cx.update(|cx| {
            let wakeups = wakeups.clone();
            cx.subscribe(&terminal, move |_, event: &Event, _| {
                if event == &Event::Wakeup {
                    wakeups.fetch_add(1, Ordering::SeqCst);
                }
            })
        });

        assert!(handle.push_output(0, b"hello\r\n".to_vec(), false));
        cx.run_until_parked();

        assert_eq!(wakeups.load(Ordering::SeqCst), 1);
        assert!(content(&terminal, cx).contains("hello"));
        assert_eq!(*transport.acks.lock(), vec![7]);
        assert_eq!(
            terminal.update(cx, |terminal, _| terminal.remote_next_offset()),
            Some(7)
        );
    }

    #[gpui::test]
    async fn duplicate_prefix_is_dropped(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        handle.push_output(0, b"abc".to_vec(), false);
        cx.run_until_parked();
        handle.push_output(1, b"bcdef".to_vec(), false);
        cx.run_until_parked();
        handle.push_output(0, b"abc".to_vec(), false);
        cx.run_until_parked();

        assert!(content(&terminal, cx).contains("abcdef"));
        assert_eq!(
            terminal.update(cx, |terminal, _| terminal.remote_next_offset()),
            Some(6)
        );
        assert_eq!(*transport.acks.lock(), vec![3, 6]);
    }

    #[gpui::test]
    async fn reset_clears_grid(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        handle.push_output(0, b"old".to_vec(), false);
        cx.run_until_parked();
        handle.push_output(100, b"new".to_vec(), true);
        cx.run_until_parked();

        let content = content(&terminal, cx);
        assert!(content.contains("new"), "{content:?}");
        assert!(!content.contains("old"), "{content:?}");
        assert_eq!(
            terminal.update(cx, |terminal, _| terminal.remote_next_offset()),
            Some(103)
        );
    }

    #[gpui::test]
    async fn gap_requests_resync_without_clearing(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        handle.push_output(0, b"abc".to_vec(), false);
        cx.run_until_parked();
        handle.push_output(10, b"xyz".to_vec(), false);
        cx.run_until_parked();

        assert!(content(&terminal, cx).contains("abc"));
        assert!(!content(&terminal, cx).contains("xyz"));
        assert_eq!(*transport.resyncs.lock(), vec![3]);
        assert_eq!(
            terminal.update(cx, |terminal, _| terminal.remote_next_offset()),
            Some(3)
        );

        // A second gap chunk does not ask again.
        handle.push_output(20, b"q".to_vec(), false);
        cx.run_until_parked();
        assert_eq!(*transport.resyncs.lock(), vec![3]);

        // The replay lands, and a later gap asks again.
        handle.push_output(3, b"def".to_vec(), false);
        cx.run_until_parked();
        assert!(content(&terminal, cx).contains("abcdef"));
        handle.push_output(40, b"!".to_vec(), false);
        cx.run_until_parked();
        assert_eq!(*transport.resyncs.lock(), vec![3, 6]);
    }

    #[gpui::test]
    async fn input_and_resize_go_to_transport(cx: &mut TestAppContext) {
        init_test(cx);
        let transport = Arc::new(FakeTransport::default());
        let window = cx.add_empty_window();
        let (builder, _handle) = window.update(|_, cx| {
            TerminalBuilder::new_remote(
                remote_options(TerminalMode::interactive()),
                Arc::new(transport.clone()),
                cx.background_executor(),
            )
        });
        let terminal = window.new(|cx| builder.subscribe(cx));

        terminal.update(window, |terminal, _| terminal.input(b"ls\r".to_vec()));
        assert_eq!(*transport.inputs.lock(), vec![b"ls\r".to_vec()]);

        let terminal_bounds = TerminalBounds::new(
            px(10.),
            px(5.),
            gpui::bounds(
                gpui::point(px(0.), px(0.)),
                gpui::size(px(5. * 80.), px(10. * 24.)),
            ),
        );
        window.update(|window, cx| {
            terminal.update(cx, |terminal, cx| {
                terminal.set_size(terminal_bounds);
                terminal.sync(window, cx);
            })
        });
        assert_eq!(*transport.resizes.lock(), vec![(80, 24)]);

        // An identical resize is deduplicated before it reaches the transport.
        window.update(|window, cx| {
            terminal.update(cx, |terminal, cx| {
                terminal.set_size(terminal_bounds);
                terminal.sync(window, cx);
            })
        });
        assert_eq!(*transport.resizes.lock(), vec![(80, 24)]);
    }

    #[gpui::test]
    async fn crlf_is_not_inserted(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        handle.push_output(0, b"a\nb".to_vec(), false);
        cx.run_until_parked();

        let column = terminal.update(cx, |terminal, _| {
            terminal.term.lock().grid().cursor.point.column.0
        });
        assert_eq!(column, 2, "a bare LF must not be rewritten to CRLF");
    }

    fn task_mode() -> TerminalMode {
        TerminalMode::task(SpawnInTerminal {
            label: "t".to_string(),
            full_label: "t".to_string(),
            show_summary: true,
            ..SpawnInTerminal::default()
        })
    }

    #[gpui::test]
    async fn exit_completes_task_with_summary(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, task_mode());

        assert!(handle.push_exit(Some(2), None));
        cx.run_until_parked();

        let status = terminal
            .update(cx, |terminal, cx| terminal.wait_for_completed_task(cx))
            .await;
        assert_eq!(status.and_then(|status| status.code()), Some(2));
        terminal.update(cx, |terminal, _| {
            assert_eq!(
                terminal.task().map(|task| task.status),
                Some(crate::TaskStatus::Completed { success: false })
            );
            assert!(!terminal.has_active_pty_resources());
        });
        assert!(content(&terminal, cx).contains("finished with exit code: 2"));

        // A second exit is ignored.
        handle.push_exit(Some(0), None);
        cx.run_until_parked();
        terminal.update(cx, |terminal, _| {
            assert_eq!(
                terminal.task().map(|task| task.status),
                Some(crate::TaskStatus::Completed { success: false })
            );
        });
    }

    #[cfg(unix)]
    #[gpui::test]
    async fn signal_exit_is_reported(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, task_mode());

        handle.push_exit(None, Some(15));
        cx.run_until_parked();

        let status = terminal
            .update(cx, |terminal, cx| terminal.wait_for_completed_task(cx))
            .await;
        let status = status.expect("task completed");
        assert_eq!(status.code(), None);
        assert!(content(&terminal, cx).contains("terminated by signal: 15"));
    }

    #[gpui::test]
    async fn interactive_exit_closes_once(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());
        let closes = Arc::new(AtomicUsize::new(0));
        let _subscription = cx.update(|cx| {
            let closes = closes.clone();
            cx.subscribe(&terminal, move |_, event: &Event, _| {
                if event == &Event::CloseTerminal {
                    closes.fetch_add(1, Ordering::SeqCst);
                }
            })
        });

        terminal.update(cx, |terminal, _| terminal.input(b"exit\r".to_vec()));
        handle.push_exit(Some(0), None);
        cx.run_until_parked();
        handle.push_exit(Some(0), None);
        cx.run_until_parked();

        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn osc_title_still_reaches_breadcrumbs(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        handle.push_output(0, b"\x1b]0;my title\x07".to_vec(), false);
        cx.run_until_parked();

        terminal.update(cx, |terminal, _| {
            assert_eq!(terminal.breadcrumb_text, "my title");
            assert_eq!(terminal.title(true), "host — Terminal");
        });
    }

    #[gpui::test]
    async fn drop_closes_transport_once(cx: &mut TestAppContext) {
        let (terminal, transport, _handle) = build_remote_terminal(cx, TerminalMode::interactive());

        terminal.update(cx, |terminal, _| {
            terminal.release_pty_resources();
            terminal.release_pty_resources();
        });
        assert_eq!(transport.closes.load(Ordering::SeqCst), 1);

        drop(terminal);
        cx.run_until_parked();
        assert_eq!(transport.closes.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn exited_terminal_closes_on_drop(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        handle.push_exit(Some(0), None);
        cx.run_until_parked();
        terminal.update(cx, |terminal, _| {
            assert!(!terminal.has_active_pty_resources());
        });
        // The exit alone tells the server nothing: it keeps the entry and its
        // scrollback until the tab goes away.
        assert_eq!(transport.closes.load(Ordering::SeqCst), 0);

        drop(terminal);
        // An empty update flushes effects, which is when GPUI releases the
        // dropped entity and runs `Drop for Terminal`.
        cx.update(|_| {});
        cx.run_until_parked();
        assert_eq!(transport.closes.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn kill_active_task_stays_attached_until_exit(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, task_mode());

        terminal.update(cx, |terminal, _| terminal.kill_active_task());
        assert_eq!(transport.closes.load(Ordering::SeqCst), 1);
        terminal.update(cx, |terminal, _| {
            assert!(terminal.has_active_pty_resources())
        });

        // Output produced between the kill and the exit still reaches the grid.
        handle.push_output(0, b"bye".to_vec(), false);
        cx.run_until_parked();
        assert!(content(&terminal, cx).contains("bye"));

        handle.push_exit(None, Some(15));
        cx.run_until_parked();
        terminal.update(cx, |terminal, _| {
            assert!(!terminal.has_active_pty_resources());
            assert_ne!(terminal.task().map(|task| task.status), None);
        });

        // The kill already asked the server to remove the entry on exit, so the
        // drop sends no second close.
        drop(terminal);
        cx.update(|_| {});
        cx.run_until_parked();
        assert_eq!(transport.closes.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn lost_marks_detached_and_finishes(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, task_mode());

        assert!(handle.push_lost());
        cx.run_until_parked();

        assert!(content(&terminal, cx).contains("no longer available"));
        let status = terminal
            .update(cx, |terminal, cx| terminal.wait_for_completed_task(cx))
            .await;
        assert!(status.is_none());
        terminal.update(cx, |terminal, _| {
            assert!(!terminal.has_active_pty_resources());
            terminal.input(b"ls\r".to_vec());
        });
        assert!(transport.inputs.lock().is_empty());

        // The server no longer has the terminal, so there is nothing to close.
        drop(terminal);
        cx.update(|_| {});
        cx.run_until_parked();
        assert_eq!(transport.closes.load(Ordering::SeqCst), 0);
    }

    #[gpui::test]
    async fn lost_interactive_shell_keeps_tab_until_input_was_sent(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());
        let closes = Arc::new(AtomicUsize::new(0));
        let wakeups = Arc::new(AtomicUsize::new(0));
        let _subscription = cx.update(|cx| {
            let closes = closes.clone();
            let wakeups = wakeups.clone();
            cx.subscribe(&terminal, move |_, event: &Event, _| match event {
                Event::CloseTerminal => {
                    closes.fetch_add(1, Ordering::SeqCst);
                }
                Event::Wakeup => {
                    wakeups.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            })
        });

        // Nothing was typed: the tab stays open so the notice can be read.
        handle.push_output(0, b"prompt$ ".to_vec(), false);
        cx.run_until_parked();
        assert!(handle.push_lost());
        cx.run_until_parked();

        assert_eq!(closes.load(Ordering::SeqCst), 0);
        assert!(wakeups.load(Ordering::SeqCst) >= 2);
        let grid = content(&terminal, cx);
        assert!(grid.contains("prompt$"), "{grid:?}");
        assert!(grid.contains("no longer available"), "{grid:?}");
        terminal.update(cx, |terminal, _| {
            assert!(!terminal.has_active_pty_resources());
        });
        drop(terminal);
        cx.update(|_| {});
        cx.run_until_parked();
        assert_eq!(transport.closes.load(Ordering::SeqCst), 0);

        // A shell the user had typed into closes like one that exited.
        let (terminal, _transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());
        let closes = Arc::new(AtomicUsize::new(0));
        let _subscription = cx.update(|cx| {
            let closes = closes.clone();
            cx.subscribe(&terminal, move |_, event: &Event, _| {
                if event == &Event::CloseTerminal {
                    closes.fetch_add(1, Ordering::SeqCst);
                }
            })
        });
        terminal.update(cx, |terminal, _| terminal.input(b"ls\r".to_vec()));
        handle.push_lost();
        cx.run_until_parked();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[gpui::test]
    async fn ack_flushes_at_batch_end_and_threshold(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        let chunk = vec![b'.'; 30 * 1024];
        for index in 0..3 {
            handle.push_output(index * chunk.len() as u64, chunk.clone(), false);
        }
        cx.run_until_parked();
        terminal.update(cx, |terminal, _| {
            assert_eq!(
                terminal.remote_next_offset(),
                transport.acks.lock().last().copied()
            );
        });

        let acks_before = transport.acks.lock().len();
        let big = vec![b'.'; 70 * 1024];
        handle.push_output(3 * chunk.len() as u64, big, false);
        cx.run_until_parked();
        assert!(transport.acks.lock().len() > acks_before);
        terminal.update(cx, |terminal, _| {
            assert_eq!(
                terminal.remote_next_offset(),
                transport.acks.lock().last().copied()
            );
        });
    }

    #[gpui::test]
    async fn parse_budget_splits_large_batches(cx: &mut TestAppContext) {
        let (terminal, transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        let chunk = vec![b'.'; 64 * 1024];
        for index in 0..10u64 {
            handle.push_output(index * chunk.len() as u64, chunk.clone(), false);
        }
        cx.run_until_parked();

        assert!(
            transport.acks.lock().len() >= 2,
            "the parse budget should split 640 KiB into several batches: {:?}",
            transport.acks.lock()
        );
        terminal.update(cx, |terminal, _| {
            assert_eq!(terminal.remote_next_offset(), Some(10 * 64 * 1024));
        });
    }

    #[gpui::test]
    async fn forget_remote_transport_sends_no_close(cx: &mut TestAppContext) {
        let (terminal, transport, _handle) = build_remote_terminal(cx, TerminalMode::interactive());

        terminal.update(cx, |terminal, _| {
            assert_eq!(
                terminal.remote_working_directory(),
                Some(Path::new("/workspaces/project"))
            );
            assert_eq!(terminal.title_override(), Some("host — Terminal"));
            terminal.forget_remote_transport();
            assert!(!terminal.has_active_pty_resources());
            terminal.input(b"ls\r".to_vec());
        });

        drop(terminal);
        cx.run_until_parked();
        assert_eq!(transport.closes.load(Ordering::SeqCst), 0);
        assert!(transport.inputs.lock().is_empty());
    }

    #[gpui::test]
    async fn sync_update_expires(cx: &mut TestAppContext) {
        let (terminal, _transport, handle) = build_remote_terminal(cx, TerminalMode::interactive());

        // Begin a synchronized update and never end it.
        handle.push_output(0, b"\x1b[?2026hhello".to_vec(), false);
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_secs(2));
        cx.run_until_parked();

        assert!(content(&terminal, cx).contains("hello"));
    }
}
