//! Synchronized-update timeout for the terminal's own parser (`write_output` and
//! the remote PTY path). Natively the alacritty event loop owns the PTY parser's
//! timeout; this one covers `Terminal::output_processor`.
//!
//! vte's `StdSyncHandler` calls `std::time::Instant::now()` when a program begins a
//! synchronized update (`CSI ? 2026 h`), which panics on wasm32-unknown-unknown, so
//! the browser build uses a `web_time`-based handler with the same shape.

#[cfg(target_family = "wasm")]
use std::time::Duration;

/// The parser's timeout handler: vte's own natively, [`WebSyncHandler`] in the browser.
#[cfg(not(target_family = "wasm"))]
pub(crate) type SyncHandler = vte::ansi::StdSyncHandler;

/// The parser's timeout handler: vte's own natively, [`WebSyncHandler`] in the browser.
#[cfg(target_family = "wasm")]
pub(crate) type SyncHandler = WebSyncHandler;

/// `vte::ansi::StdSyncHandler` over `web_time::Instant`, for the browser.
#[cfg(target_family = "wasm")]
#[derive(Default)]
pub(crate) struct WebSyncHandler {
    timeout: Option<web_time::Instant>,
}

#[cfg(target_family = "wasm")]
impl WebSyncHandler {
    /// Synchronized update expiration time; the same accessor `StdSyncHandler` has.
    pub fn sync_timeout(&self) -> Option<web_time::Instant> {
        self.timeout
    }
}

#[cfg(target_family = "wasm")]
impl vte::ansi::Timeout for WebSyncHandler {
    fn set_timeout(&mut self, duration: Duration) {
        self.timeout = Some(web_time::Instant::now() + duration);
    }

    fn clear_timeout(&mut self) {
        self.timeout = None;
    }

    fn pending_timeout(&self) -> bool {
        self.timeout.is_some()
    }
}
