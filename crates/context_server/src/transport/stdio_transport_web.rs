//! Browser stand-in for the process-backed stdio transport.
//!
//! The browser cannot spawn processes, so [`StdioTransport::new`] always fails;
//! stdio MCP servers run on the remote server instead. The type exists on wasm
//! only so `Client::stdio` and `ContextServer::stdio` keep one signature on
//! every target and callers need no gates of their own.

use std::path::PathBuf;
use std::pin::Pin;

use anyhow::{Result, bail};
use async_trait::async_trait;
use futures::Stream;
use gpui::AsyncApp;

use crate::client::ModelContextServerBinary;
use crate::transport::Transport;

/// A stdio transport that can never be constructed in the browser.
pub struct StdioTransport {
    _private: (),
}

impl StdioTransport {
    /// Always fails: the browser has no processes to spawn `binary` into.
    pub fn new(
        binary: ModelContextServerBinary,
        _working_directory: &Option<PathBuf>,
        _cx: &AsyncApp,
    ) -> Result<Self> {
        bail!(
            "cannot start stdio context server {}: spawning processes is not supported in the browser",
            binary.executable.display()
        )
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn send(&self, _message: String) -> Result<()> {
        bail!("the stdio transport is not available in the browser")
    }

    fn receive(&self) -> Pin<Box<dyn Stream<Item = String> + Send>> {
        Box::pin(futures::stream::empty())
    }

    fn receive_err(&self) -> Pin<Box<dyn Stream<Item = String> + Send>> {
        Box::pin(futures::stream::empty())
    }
}
