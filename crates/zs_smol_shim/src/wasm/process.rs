//! `smol::process` on wasm32-unknown-unknown: async-process 2's surface (sized to the calls in
//! the browser crate set). `Command` keeps a real `std::process::Command` so builder calls and
//! `From<std::process::Command>` work; spawning fails with `io::ErrorKind::Unsupported`
//! immediately. Processes run in the sandbox, reached over the remote protocol.
// The `stdin`/`stdout`/`stderr` forwarders call the `std::process::Command` methods clippy.toml
// bans; they only configure a command that can never spawn here.
#![allow(clippy::disallowed_methods)]

use std::ffi::OsStr;
use std::future::{Future, ready};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::{AsyncRead, AsyncWrite};

pub use std::process::{ExitStatus, Output, Stdio};

use super::unsupported;

/// A builder for a child process; `spawn`, `status` and `output` fail in the browser.
pub struct Command {
    inner: std::process::Command,
    kill_on_drop: bool,
    reap_on_drop: bool,
}

impl Command {
    /// Creates a builder for `program`.
    pub fn new<S: AsRef<OsStr>>(program: S) -> Command {
        Command {
            inner: std::process::Command::new(program),
            kill_on_drop: false,
            reap_on_drop: true,
        }
    }

    /// Adds one argument.
    pub fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Command {
        self.inner.arg(arg);
        self
    }

    /// Adds several arguments.
    pub fn args<I, S>(&mut self, args: I) -> &mut Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inner.args(args);
        self
    }

    /// Sets one environment variable.
    pub fn env<K: AsRef<OsStr>, V: AsRef<OsStr>>(&mut self, key: K, val: V) -> &mut Command {
        self.inner.env(key, val);
        self
    }

    /// Sets several environment variables.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Command
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.envs(vars);
        self
    }

    /// Removes one environment variable.
    pub fn env_remove<K: AsRef<OsStr>>(&mut self, key: K) -> &mut Command {
        self.inner.env_remove(key);
        self
    }

    /// Clears the environment.
    pub fn env_clear(&mut self) -> &mut Command {
        self.inner.env_clear();
        self
    }

    /// Sets the working directory.
    pub fn current_dir<P: AsRef<Path>>(&mut self, dir: P) -> &mut Command {
        self.inner.current_dir(dir);
        self
    }

    /// Configures stdin.
    pub fn stdin<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Command {
        self.inner.stdin(cfg);
        self
    }

    /// Configures stdout.
    pub fn stdout<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Command {
        self.inner.stdout(cfg);
        self
    }

    /// Configures stderr.
    pub fn stderr<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Command {
        self.inner.stderr(cfg);
        self
    }

    /// Sets whether the child is killed when its `Child` is dropped (no effect here).
    pub fn kill_on_drop(&mut self, kill: bool) -> &mut Command {
        self.kill_on_drop = kill;
        self
    }

    /// Sets whether the child is reaped in the background when dropped (no effect here).
    pub fn reap_on_drop(&mut self, reap: bool) -> &mut Command {
        self.reap_on_drop = reap;
        self
    }

    /// The program to run.
    pub fn get_program(&self) -> &OsStr {
        self.inner.get_program()
    }

    /// The configured arguments.
    pub fn get_args(&self) -> std::process::CommandArgs<'_> {
        self.inner.get_args()
    }

    /// The configured environment changes.
    pub fn get_envs(&self) -> std::process::CommandEnvs<'_> {
        self.inner.get_envs()
    }

    /// The configured working directory.
    pub fn get_current_dir(&self) -> Option<&Path> {
        self.inner.get_current_dir()
    }

    /// Unsupported in the browser.
    pub fn spawn(&mut self) -> io::Result<Child> {
        let _ = (self.kill_on_drop, self.reap_on_drop);
        Err(unsupported("smol::process::Command::spawn"))
    }

    /// Unsupported in the browser: resolves to `Err` immediately.
    pub fn status(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send + use<> {
        ready(Err(unsupported("smol::process::Command::status")))
    }

    /// Unsupported in the browser: resolves to `Err` immediately.
    pub fn output(&mut self) -> impl Future<Output = io::Result<Output>> + Send + use<> {
        ready(Err(unsupported("smol::process::Command::output")))
    }
}

impl From<std::process::Command> for Command {
    fn from(inner: std::process::Command) -> Self {
        Command {
            inner,
            kill_on_drop: false,
            reap_on_drop: true,
        }
    }
}

impl AsRef<std::process::Command> for Command {
    fn as_ref(&self) -> &std::process::Command {
        &self.inner
    }
}

impl AsMut<std::process::Command> for Command {
    fn as_mut(&mut self) -> &mut std::process::Command {
        &mut self.inner
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Command")
            .field("inner", &self.inner)
            .field("kill_on_drop", &self.kill_on_drop)
            .field("reap_on_drop", &self.reap_on_drop)
            .finish()
    }
}

/// A spawned child; never constructed in the browser (`Command::spawn` fails).
#[derive(Debug)]
pub struct Child {
    /// The child's stdin handle, if piped.
    pub stdin: Option<ChildStdin>,
    /// The child's stdout handle, if piped.
    pub stdout: Option<ChildStdout>,
    /// The child's stderr handle, if piped.
    pub stderr: Option<ChildStderr>,
}

impl Child {
    /// The OS-assigned process id (always 0 here).
    pub fn id(&self) -> u32 {
        0
    }

    /// Unsupported in the browser.
    pub fn kill(&mut self) -> io::Result<()> {
        Err(unsupported("smol::process::Child::kill"))
    }

    /// Unsupported in the browser.
    pub fn try_status(&mut self) -> io::Result<Option<ExitStatus>> {
        Err(unsupported("smol::process::Child::try_status"))
    }

    /// Unsupported in the browser: resolves to `Err` immediately.
    pub fn status(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send + use<> {
        ready(Err(unsupported("smol::process::Child::status")))
    }

    /// Unsupported in the browser: resolves to `Err` immediately.
    pub fn output(self) -> impl Future<Output = io::Result<Output>> + Send + use<> {
        ready(Err(unsupported("smol::process::Child::output")))
    }
}

/// The child's stdin; never constructed in the browser.
#[derive(Debug)]
pub struct ChildStdin {
    _private: (),
}

impl AsyncWrite for ChildStdin {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::process::ChildStdin")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported("smol::process::ChildStdin")))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported("smol::process::ChildStdin")))
    }
}

/// The child's stdout; never constructed in the browser.
#[derive(Debug)]
pub struct ChildStdout {
    _private: (),
}

impl AsyncRead for ChildStdout {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::process::ChildStdout")))
    }
}

/// The child's stderr; never constructed in the browser.
#[derive(Debug)]
pub struct ChildStderr {
    _private: (),
}

impl AsyncRead for ChildStderr {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::process::ChildStderr")))
    }
}

/// async-process's global reaper driver; smol's native `spawn` runs it. Nothing to drive here.
pub async fn driver() {}
