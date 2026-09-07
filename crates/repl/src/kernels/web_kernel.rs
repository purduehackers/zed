//! Browser transport for the existing inline REPL. Python runs in the host's sandbox.

use super::{KernelSession, KernelSpecification, RunningKernel};
use anyhow::{Context as _, Result, ensure};
use futures::{
    AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite,
    AsyncWriteExt as _, FutureExt as _, StreamExt as _, channel::mpsc, io::BufReader, pin_mut,
    select,
};
use gpui::{App, AsyncApp, Entity, Global, SharedString, Task, Window};
use jupyter_protocol::{
    Channel, ExecutionState, JupyterMessage, JupyterMessageContent, KernelInfoReply,
};
use language::LanguageName;
use project::{Project, ProjectPath, Toolchains, WorktreeId};
use std::{collections::BTreeSet, fmt, path::PathBuf, time::Duration};
use util::rel_path::RelPath;

const MAX_MESSAGE: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebKernelSpecification {
    pub name: SharedString,
    /// None selects the Python environment bundled by the browser host.
    pub python: Option<SharedString>,
}

pub struct WebKernelConnection {
    pub reader: Box<dyn AsyncRead + Unpin + Send>,
    pub writer: Box<dyn AsyncWrite + Unpin + Send>,
    pub keep_alive: Task<()>,
}

type Factory =
    fn(WebKernelSpecification, PathBuf, &mut AsyncApp) -> Task<Result<WebKernelConnection>>;
struct WebKernelProvider(Factory);
impl Global for WebKernelProvider {}

pub fn set_web_kernel_factory(cx: &mut App, factory: Factory) {
    cx.set_global(WebKernelProvider(factory));
}

pub fn python_env_kernel_specifications(
    project: &Entity<Project>,
    worktree_id: WorktreeId,
    cx: &mut App,
) -> impl Future<Output = Result<Vec<KernelSpecification>>> + use<> {
    let toolchains = project.read(cx).available_toolchains(
        ProjectPath {
            worktree_id,
            path: RelPath::empty_arc(),
        },
        LanguageName::new_static("Python"),
        cx,
    );
    async move {
        let mut specifications = vec![KernelSpecification::Web(WebKernelSpecification {
            name: "Python (sandbox)".into(),
            python: None,
        })];
        if let Some(Toolchains {
            toolchains,
            user_toolchains,
            ..
        }) = toolchains.await
        {
            let mut seen = BTreeSet::new();
            for toolchain in user_toolchains
                .into_values()
                .flatten()
                .chain(toolchains.toolchains)
            {
                if seen.insert(toolchain.path.clone()) {
                    specifications.push(KernelSpecification::Web(WebKernelSpecification {
                        name: toolchain.name.clone(),
                        python: Some(toolchain.path),
                    }));
                }
            }
        }
        Ok(specifications)
    }
}

async fn read_message(
    reader: &mut (impl AsyncBufRead + Unpin),
) -> Result<Option<serde_json::Value>> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_MESSAGE + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .await?;
    if bytes.is_empty() {
        return Ok(None);
    }
    ensure!(
        bytes.len() <= MAX_MESSAGE && bytes.last() == Some(&b'\n'),
        "Kernel message exceeds the 8 MiB limit or was interrupted"
    );
    let message: serde_json::Value = serde_json::from_slice(&bytes)?;
    if let Some(error) = message.get("error").and_then(|v| v.as_str()) {
        anyhow::bail!("{error}");
    }
    Ok(Some(message))
}

pub struct WebRunningKernel {
    directory: PathBuf,
    requests: mpsc::Sender<JupyterMessage>,
    state: ExecutionState,
    info: Option<KernelInfoReply>,
    task: Option<Task<()>>,
    connection: Option<Task<()>>,
}

impl fmt::Debug for WebRunningKernel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebRunningKernel")
            .field("directory", &self.directory)
            .finish()
    }
}

impl WebRunningKernel {
    pub fn new<S: KernelSession + 'static>(
        specification: WebKernelSpecification,
        directory: PathBuf,
        session: Entity<S>,
        window: &mut Window,
        cx: &mut App,
    ) -> Task<Result<Box<dyn RunningKernel>>> {
        let factory = cx.global::<WebKernelProvider>().0;
        let session = session.downgrade();
        window.spawn(cx, async move |cx| {
            let connection = factory(specification, directory.clone(), cx).await?;
            let mut reader = BufReader::new(connection.reader);
            let mut writer = connection.writer;
            let ready = read_message(&mut reader).fuse();
            let timeout = cx
                .background_executor()
                .timer(Duration::from_secs(35))
                .fuse();
            {
                pin_mut!(ready, timeout);
                let ready = select! {
                    ready = ready => ready?.context("Python kernel closed before startup")?,
                    _ = timeout => anyhow::bail!("Timed out starting the Python kernel"),
                };
                ensure!(ready["ready"] == true, "Invalid kernel startup response");
            }
            let (requests, mut messages) = mpsc::channel::<JupyterMessage>(100);
            let task = cx.spawn(async move |cx| {
                let send = async move {
                    while let Some(mut message) = messages.next().await {
                        message.channel = Some(match &message.content {
                            JupyterMessageContent::InputReply(_) => Channel::Stdin,
                            JupyterMessageContent::InterruptRequest(_)
                            | JupyterMessageContent::ShutdownRequest(_)
                            | JupyterMessageContent::DebugRequest(_) => Channel::Control,
                            _ => Channel::Shell,
                        });
                        let mut bytes = serde_json::to_vec(&message)?;
                        ensure!(
                            bytes.len() < MAX_MESSAGE,
                            "Kernel request exceeds the 8 MiB limit"
                        );
                        bytes.push(b'\n');
                        writer.write_all(&bytes).await?;
                    }
                    anyhow::Ok(())
                }
                .fuse();
                let receive = async {
                    while let Some(message) = read_message(&mut reader).await? {
                        let message = serde_json::from_value(message)?;
                        session.update_in(cx, |session, window, cx| {
                            session.route(&message, window, cx)
                        })?;
                    }
                    anyhow::bail!("Python kernel disconnected")
                }
                .fuse();
                let result = {
                    pin_mut!(send, receive);
                    select! { result = send => result, result = receive => result }
                };
                if let Err(error) = result {
                    session
                        .update(cx, |session, cx| {
                            session.kernel_errored(error.to_string(), cx);
                            cx.notify();
                        })
                        .ok();
                }
            });
            Ok(Box::new(Self {
                directory,
                requests,
                state: ExecutionState::Starting,
                info: None,
                task: Some(task),
                connection: Some(connection.keep_alive),
            }) as Box<dyn RunningKernel>)
        })
    }
}

impl RunningKernel for WebRunningKernel {
    fn request_tx(&self) -> mpsc::Sender<JupyterMessage> {
        self.requests.clone()
    }
    fn stdin_tx(&self) -> mpsc::Sender<JupyterMessage> {
        self.requests.clone()
    }
    fn working_directory(&self) -> &PathBuf {
        &self.directory
    }
    fn execution_state(&self) -> &ExecutionState {
        &self.state
    }
    fn set_execution_state(&mut self, state: ExecutionState) {
        self.state = state;
    }
    fn kernel_info(&self) -> Option<&KernelInfoReply> {
        self.info.as_ref()
    }
    fn set_kernel_info(&mut self, info: KernelInfoReply) {
        self.info = Some(info);
    }
    fn force_shutdown(&mut self, _: &mut Window, _: &mut App) -> Task<Result<()>> {
        self.kill();
        Task::ready(Ok(()))
    }
    fn kill(&mut self) {
        self.task.take();
        self.connection.take();
    }
}
