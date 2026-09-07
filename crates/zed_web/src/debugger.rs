//! DAP byte streams over the host's authenticated, per-session sandbox socket.

use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use dap::{
    adapters::{DebugAdapterBinary, TcpArguments},
    transport::Transport,
};
use futures::{
    AsyncRead, AsyncWrite, FutureExt as _, Sink, SinkExt as _, StreamExt as _, TryStreamExt as _,
    channel::mpsc, pin_mut, select,
};
use gpui::{App, AsyncApp, Task};
use yawc::{
    Options, WebSocket,
    frame::{Frame, OpCode},
};

type Streams = (
    Box<dyn AsyncWrite + Unpin + Send>,
    Box<dyn AsyncRead + Unpin + Send>,
);

pub fn init(cx: &mut App) {
    dap::transport::set_web_transport_factory(cx, |binary, cx| {
        cx.spawn(async move |cx| Ok(Box::new(connect(binary, cx).await?) as Box<dyn Transport>))
    });
    repl::kernels::set_web_kernel_factory(cx, |spec, directory, cx| {
        cx.spawn(async move |cx| {
            let info =
                crate::bridge::connect_kernel(spec.python.as_deref(), &directory.to_string_lossy())
                    .await?;
            let launch = info["launch"]
                .as_str()
                .context("Missing kernel launch")?
                .to_owned();
            let mut transport = open_socket(launch, info, None, cx).await?;
            let (writer, reader) = transport.connect().await?;
            Ok(repl::kernels::WebKernelConnection {
                writer,
                reader,
                keep_alive: transport.task.take().context("Missing kernel transport")?,
            })
        })
    });
}

struct BrowserTransport {
    streams: Mutex<Option<Streams>>,
    connection: Option<TcpArguments>,
    task: Option<Task<()>>,
}

impl Transport for BrowserTransport {
    fn has_adapter_logs(&self) -> bool {
        false
    }
    fn tcp_arguments(&self) -> Option<TcpArguments> {
        self.connection.clone()
    }
    fn connect(&mut self) -> Task<Result<Streams>> {
        Task::ready(
            self.streams
                .get_mut()
                .unwrap()
                .take()
                .context("Debug adapter is already connected"),
        )
    }
    fn kill(&mut self) {
        self.streams.get_mut().unwrap().take();
        self.task.take();
    }
}

async fn connect(binary: DebugAdapterBinary, cx: &mut AsyncApp) -> Result<BrowserTransport> {
    let launch = serde_json::to_string(&serde_json::json!({
        "command": binary.command, "arguments": binary.arguments, "envs": binary.envs,
        "cwd": binary.cwd, "connection": binary.connection,
    }))?;
    let info = crate::bridge::connect_debug_adapter(&launch).await?;
    open_socket(launch, info, binary.connection, cx).await
}

async fn open_socket(
    launch: String,
    info: serde_json::Value,
    connection: Option<TcpArguments>,
    cx: &mut AsyncApp,
) -> Result<BrowserTransport> {
    let url = info["url"]
        .as_str()
        .context("Missing debug socket URL")?
        .parse()?;
    let token = info["token"]
        .as_str()
        .context("Missing debug socket token")?;
    let protocols = ["zs.dap.v1", token];
    let options = Options::default()
        .with_max_payload_read(8 * 1024 * 1024)
        .with_max_read_buffer(16 * 1024 * 1024)
        .with_max_write_buffer(16 * 1024 * 1024);
    let opening = async {
        let mut socket = WebSocket::connect_with_protocols_and_options(url, &protocols, options)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        socket
            .send(Frame::text(launch))
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let first = socket
            .next()
            .await
            .context("Debug socket closed before startup")?
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let response: serde_json::Value = serde_json::from_slice(first.payload())?;
        if response["ready"] != true {
            bail!(
                "{}",
                response["error"]
                    .as_str()
                    .unwrap_or("Debug adapter did not start")
            );
        }
        Ok::<_, anyhow::Error>(socket)
    }
    .fuse();
    let timeout = cx
        .background_executor()
        .timer(Duration::from_secs(30))
        .fuse();
    pin_mut!(opening, timeout);
    let socket = select! {
        result = opening => result?,
        _ = timeout => bail!("Timed out starting the sandbox debug adapter"),
    };
    let (mut sink, mut source) = socket.split();
    let (outgoing, mut writes) = mpsc::channel::<Vec<u8>>(16);
    let (mut incoming, reads) = mpsc::channel::<io::Result<Vec<u8>>>(8);
    let task = cx.foreground_executor().spawn(async move {
        let send = async move {
            while let Some(bytes) = writes.next().await {
                sink.send(Frame::binary(bytes)).await?;
            }
            Ok::<_, yawc::WebSocketError>(())
        }
        .fuse();
        let receive = async move {
            while let Some(frame) = source.next().await {
                let bytes = match frame {
                    Ok(frame) if frame.opcode() == OpCode::Binary => Ok(frame.payload().to_vec()),
                    Ok(frame) if frame.opcode() == OpCode::Close => break,
                    Ok(frame) if frame.opcode() == OpCode::Text => {
                        let response: serde_json::Value =
                            serde_json::from_slice(frame.payload()).unwrap_or_default();
                        Err(io::Error::other(
                            response["error"]
                                .as_str()
                                .unwrap_or("Debug adapter stopped"),
                        ))
                    }
                    Ok(_) => continue,
                    Err(error) => Err(io::Error::other(error.to_string())),
                };
                let failed = bytes.is_err();
                if incoming.send(bytes).await.is_err() || failed {
                    break;
                }
            }
        }
        .fuse();
        pin_mut!(send, receive);
        select! { _ = send => {}, _ = receive => {} }
    });
    Ok(BrowserTransport {
        streams: Mutex::new(Some((
            Box::new(Writer(outgoing)),
            Box::new(reads.into_async_read()),
        ))),
        connection,
        task: Some(task),
    })
}

struct Writer(mpsc::Sender<Vec<u8>>);
impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures::ready!(Pin::new(&mut self.0).poll_ready(cx)).map_err(io::Error::other)?;
        let count = bytes.len().min(64 * 1024);
        Pin::new(&mut self.0)
            .start_send(bytes[..count].to_vec())
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(count))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0)
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}
