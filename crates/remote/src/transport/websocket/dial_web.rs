//! Browser dial: `new WebSocket(url, ["zs.v1", token])` through the vendored yawc's
//! `connect_with_protocols`, then a bridge task on the foreground executor (the browser
//! socket is `!Send`; messages are delivered on the main thread anyway).

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{Context as _, Result};
use futures::{FutureExt as _, Sink, Stream, StreamExt as _, pin_mut, select_biased};
use gpui::AsyncApp;
use url::Url;
use yawc::{Options, WebSocket, WebSocketError, close::CloseCode, frame::Frame};

use super::{
    CONNECT_TIMEOUT, SocketBridge, bridge_channels, run_bridge,
    wire::{MAX_FRAME_BYTES, subprotocols},
    ws_err,
};

pub(super) async fn dial(url: Url, token: &str, cx: &mut AsyncApp) -> Result<SocketBridge> {
    let protocols = subprotocols(token)?;
    // The same read-side ceilings as the native dial: the 16 MiB frame limit is enforced
    // before a message is copied out of the browser, and the queue behind the bridge is
    // bounded so a flooding server closes the socket instead of exhausting the tab.
    let options = Options::default()
        .with_max_payload_read(MAX_FRAME_BYTES)
        .with_max_read_buffer(2 * MAX_FRAME_BYTES);
    let connect = WebSocket::connect_with_protocols_and_options(url, &protocols, options).fuse();
    let timeout = cx.background_executor().timer(CONNECT_TIMEOUT).fuse();
    pin_mut!(connect, timeout);
    let socket = select_biased! {
        result = connect => result
            .map_err(ws_err)
            .context("failed to connect to the workspace")?,
        _ = timeout => anyhow::bail!("timed out connecting to the workspace"),
    };

    let (sink, stream) = ClosingSocket(socket).split();
    let stream = stream.map(|result| result.map_err(|error| ws_err(error).to_string()));
    let ((frames_tx, outbound), (inbound, frames_rx)) = bridge_channels();
    Ok(SocketBridge {
        frames_tx,
        frames_rx: Some(frames_rx),
        _task: cx
            .foreground_executor()
            .spawn(run_bridge(sink, stream, outbound, inbound)),
    })
}

/// Closes the browser socket when dropped. The vendored yawc also closes on drop; this keeps
/// the transport independent of that patch.
struct ClosingSocket(WebSocket);

impl Drop for ClosingSocket {
    fn drop(&mut self) {
        // `start_send` of a close frame is synchronous in the browser implementation.
        if let Err(error) =
            Pin::new(&mut self.0).start_send(Frame::close(CloseCode::Normal, "client gone"))
        {
            log::debug!("closing the browser socket on drop failed: {error}");
        }
    }
}

impl Stream for ClosingSocket {
    type Item = yawc::Result<Frame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl Sink<Frame> for ClosingSocket {
    type Error = WebSocketError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<yawc::Result<()>> {
        Pin::new(&mut self.0).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Frame) -> yawc::Result<()> {
        Pin::new(&mut self.0).start_send(frame)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<yawc::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<yawc::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}
