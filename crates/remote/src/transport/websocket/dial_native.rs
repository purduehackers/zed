//! Native dial: TCP/TLS and the HTTP upgrade run on the `gpui_tokio` runtime (yawc's client
//! handshake spawns its connection driver with `tokio::spawn`), then the socket is handed to a
//! bridge task on the background executor.

use anyhow::{Context as _, Result};
use futures::{FutureExt as _, StreamExt as _, pin_mut, select_biased};
use gpui::{AppContext as _, AsyncApp};
use url::Url;
use yawc::{HttpRequestBuilder, Options, TcpWebSocket, WebSocket, frame::Frame};

use super::{
    CONNECT_TIMEOUT, SocketBridge, bridge_channels, run_bridge,
    wire::{MAX_FRAME_BYTES, subprotocol_header_value},
    ws_err,
};

pub(super) async fn dial(url: Url, token: &str, cx: &mut AsyncApp) -> Result<SocketBridge> {
    let header_value = subprotocol_header_value(token)?;
    let dial = gpui_tokio::Tokio::spawn_result(cx, async move {
        let options = Options::default()
            .with_max_payload_read(MAX_FRAME_BYTES)
            .with_max_read_buffer(2 * MAX_FRAME_BYTES)
            .with_backpressure_boundary(1024 * 1024)
            .with_no_delay();
        WebSocket::connect(url)
            .with_options(options)
            .with_request(HttpRequestBuilder::new().header("Sec-WebSocket-Protocol", header_value))
            .await
            .map_err(ws_err)
    })
    .fuse();
    let timeout = cx.background_executor().timer(CONNECT_TIMEOUT).fuse();
    pin_mut!(dial, timeout);
    let socket: TcpWebSocket = select_biased! {
        result = dial => result.context("failed to connect to the workspace")?,
        _ = timeout => anyhow::bail!("timed out connecting to the workspace"),
    };

    let (sink, stream) = socket.split();
    // Native yawc turns every read error (including an oversize frame) into end-of-stream,
    // so there is nothing to map here beyond the item type.
    let stream = stream.map(Ok::<Frame, String>);
    let ((frames_tx, outbound), (inbound, frames_rx)) = bridge_channels();
    Ok(SocketBridge {
        frames_tx,
        frames_rx: Some(frames_rx),
        _task: cx.background_spawn(run_bridge(sink, stream, outbound, inbound)),
    })
}
