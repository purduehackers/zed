use futures::{
    channel::mpsc::{channel, unbounded, Sender, UnboundedReceiver, UnboundedSender},
    stream::StreamExt,
};
use std::{
    cell::Cell,
    pin::Pin,
    rc::Rc,
    str::FromStr,
    task::{ready, Context, Poll},
};
use url::Url;
use wasm_bindgen::prelude::*;
use web_sys::MessageEvent;

use crate::{
    close::CloseCode,
    frame::{Frame, OpCode},
    Result, WebSocketError,
};

/// Read-side limits for a browser socket, the counterpart of the native `Options`'
/// `max_payload_read` / `max_read_buffer`. Both are unlimited by default, matching the
/// browser API.
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    max_payload_read: Option<usize>,
    max_read_buffer: Option<usize>,
    max_write_buffer: Option<usize>,
}

impl Options {
    /// Rejects any incoming message larger than `bytes`: the socket is closed with
    /// `CloseCode::Size` (1009) before the message is copied out of the browser, so an
    /// oversize message never reaches the Rust heap.
    pub fn with_max_payload_read(mut self, bytes: usize) -> Self {
        self.max_payload_read = Some(bytes);
        self
    }

    /// Bounds the bytes of received messages that may be queued for the consumer at any
    /// one time. The browser delivers messages from a synchronous event handler that cannot
    /// wait for the consumer, so when the queue would exceed `bytes` the socket is closed
    /// with `CloseCode::Policy` (1008) instead of growing without bound.
    pub fn with_max_read_buffer(mut self, bytes: usize) -> Self {
        self.max_read_buffer = Some(bytes);
        self
    }

    /// Caps the browser-owned send queue; a stalled peer cannot grow it forever.
    pub fn with_max_write_buffer(mut self, bytes: usize) -> Self {
        self.max_write_buffer = Some(bytes);
        self
    }
}

/// A WebSocket wrapper for WASM applications that provides an async interface
/// for WebSocket communication. This implementation wraps the browser's native
/// WebSocket API and provides Rust-friendly methods for sending and receiving messages.
pub struct WebSocket {
    /// The underlying browser WebSocket instance
    stream: web_sys::WebSocket,
    /// Channel receiver for incoming messages and errors
    receiver: UnboundedReceiver<Result<Frame>>,
    /// Payload bytes currently queued in `receiver`; maintained by the message handler and
    /// by `poll_next`, and compared against `Options::max_read_buffer`.
    queued_bytes: Rc<Cell<usize>>,
    max_write_buffer: Option<usize>,
}

/// Closes a browser socket that is still connecting when its `connect` future is dropped
/// (a timeout racing the dial, for instance). Without this the browser would finish the
/// handshake on its own and leave an open socket that nothing owns.
struct PendingSocket(Option<web_sys::WebSocket>);

impl PendingSocket {
    fn get(&self) -> &web_sys::WebSocket {
        self.0.as_ref().expect("pending socket already taken")
    }

    fn take(mut self) -> web_sys::WebSocket {
        self.0.take().expect("pending socket already taken")
    }
}

impl Drop for PendingSocket {
    fn drop(&mut self) {
        if let Some(stream) = self.0.take() {
            let _ = stream.close();
        }
    }
}

impl WebSocket {
    /// Creates a new WebSocket connection to the specified URL
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket server URL, usually starts with "ws://" or "wss://"
    ///
    /// # Returns
    ///
    /// A Result containing the WebSocket instance if successful, or a JsValue error
    ///
    /// # Example
    ///
    /// ```
    /// let websocket = WebSocket::connect("wss://example.com/socket").await?;
    /// ```
    pub async fn connect(url: Url) -> Result<Self> {
        // Initialize the WebSocket connection
        let stream = web_sys::WebSocket::new(url.as_str()).map_err(WebSocketError::Js)?;
        Self::from_stream(stream, Options::default()).await
    }

    /// Creates a new WebSocket connection to the specified URL, offering the given
    /// subprotocols (`new WebSocket(url, protocols)`).
    ///
    /// This is the only way a browser can offer subprotocols: the `Sec-WebSocket-Protocol`
    /// request header cannot be set from JavaScript, so clients that carry credentials in
    /// the subprotocol list (as `zed-remote-server serve` expects) need this entry point.
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket server URL, usually starts with "ws://" or "wss://"
    /// * `protocols` - Subprotocol names to offer, in order of preference
    pub async fn connect_with_protocols(url: Url, protocols: &[&str]) -> Result<Self> {
        Self::connect_with_protocols_and_options(url, protocols, Options::default()).await
    }

    /// [`WebSocket::connect_with_protocols`] with read-side limits (see [`Options`]).
    pub async fn connect_with_protocols_and_options(
        url: Url,
        protocols: &[&str],
        options: Options,
    ) -> Result<Self> {
        let array = js_sys::Array::new();
        for protocol in protocols {
            array.push(&JsValue::from_str(protocol));
        }
        let stream = web_sys::WebSocket::new_with_str_sequence(url.as_str(), &array)
            .map_err(WebSocketError::Js)?;
        Self::from_stream(stream, options).await
    }

    /// Wires the event handlers onto a freshly constructed browser socket and waits for it
    /// to open. Dropping the returned future before it resolves closes the socket.
    async fn from_stream(stream: web_sys::WebSocket, options: Options) -> Result<Self> {
        let pending = PendingSocket(Some(stream));
        let stream = pending.get();

        // Set the binary type to be arraybuffers so that we can wrap them in `Bytes`
        stream.set_binary_type(web_sys::BinaryType::Arraybuffer);

        // Create a communication channel
        let (tx, rx) = unbounded();
        let queued_bytes = Rc::new(Cell::new(0));

        let (connection_tx, mut connection_rx) = channel(1);

        // Set up the event handlers
        Self::setup_message_handler(stream, tx.clone(), options, queued_bytes.clone());
        Self::setup_close_handler(stream, tx, connection_tx.clone());
        Self::setup_error_handler(stream, connection_tx.clone());
        Self::setup_open_handler(stream, connection_tx);

        connection_rx
            .next()
            .await
            .ok_or(WebSocketError::ConnectionClosed)??;

        Ok(Self {
            stream: pending.take(),
            receiver: rx,
            queued_bytes,
            max_write_buffer: options.max_write_buffer,
        })
    }

    /// Sets up the close handler for the WebSocket
    ///
    /// # Arguments
    ///
    /// * `stream` - Reference to the WebSocket instance
    /// * `tx` - Channel sender to forward close events
    fn setup_close_handler(
        stream: &web_sys::WebSocket,
        tx: UnboundedSender<Result<Frame>>,
        mut connection_tx: Sender<Result<()>>,
    ) {
        let onclose_callback: Closure<dyn FnMut(web_sys::CloseEvent)> =
            Closure::new(move |close_event: web_sys::CloseEvent| {
                let _ = connection_tx.try_send(Err(WebSocketError::ConnectionClosed));
                if !close_event.was_clean() {
                    web_sys::console::warn_1(
                        &js_sys::JsString::from_str("WebSocket CloseEvent wasClean() == false")
                            .unwrap(), // SAFETY: This always succeeds
                    );
                }
                let close_frame = Frame::close(close_event.code().into(), close_event.reason());
                let _ = tx.unbounded_send(Ok(close_frame));
                let _ = tx.unbounded_send(Err(WebSocketError::ConnectionClosed));
            });

        stream.set_onclose(Some(onclose_callback.as_ref().unchecked_ref()));
        onclose_callback.forget();
    }

    /// Sets up the error handler for the WebSocket.
    fn setup_error_handler(stream: &web_sys::WebSocket, mut connection_tx: Sender<Result<()>>) {
        let onerror_callback: Closure<dyn FnMut(JsValue)> = Closure::new(move |error: JsValue| {
            let _ = connection_tx.try_send(Err(WebSocketError::Js(error)));
        });

        stream.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
        onerror_callback.forget();
    }

    /// Sets up the open handler for the WebSocket.
    ///
    /// # Arguments
    ///
    /// * `stream` - Reference to the WebSocket instance
    ///
    fn setup_open_handler(stream: &web_sys::WebSocket, mut connection_tx: Sender<Result<()>>) {
        let onopen_callback = Closure::<dyn FnMut(_)>::new(move |_: MessageEvent| {
            let _ = connection_tx.try_send(Ok(()));
        });

        stream.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
        onopen_callback.forget();
    }

    /// Sets up the message handler for the WebSocket
    ///
    /// # Arguments
    ///
    /// * `stream` - Reference to the WebSocket instance
    /// * `tx` - Channel sender to forward received messages
    /// * `options` - Read-side limits; exceeding one closes the socket from the handler
    /// * `queued_bytes` - Bytes queued in `tx` and not yet taken by the consumer
    fn setup_message_handler(
        stream: &web_sys::WebSocket,
        tx: UnboundedSender<Result<Frame>>,
        options: Options,
        queued_bytes: Rc<Cell<usize>>,
    ) {
        let socket = stream.clone();
        let onmessage_callback: Closure<dyn Fn(_)> = Closure::new(move |e: MessageEvent| {
            let data = e.data();
            let payload_len = if data.has_type::<js_sys::JsString>() {
                data.unchecked_ref::<js_sys::JsString>().length() as usize
            } else if data.has_type::<js_sys::ArrayBuffer>() {
                data.unchecked_ref::<js_sys::ArrayBuffer>().byte_length() as usize
            } else {
                return;
            };

            // Enforce the limits before the message is copied out of the browser: the
            // handler cannot wait for the consumer, so the only alternatives to closing
            // are dropping frames silently or growing the queue without bound.
            if options
                .max_payload_read
                .is_some_and(|limit| payload_len > limit)
            {
                let _ = socket.close_with_code_and_reason(
                    u16::from(CloseCode::Size),
                    "message too large",
                );
                return;
            }
            let queued = queued_bytes.get();
            if options
                .max_read_buffer
                .is_some_and(|limit| queued.saturating_add(payload_len) > limit)
            {
                let _ = socket
                    .close_with_code_and_reason(u16::from(CloseCode::Policy), "slow consumer");
                return;
            }

            let frame = if data.has_type::<js_sys::JsString>() {
                let str_value = data.unchecked_into::<js_sys::JsString>();
                Frame::text(String::from(str_value))
            } else {
                let buffer_value =
                    js_sys::Uint8Array::new(&data.unchecked_into::<js_sys::ArrayBuffer>()).to_vec();
                Frame::binary(buffer_value)
            };

            queued_bytes.set(queued.saturating_add(frame.payload().len()));
            // ignore the error, it could be that the other end closed the
            // connection and we don't want to panic
            let _ = tx.unbounded_send(Ok(frame));
        });

        stream.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
        onmessage_callback.forget();
    }

    /// Receive the next frame from the websocket
    ///
    /// This is an alias for the `next` method, providing a more semantically clear way
    /// to request the next frame from the WebSocket connection.
    ///
    /// # Returns
    ///
    /// A Result containing the received frame or an error
    pub async fn next_frame(&mut self) -> Result<Frame> {
        use futures::StreamExt;
        match self.next().await {
            Some(res) => res,
            None => Err(WebSocketError::ConnectionClosed),
        }
    }
}

impl Drop for WebSocket {
    /// The event closures are `forget()`-ed, so without this the browser socket would stay
    /// open after the Rust value is gone.
    fn drop(&mut self) {
        if let Err(error) = self.stream.close() {
            web_sys::console::warn_2(
                &JsValue::from_str("WebSocket close() on drop failed"),
                &error,
            );
        }
    }
}

impl futures::Sink<Frame> for WebSocket {
    type Error = WebSocketError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // WebSocket's send is always ready in this implementation
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, frame: Frame) -> Result<()> {
        if self.max_write_buffer.is_some_and(|limit| {
            (self.stream.buffered_amount() as usize).saturating_add(frame.payload().len()) > limit
        }) {
            let _ = self.stream.close();
            return Err(WebSocketError::ConnectionClosed);
        }
        match frame.opcode() {
            OpCode::Text => self
                .stream
                .send_with_str(frame.as_str())
                .map_err(|_| WebSocketError::ConnectionClosed),
            OpCode::Binary => self
                .stream
                .send_with_js_u8_array(&js_sys::Uint8Array::from(frame.payload().as_ref()))
                .map_err(|_| WebSocketError::ConnectionClosed),
            OpCode::Close => {
                let code = frame.close_code().ok_or(WebSocketError::ConnectionClosed)?;

                match frame.close_reason() {
                    Ok(Some(reason)) => self.stream.close_with_code_and_reason(code.into(), reason),
                    Ok(None) => self.stream.close_with_code(code.into()),
                    Err(err) => return Err(err),
                }
                .map_err(|_| WebSocketError::ConnectionClosed)
            }
            // All other types of payloads are taken care by the browser behind the scenes
            _ => Ok(()),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // WebSocket sends immediately, no need for explicit flush
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        let ret = self.stream.close().map_err(WebSocketError::Js);
        Poll::Ready(ret)
    }
}

impl futures::Stream for WebSocket {
    type Item = Result<Frame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Use the underlying receiver's poll_next and map the result
        match ready!(self.receiver.poll_next_unpin(cx)) {
            Some(Ok(message)) => {
                let queued = self.queued_bytes.get();
                self.queued_bytes
                    .set(queued.saturating_sub(message.payload().len()));
                Poll::Ready(Some(Ok(message)))
            }
            Some(Err(e)) => {
                if matches!(e, WebSocketError::ConnectionClosed) {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(e)))
                }
            }
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::future::{select, Either};
    use js_sys::{Function, Promise, Reflect};
    use wasm_bindgen::{closure::Closure, JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::WebSocket;

    async fn timeout(milliseconds: u32) {
        let promise = Promise::new(&mut |resolve, _reject| {
            let callback = Closure::once(move || {
                let _ = resolve.call0(&JsValue::NULL);
            });
            let global = js_sys::global();
            let set_timeout = Reflect::get(&global, &JsValue::from_str("setTimeout"))
                .unwrap()
                .unchecked_into::<Function>();
            set_timeout
                .call2(
                    &global,
                    callback.as_ref(),
                    &JsValue::from_f64(milliseconds.into()),
                )
                .unwrap();
            callback.forget();
        });
        JsFuture::from(promise).await.unwrap();
    }

    #[wasm_bindgen_test(async)]
    async fn connect_returns_error_when_connection_fails() {
        let global = js_sys::global();
        if !Reflect::has(&global, &JsValue::from_str("WebSocket")).unwrap() {
            return;
        }

        let location = Reflect::get(&global, &JsValue::from_str("location")).unwrap();
        let host = Reflect::get(&location, &JsValue::from_str("host"))
            .unwrap()
            .as_string()
            .unwrap();
        let url = format!("ws://{host}/rejected-websocket").parse().unwrap();
        let connect = pin!(WebSocket::connect(url));
        let timeout = pin!(timeout(1_000));

        match select(connect, timeout).await {
            Either::Left((result, _timeout)) => assert!(result.is_err()),
            Either::Right(((), _connect)) => panic!("connection attempt remained pending"),
        }
    }
}
