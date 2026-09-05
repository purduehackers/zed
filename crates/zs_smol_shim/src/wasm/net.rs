//! `smol::net` on wasm32-unknown-unknown: async-net 2's surface (sized to the calls in the
//! browser crate set), every operation returning `io::ErrorKind::Unsupported` immediately and
//! never hanging. Browser code reaches the network through the WebSocket transport only.

use std::io;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::{AsyncRead, AsyncWrite, Stream};

pub use std::net::{
    AddrParseError, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6,
};

use super::unsupported;

/// A TCP connection; never constructed in the browser (`connect` fails).
#[derive(Debug)]
pub struct TcpStream {
    _private: (),
}

impl TcpStream {
    /// Unsupported in the browser.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<TcpStream> {
        let _ = addr;
        Err(unsupported("smol::net::TcpStream::connect"))
    }

    /// Unsupported in the browser.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported("smol::net::TcpStream::local_addr"))
    }

    /// Unsupported in the browser.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported("smol::net::TcpStream::peer_addr"))
    }

    /// Unsupported in the browser.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        let _ = how;
        Err(unsupported("smol::net::TcpStream::shutdown"))
    }

    /// Unsupported in the browser.
    pub fn nodelay(&self) -> io::Result<bool> {
        Err(unsupported("smol::net::TcpStream::nodelay"))
    }

    /// Unsupported in the browser.
    pub fn set_nodelay(&self, v: bool) -> io::Result<()> {
        let _ = v;
        Err(unsupported("smol::net::TcpStream::set_nodelay"))
    }

    /// Unsupported in the browser.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let _ = buf;
        Err(unsupported("smol::net::TcpStream::peek"))
    }
}

impl Clone for TcpStream {
    fn clone(&self) -> Self {
        TcpStream { _private: () }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::net::TcpStream")))
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("smol::net::TcpStream")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported("smol::net::TcpStream")))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported("smol::net::TcpStream")))
    }
}

/// A TCP listener; never constructed in the browser (`bind` fails).
#[derive(Debug)]
pub struct TcpListener {
    _private: (),
}

impl TcpListener {
    /// Unsupported in the browser.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<TcpListener> {
        let _ = addr;
        Err(unsupported("smol::net::TcpListener::bind"))
    }

    /// Unsupported in the browser.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported("smol::net::TcpListener::local_addr"))
    }

    /// Unsupported in the browser.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        Err(unsupported("smol::net::TcpListener::accept"))
    }

    /// A stream of incoming connections; ends immediately.
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming(PhantomData)
    }
}

/// The stream returned by `TcpListener::incoming`; always empty in the browser.
#[derive(Debug)]
pub struct Incoming<'a>(PhantomData<&'a ()>);

impl Stream for Incoming<'_> {
    type Item = io::Result<TcpStream>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// A UDP socket; never constructed in the browser (`bind` fails).
#[derive(Debug)]
pub struct UdpSocket {
    _private: (),
}

impl UdpSocket {
    /// Unsupported in the browser.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<UdpSocket> {
        let _ = addr;
        Err(unsupported("smol::net::UdpSocket::bind"))
    }

    /// Unsupported in the browser.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported("smol::net::UdpSocket::local_addr"))
    }

    /// Unsupported in the browser.
    pub async fn send_to(&self, buf: &[u8], addr: impl ToSocketAddrs) -> io::Result<usize> {
        let _ = (buf, addr);
        Err(unsupported("smol::net::UdpSocket::send_to"))
    }

    /// Unsupported in the browser.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let _ = buf;
        Err(unsupported("smol::net::UdpSocket::recv_from"))
    }
}

/// Unix-domain sockets. Present because `crates/net/src/async_net.rs` re-exports these types
/// under `cfg(not(target_os = "windows"))`, which includes wasm; every operation fails.
pub mod unix {
    use std::io;
    use std::path::Path;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_lite::{AsyncRead, AsyncWrite};

    use super::super::unsupported;

    /// A Unix-domain connection; never constructed in the browser.
    #[derive(Debug)]
    pub struct UnixStream {
        _private: (),
    }

    impl UnixStream {
        /// Unsupported in the browser.
        pub async fn connect(path: impl AsRef<Path>) -> io::Result<UnixStream> {
            let _ = path;
            Err(unsupported("smol::net::unix::UnixStream::connect"))
        }

        /// Unsupported in the browser.
        pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
            Err(unsupported("smol::net::unix::UnixStream::pair"))
        }

        /// Unsupported in the browser.
        pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
            let _ = how;
            Err(unsupported("smol::net::unix::UnixStream::shutdown"))
        }
    }

    impl Clone for UnixStream {
        fn clone(&self) -> Self {
            UnixStream { _private: () }
        }
    }

    impl AsyncRead for UnixStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(unsupported("smol::net::unix::UnixStream")))
        }
    }

    impl AsyncWrite for UnixStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(unsupported("smol::net::unix::UnixStream")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(unsupported("smol::net::unix::UnixStream")))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(unsupported("smol::net::unix::UnixStream")))
        }
    }

    /// A Unix-domain listener; never constructed in the browser (`bind` fails).
    #[derive(Debug)]
    pub struct UnixListener {
        _private: (),
    }

    impl UnixListener {
        /// Unsupported in the browser (synchronous, as in async-net).
        pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixListener> {
            let _ = path;
            Err(unsupported("smol::net::unix::UnixListener::bind"))
        }

        /// Unsupported in the browser. async-net yields the peer's `SocketAddr` as the second
        /// element; `std::os::unix::net::SocketAddr` does not exist on wasm, so this is `()`.
        pub async fn accept(&self) -> io::Result<(UnixStream, ())> {
            Err(unsupported("smol::net::unix::UnixListener::accept"))
        }
    }
}
