#[cfg(not(target_arch = "wasm32"))]
use crate::platform;
use crate::transport::{Transport, TransportError};

#[cfg(not(target_arch = "wasm32"))]
use crate::platform::ws_native::WebSocketNative;

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
pub struct TcpStreamNative {
    pub inner: TcpStream,
}

#[cfg(not(target_arch = "wasm32"))]
impl TcpStreamNative {
    /// Get the remote peer address
    pub fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.inner.peer_addr().ok()
    }

    pub async fn connect(addr: &str) -> Result<Self, TransportError> {
        // Try to connect with backoff
        let stream = loop {
            match TcpStream::connect(addr) {
                Ok(stream) => break stream,
                Err(e)
                    if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::NotConnected =>
                {
                    // Connection in progress, yield and retry
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        };

        stream.set_nonblocking(true).map_err(TransportError::Io)?;

        Ok(Self { inner: stream })
    }
}

use async_trait::async_trait;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Transport for TcpStreamNative {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        let mut written = 0;

        while written < data.len() {
            match self.inner.write(&data[written..]) {
                Ok(0) => {
                    return Err(TransportError::Io(std::io::Error::new(
                        ErrorKind::WriteZero,
                        "write returned 0",
                    )));
                }
                Ok(n) => {
                    written += n;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // Socket not ready, yield to scheduler
                    tokio::task::yield_now().await;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {
                    // Interrupted, just retry
                    continue;
                }
                Err(e) => {
                    return Err(TransportError::Io(e));
                }
            }
        }

        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        loop {
            match self.inner.read(buf) {
                Ok(0) => {
                    // Connection closed
                    return Err(TransportError::Closed);
                }
                Ok(n) => {
                    return Ok(n);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // No data available yet, yield to scheduler
                    tokio::task::yield_now().await;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {
                    // Interrupted, just retry
                    continue;
                }
                Err(e) => {
                    return Err(TransportError::Io(e));
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct TcpListenerNative {
    inner: std::net::TcpListener,
}

#[cfg(not(target_arch = "wasm32"))]
impl TcpListenerNative {
    pub fn bind(addr: &str) -> Result<Self, TransportError> {
        let listener = std::net::TcpListener::bind(addr).map_err(TransportError::Io)?;
        listener.set_nonblocking(true).map_err(TransportError::Io)?;

        Ok(Self { inner: listener })
    }

    /// Accept a raw TCP connection and return the `std::net::TcpStream`.
    ///
    /// The stream is set to non-blocking mode before returning. This is the
    /// shared primitive used by both `accept_websocket()` and `AutoDetectListener`
    /// — it owns the accept loop so neither caller needs access to the private
    /// inner listener.
    pub async fn accept_std(&self) -> Result<std::net::TcpStream, TransportError> {
        loop {
            match self.inner.accept() {
                Ok((stream, _addr)) => {
                    stream.set_nonblocking(true).map_err(TransportError::Io)?;

                    log::info!(
                        "[TCP Native] Accepting TCP connection from {:?}",
                        stream.peer_addr()
                    );

                    return Ok(stream);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl platform::server::Listener for TcpListenerNative {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError> {
        let stream = self.accept_std().await?;
        Ok(Box::new(TcpStreamNative { inner: stream }))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl TcpListenerNative {
    /// Accept a connection and upgrade it to WebSocket.
    ///
    /// Delegates to `accept_std()` for the raw accept, then converts to a tokio
    /// stream and performs the tungstenite handshake.
    pub async fn accept_websocket(&self) -> Result<WebSocketNative, TransportError> {
        let stream = self.accept_std().await?;

        let tokio_stream = tokio::net::TcpStream::from_std(stream).map_err(TransportError::Io)?;

        WebSocketNative::accept(tokio_stream).await
    }
}
