mod tcp;
mod websocket;
mod buffered;

pub enum TransportKind {
    Tcp,
    WebSocket,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use buffered::BufferedTransport;

#[derive(Debug)]
pub enum TransportError {
    /// The transport type is not supported on this platform
    Unsupported,

    /// A normal I/O error (native TCP, file descriptors, etc.)
    Io(std::io::Error),

    /// WASI Preview 2 socket or stream error
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    Wasi(wasip2::io::error::Error),

    /// Browser WebSocket error (stringified because JS errors are not typed)
    WebSocket(String),

    /// Connection closed cleanly by the remote side
    Closed,

    /// Protocol-level error (bad frame, handshake failure, etc.)
    Protocol(String),
}

// For native and WASI: Use async_trait with Send
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use async_trait::async_trait;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
pub trait Transport: Send {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}

// For browser: Use async_trait WITHOUT Send
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[async_trait(?Send)]  // <-- Note the ?Send here
pub trait Transport {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}