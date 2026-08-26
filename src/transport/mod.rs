pub mod bridge;
mod buffered;
mod tcp;
mod websocket;
pub mod rtc_signaling;
pub mod p2p;
pub mod signaling_hub;
pub use bridge::TransportBridge;
pub use p2p::connect_p2p;
#[derive(Debug)]
pub enum TransportKind {
    Tcp,
    WebSocket,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use buffered::BufferedTransport;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    /// The transport type is not supported on this platform
    #[error("Unsupported transport: {0}")]
    Unsupported(String),

    /// A normal I/O error (native TCP, file descriptors, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// WASI Preview 2 socket or stream error
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    #[error("WASI error: {0}")]
    Wasi(#[from] wasip2::io::error::Error),

    /// Browser WebSocket error (stringified because JS errors are not typed)
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Connection closed cleanly by the remote side
    #[error("Connection closed cleanly")]
    Closed,

    /// Protocol-level error (bad frame, handshake failure, etc.)
    #[error("Protocol error: {0}")]
    Protocol(String),
}

// For native and WASI: Use async_trait with Send
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use async_trait::async_trait;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}

// For browser: Use async_trait WITHOUT Send
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[async_trait(?Send)] // <-- Note the ?Send here
pub trait Transport {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}

/// Platform-specific transport creation
pub async fn connect(addr: &str) -> Result<Box<dyn Transport>, TransportError> {
    // WebSocket
    if addr.starts_with("ws://") || addr.starts_with("wss://") {
        #[cfg(not(all(target_arch = "wasm32")))]
        {
            use crate::platform::ws_native::WebSocketNative;
            let ws = WebSocketNative::connect(addr)
                .await
                .map_err(|e| TransportError::WebSocket(format!("{:?}", e)))?;
            return Ok(Box::new(ws) as Box<dyn Transport>);
        }

        #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
        {
            use crate::platform::ws_wasi::WebSocketWasi;
            let ws = WebSocketWasi::connect(addr)
                .await
                .map_err(|e| TransportError::WebSocket(format!("{:?}", e)))?;
            return Ok(Box::new(ws) as Box<dyn Transport>);
        }

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            use crate::platform::ws_browser::WebSocketBrowser;
            let ws = WebSocketBrowser::connect(addr)
                .await
                .map_err(|e| TransportError::WebSocket(format!("{:?}", e)))?;
            return Ok(Box::new(ws) as Box<dyn Transport>);
        }
    } else {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use crate::platform::tcp_native::TcpStreamNative;
            let tcp = TcpStreamNative::connect(addr).await?;
            return Ok(Box::new(tcp) as Box<dyn Transport>);
        }

        #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
        {
            use crate::platform::tcp_wasi::TcpStreamWasi;
            let tcp = TcpStreamWasi::connect(addr).await?;
            return Ok(Box::new(tcp) as Box<dyn Transport>);
        }

        // TCP (not available on browser)
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Err(TransportError::Unsupported(
                "TCP not available in browser".to_string(),
            ))
        }
    }
}
