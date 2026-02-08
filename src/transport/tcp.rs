
use std::fmt::format;

use crate::transport::{TransportKind, TransportError, Transport};

#[cfg(not(target_arch = "wasm32"))]
use crate::platform::tcp_native::TcpStreamNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::tcp_wasi::TcpStreamWasi;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::platform::ws_browser::WebSocketBrowser;


pub async fn connect(addr: &str, kind: TransportKind) -> Result<Box<dyn Transport>, TransportError> {
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(kind, TransportKind::Tcp) {
        return Ok(Box::new(TcpStreamNative::connect(addr).await?));
    }

    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    if matches!(kind, TransportKind::Tcp) {
        return Ok(Box::new(TcpStreamWasi::connect(addr).await?));
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    if matches!(kind, TransportKind::WebSocket) {
        return Ok(Box::new(WebSocketBrowser::connect(addr).await?));
    }

    Err(TransportError::Unsupported(format!("{:?}",kind)))
}

pub async fn connect_tcp(addr: &str) -> Result<Box<dyn Transport>, TransportError> {
    #[cfg(not(target_arch = "wasm32"))]
    return Ok(Box::new(TcpStreamNative::connect(addr).await?));
    
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    return Ok(Box::new(TcpStreamWasi::connect(addr).await?));
    
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    return Err(TransportError::Unsupported("TCP Not Supported For This Action On Browser".to_string()));
}
