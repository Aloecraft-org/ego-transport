use crate::transport::{Transport, TransportError};
use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::tcp_wasi::TcpStreamWasi;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::wasi_sync_adapter::WasiSyncStream;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use tungstenite::{
    accept, client,
    protocol::{Message, WebSocket},
    handshake::client::Request,
};

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct WebSocketWasi {
    ws: WebSocket<WasiSyncStream>,
    peer_addr: Option<String>,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl WebSocketWasi {

    /// Get the remote peer address from the underlying TCP stream
    pub fn peer_addr(&self) -> Option<String> {
        // We'll have this if we're the listener, not if we're the initiator
        self.peer_addr.clone()
    }

    /// Connect to a WebSocket server
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        log::info!("[WS WASI] Connecting to {}", url);
        
        // Parse URL to get host and port
        let url = url.strip_prefix("ws://")
            .ok_or_else(|| TransportError::Protocol("URL must start with ws://".to_string()))?;
        
        let (host, path) = if let Some(idx) = url.find('/') {
            (&url[..idx], &url[idx..])
        } else {
            (url, "/")
        };
        
        let addr = if host.contains(':') {
            host.to_string()
        } else {
            format!("{}:80", host)
        };
        
        log::info!("[WS WASI] Connecting to TCP {}", addr);
        
        // Connect via TCP
        let tcp_stream = TcpStreamWasi::connect(&addr).await?;
        let sync_stream = WasiSyncStream::new(tcp_stream);
        
        // Build WebSocket request
        let request = Request::builder()
            .uri(format!("ws://{}{}", host, path))
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tungstenite::handshake::client::generate_key())
            .body(())
            .map_err(|e| TransportError::Protocol(format!("Failed to build request: {}", e)))?;
        
        log::info!("[WS WASI] Performing WebSocket handshake");
        
        // Perform handshake
        let (ws, _response) = client(request, sync_stream)
            .map_err(|e| TransportError::Protocol(format!("WebSocket handshake failed: {}", e)))?;
        
        log::info!("[WS WASI] Connected successfully");
        
        Ok(Self { ws, peer_addr: None })
    }
    
    /// Accept a WebSocket connection from a raw TCP stream.
    pub async fn accept(tcp_stream: TcpStreamWasi) -> Result<Self, TransportError> {
        let peer_addr = tcp_stream.peer_addr();

        log::info!("[WS WASI] Accepting WebSocket connection from {:?}", peer_addr);
        let sync_stream = WasiSyncStream::new(tcp_stream);
        let ws = accept(sync_stream)
            .map_err(|e| TransportError::Protocol(format!("WebSocket handshake failed: {}", e)))?;
        log::info!("[WS WASI] WebSocket handshake complete");
        Ok(Self { ws, peer_addr })
    }

    /// Accept a WebSocket connection from a TCP stream where `prefix` bytes have
    /// already been consumed (e.g., during protocol auto-detection).
    ///
    /// The prefix bytes are replayed through `WasiSyncStream` so that tungstenite
    /// sees the complete HTTP upgrade request. On WASI, streams have no `peek()`
    /// support, so `AutoDetectListener` reads the first few bytes to sniff the
    /// protocol and must replay them for the handshake to succeed.
    pub async fn accept_with_prefix(
        tcp_stream: TcpStreamWasi,
        prefix: Vec<u8>,
    ) -> Result<Self, TransportError> {
        let peer_addr = tcp_stream.peer_addr();
        log::info!(
            "[WS WASI] Accepting WebSocket connection from {:?} ({} prefix bytes)",
            peer_addr, prefix.len()
        );

        let sync_stream = WasiSyncStream::with_prefix(tcp_stream, prefix);

        let ws = accept(sync_stream)
            .map_err(|e| TransportError::Protocol(format!("WebSocket handshake failed: {}", e)))?;

        log::info!("[WS WASI] WebSocket handshake complete");

        Ok(Self { ws, peer_addr })
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
#[async_trait]
impl Transport for WebSocketWasi {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        log::debug!("[WS WASI] Sending {} bytes", data.len());
        
        let message = Message::Binary(data.to_vec());
        
        self.ws
            .send(message)
            .map_err(|e| TransportError::Protocol(format!("WebSocket send failed: {}", e)))?;
        
        log::debug!("[WS WASI] Send complete");
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        log::debug!("[WS WASI] Waiting for message");
        
        loop {
            tokio::task::yield_now().await;
            match self.ws.read() {
                Ok(message) => {
                    match message {
                        Message::Binary(data) => {
                            log::debug!("[WS WASI] Received binary message ({} bytes)", data.len());
                            let n = data.len().min(buf.len());
                            buf[..n].copy_from_slice(&data[..n]);
                            return Ok(n);
                        }
                        Message::Text(text) => {
                            log::debug!("[WS WASI] Received text message ({} bytes)", text.len());
                            let data = text.as_bytes();
                            let n = data.len().min(buf.len());
                            buf[..n].copy_from_slice(&data[..n]);
                            return Ok(n);
                        }
                        Message::Ping(payload) => {
                            log::debug!("[WS WASI] Received ping, sending pong");
                            self.ws.send(Message::Pong(payload)).ok();
                        }
                        Message::Pong(_) => {
                            log::debug!("[WS WASI] Received pong");
                        }
                        Message::Close(frame) => {
                            log::info!("[WS WASI] Received close frame: {:?}", frame);
                            return Err(TransportError::Closed);
                        }
                        Message::Frame(_) => {
                            log::warn!("[WS WASI] Received raw frame (unexpected)");
                            continue;
                        }
                    }
                }
                Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available, yield and retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(e) => {
                    return Err(TransportError::Protocol(format!("WebSocket read failed: {}", e)));
                }
            }
        }
    }
}
