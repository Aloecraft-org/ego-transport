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
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl WebSocketWasi {
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
        
        Ok(Self { ws })
    }
    
    /// Accept a WebSocket connection from a raw TCP stream.
    pub async fn accept(tcp_stream: TcpStreamWasi) -> Result<Self, TransportError> {
        log::info!("[WS WASI] Accepting WebSocket connection");
        
        let sync_stream = WasiSyncStream::new(tcp_stream);
        
        let ws = accept(sync_stream)
            .map_err(|e| TransportError::Protocol(format!("WebSocket handshake failed: {}", e)))?;
        
        log::info!("[WS WASI] WebSocket handshake complete");
        
        Ok(Self { ws })
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
        log::info!(
            "[WS WASI] Accepting WebSocket connection ({} prefix bytes)",
            prefix.len()
        );

        let sync_stream = WasiSyncStream::with_prefix(tcp_stream, prefix);

        let ws = accept(sync_stream)
            .map_err(|e| TransportError::Protocol(format!("WebSocket handshake failed: {}", e)))?;

        log::info!("[WS WASI] WebSocket handshake complete");

        Ok(Self { ws })
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
                Err(e) => {
                    log::error!("[WS WASI] Receive error: {}", e);
                    return Err(TransportError::Protocol(format!("WebSocket error: {}", e)));
                }
            }
        }
    }
}
