use crate::platform::sleep::sleep;
use crate::transport::{Transport, TransportError};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, connect_async, tungstenite::protocol::Message,
};

#[cfg(not(target_arch = "wasm32"))]
use tokio::net::TcpStream;

#[cfg(not(target_arch = "wasm32"))]
use futures::{SinkExt, StreamExt};

#[cfg(not(target_arch = "wasm32"))]
pub struct WebSocketNative {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl WebSocketNative {
    /// Connect to a WebSocket server
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        log::info!("[WS Native] Connecting to {}", url);

        let (ws_stream, _response) = connect_async(url)
            .await
            .map_err(|e| TransportError::Protocol(format!("WebSocket connect failed: {}", e)))?;

        log::info!("[WS Native] Connected successfully");

        Ok(Self { stream: ws_stream })
    }
    /// Accept a WebSocket connection from a TCP stream
    pub async fn accept(tcp_stream: TcpStream) -> Result<Self, TransportError> {
        log::info!("[WS Native] Accepting WebSocket connection");

        // Wrap the TcpStream in MaybeTlsStream::Plain (no TLS)
        let stream = MaybeTlsStream::Plain(tcp_stream);

        let ws_stream = accept_async(stream)
            .await
            .map_err(|e| TransportError::Protocol(format!("WebSocket handshake failed: {}", e)))?;

        log::info!("[WS Native] WebSocket handshake complete");

        Ok(Self { stream: ws_stream })
    }
}

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Transport for WebSocketNative {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        log::debug!("[WS Native] Sending {} bytes", data.len());

        let message = Message::Binary(data.to_vec());

        self.stream
            .send(message)
            .await
            .map_err(|e| TransportError::Protocol(format!("WebSocket send failed: {}", e)))?;

        log::debug!("[WS Native] Send complete");
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        log::debug!("[WS Native] Waiting for message");

        loop {
            match self.stream.next().await {
                Some(Ok(message)) => {
                    match message {
                        Message::Binary(data) => {
                            log::debug!(
                                "[WS Native] Received binary message ({} bytes)",
                                data.len()
                            );
                            let n = data.len().min(buf.len());
                            buf[..n].copy_from_slice(&data[..n]);
                            return Ok(n);
                        }
                        Message::Text(text) => {
                            log::debug!("[WS Native] Received text message ({} bytes)", text.len());
                            let data = text.as_bytes();
                            let n = data.len().min(buf.len());
                            buf[..n].copy_from_slice(&data[..n]);
                            return Ok(n);
                        }
                        Message::Ping(payload) => {
                            log::debug!("[WS Native] Received ping, sending pong");
                            self.stream.send(Message::Pong(payload)).await.ok();
                            // Continue loop to get next message
                        }
                        Message::Pong(_) => {
                            log::debug!("[WS Native] Received pong");
                            // Continue loop to get next message
                        }
                        Message::Close(frame) => {
                            log::info!("[WS Native] Received close frame: {:?}", frame);
                            return Err(TransportError::Closed);
                        }
                        Message::Frame(_) => {
                            // Raw frame - shouldn't happen in normal usage
                            log::warn!("[WS Native] Received raw frame (unexpected)");
                            continue;
                        }
                    }
                }
                Some(Err(e)) => {
                    log::error!("[WS Native] Receive error: {}", e);
                    return Err(TransportError::Protocol(format!("WebSocket error: {}", e)));
                }
                None => {
                    log::info!("[WS Native] Connection closed");
                    return Err(TransportError::Closed);
                }
            }
        }
    }
}
