use crate::transport::{Transport, TransportError};
use std::cell::RefCell;
use std::rc::Rc;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::Uint8Array;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::JsCast;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_sys::{BinaryType, CloseEvent, ErrorEvent, MessageEvent, WebSocket};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use std::sync::mpsc::{Receiver, Sender, channel};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub struct WebSocketBrowser {
    ws: WebSocket,
    rx: Receiver<Vec<u8>>,
    // Keep closures alive
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    _onerror: Closure<dyn FnMut(ErrorEvent)>,
    _onclose: Closure<dyn FnMut(CloseEvent)>,
    _onopen: Closure<dyn FnMut(JsValue)>, // Add this!
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl WebSocketBrowser {
    /// Connect to a WebSocket server
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        log::info!("[WS Browser] Connecting to {}", url);

        // Create WebSocket
        let ws = WebSocket::new(url).map_err(|e| {
            TransportError::Protocol(format!("Failed to create WebSocket: {:?}", e))
        })?;

        // Set binary type to arraybuffer
        ws.set_binary_type(BinaryType::Arraybuffer);

        // Create channel for receiving messages
        let (tx, rx) = channel();

        // Setup onmessage callback
        let tx_msg = tx.clone();
        let onmessage = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(array_buffer) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                let uint8_array = Uint8Array::new(&array_buffer);
                let data = uint8_array.to_vec();
                log::debug!("[WS Browser] Received {} bytes", data.len());
                tx_msg.send(data).ok();
            } else if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
                let data = txt.as_string().unwrap_or_default().into_bytes();
                log::debug!("[WS Browser] Received text message ({} bytes)", data.len());
                tx_msg.send(data).ok();
            }
        }) as Box<dyn FnMut(_)>);

        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        // Setup onerror callback
        let onerror = Closure::wrap(Box::new(move |e: ErrorEvent| {
            log::error!("[WS Browser] WebSocket error: {:?}", e);
        }) as Box<dyn FnMut(_)>);

        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        // Setup onclose callback
        let onclose = Closure::wrap(Box::new(move |e: CloseEvent| {
            log::info!(
                "[WS Browser] WebSocket closed: code={}, reason={}",
                e.code(),
                e.reason()
            );
        }) as Box<dyn FnMut(_)>);

        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        // Setup onopen callback
        let connected = Rc::new(RefCell::new(false));
        let connected_clone = connected.clone();

        let onopen = Closure::wrap(Box::new(move |_| {
            *connected_clone.borrow_mut() = true;
            log::info!("[WS Browser] Connected successfully");
        }) as Box<dyn FnMut(JsValue)>);

        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));

        // Wait for connection (with longer timeout)
        let mut attempts = 0;
        while !*connected.borrow() && attempts < 500 {
            // Increased from 100 to 500
            // Yield to event loop
            aloeplatform::sleep(std::time::Duration::from_millis(10)).await;
            attempts += 1;

            // Check WebSocket state
            if ws.ready_state() == WebSocket::CLOSED {
                return Err(TransportError::Protocol("Connection failed".to_string()));
            }
        }

        if !*connected.borrow() {
            return Err(TransportError::Protocol("Connection timeout".to_string()));
        }

        log::info!("[WS Browser] Connection established");

        Ok(Self {
            ws,
            rx,
            _onmessage: onmessage,
            _onerror: onerror,
            _onclose: onclose,
            _onopen: onopen, // Keep it alive!
        })
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[async_trait(?Send)]
impl Transport for WebSocketBrowser {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        log::debug!("[WS Browser] Sending {} bytes", data.len());

        // Convert to Uint8Array
        let array = Uint8Array::new_with_length(data.len() as u32);
        array.copy_from(data);

        self.ws
            .send_with_array_buffer(&array.buffer())
            .map_err(|e| TransportError::Protocol(format!("Send failed: {:?}", e)))?;

        log::debug!("[WS Browser] Send complete");
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        log::debug!("[WS Browser] Waiting for message");

        // Poll the receiver with timeout
        let mut attempts = 0;
        loop {
            match self.rx.try_recv() {
                Ok(data) => {
                    log::debug!("[WS Browser] Received message ({} bytes)", data.len());
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    return Ok(n);
                }
                Err(_) => {
                    // Check if WebSocket is still open
                    if self.ws.ready_state() == WebSocket::CLOSED {
                        return Err(TransportError::Closed);
                    }

                    attempts += 1;
                    if attempts >= 10000 {
                        return Err(TransportError::Protocol("Receive timeout".to_string()));
                    }

                    // Yield to event loop
                    aloeplatform::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
}
