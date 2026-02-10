// bin/test_websocket_native.rs

// Native implementation
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::tcp_native::TcpListenerNative;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::ws_native::WebSocketNative;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::transport::Transport;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    platform::init_logging();
    log::info!("=== Native WebSocket Test ===");
    
    let addr = "127.0.0.1:9996";
    
    // Spawn WebSocket server
    let server_addr = addr.to_string();
    tokio::spawn(async move {
        run_ws_server(&server_addr).await;
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Run WebSocket client
    run_ws_client(&format!("ws://{}", addr)).await;
    
    log::info!("=== Test Complete ===");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_ws_server(addr: &str) {
    log::info!("[WS Server] Starting on {}", addr);
    
    let listener = TcpListenerNative::bind(addr)
        .expect("Failed to bind");
    
    loop {
        match listener.accept_websocket().await {
            Ok(mut ws) => {
                log::info!("[WS Server] Client connected");
                
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let mut msg_count = 0;
                    
                    loop {
                        match ws.recv(&mut buf).await {
                            Ok(n) => {
                                msg_count += 1;
                                let data = String::from_utf8_lossy(&buf[..n]);
                                log::info!("[WS Server] ✓ Received: {}", data);
                                
                                // Echo back
                                if let Err(e) = ws.send(&buf[..n]).await {
                                    log::error!("[WS Server] ✗ Send error: {:?}", e);
                                    break;
                                }
                                log::info!("[WS Server] ✓ Echoed {} bytes", n);
                            }
                            Err(e) => {
                                log::info!("[WS Server] Connection ended: {:?}", e);
                                break;
                            }
                        }
                    }
                    log::info!("[WS Server] Handler finished ({} messages)", msg_count);
                });
            }
            Err(e) => {
                log::error!("[WS Server] Accept error: {:?}", e);
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_ws_client(url: &str) {
    log::info!("[WS Client] Connecting to {}", url);
    
    match WebSocketNative::connect(url).await {
        Ok(mut ws) => {
            log::info!("[WS Client] ✓ Connected");
            
            // Send 3 test messages
            for i in 1..=3 {
                let msg = format!("Hello from WebSocket client, message #{}", i);
                log::info!("[WS Client] Sending: {}", msg);
                
                if let Err(e) = ws.send(msg.as_bytes()).await {
                    log::error!("[WS Client] ✗ Send error: {:?}", e);
                    return;
                }
                
                // Receive echo
                let mut buf = [0u8; 1024];
                match ws.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[WS Client] ✓ Received echo: {}", response);
                        
                        if response == msg {
                            log::info!("[WS Client] ✓ Echo matches!");
                        }
                    }
                    Err(e) => {
                        log::error!("[WS Client] ✗ Recv error: {:?}", e);
                        return;
                    }
                }
                
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            
            log::info!("[WS Client] ✓ Test complete!");

            log::info!("[WS Client] Closing connection gracefully");
            drop(ws); // Drop will close the connection
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(e) => {
            log::error!("[WS Client] ✓ Connection failed: {:?}", e);
        }
    }
}

// Stub for WASM targets
#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_websocket_native is only for native platforms");
}