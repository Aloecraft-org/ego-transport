// bin/test_websocket_wasi_client.rs

// Native: Run server
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::tcp_native::TcpListenerNative;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::transport::Transport;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

// WASI: Run client
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::ws_wasi::WebSocketWasi;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::transport::Transport;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use std::time::Duration;

// ===== NATIVE SERVER =====
#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    aloeplatform::init();
    log::info!("=== Native WebSocket Server for WASI Client ===");

    let addr = "127.0.0.1:9995";
    run_native_server(addr).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_native_server(addr: &str) {
    log::info!("[WS Server] Starting on {}", addr);

    let listener = TcpListenerNative::bind(addr).expect("Failed to bind");

    loop {
        match listener.accept_websocket().await {
            Ok(mut ws) => {
                log::info!("[WS Server] Client connected");

                aloeplatform::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let mut msg_count = 0;

                    loop {
                        match ws.recv(&mut buf).await {
                            Ok(n) => {
                                msg_count += 1;
                                let data = String::from_utf8_lossy(&buf[..n]);
                                log::info!("[WS Server] ✓ Received: {}", data);

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

// ===== WASI CLIENT =====
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    println!("WASI client starting...");

    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            println!("Tokio runtime started");
            run_wasi_client().await;
            println!("Client finished");
        });
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_wasi_client() {
    println!("run_wasi_client called");
    aloeplatform::init();
    println!("Logging initialized");
    log::info!("=== WASI WebSocket Client Test ===");

    let url = "ws://127.0.0.1:9995";

    log::info!("[WS WASI Client] Connecting to {}", url);

    match WebSocketWasi::connect(url).await {
        Ok(mut ws) => {
            log::info!("[WS WASI Client] ✓ Connected");

            for i in 1..=3 {
                let msg = format!("Hello from WASI WebSocket client, message #{}", i);
                log::info!("[WS WASI Client] Sending: {}", msg);

                if let Err(e) = ws.send(msg.as_bytes()).await {
                    log::error!("[WS WASI Client] ✗ Send error: {:?}", e);
                    return;
                }

                let mut buf = [0u8; 1024];
                match ws.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[WS WASI Client] ✓ Received echo: {}", response);

                        if response == msg {
                            log::info!("[WS WASI Client] ✓ Echo matches!");
                        }
                    }
                    Err(e) => {
                        log::error!("[WS WASI Client] ✗ Recv error: {:?}", e);
                        return;
                    }
                }

                aloeplatform::sleep(Duration::from_millis(500)).await;
            }

            log::info!("[WS WASI Client] ✓ Test complete!");
        }
        Err(e) => {
            log::error!("[WS WASI Client] ✗ Connection failed: {:?}", e);
        }
    }

    aloeplatform::sleep(Duration::from_secs(1)).await;
}

// Stub for browser
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    panic!("test_websocket_wasi_client is not for browser");
}
