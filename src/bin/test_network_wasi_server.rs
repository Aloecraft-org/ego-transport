// bin/test_network_wasi_server.rs
use aloeclient::platform;
use aloeclient::transport::{Transport, TransportError};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::tcp_native::TcpStreamNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::tcp_wasi::TcpStreamWasi;

// Native: Run the client
#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    run_native_client().await;
}

// WASI: Run the server
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run_wasi_server());
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    panic!("This test is not for browser");
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_native_client() {
    platform::init_logging();
    log::info!("=== Native TCP Client for WASI Server Test ===");

    let addr = "127.0.0.1:9997";

    // Give WASI server time to start
    aloeplatform::sleep(Duration::from_secs(1)).await;

    log::info!("[Client] Connecting to WASI server at {}", addr);

    match TcpStreamNative::connect(addr).await {
        Ok(mut transport) => {
            log::info!("[Client] ✓ Connected successfully!");

            // Send test messages
            for i in 1..=2 {
                let msg = format!("Hello from native client, message #{}", i);
                log::info!("[Client] Sending: {}", msg);

                if let Err(e) = transport.send(msg.as_bytes()).await {
                    log::error!("[Client] ✗ Send error: {:?}", e);
                    return;
                }
                log::info!("[Client] ✓ Sent {} bytes", msg.len());

                // Receive echo
                let mut buf = [0u8; 1024];
                match transport.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[Client] ✓ Received echo ({} bytes): {}", n, response);

                        if response == msg {
                            log::info!("[Client] ✓ Echo matches!");
                        } else {
                            log::error!("[Client] ✗ Echo mismatch!");
                        }
                    }
                    Err(e) => {
                        log::error!("[Client] ✗ Recv error: {:?}", e);
                        return;
                    }
                }

                aloeplatform::sleep(Duration::from_millis(500)).await;
            }

            log::info!("[Client] ✓ Test complete!");
        }
        Err(e) => {
            log::error!("[Client] ✗ Connection failed: {:?}", e);
        }
    }

    // Give logs time to flush
    aloeplatform::sleep(Duration::from_secs(1)).await;
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_wasi_server() {
    platform::init_logging();
    log::info!("=== WASI TCP Server Test ===");

    let addr = "127.0.0.1:9997";

    log::info!("[WASI Server] Attempting to bind to {}", addr);

    match TcpStreamWasi::bind(addr).await {
        Ok(listener) => {
            log::info!("[WASI Server] ✓ Bound successfully, listening...");

            // Accept just ONE connection and handle it inline (no spawn)
            match listener.accept().await {
                Ok(mut transport) => {
                    log::info!("[WASI Server] ✓ Accepted connection");

                    let mut buf = [0u8; 1024];
                    let mut msg_count = 0;

                    log::info!("[WASI Server] Starting echo loop...");

                    loop {
                        log::info!("[WASI Server] Waiting for data...");
                        match transport.recv(&mut buf).await {
                            Ok(n) => {
                                msg_count += 1;
                                let data = String::from_utf8_lossy(&buf[..n]);
                                log::info!(
                                    "[WASI Server] ✓ Received message #{}: {}",
                                    msg_count,
                                    data
                                );

                                // Echo back
                                if let Err(e) = transport.send(&buf[..n]).await {
                                    log::error!("[WASI Server] ✗ Send error: {:?}", e);
                                    break;
                                }
                                log::info!("[WASI Server] ✓ Echoed {} bytes back", n);

                                // Exit after 3 messages
                                if msg_count >= 3 {
                                    log::info!("[WASI Server] Test complete!");
                                    break;
                                }
                            }
                            Err(TransportError::Closed) => {
                                log::info!("[WASI Server] Connection closed cleanly");
                                break;
                            }
                            Err(e) => {
                                log::error!("[WASI Server] ✗ Recv error: {:?}", e);
                                break;
                            }
                        }
                    }

                    log::info!(
                        "[WASI Server] Connection finished (processed {} messages)",
                        msg_count
                    );
                }
                Err(e) => {
                    log::error!("[WASI Server] ✗ Accept error: {:?}", e);
                }
            }
        }
        Err(e) => {
            log::error!("[WASI Server] ✗ Bind failed: {:?}", e);
        }
    }

    log::info!("[WASI Server] Server exiting");
}
