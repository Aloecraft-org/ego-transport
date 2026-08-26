// bin/test_server.rs
use ego_transport::platform;
use ego_transport::platform::server::ServerBuilder;
use ego_transport::transport::Transport;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::tcp_native::{TcpListenerNative, TcpStreamNative};

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use ego_transport::platform::tcp_wasi::{TcpListenerWasi, TcpStreamWasi};

// ===== NATIVE =====
#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    run_native().await;
}

// ===== WASI =====
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run_wasi());
}

// ===== BROWSER =====
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    panic!("This test is not for browser");
}

// =====================================
// NATIVE IMPLEMENTATION
// =====================================
#[cfg(not(target_arch = "wasm32"))]
async fn run_native() {
    ego_platform::init();
    log::info!("=== Native Server Test with ServerBuilder ===");

    let addr = "127.0.0.1:9997";

    // Start server in background
    let server_addr = addr.to_string();
    ego_platform::spawn(async move {
        run_native_server(&server_addr).await;
    });

    // Give server time to start
    ego_platform::sleep(Duration::from_millis(100)).await;

    // Run multiple clients concurrently
    log::info!("[Native] Spawning 3 concurrent clients...");

    let mut handles = vec![];
    for client_id in 1..=3 {
        let addr = addr.to_string();
        let handle = ego_platform::spawn(async move {
            run_native_client(&addr, client_id).await;
        });
        handles.push(handle);
    }

    // Wait for all clients
    for handle in handles {
        handle.await.ok();
    }

    log::info!("=== Native Test Complete ===");
    ego_platform::sleep(Duration::from_secs(1)).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_native_server(addr: &str) {
    log::info!("[Server] Starting on {}", addr);

    let listener = TcpListenerNative::bind(addr).expect("Failed to bind");

    // Using ServerBuilder with CONCURRENT mode (default on native)
    ServerBuilder::new(listener)
        .concurrent() // Explicit concurrent mode
        .run(|mut transport| async move {
            log::info!("[Server] Handler started for connection");

            let mut buf = [0u8; 1024];
            let mut msg_count = 0;

            loop {
                match transport.recv(&mut buf).await {
                    Ok(n) => {
                        msg_count += 1;
                        let data = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[Server] ✓ Received: {}", data);

                        // Echo back
                        if let Err(e) = transport.send(&buf[..n]).await {
                            log::error!("[Server] ✗ Send error: {:?}", e);
                            break;
                        }

                        // Exit after 2 messages per connection
                        if msg_count >= 2 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            log::info!(
                "[Server] Handler finished (processed {} messages)",
                msg_count
            );
        })
        .await
        .ok();
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_native_client(addr: &str, client_id: u32) {
    log::info!("[Client #{}] Connecting to {}", client_id, addr);

    match TcpStreamNative::connect(addr).await {
        Ok(mut transport) => {
            log::info!("[Client #{}] ✓ Connected", client_id);

            // Send 2 messages
            for i in 1..=2 {
                let msg = format!("Hello from client #{}, message #{}", client_id, i);

                if let Err(e) = transport.send(msg.as_bytes()).await {
                    log::error!("[Client #{}] ✗ Send error: {:?}", client_id, e);
                    return;
                }

                let mut buf = [0u8; 1024];
                match transport.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[Client #{}] ✓ Received echo: {}", client_id, response);
                    }
                    Err(e) => {
                        log::error!("[Client #{}] ✗ Recv error: {:?}", client_id, e);
                        return;
                    }
                }

                ego_platform::sleep(Duration::from_millis(100)).await;
            }

            log::info!("[Client #{}] ✓ Complete", client_id);
        }
        Err(e) => {
            log::error!("[Client #{}] ✗ Connection failed: {:?}", client_id, e);
        }
    }
}

// =====================================
// WASI IMPLEMENTATION
// =====================================
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_wasi() {
    ego_platform::init();
    log::info!("=== WASI Server Test with ServerBuilder ===");

    let addr = "127.0.0.1:9997";

    log::info!("[WASI Server] Starting on {}", addr);

    let listener = TcpStreamWasi::bind(addr).await.expect("Failed to bind");

    // Using ServerBuilder with SEQUENTIAL mode (only option on WASI)
    ServerBuilder::new(listener)
        .sequential() // Explicit sequential mode (this is the only option that compiles)
        .run(|mut transport| async move {
            log::info!("[WASI Server] Handler started for connection");

            let mut buf = [0u8; 1024];
            let mut msg_count = 0;

            loop {
                match transport.recv(&mut buf).await {
                    Ok(n) => {
                        msg_count += 1;
                        let data = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[WASI Server] ✓ Received: {}", data);

                        // Echo back
                        if let Err(e) = transport.send(&buf[..n]).await {
                            log::error!("[WASI Server] ✗ Send error: {:?}", e);
                            break;
                        }

                        // Exit after 2 messages
                        if msg_count >= 2 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            log::info!(
                "[WASI Server] Handler finished (processed {} messages)",
                msg_count
            );

            // Note: In sequential mode, server will only handle ONE connection
            // then exit. For a real server, we'd want the run() loop to continue,
            // but for this test we handle one connection and exit.
        })
        .await
        .ok();

    log::info!("[WASI Server] Server exiting");
}
