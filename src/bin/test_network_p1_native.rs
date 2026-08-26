// bin/test_network.rs
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::{Transport, TransportError};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::tcp_native::TcpStreamNative;

#[cfg(not(target_arch = "wasm32"))]
use std::net::TcpListener;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    run_native().await;
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run_wasi());
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    wasm_bindgen_futures::spawn_local(run_browser());
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_native() {
    ego_platform::init();
    log::info!("=== Native TCP Test Starting ===");

    let addr = "127.0.0.1:9999";

    // Spawn listener task
    let listener_addr = addr.to_string();
    ego_platform::spawn(async move {
        log::info!("[Listener] Starting on {}", listener_addr);

        let listener = TcpListener::bind(&listener_addr).expect("Failed to bind listener");
        listener
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");

        log::info!("[Listener] Bound successfully, waiting for connections...");

        loop {
            match listener.accept() {
                Ok((stream, peer_addr)) => {
                    log::info!("[Listener] Accepted connection from {}", peer_addr);

                    // Spawn handler for this connection
                    ego_platform::spawn(async move {
                        let mut transport = TcpStreamNative { inner: stream };
                        transport.inner.set_nonblocking(true).ok();

                        let mut buf = [0u8; 1024];
                        loop {
                            match transport.recv(&mut buf).await {
                                Ok(n) => {
                                    log::info!("[Listener] Received {} bytes", n);
                                    log::info!(
                                        "[Listener] Data: {:?}",
                                        String::from_utf8_lossy(&buf[..n])
                                    );

                                    // Echo back
                                    if let Err(e) = transport.send(&buf[..n]).await {
                                        log::error!("[Listener] Send error: {:?}", e);
                                        break;
                                    }
                                    log::info!("[Listener] Echoed {} bytes back", n);
                                }
                                Err(TransportError::Closed) => {
                                    log::info!("[Listener] Connection closed");
                                    break;
                                }
                                Err(e) => {
                                    log::error!("[Listener] Recv error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No connection yet, yield
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    log::error!("[Listener] Accept error: {:?}", e);
                    break;
                }
            }
        }
    });

    // Give listener time to start
    ego_platform::sleep(Duration::from_millis(100)).await;

    // Spawn client task
    let client_addr = addr.to_string();
    ego_platform::spawn(async move {
        log::info!("[Client] Connecting to {}", client_addr);

        match TcpStreamNative::connect(&client_addr).await {
            Ok(mut transport) => {
                log::info!("[Client] Connected successfully!");

                // Send a few test messages
                for i in 1..=3 {
                    let msg = format!("Hello from client, message #{}", i);
                    log::info!("[Client] Sending: {}", msg);

                    if let Err(e) = transport.send(msg.as_bytes()).await {
                        log::error!("[Client] Send error: {:?}", e);
                        return;
                    }

                    // Receive echo
                    let mut buf = [0u8; 1024];
                    match transport.recv(&mut buf).await {
                        Ok(n) => {
                            let response = String::from_utf8_lossy(&buf[..n]);
                            log::info!("[Client] Received echo: {}", response);
                        }
                        Err(e) => {
                            log::error!("[Client] Recv error: {:?}", e);
                            return;
                        }
                    }

                    ego_platform::sleep(Duration::from_secs(1)).await;
                }

                log::info!("[Client] Test complete!");
            }
            Err(e) => {
                log::error!("[Client] Connection failed: {:?}", e);
            }
        }
    });

    // Keep main alive to see the test complete
    for i in 0..10 {
        ego_platform::sleep(Duration::from_secs(1)).await;
        if i == 0 {
            log::info!("[Main] Test running...");
        }
    }

    log::info!("=== Test Complete ===");
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_wasi() {
    ego_platform::init();
    log::info!("WASI test - not implemented yet");
    loop {
        ego_platform::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn run_browser() {
    ego_platform::init();
    log::info!("Browser test - not implemented yet");
    loop {
        ego_platform::sleep(Duration::from_secs(5)).await;
    }
}
