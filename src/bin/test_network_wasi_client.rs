// bin/test_network_wasi_client.rs
use aloeclient::platform;
use aloeclient::transport::{Transport, TransportError};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::tcp_native::TcpStreamNative;
#[cfg(not(target_arch = "wasm32"))]
use std::net::TcpListener;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::tcp_wasi::TcpStreamWasi;

// Native: Run the server
#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    run_native_server().await;
}

// WASI: Run the client
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run_wasi_client());
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    panic!("This test is not for browser");
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_native_server() {
    platform::init_logging();
    log::info!("=== Native TCP Server for WASI Client Test ===");

    let addr = "127.0.0.1:9999";

    log::info!("[Server] Starting on {}", addr);

    let listener = TcpListener::bind(addr).expect("Failed to bind listener");
    listener
        .set_nonblocking(true)
        .expect("Failed to set nonblocking");

    // #[cfg(unix)]
    // {
    //     use std::os::unix::io::AsRawFd;
    //     unsafe {
    //         let fd = listener.as_raw_fd();
    //         let optval: libc::c_int = 1;
    //         libc::setsockopt(
    //             fd,
    //             libc::SOL_SOCKET,
    //             libc::SO_REUSEADDR,
    //             &optval as *const _ as *const libc::c_void,
    //             std::mem::size_of_val(&optval) as libc::socklen_t,
    //         );
    //     }
    // }

    log::info!("[Server] Listening, waiting for WASI client...");

    loop {
        match listener.accept() {
            Ok((stream, peer_addr)) => {
                log::info!("[Server] ✓ Accepted connection from {}", peer_addr);

                // Spawn handler for this connection
                platform::spawn::spawn(async move {
                    let mut transport = TcpStreamNative { inner: stream };
                    transport.inner.set_nonblocking(true).ok();

                    let mut buf = [0u8; 1024];
                    let mut msg_count = 0;

                    loop {
                        match transport.recv(&mut buf).await {
                            Ok(n) => {
                                msg_count += 1;
                                let data = String::from_utf8_lossy(&buf[..n]);
                                log::info!("[Server] ✓ Received message #{}: {}", msg_count, data);

                                // Echo back
                                if let Err(e) = transport.send(&buf[..n]).await {
                                    log::error!("[Server] ✗ Send error: {:?}", e);
                                    break;
                                }
                                log::info!("[Server] ✓ Echoed {} bytes back", n);
                            }
                            Err(TransportError::Closed) => {
                                log::info!("[Server] Connection closed cleanly");
                                break;
                            }
                            Err(e) => {
                                log::error!("[Server] ✗ Recv error: {:?}", e);
                                break;
                            }
                        }
                    }

                    log::info!(
                        "[Server] Connection handler finished (processed {} messages)",
                        msg_count
                    );
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(e) => {
                log::error!("[Server] ✗ Accept error: {:?}", e);
                break;
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_wasi_client() {
    platform::init_logging();
    log::info!("=== WASI TCP Client Test ===");

    let addr = "127.0.0.1:9999";

    log::info!("[WASI Client] Connecting to {}", addr);

    match TcpStreamWasi::connect(addr).await {
        Ok(mut transport) => {
            log::info!("[WASI Client] ✓ Connected successfully!");

            // Send test messages
            for i in 1..=3 {
                let msg = format!("Hello from WASI client, message #{}", i);
                log::info!("[WASI Client] Sending: {}", msg);

                if let Err(e) = transport.send(msg.as_bytes()).await {
                    log::error!("[WASI Client] ✗ Send error: {:?}", e);
                    return;
                }
                log::info!("[WASI Client] ✓ Sent {} bytes", msg.len());

                // Give server time to echo back
                platform::sleep::sleep(Duration::from_millis(50)).await;

                // Receive echo
                let mut buf = [0u8; 1024];
                match transport.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[WASI Client] ✓ Received echo ({} bytes): {}", n, response);

                        if response == msg {
                            log::info!("[WASI Client] ✓ Echo matches!");
                        } else {
                            log::error!("[WASI Client] ✗ Echo mismatch!");
                        }
                    }
                    Err(e) => {
                        log::error!("[WASI Client] ✗ Recv error: {:?}", e);
                        return;
                    }
                }

                platform::sleep::sleep(Duration::from_millis(500)).await;
            }

            log::info!("[WASI Client] ✓ Test complete!");
        }
        Err(e) => {
            log::error!("[WASI Client] ✗ Connection failed: {:?}", e);
        }
    }

    // Give logs time to flush
    platform::sleep::sleep(Duration::from_secs(1)).await;
}
