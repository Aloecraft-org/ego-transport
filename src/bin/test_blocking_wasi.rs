// bin/test_blocking_wasi.rs
//
// PROOF OF BUG:
// 1. Runtime Blocking: Accept loop prevents other tasks from running.
// 2. Connection Timeout: Hardcoded loop limit kills idle connections.

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::tcp_wasi::{TcpListenerWasi, TcpStreamWasi};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::transport::Transport;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use std::time::{Duration, Instant};

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_test());
}

#[cfg(not(all(target_arch = "wasm32", target_env = "p2")))]
fn main() {
    println!("This test is for WASI P2 only.");
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_test() {
    aloeclient::platform::init_logging();
    log::info!("=== WASI Blocking Proof Test ===");

    // 1. Spawn a "Heartbeat" task to detect runtime freezing
    aloeplatform::spawn(async {
        let mut i = 0;
        loop {
            i += 1;
            log::info!("💓 Heartbeat tick #{}", i);
            aloeplatform::sleep(Duration::from_millis(500)).await;
        }
    });

    let addr = "127.0.0.1:9998";
    let listener = TcpListenerWasi::bind(addr).await.expect("Bind failed");

    log::info!("[Server] Bound. Accepting... (Heartbeats should continue!)");

    // This accept call SHOULD yield to the runtime, allowing heartbeats.
    // If heartbeats stop appearing here, BUG #1 is confirmed.
    match listener.accept().await {
        Ok(mut transport) => {
            log::info!("[Server] Accepted connection!");

            // Wait for data. The client will be silent for 2 seconds.
            // If recv() returns an error before 2 seconds, BUG #2 is confirmed.
            let mut buf = [0u8; 128];
            match transport.recv(&mut buf).await {
                Ok(n) => log::info!("[Server] Received {} bytes: {:?}", n, &buf[..n]),
                Err(e) => log::error!("[Server] ❌ Connection killed early: {:?}", e),
            }
        }
        Err(e) => log::error!("[Server] Accept failed: {:?}", e),
    }
}
