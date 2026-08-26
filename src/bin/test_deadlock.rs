// bin/test_deadlock.rs
//
// SELF-CONTAINED REPRODUCTION:
// 1. Spawns a background task to connect to itself after 1 second.
// 2. Main task enters accept().
//
// FAILURE MODE (Current):
//   The main task enters accept(), calls blocking poll(), and freezes the thread.
//   The background task NEVER runs. The connection never happens. The test hangs forever.
//
// SUCCESS MODE (Fixed):
//   The main task enters accept(), yields to runtime.
//   Heartbeats print.
//   Background task wakes up, connects.
//   accept() returns.

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use ego_transport::platform::tcp_wasi::{TcpListenerWasi, TcpStreamWasi};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use ego_transport::transport::Transport;

use ego_platform::Instant;
use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_deadlock_test());
}

#[cfg(not(all(target_arch = "wasm32", target_env = "p2")))]
fn main() {
    println!("This test is for WASI P2 only.");
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_deadlock_test() {
    ego_platform::init();
    log::info!("=== WASI Deadlock Test ===");

    let addr = "127.0.0.1:9990";
    let listener = TcpListenerWasi::bind(addr).await.expect("Bind failed");

    // 1. Spawn the "Rescue" Client
    // If the runtime works, this will wake up in 1s and unblock the server.
    ego_platform::spawn(async move {
        log::info!("[Client] Scheduled. Sleeping 1s...");
        ego_platform::sleep(Duration::from_secs(1)).await;

        log::info!("[Client] Waking up! Connecting...");
        match TcpStreamWasi::connect(addr).await {
            Ok(mut stream) => {
                log::info!("[Client] Connected! Sending data in 2s...");
                // Wait 2s to test the "100 attempts" timeout bug too
                ego_platform::sleep(Duration::from_secs(2)).await;
                let _ = stream.send(b"Hello").await;
            }
            Err(e) => log::error!("[Client] Connection failed: {:?}", e),
        }
    });

    // 2. Spawn Heartbeat (Visual Proof of Liveness)
    ego_platform::spawn(async {
        let mut i = 0;
        loop {
            i += 1;
            log::info!("💓 Tick #{}", i);
            ego_platform::sleep(Duration::from_millis(200)).await;
        }
    });

    log::info!("[Server] Entering accept(). If this hangs > 1s, it is BROKEN.");

    // THE TRAP:
    match listener.accept().await {
        Ok(mut transport) => {
            log::info!("[Server] Connection Accepted!");

            let mut buf = [0u8; 128];
            log::info!("[Server] Waiting for data...");
            match transport.recv(&mut buf).await {
                Ok(n) => log::info!("[Server] Got {} bytes.", n),
                Err(e) => log::error!("[Server] Read Error: {:?}", e),
            }
        }
        Err(e) => log::error!("[Server] Accept Error: {:?}", e),
    }
}
