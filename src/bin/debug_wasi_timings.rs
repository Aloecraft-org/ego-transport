// bin/debug_wasi_timings.rs
//
// DIAGNOSTIC GOAL:
// 1. Prove accept() blocks the thread (Task heartbeats stop).
// 2. Prove recv() spins instantly (100 attempts take ~0ms).

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::tcp_wasi::TcpListenerWasi;
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
        .block_on(run_diagnostics());
}

#[cfg(not(all(target_arch = "wasm32", target_env = "p2")))]
fn main() {}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_diagnostics() {
    aloeclient::platform::init_logging();
    log::info!("=== WASI Timing Diagnostics ===");

    // 1. Start a Heartbeat Monitor
    // If accept() yields properly, this logs every 500ms.
    // If accept() blocks, this will SILENTLY PAUSE.
    aloeplatform::spawn(async {
        let start = Instant::now();
        let mut tick = 0;
        loop {
            tick += 1;
            log::info!(
                "💓 Tick #{}: {:.2}s elapsed",
                tick,
                start.elapsed().as_secs_f32()
            );
            aloeplatform::sleep(Duration::from_millis(500)).await;
        }
    });

    let addr = "127.0.0.1:9998";
    log::info!("[Test] Binding to {}...", addr);
    let listener = TcpListenerWasi::bind(addr).await.expect("Bind failed");

    log::info!("[Test] Entering accept()... (Please connect via Native client now)");
    log::info!("[Test] Expectation: If code is correct, Ticks continue. If blocking, Ticks stop.");

    let accept_start = Instant::now();

    match listener.accept().await {
        Ok(mut transport) => {
            let accept_duration = accept_start.elapsed();
            log::info!(
                "[Test] ✅ Connection Accepted after {:.2}s",
                accept_duration.as_secs_f32()
            );

            // 2. Measure Recv Spin Speed
            // We expect the native client to wait 2 seconds before sending data.
            // If the loop spins, it will die in <1ms.
            log::info!("[Test] Attempting read (Client should be silent for 2s)...");

            let read_start = Instant::now();
            let mut buf = [0u8; 128];

            let result = transport.recv(&mut buf).await;
            let read_duration = read_start.elapsed();

            match result {
                Ok(n) => log::info!(
                    "[Test] Data received: {} bytes in {:.4}s",
                    n,
                    read_duration.as_secs_f32()
                ),
                Err(e) => {
                    log::error!(
                        "[Test] ❌ Read FAILED in {:.4}s. Error: {:?}",
                        read_duration.as_secs_f32(),
                        e
                    );
                    if read_duration.as_millis() < 100 {
                        log::error!(
                            "[Test] CONCLUSION: The recv loop is spinning furiously (CPU hog)."
                        );
                    } else {
                        log::info!("[Test] CONCLUSION: The recv loop waited correctly.");
                    }
                }
            }
        }
        Err(e) => log::error!("[Test] Accept failed: {:?}", e),
    }

    // Keep alive to see final heartbeats
    aloeplatform::sleep(Duration::from_secs(2)).await;
}
