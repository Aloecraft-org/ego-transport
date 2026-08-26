// bin/test_network.rs
use ego_transport::platform;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    run().await;
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    // WASI P2 with tokio
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run());
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    wasm_bindgen_futures::spawn_local(run());
}

async fn run() {
    ego_platform::init();
    log::info!("Test network starting...");

    // Stub listener task
    ego_platform::spawn(async {
        log::info!("Listener task started (stub)");
        loop {
            ego_platform::sleep(Duration::from_secs(1)).await;
        }
    });

    // Stub dialer task
    ego_platform::spawn(async {
        log::info!("Dialer task started (stub)");
        loop {
            ego_platform::sleep(Duration::from_secs(1)).await;
        }
    });

    // Keep main alive
    loop {
        log::info!("Main loop tick");
        ego_platform::sleep(Duration::from_secs(5)).await;
    }
}
