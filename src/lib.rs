pub mod transport;
pub mod platform;
pub mod log_impl;
use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;

// #[cfg_attr(
//     all(target_arch = "wasm32", target_os = "unknown"),
//     wasm_bindgen(start)
// )]
pub async fn start() {
    platform::init_logging();
    log::info!("Test network starting...");
    
    // Stub listener task
    platform::spawn::spawn(async {
        log::info!("Listener task started (stub)");
        loop {
            platform::sleep::sleep(Duration::from_secs(1)).await;
        }
    });
    
    // Stub dialer task
    platform::spawn::spawn(async {
        log::info!("Dialer task started (stub)");
        loop {
            platform::sleep::sleep(Duration::from_secs(1)).await;
        }
    });
    
    // Keep main alive
    loop {
        log::info!("Main loop tick");
        platform::sleep::sleep(Duration::from_secs(5)).await;
    }
}