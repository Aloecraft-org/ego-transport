pub mod log_impl;
pub mod platform;
pub mod transport;

#[cfg(not(target_arch = "wasm32"))]
pub use platform::ws_native::WebSocketNative;

#[cfg(not(target_arch = "wasm32"))]
pub use platform::tcp_native::TcpStreamNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub use platform::tcp_wasi::TcpStreamWasi;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub use platform::ws_wasi::WebSocketWasi;

use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;

pub async fn start() {
    platform::init_logging();
    log::info!("Test network starting...");

    // Stub listener task
    aloeplatform::spawn(async {
        log::info!("Listener task started (stub)");
        loop {
            aloeplatform::sleep(Duration::from_secs(1)).await;
        }
    });

    // Stub dialer task
    aloeplatform::spawn(async {
        log::info!("Dialer task started (stub)");
        loop {
            aloeplatform::sleep(Duration::from_secs(1)).await;
        }
    });

    // Keep main alive
    loop {
        log::info!("Main loop tick");
        aloeplatform::sleep(Duration::from_secs(5)).await;
    }
}
