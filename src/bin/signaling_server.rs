// bin/signaling_server.rs
//
// WebRTC signaling server — now just a thin wrapper around AutoDetectListener
// with an embedded SignalingHub.
//
// All the signaling logic lives in SignalingHub. This binary just binds a port
// and lets AutoDetectListener handle everything.
//
// Usage:
//   cargo run --bin signaling_server
//   # Listens on 127.0.0.1:9995 (configurable via SIGNAL_ADDR env var)

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::signaling_hub::SignalingHub;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    ego_platform::init();

    let addr = std::env::var("SIGNAL_ADDR").unwrap_or_else(|_| "127.0.0.1:9995".to_string());
    log::info!("[Signaling] Starting on {}", addr);

    let hub = SignalingHub::new();

    let listener = AutoDetectListener::bind(&addr)
        .await
        .expect("Failed to bind")
        .with_signaling(hub);

    // All connections are signaling — the app handler is never called.
    // But ServerBuilder still needs a handler, so we provide a no-op.
    if let Err(e) = ServerBuilder::new(listener)
        .concurrent()
        .run(|_transport| async {
            log::debug!("[Signaling] Non-signaling connection (unexpected)");
        })
        .await
    {
        log::error!("[Signaling] Server error: {:?}", e);
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("signaling_server is native-only");
}
