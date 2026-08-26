// bin/test_p2p_native_peer.rs
//
// Standalone native peer for cross-platform WebRTC testing.
//
// Connects to an EXTERNAL signaling server (not embedded) and joins a room.
// Intended to be used alongside a browser peer to test browser ↔ native
// WebRTC data channel connections.
//
// Usage:
//   1. cargo run --bin signaling_server
//   2. cargo run --bin test_p2p_native_peer         (this binary)
//   3. Open test_rtc_browser.html in a browser
//
// Environment variables:
//   SIGNAL_URL  - Signaling server URL (default: ws://127.0.0.1:9995)
//   ROOM        - Room name (default: test-rtc-room)

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::rtc_signaling::IceServerConfig;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::Transport;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    ego_platform::init();

    let signal_url =
        std::env::var("SIGNAL_URL").unwrap_or_else(|_| "ws://127.0.0.1:9995".to_string());
    let room = std::env::var("ROOM").unwrap_or_else(|_| "test-rtc-room".to_string());

    log::info!("=== Native P2P Peer ===");
    log::info!("Signaling: {}", signal_url);
    log::info!("Room:      {}", room);
    log::info!("");

    let ice_servers = IceServerConfig::default_config();

    log::info!("[Native] Connecting via connect_p2p...");
    let mut transport =
        match ego_transport::transport::connect_p2p(&signal_url, &room, &ice_servers).await {
            Ok(t) => t,
            Err(e) => {
                log::error!("[Native] ✗ connect_p2p failed: {:?}", e);
                return;
            }
        };

    log::info!("[Native] ✓ P2P connection established!");
    log::info!("[Native] Entering echo loop (Ctrl+C to quit)...\n");

    // Echo loop — receive a message, print it, send it back
    let mut msg_count = 0u32;
    let mut buf = [0u8; 65536];

    loop {
        match transport.recv(&mut buf).await {
            Ok(n) => {
                msg_count += 1;
                let data = String::from_utf8_lossy(&buf[..n]);
                log::info!("[Native] ✓ #{} Received ({} bytes): {}", msg_count, n, data);

                // Echo back with a prefix
                let echo = format!("[native echo] {}", data);
                if let Err(e) = transport.send(echo.as_bytes()).await {
                    log::error!("[Native] ✗ Send error: {:?}", e);
                    break;
                }
                log::info!("[Native] ✓ #{} Echoed", msg_count);
            }
            Err(e) => {
                log::info!("[Native] Connection ended: {:?}", e);
                break;
            }
        }
    }

    log::info!(
        "\n[Native] Done — handled {} messages",
        msg_count
    );
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_p2p_native_peer is native-only");
}