// bin/test_rtc_browser.rs
//
// Browser-side WebRTC test.
//
// Opens a connection to the signaling server, joins a room, establishes
// a WebRTC data channel with the peer, and sends/receives test messages.
//
// Run:
//   1. Start signaling server:  cargo run --bin signaling_server
//   2. Build browser WASM:      cargo build --target wasm32-unknown-unknown --bin test_rtc_browser
//   3. Serve with trunk:        trunk serve --port 9001 test_rtc_browser.html
//   4. Open two browser tabs at http://localhost:9001
//   5. Check console (F12) for output — both tabs should exchange messages

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use ego_transport::platform::rtc_browser::RtcBrowser;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use ego_transport::transport::Transport;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use ego_transport::transport::rtc_signaling::IceServerConfig;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_futures::spawn_local;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen(start)]
pub fn run() {
    console_error_panic_hook::set_once();
    ego_platform::init();

    spawn_local(async {
        // ego_platform::sleep(std::time::Duration::from_millis(0)).await;
        run_test().await;
    });
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn run_test() {
    log::info!("=== Browser WebRTC Test ===");
    log::info!("[Browser] Connecting to signaling server...");

    let signal_url = "ws://127.0.0.1:9995";
    let room = "test-rtc-room";

    // Use Google's public STUN server for NAT traversal
    let ice_servers = IceServerConfig::default_config();

    match RtcBrowser::connect(signal_url, room, &ice_servers).await {
        Ok(mut rtc) => {
            log::info!("[Browser] ✓ WebRTC data channel established!");

            // Send test messages
            for i in 1..=3 {
                let msg = format!("Hello from browser, message #{}", i);
                log::info!("[Browser] Sending: {}", msg);

                if let Err(e) = rtc.send(msg.as_bytes()).await {
                    log::error!("[Browser] ✗ Send error: {:?}", e);
                    return;
                }

                // Receive echo or response from peer
                let mut buf = [0u8; 1024];
                match rtc.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[Browser] ✓ Received: {}", response);
                    }
                    Err(e) => {
                        log::error!("[Browser] ✗ Recv error: {:?}", e);
                        return;
                    }
                }

                ego_platform::sleep(std::time::Duration::from_millis(500)).await;
            }

            log::info!("[Browser] ✓ Test complete!");
        }
        Err(e) => {
            log::error!("[Browser] ✗ Connection failed: {:?}", e);
        }
    }
}

// Main stub
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {}

// Non-browser stub
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn main() {
    panic!("test_rtc_browser is browser-only");
}
