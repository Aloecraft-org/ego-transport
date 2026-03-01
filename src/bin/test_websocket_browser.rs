// bin/test_websocket_browser.rs

// Browser implementation
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use aloeclient::platform;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use aloeclient::platform::ws_browser::WebSocketBrowser;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use aloeclient::transport::Transport;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_futures::spawn_local;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen(start)]
pub fn run() {
    // Setup panic hook for better error messages
    console_error_panic_hook::set_once();

    aloeplatform::init();

    // Spawn the async test
    spawn_local(async {
        run_test().await;
    });
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn run_test() {
    log::info!("=== Browser WebSocket Client Test ===");

    // Connect to server (adjust URL as needed)
    let url = "ws://127.0.0.1:9995";

    log::info!("[Browser Client] Connecting to {}", url);

    match WebSocketBrowser::connect(url).await {
        Ok(mut ws) => {
            log::info!("[Browser Client] ✓ Connected");

            // Send 3 test messages
            for i in 1..=3 {
                let msg = format!("Hello from Browser WebSocket client, message #{}", i);
                log::info!("[Browser Client] Sending: {}", msg);

                if let Err(e) = ws.send(msg.as_bytes()).await {
                    log::error!("[Browser Client] ✗ Send error: {:?}", e);
                    return;
                }

                // Receive echo
                let mut buf = [0u8; 1024];
                match ws.recv(&mut buf).await {
                    Ok(n) => {
                        let response = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[Browser Client] ✓ Received echo: {}", response);

                        if response == msg {
                            log::info!("[Browser Client] ✓ Echo matches!");
                        }
                    }
                    Err(e) => {
                        log::error!("[Browser Client] ✗ Recv error: {:?}", e);
                        return;
                    }
                }

                // Small delay between messages
                aloeplatform::sleep(std::time::Duration::from_millis(500)).await;
            }

            log::info!("[Browser Client] ✓ Test complete!");
        }
        Err(e) => {
            log::error!("[Browser Client] ✗ Connection failed: {:?}", e);
        }
    }
}

// Main function for the binary (required even though we use wasm_bindgen(start))
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    // Empty - wasm_bindgen(start) handles initialization
}

// Stub for non-browser targets
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn main() {
    panic!("test_websocket_browser is only for browser");
}
