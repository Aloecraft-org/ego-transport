// bin/test_auto_detect_native.rs
//
// Tests AutoDetectListener on native platforms:
//   1. A single server accepts both TCP and WebSocket connections concurrently,
//      correctly detecting and echoing each.
//   2. A ws_only server rejects raw TCP connections.

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::tcp_native::TcpStreamNative;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::ws_native::WebSocketNative;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::Transport;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    ego_platform::init();
    log::info!("=== Native AutoDetect Test ===\n");

    // ─── Test 1: Both protocols accepted concurrently ───────────────────────
    let addr = "127.0.0.1:9990";

    let server_addr = addr.to_string();
    ego_platform::spawn(async move {
        run_echo_server(&server_addr).await;
    });

    ego_platform::sleep(Duration::from_millis(100)).await;

    log::info!("[Test 1] Spawning concurrent TCP + WebSocket clients...\n");

    let tcp_handle = ego_platform::spawn(run_tcp_client(addr));

    let ws_addr = format!("ws://{}", addr);
    let ws_handle = ego_platform::spawn(async move { run_ws_client(&ws_addr).await });

    let tcp_ok = tcp_handle.await.unwrap_or(false);
    let ws_ok = ws_handle.await.unwrap_or(false);

    log::info!(
        "\n[Test 1] TCP: {}  |  WS: {}",
        if tcp_ok { "✓ PASS" } else { "✗ FAIL" },
        if ws_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    // ─── Test 2: ws_only server rejects TCP ──────────────────────────────────
    log::info!("\n[Test 2] Starting ws_only rejection test...\n");

    let ws_only_addr = "127.0.0.1:9991";
    let ws_only_addr_owned = ws_only_addr.to_string();
    ego_platform::spawn(async move {
        run_ws_only_server(&ws_only_addr_owned).await;
    });

    ego_platform::sleep(Duration::from_millis(100)).await;

    let reject_ok = run_rejected_tcp_client(ws_only_addr).await;
    log::info!(
        "\n[Test 2] TCP rejection: {}",
        if reject_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    // ─── Summary ─────────────────────────────────────────────────────────────
    log::info!(
        "\n=== Native AutoDetect Test {} ===",
        if tcp_ok && ws_ok && reject_ok {
            "PASSED"
        } else {
            "FAILED"
        }
    );

    ego_platform::sleep(Duration::from_secs(1)).await;
}

// ─── Servers ─────────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn run_echo_server(addr: &str) {
    log::info!("[Echo Server] Starting AutoDetect server on {}", addr);

    let listener = AutoDetectListener::bind(addr)
        .await
        .expect("Failed to bind echo server");

    ServerBuilder::new(listener)
        .concurrent()
        .run(|mut transport| async move {
            log::info!("[Echo Server] New connection accepted");

            let mut buf = [0u8; 1024];
            let mut msg_count = 0;

            loop {
                match transport.recv(&mut buf).await {
                    Ok(n) => {
                        msg_count += 1;
                        let data = String::from_utf8_lossy(&buf[..n]);
                        log::info!("[Echo Server] ✓ Received: {}", data);

                        if let Err(e) = transport.send(&buf[..n]).await {
                            log::error!("[Echo Server] ✗ Send error: {:?}", e);
                            break;
                        }

                        if msg_count >= 2 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            log::info!("[Echo Server] Handler complete ({} messages)", msg_count);
        })
        .await
        .ok();
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_ws_only_server(addr: &str) {
    log::info!("[WS-Only Server] Starting on {}", addr);

    let listener = AutoDetectListener::bind(addr)
        .await
        .expect("Failed to bind ws_only server")
        .ws_only();

    ServerBuilder::new(listener)
        .concurrent()
        .run(|mut transport| async move {
            log::info!("[WS-Only Server] WebSocket connection accepted");
            let mut buf = [0u8; 1024];
            if let Ok(n) = transport.recv(&mut buf).await {
                transport.send(&buf[..n]).await.ok();
            }
        })
        .await
        .ok();
}

// ─── Clients ─────────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn run_tcp_client(addr: &str) -> bool {
    log::info!("[TCP Client] Connecting to {}", addr);

    let mut transport = match TcpStreamNative::connect(addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[TCP Client] ✗ Connect failed: {:?}", e);
            return false;
        }
    };

    log::info!("[TCP Client] ✓ Connected");

    for i in 1..=2 {
        let msg = format!("TCP message #{}", i);
        if let Err(e) = transport.send(msg.as_bytes()).await {
            log::error!("[TCP Client] ✗ Send #{} error: {:?}", i, e);
            return false;
        }
        log::info!("[TCP Client] Sent: {}", msg);

        let mut buf = [0u8; 1024];
        match transport.recv(&mut buf).await {
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]);
                log::info!("[TCP Client] ✓ Echo: {}", response);
                if response != msg {
                    log::error!("[TCP Client] ✗ Echo mismatch! Expected: {}", msg);
                    return false;
                }
            }
            Err(e) => {
                log::error!("[TCP Client] ✗ Recv #{} error: {:?}", i, e);
                return false;
            }
        }

        ego_platform::sleep(Duration::from_millis(100)).await;
    }

    log::info!("[TCP Client] ✓ Complete");
    true
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_ws_client(url: &str) -> bool {
    log::info!("[WS Client] Connecting to {}", url);

    let mut transport = match WebSocketNative::connect(url).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[WS Client] ✗ Connect failed: {:?}", e);
            return false;
        }
    };

    log::info!("[WS Client] ✓ Connected");

    for i in 1..=2 {
        let msg = format!("WebSocket message #{}", i);
        if let Err(e) = transport.send(msg.as_bytes()).await {
            log::error!("[WS Client] ✗ Send #{} error: {:?}", i, e);
            return false;
        }
        log::info!("[WS Client] Sent: {}", msg);

        let mut buf = [0u8; 1024];
        match transport.recv(&mut buf).await {
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]);
                log::info!("[WS Client] ✓ Echo: {}", response);
                if response != msg {
                    log::error!("[WS Client] ✗ Echo mismatch! Expected: {}", msg);
                    return false;
                }
            }
            Err(e) => {
                log::error!("[WS Client] ✗ Recv #{} error: {:?}", i, e);
                return false;
            }
        }

        ego_platform::sleep(Duration::from_millis(100)).await;
    }

    log::info!("[WS Client] ✓ Complete");
    true
}

/// Connects a raw TCP client to a ws_only server and verifies the connection
/// is rejected. Returns true if rejection was observed.
#[cfg(not(target_arch = "wasm32"))]
async fn run_rejected_tcp_client(addr: &str) -> bool {
    log::info!("[Rejected TCP] Connecting to ws_only server at {}", addr);

    let mut transport = match TcpStreamNative::connect(addr).await {
        Ok(t) => t,
        Err(e) => {
            // Connection refused is also a valid rejection signal if the
            // server closed before we finished connecting.
            log::info!("[Rejected TCP] ✓ Connection refused: {:?}", e);
            return true;
        }
    };

    // Send raw TCP data — this triggers the server to peek, detect non-WS,
    // and drop the connection.
    if let Err(_) = transport
        .send(b"This is raw TCP, not a WebSocket handshake")
        .await
    {
        log::info!("[Rejected TCP] ✓ Send failed (connection already closed)");
        return true;
    }
    log::info!("[Rejected TCP] Data sent, waiting for server to close...");

    // The server will drop the stream after peeking. We should see a closed
    // connection on the next recv.
    let mut buf = [0u8; 1024];
    match transport.recv(&mut buf).await {
        Ok(n) => {
            log::error!(
                "[Rejected TCP] ✗ Received {} bytes — should have been rejected!",
                n
            );
            false
        }
        Err(e) => {
            log::info!("[Rejected TCP] ✓ Connection closed as expected: {:?}", e);
            true
        }
    }
}

// Stub for WASM targets
#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_auto_detect_native is only for native platforms");
}
