// bin/test_embedded_signaling.rs
//
// Tests that AutoDetectListener with an embedded SignalingHub correctly
// routes signaling peers to the hub while passing application connections
// through to the ServerBuilder handler.
//
// Test plan:
//   1. Start a server with AutoDetectListener + SignalingHub
//   2. Connect two signaling peers → they should complete the handshake
//   3. Connect a regular TCP client → it should reach the echo handler
//   4. Connect a regular WebSocket client → it should reach the echo handler
//
// Usage:
//   cargo run --bin test_embedded_signaling

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::tcp_native::TcpStreamNative;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::ws_native::WebSocketNative;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::Transport;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::rtc_signaling::*;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::signaling_hub::SignalingHub;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    ego_platform::init();
    log::info!("=== Embedded Signaling Test ===\n");

    let addr = "127.0.0.1:9982";
    let app_handler_count = Arc::new(AtomicU32::new(0));

    // Start server with embedded signaling
    let count = app_handler_count.clone();
    let server_addr = addr.to_string();
    tokio::spawn(async move {
        let hub = SignalingHub::new();
        let listener = AutoDetectListener::bind(&server_addr)
            .await
            .expect("Failed to bind")
            .with_signaling(hub);

        ServerBuilder::new(listener)
            .concurrent()
            .run(move |mut transport| {
                let count = count.clone();
                async move {
                    // Echo handler for non-signaling connections
                    count.fetch_add(1, Ordering::SeqCst);
                    log::info!("[Echo] App connection received");
                    let mut buf = [0u8; 1024];
                    loop {
                        match transport.recv(&mut buf).await {
                            Ok(n) => {
                                let data = String::from_utf8_lossy(&buf[..n]);
                                log::info!("[Echo] Received: {}", data);
                                transport.send(&buf[..n]).await.ok();
                            }
                            Err(_) => break,
                        }
                    }
                }
            })
            .await
            .ok();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── Test 1: Two signaling peers ──────────────────────────────────────

    log::info!("[Test 1] Two signaling peers on the same port\n");

    let peer_a = tokio::spawn({
        let addr = addr.to_string();
        async move { run_signaling_peer(&addr, "test-room", "Peer-A").await }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let peer_b = tokio::spawn({
        let addr = addr.to_string();
        async move { run_signaling_peer(&addr, "test-room", "Peer-B").await }
    });

    let a_ok = peer_a.await.unwrap_or(false);
    let b_ok = peer_b.await.unwrap_or(false);

    log::info!(
        "\n[Test 1] Signaling: {} (A={}, B={})",
        if a_ok && b_ok { "✓ PASS" } else { "✗ FAIL" },
        if a_ok { "ok" } else { "fail" },
        if b_ok { "ok" } else { "fail" }
    );

    // Verify no app handler was called for signaling connections
    let count_after_signaling = app_handler_count.load(Ordering::SeqCst);
    log::info!(
        "[Test 1] App handler calls: {} (should be 0) {}",
        count_after_signaling,
        if count_after_signaling == 0 {
            "✓"
        } else {
            "✗"
        }
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Test 2: Regular TCP echo client ──────────────────────────────────

    log::info!("\n[Test 2] Regular TCP client on the same port\n");

    let tcp_ok = run_tcp_echo_client(addr).await;
    log::info!(
        "[Test 2] TCP echo: {}",
        if tcp_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Test 3: Regular WebSocket echo client ────────────────────────────

    log::info!("\n[Test 3] Regular WebSocket client on the same port\n");

    let ws_ok = run_ws_echo_client(addr).await;
    log::info!(
        "[Test 3] WS echo: {}",
        if ws_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    // Verify app handler was called for echo connections
    let count_after_echo = app_handler_count.load(Ordering::SeqCst);
    log::info!(
        "\n[Summary] App handler calls: {} (should be 2) {}",
        count_after_echo,
        if count_after_echo == 2 { "✓" } else { "✗" }
    );

    // ── Summary ──────────────────────────────────────────────────────────

    let all_ok = a_ok && b_ok && tcp_ok && ws_ok && count_after_echo == 2;
    log::info!(
        "\n=== Embedded Signaling Test {} ===",
        if all_ok { "PASSED" } else { "FAILED" }
    );

    tokio::time::sleep(Duration::from_secs(1)).await;
}

// ─── Signaling peer ──────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn run_signaling_peer(addr: &str, room: &str, name: &str) -> bool {
    let ws_url = format!("ws://{}", addr);
    let mut transport: Box<dyn Transport> = match ego_transport::transport::connect(&ws_url).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[{}] ✗ Connect failed: {:?}", name, e);
            return false;
        }
    };

    // Join room
    let mut client = SignalingClient::new(room);
    let role = match client.join_and_wait(&mut transport).await {
        Ok(r) => r,
        Err(e) => {
            log::error!("[{}] ✗ Join failed: {:?}", name, e);
            return false;
        }
    };

    log::info!("[{}] ✓ Role: {:?}", name, role);

    match role {
        PeerRole::Offerer => {
            // Send offer + ICE
            let sdp = SdpBuilder::new().build_offer();
            client
                .send_message(&mut transport, &SignalingMessage::offer(room, &sdp))
                .await
                .ok();
            let ice = IceCandidate::new(
                "candidate:1 1 udp 2130706431 10.0.0.1 5000 typ host",
                "0",
                0,
            );
            client
                .send_message(&mut transport, &SignalingMessage::ice(room, &ice))
                .await
                .ok();
            client
                .send_message(&mut transport, &SignalingMessage::ice_done(room))
                .await
                .ok();

            // Wait for answer
            let mut got_answer = false;
            for _ in 0..10 {
                match client.recv_message(&mut transport).await {
                    Ok(msg) => match msg.kind {
                        SignalingKind::Answer => {
                            log::info!("[{}] ✓ Received answer", name);
                            got_answer = true;
                        }
                        SignalingKind::IceDone => {
                            if got_answer {
                                break;
                            }
                        }
                        _ => {}
                    },
                    Err(_) => break,
                }
            }
            got_answer
        }
        PeerRole::Answerer => {
            // Wait for offer
            let mut got_offer = false;
            for _ in 0..10 {
                match client.recv_message(&mut transport).await {
                    Ok(msg) => match msg.kind {
                        SignalingKind::Offer => {
                            log::info!("[{}] ✓ Received offer", name);
                            got_offer = true;
                        }
                        SignalingKind::IceDone => {
                            if got_offer {
                                break;
                            }
                        }
                        _ => {}
                    },
                    Err(_) => break,
                }
            }

            if !got_offer {
                return false;
            }

            // Send answer + ICE
            let sdp = SdpBuilder::new().build_answer();
            client
                .send_message(&mut transport, &SignalingMessage::answer(room, &sdp))
                .await
                .ok();
            client
                .send_message(&mut transport, &SignalingMessage::ice_done(room))
                .await
                .ok();

            true
        }
    }
}

// ─── TCP echo client ─────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn run_tcp_echo_client(addr: &str) -> bool {
    let mut transport = match TcpStreamNative::connect(addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[TCP Client] ✗ Connect failed: {:?}", e);
            return false;
        }
    };

    let msg = b"Hello from TCP client!";
    if let Err(e) = transport.send(msg).await {
        log::error!("[TCP Client] ✗ Send failed: {:?}", e);
        return false;
    }

    let mut buf = [0u8; 1024];
    match transport.recv(&mut buf).await {
        Ok(n) => {
            let response = String::from_utf8_lossy(&buf[..n]);
            log::info!("[TCP Client] ✓ Echo: {}", response);
            &buf[..n] == msg
        }
        Err(e) => {
            log::error!("[TCP Client] ✗ Recv failed: {:?}", e);
            false
        }
    }
}

// ─── WebSocket echo client ───────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn run_ws_echo_client(addr: &str) -> bool {
    let ws_url = format!("ws://{}", addr);
    let mut transport = match WebSocketNative::connect(&ws_url).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[WS Client] ✗ Connect failed: {:?}", e);
            return false;
        }
    };

    let msg = b"Hello from WebSocket client!";
    if let Err(e) = transport.send(msg).await {
        log::error!("[WS Client] ✗ Send failed: {:?}", e);
        return false;
    }

    let mut buf = [0u8; 1024];
    match transport.recv(&mut buf).await {
        Ok(n) => {
            let response = String::from_utf8_lossy(&buf[..n]);
            log::info!("[WS Client] ✓ Echo: {}", response);
            &buf[..n] == msg
        }
        Err(e) => {
            log::error!("[WS Client] ✗ Recv failed: {:?}", e);
            false
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_embedded_signaling is native-only");
}
