// bin/test_routed_signaling.rs
//
// Tests the "signaling over routed packets" use case:
//
//   Z ──(tcp)──> A ──(tcp)──> C
//
// Z wants a WebRTC data channel to C, but can't reach C directly.
// Z sends signaling messages (OFFER, ICE) to A, which relays them to C.
// C sends signaling messages (ANSWER, ICE) back through A to Z.
//
// This proves that signaling can flow over any Transport — not just a
// dedicated WebSocket to a signaling server. In production, A would be
// an Ego2 node that routes packets; here we simulate it with a simple
// TCP relay.
//
// The test uses TransportSignalingChannel to wrap the relay connections
// and exercises the full signaling handshake without a SignalingHub.
//
// Usage:
//   cargo run --bin test_routed_signaling

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::server::{AutoDetectListener, Listener};
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::rtc_signaling::*;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    ego_platform::init();
    log::info!("=== Routed Signaling Test ===\n");
    log::info!("Topology: Z ──relay──> A ──relay──> C");
    log::info!("Z and C exchange signaling through A without a signaling server\n");

    let relay_addr = "127.0.0.1:9983";

    // ── Start relay node A ───────────────────────────────────────────────
    // A accepts two TCP connections and forwards all messages between them.
    let relay_handle = tokio::spawn(run_relay(relay_addr.to_string()));

    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── Connect Z and C through A ────────────────────────────────────────
    let z_handle = tokio::spawn(run_peer_z(relay_addr.to_string()));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let c_handle = tokio::spawn(run_peer_c(relay_addr.to_string()));

    let z_ok = z_handle.await.unwrap_or(false);
    let c_ok = c_handle.await.unwrap_or(false);

    log::info!(
        "\n=== Routed Signaling Test {} ===",
        if z_ok && c_ok { "PASSED" } else { "FAILED" }
    );
    log::info!("  Z (offerer):  {}", if z_ok { "✓" } else { "✗" });
    log::info!("  C (answerer): {}", if c_ok { "✓" } else { "✗" });

    tokio::time::sleep(Duration::from_secs(1)).await;
    relay_handle.abort();
}

/// Relay node A — accepts two connections and forwards messages between them.
#[cfg(not(target_arch = "wasm32"))]
async fn run_relay(addr: String) {
    let listener = AutoDetectListener::bind(&addr)
        .await
        .expect("Failed to bind relay");

    log::info!("[Relay A] Listening on {}", addr);

    // Accept Z
    log::debug!("[Relay A] Waiting for Z to connect...");
    let mut z_conn = listener.accept().await.expect("Failed to accept Z");
    log::info!("[Relay A] Z connected");

    // Accept C
    log::debug!("[Relay A] Waiting for C to connect...");
    let mut c_conn = listener.accept().await.expect("Failed to accept C");
    log::info!("[Relay A] C connected");

    // Bidirectional relay
    let mut buf_zc = [0u8; 65536];
    let mut buf_cz = [0u8; 65536];

    log::info!("[Relay A] Both peers connected, entering relay loop");

    loop {
        log::debug!("[Relay A] Waiting on select...");
        tokio::select! {
            result = z_conn.recv(&mut buf_zc) => {
                match result {
                    Ok(n) => {
                        log::debug!("[Relay A] Z→C: {} bytes", n);
                        if c_conn.send(&buf_zc[..n]).await.is_err() { return; }
                    }
                    Err(e) => {
                        log::error!("[Relay A] Z recv error: {:?}", e);
                        return;
                    }
                }
            }
            result = c_conn.recv(&mut buf_cz) => {
                match result {
                    Ok(n) => {
                        log::debug!("[Relay A] C→Z: {} bytes", n);
                        if z_conn.send(&buf_cz[..n]).await.is_err() { return; }
                    }
                    Err(e) => {
                        log::error!("[Relay A] C recv error: {:?}", e);
                        return;
                    }
                }
            }
        }
    }
}

/// Peer Z — the offerer. Connects through relay to exchange signaling with C.
#[cfg(not(target_arch = "wasm32"))]
async fn run_peer_z(relay_addr: String) -> bool {
    let transport = match ego_transport::transport::connect(&relay_addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[Z] Connect failed: {:?}", e);
            return false;
        }
    };

    log::info!("[Z] Connected to relay");

    // Wrap as SignalingChannel
    let mut channel = TransportSignalingChannel::new(transport);

    // Send offer
    let sdp = SdpBuilder::new().build_offer();
    log::info!("[Z] Sending SDP offer ({} bytes)", sdp.len());
    if let Err(e) = channel
        .send_signal(&SignalingMessage::offer("routed-room", &sdp))
        .await
    {
        log::error!("[Z] Send offer failed: {:?}", e);
        return false;
    }

    // Send ICE candidate
    let ice = IceCandidate::new(
        "candidate:1 1 udp 2130706431 10.0.0.1 5000 typ host",
        "0",
        0,
    );
    channel
        .send_signal(&SignalingMessage::ice("routed-room", &ice))
        .await
        .ok();
    channel
        .send_signal(&SignalingMessage::ice_done("routed-room"))
        .await
        .ok();

    log::info!("[Z] All signals sent, waiting for answer...");

    // Wait for answer
    let mut got_answer = false;
    let mut got_ice_done = false;

    for _ in 0..100 {
        match channel.recv_signal().await {
            Ok(msg) => match msg.kind {
                SignalingKind::Answer => {
                    log::info!("[Z] ✓ Received answer ({} bytes)", msg.payload.len());
                    got_answer = true;
                }
                SignalingKind::Ice => {
                    log::info!("[Z] ✓ Received ICE candidate");
                }
                SignalingKind::IceDone => {
                    log::info!("[Z] ✓ Received ICE done");
                    got_ice_done = true;
                    if got_answer {
                        break;
                    }
                }
                _ => {}
            },
            Err(e) => {
                log::error!("[Z] Recv error: {:?}", e);
                return false;
            }
        }
    }

    log::info!(
        "[Z] Signaling complete: answer={}, ice_done={}",
        got_answer,
        got_ice_done
    );
    got_answer && got_ice_done
}

/// Peer C — the answerer. Receives offer through relay, sends answer back.
#[cfg(not(target_arch = "wasm32"))]
async fn run_peer_c(relay_addr: String) -> bool {
    let transport = match ego_transport::transport::connect(&relay_addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[C] Connect failed: {:?}", e);
            return false;
        }
    };

    log::info!("[C] Connected to relay");

    let mut channel = TransportSignalingChannel::new(transport);

    // Wait for offer
    let mut got_offer = false;
    let mut _got_ice_done = false;

    log::info!("[C] Entering recv loop, waiting for offer...");

    for _ in 0..100 {
        log::debug!("[C] recv loop iteration, calling recv_signal...");
        match channel.recv_signal().await {
            Ok(msg) => match msg.kind {
                SignalingKind::Offer => {
                    log::info!("[C] ✓ Received offer ({} bytes)", msg.payload.len());
                    got_offer = true;
                }
                SignalingKind::Ice => {
                    log::info!("[C] ✓ Received ICE candidate");
                }
                SignalingKind::IceDone => {
                    log::info!("[C] ✓ Received ICE done");
                    _got_ice_done = true;
                    if got_offer {
                        break;
                    }
                }
                _ => {}
            },
            Err(e) => {
                log::error!("[C] Recv error: {:?}", e);
                return false;
            }
        }
    }

    if !got_offer {
        log::error!("[C] Never received offer");
        return false;
    }

    // Send answer
    let sdp = SdpBuilder::new().build_answer();
    log::info!("[C] Sending SDP answer ({} bytes)", sdp.len());
    channel
        .send_signal(&SignalingMessage::answer("routed-room", &sdp))
        .await
        .ok();

    let ice = IceCandidate::new(
        "candidate:1 1 udp 2130706431 10.0.0.2 5001 typ host",
        "0",
        0,
    );
    channel
        .send_signal(&SignalingMessage::ice("routed-room", &ice))
        .await
        .ok();
    channel
        .send_signal(&SignalingMessage::ice_done("routed-room"))
        .await
        .ok();

    log::info!("[C] ✓ Signaling complete");
    true
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_routed_signaling is native-only");
}
