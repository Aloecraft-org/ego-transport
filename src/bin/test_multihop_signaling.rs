// bin/test_multihop_signaling.rs
//
// Tests signaling over multiple relay hops:
//
//   Z ──(tcp)──> A ──(tcp)──> B ──(tcp)──> C
//
// Z wants to exchange signaling with C. Messages traverse A and B.
// This proves that SignalingChannel works over arbitrary relay chains,
// which maps directly to the Ego2 multi-hop routing use case.
//
// Usage:
//   cargo run --bin test_multihop_signaling

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
    log::info!("=== Multi-Hop Signaling Test ===\n");
    log::info!("Topology: Z ──> A ──> B ──> C");
    log::info!("Z and C exchange signaling through A and B\n");

    let a_addr = "127.0.0.1:9984";
    let b_addr = "127.0.0.1:9985";

    // Start relay nodes
    tokio::spawn(run_relay("A".to_string(), a_addr.to_string()));
    tokio::spawn(run_relay("B".to_string(), b_addr.to_string()));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Z connects to A
    let z_handle = tokio::spawn(run_endpoint(
        "Z".to_string(),
        a_addr.to_string(),
        true, // offerer
    ));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // A→B bridge: accept from A's listener, connect to B
    let bridge_handle = tokio::spawn(run_bridge(a_addr.to_string(), b_addr.to_string()));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // C connects to B
    let c_handle = tokio::spawn(run_endpoint(
        "C".to_string(),
        b_addr.to_string(),
        false, // answerer
    ));

    let z_ok = z_handle.await.unwrap_or(false);
    let c_ok = c_handle.await.unwrap_or(false);
    let bridge_ok = bridge_handle.await.unwrap_or(false);

    log::info!(
        "\n=== Multi-Hop Signaling Test {} ===",
        if z_ok && c_ok { "PASSED" } else { "FAILED" }
    );
    log::info!("  Z (offerer):  {}", if z_ok { "✓" } else { "✗" });
    log::info!("  Bridge A→B:   {}", if bridge_ok { "✓" } else { "✗" });
    log::info!("  C (answerer): {}", if c_ok { "✓" } else { "✗" });

    tokio::time::sleep(Duration::from_secs(1)).await;
    std::process::exit(if z_ok && c_ok { 0 } else { 1 });
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_relay(name: String, addr: String) {
    let listener = AutoDetectListener::bind(&addr)
        .await
        .expect("Failed to bind relay");
    log::info!("[Relay {}] Listening on {}", name, addr);

    // Accept two connections, relay between them
    let mut conn1 = listener.accept().await.expect("accept 1");
    log::info!("[Relay {}] Connection 1 accepted", name);
    let mut conn2 = listener.accept().await.expect("accept 2");
    log::info!("[Relay {}] Connection 2 accepted", name);

    let mut buf1 = [0u8; 65536];
    let mut buf2 = [0u8; 65536];

    loop {
        tokio::select! {
            r = conn1.recv(&mut buf1) => {
                match r {
                    Ok(n) => { conn2.send(&buf1[..n]).await.ok(); }
                    Err(_) => return,
                }
            }
            r = conn2.recv(&mut buf2) => {
                match r {
                    Ok(n) => { conn1.send(&buf2[..n]).await.ok(); }
                    Err(_) => return,
                }
            }
        }
    }
}

/// Bridge between relay A and relay B.
#[cfg(not(target_arch = "wasm32"))]
async fn run_bridge(a_addr: String, b_addr: String) -> bool {
    // Connect to A (as A's second peer)
    let mut to_a = match ego_transport::transport::connect(&a_addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[Bridge] Connect to A failed: {:?}", e);
            return false;
        }
    };
    log::info!("[Bridge] Connected to A");

    // Connect to B (as B's first peer)
    let mut to_b = match ego_transport::transport::connect(&b_addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[Bridge] Connect to B failed: {:?}", e);
            return false;
        }
    };
    log::info!("[Bridge] Connected to B");

    // Relay between A and B
    let mut buf_ab = [0u8; 65536];
    let mut buf_ba = [0u8; 65536];
    let mut relayed = 0u32;

    loop {
        tokio::select! {
            r = to_a.recv(&mut buf_ab) => {
                match r {
                    Ok(n) => {
                        relayed += 1;
                        to_b.send(&buf_ab[..n]).await.ok();
                    }
                    Err(_) => break,
                }
            }
            r = to_b.recv(&mut buf_ba) => {
                match r {
                    Ok(n) => {
                        relayed += 1;
                        to_a.send(&buf_ba[..n]).await.ok();
                    }
                    Err(_) => break,
                }
            }
        }
    }

    log::info!("[Bridge] Done, relayed {} messages", relayed);
    relayed > 0
}

/// Endpoint — either offerer (Z) or answerer (C).
#[cfg(not(target_arch = "wasm32"))]
async fn run_endpoint(name: String, relay_addr: String, is_offerer: bool) -> bool {
    let transport = match ego_transport::transport::connect(&relay_addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[{}] Connect failed: {:?}", name, e);
            return false;
        }
    };

    log::info!("[{}] Connected to relay", name);
    let mut channel = TransportSignalingChannel::new(transport);

    if is_offerer {
        // Send offer + ICE
        let sdp = SdpBuilder::new().build_offer();
        channel
            .send_signal(&SignalingMessage::offer("multihop-room", &sdp))
            .await
            .ok();
        let ice = IceCandidate::new(
            "candidate:1 1 udp 2130706431 10.0.0.1 5000 typ host",
            "0",
            0,
        );
        channel
            .send_signal(&SignalingMessage::ice("multihop-room", &ice))
            .await
            .ok();
        channel
            .send_signal(&SignalingMessage::ice_done("multihop-room"))
            .await
            .ok();

        // Wait for answer
        let mut got_answer = false;
        for _ in 0..100 {
            match channel.recv_signal().await {
                Ok(msg) => {
                    log::info!("[{}] ✓ Received {:?}", name, msg.kind);
                    match msg.kind {
                        SignalingKind::Answer => got_answer = true,
                        SignalingKind::IceDone if got_answer => return true,
                        _ => {}
                    }
                }
                Err(_) => return false,
            }
        }
        got_answer
    } else {
        // Wait for offer
        let mut got_offer = false;
        for _ in 0..100 {
            match channel.recv_signal().await {
                Ok(msg) => {
                    log::info!("[{}] ✓ Received {:?}", name, msg.kind);
                    match msg.kind {
                        SignalingKind::Offer => got_offer = true,
                        SignalingKind::IceDone if got_offer => break,
                        _ => {}
                    }
                }
                Err(_) => return false,
            }
        }

        if !got_offer {
            return false;
        }

        // Send answer + ICE
        let sdp = SdpBuilder::new().build_answer();
        channel
            .send_signal(&SignalingMessage::answer("multihop-room", &sdp))
            .await
            .ok();
        let ice = IceCandidate::new(
            "candidate:1 1 udp 2130706431 10.0.0.2 5001 typ host",
            "0",
            0,
        );
        channel
            .send_signal(&SignalingMessage::ice("multihop-room", &ice))
            .await
            .ok();
        channel
            .send_signal(&SignalingMessage::ice_done("multihop-room"))
            .await
            .ok();

        log::info!("[{}] ✓ Signaling complete", name);
        true
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_multihop_signaling is native-only");
}
