// bin/test_signaling.rs
//
// End-to-end test for the signaling protocol.
//
// 1. Starts the signaling server on a local port
// 2. Spawns two "peers" that connect, join the same room, and exchange
//    simulated SDP offer/answer and ICE candidates
// 3. Verifies that each peer receives the other's messages correctly
//
// This validates the signaling server + shared types WITHOUT needing
// a real WebRTC stack. The actual RTC transport tests come later.
//
// Usage:
//   cargo run --bin test_signaling

#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::rtc_signaling::*;
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::transport::{Transport, TransportError};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Mutex;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    ego_platform::init();
    log::info!("=== Signaling Protocol Test ===\n");

    let addr = "127.0.0.1:9980";

    // Start signaling server in background
    let server_addr = addr.to_string();
    ego_platform::spawn(async move {
        run_signaling_server(&server_addr).await;
    });

    ego_platform::sleep(Duration::from_millis(200)).await;

    // Spawn two peers
    let room = "test-room-1";

    let peer_a = ego_platform::spawn({
        let addr = addr.to_string();
        let room = room.to_string();
        async move { run_peer(&addr, &room, "Peer-A").await }
    });

    // Small delay so Peer-A joins first and becomes offerer
    ego_platform::sleep(Duration::from_millis(100)).await;

    let peer_b = ego_platform::spawn({
        let addr = addr.to_string();
        let room = room.to_string();
        async move { run_peer(&addr, &room, "Peer-B").await }
    });

    let a_ok = peer_a.await.unwrap_or(false);
    let b_ok = peer_b.await.unwrap_or(false);

    log::info!(
        "\n=== Signaling Protocol Test {} ===",
        if a_ok && b_ok { "PASSED" } else { "FAILED" }
    );
    log::info!(
        "  Peer-A (offerer):  {}",
        if a_ok { "✓ PASS" } else { "✗ FAIL" }
    );
    log::info!(
        "  Peer-B (answerer): {}",
        if b_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    ego_platform::sleep(Duration::from_secs(1)).await;
}

// ─── Signaling Server (inline for test) ──────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
struct Room {
    peer_a_tx: tokio::sync::mpsc::UnboundedSender<String>,
    peer_b_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

#[cfg(not(target_arch = "wasm32"))]
type RoomMap = Arc<Mutex<HashMap<String, Room>>>;

#[cfg(not(target_arch = "wasm32"))]
async fn run_signaling_server(addr: &str) {
    log::info!("[Server] Starting signaling server on {}", addr);

    let rooms: RoomMap = Arc::new(Mutex::new(HashMap::new()));
    let listener = AutoDetectListener::bind(addr)
        .await
        .expect("Failed to bind");

    ServerBuilder::new(listener)
        .concurrent()
        .run(move |transport| {
            let rooms = rooms.clone();
            async move {
                if let Err(e) = handle_server_peer(transport, rooms).await {
                    log::debug!("[Server] Handler ended: {:?}", e);
                }
            }
        })
        .await
        .ok();
}

#[cfg(not(target_arch = "wasm32"))]
async fn handle_server_peer(
    mut transport: Box<dyn Transport>,
    rooms: RoomMap,
) -> Result<(), TransportError> {
    let mut buf = [0u8; 65536];

    // Wait for JOIN
    let n = transport.recv(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf[..n]);
    let join_msg = SignalingMessage::deserialize(&text)
        .ok_or_else(|| TransportError::Protocol("Expected JOIN".to_string()))?;

    if join_msg.kind != SignalingKind::Join {
        return Err(TransportError::Protocol("Expected JOIN".to_string()));
    }

    let room_name = join_msg.room.clone();
    log::info!("[Server] Peer joining room '{}'", room_name);

    let (my_tx, mut my_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let role;

    {
        let mut map = rooms.lock().await;
        if let Some(room) = map.get_mut(&room_name) {
            room.peer_b_tx = Some(my_tx);
            role = PeerRole::Answerer;

            let ready_a = SignalingMessage::ready(&room_name, PeerRole::Offerer);
            room.peer_a_tx.send(ready_a.serialize()).ok();
            let ready_b = SignalingMessage::ready(&room_name, PeerRole::Answerer);
            transport.send(ready_b.serialize().as_bytes()).await?;

            log::info!("[Server] Room '{}' matched!", room_name);
        } else {
            map.insert(
                room_name.clone(),
                Room {
                    peer_a_tx: my_tx,
                    peer_b_tx: None,
                },
            );
            role = PeerRole::Offerer;
            log::info!("[Server] Room '{}' created, waiting for peer", room_name);
        }
    }

    // Relay loop
    loop {
        tokio::select! {
            result = transport.recv(&mut buf) => {
                let n = result?;
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                let map = rooms.lock().await;
                if let Some(room) = map.get(&room_name) {
                    let other_tx = match role {
                        PeerRole::Offerer => room.peer_b_tx.as_ref(),
                        PeerRole::Answerer => Some(&room.peer_a_tx),
                    };
                    if let Some(tx) = other_tx {
                        tx.send(text).ok();
                    }
                }
            }
            result = my_rx.recv() => {
                match result {
                    Some(msg) => { transport.send(msg.as_bytes()).await?; }
                    None => return Ok(()),
                }
            }
        }
    }
}

// ─── Peer Implementation ─────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn run_peer(addr: &str, room: &str, name: &str) -> bool {
    log::info!("[{}] Connecting to signaling server at {}", name, addr);

    let ws_url = format!("ws://{}", addr);
    let mut transport: Box<dyn Transport> = match ego_transport::transport::connect(&ws_url).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[{}] ✗ Connect failed: {:?}", name, e);
            return false;
        }
    };

    log::info!("[{}] ✓ Connected", name);

    // Join and wait for Ready
    let mut client = SignalingClient::new(room);
    let role = match client.join_and_wait(&mut transport).await {
        Ok(r) => r,
        Err(e) => {
            log::error!("[{}] ✗ Join failed: {:?}", name, e);
            return false;
        }
    };

    log::info!("[{}] ✓ Assigned role: {:?}", name, role);

    match role {
        PeerRole::Offerer => run_offerer(&mut transport, &mut client, name).await,
        PeerRole::Answerer => run_answerer(&mut transport, &mut client, name).await,
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_offerer(
    transport: &mut Box<dyn Transport>,
    client: &mut SignalingClient,
    name: &str,
) -> bool {
    // Create and send offer
    let sdp = SdpBuilder::new().build_offer();
    let offer = SignalingMessage::offer(client.room(), &sdp);

    log::info!("[{}] Sending SDP offer ({} bytes)", name, sdp.len());
    if let Err(e) = client.send_message(transport, &offer).await {
        log::error!("[{}] ✗ Send offer failed: {:?}", name, e);
        return false;
    }

    // Send an ICE candidate
    let candidate = IceCandidate::new(
        "candidate:1 1 udp 2130706431 192.168.1.10 50000 typ host",
        "0",
        0,
    );
    let ice_msg = SignalingMessage::ice(client.room(), &candidate);
    if let Err(e) = client.send_message(transport, &ice_msg).await {
        log::error!("[{}] ✗ Send ICE failed: {:?}", name, e);
        return false;
    }
    log::info!("[{}] ✓ Sent ICE candidate", name);

    // Send ICE done
    let done = SignalingMessage::ice_done(client.room());
    client.send_message(transport, &done).await.ok();

    // Wait for answer
    let mut got_answer = false;
    let mut got_ice = false;

    for _ in 0..10 {
        match client.recv_message(transport).await {
            Ok(msg) => match msg.kind {
                SignalingKind::Answer => {
                    log::info!(
                        "[{}] ✓ Received SDP answer ({} bytes)",
                        name,
                        msg.payload.len()
                    );
                    assert!(msg.payload.contains("a=setup:active"));
                    got_answer = true;
                }
                SignalingKind::Ice => {
                    let candidate = IceCandidate::deserialize(&msg.payload);
                    log::info!("[{}] ✓ Received ICE candidate: {:?}", name, candidate);
                    got_ice = true;
                }
                SignalingKind::IceDone => {
                    log::info!("[{}] ✓ Received ICE done", name);
                    if got_answer && got_ice {
                        break;
                    }
                }
                other => {
                    log::debug!("[{}] Received {:?}", name, other);
                }
            },
            Err(e) => {
                log::error!("[{}] ✗ Recv failed: {:?}", name, e);
                return false;
            }
        }
    }

    let ok = got_answer && got_ice;
    log::info!(
        "[{}] {} (answer={}, ice={})",
        name,
        if ok { "✓ Complete" } else { "✗ Incomplete" },
        got_answer,
        got_ice
    );
    ok
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_answerer(
    transport: &mut Box<dyn Transport>,
    client: &mut SignalingClient,
    name: &str,
) -> bool {
    // Wait for offer
    let mut got_offer = false;
    let mut got_ice = false;
    let mut _offer_sdp = String::new();

    for _ in 0..10 {
        match client.recv_message(transport).await {
            Ok(msg) => match msg.kind {
                SignalingKind::Offer => {
                    log::info!(
                        "[{}] ✓ Received SDP offer ({} bytes)",
                        name,
                        msg.payload.len()
                    );
                    assert!(msg.payload.contains("a=setup:actpass"));
                    _offer_sdp = msg.payload;
                    got_offer = true;
                }
                SignalingKind::Ice => {
                    let candidate = IceCandidate::deserialize(&msg.payload);
                    log::info!("[{}] ✓ Received ICE candidate: {:?}", name, candidate);
                    got_ice = true;
                }
                SignalingKind::IceDone => {
                    log::info!("[{}] ✓ Received ICE done from offerer", name);
                    if got_offer {
                        break;
                    }
                }
                other => {
                    log::debug!("[{}] Received {:?}", name, other);
                }
            },
            Err(e) => {
                log::error!("[{}] ✗ Recv failed: {:?}", name, e);
                return false;
            }
        }
    }

    if !got_offer {
        log::error!("[{}] ✗ Never received offer", name);
        return false;
    }

    // Send answer
    let sdp = SdpBuilder::new().build_answer();
    let answer = SignalingMessage::answer(client.room(), &sdp);
    log::info!("[{}] Sending SDP answer ({} bytes)", name, sdp.len());
    if let Err(e) = client.send_message(transport, &answer).await {
        log::error!("[{}] ✗ Send answer failed: {:?}", name, e);
        return false;
    }

    // Send ICE candidate
    let candidate = IceCandidate::new(
        "candidate:1 1 udp 2130706431 192.168.1.20 50001 typ host",
        "0",
        0,
    );
    let ice_msg = SignalingMessage::ice(client.room(), &candidate);
    client.send_message(transport, &ice_msg).await.ok();
    log::info!("[{}] ✓ Sent ICE candidate", name);

    // Send ICE done
    let done = SignalingMessage::ice_done(client.room());
    client.send_message(transport, &done).await.ok();

    let ok = got_offer && got_ice;
    log::info!(
        "[{}] {} (offer={}, ice={})",
        name,
        if ok { "✓ Complete" } else { "✗ Incomplete" },
        got_offer,
        got_ice
    );
    ok
}

// ─── Platform stubs ──────────────────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_signaling is native-only");
}
