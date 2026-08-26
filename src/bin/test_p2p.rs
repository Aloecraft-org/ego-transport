// bin/test_p2p.rs
//
// End-to-end P2P connection test.
//
// Starts a signaling server, then connects two native peers through it.
// Both peers use connect_p2p() and exchange messages over the resulting
// Transport — which is a direct WebRTC data channel on native.
//
// Usage:
//   cargo run --bin test_p2p
//
// For cross-platform testing (browser ↔ native):
//   1. cargo run --bin signaling_server
//   2. cargo run --bin test_p2p_native_peer
//   3. Open test_rtc_browser.html in a browser

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
    log::info!("=== P2P Connection Test ===\n");

    let signal_addr = "127.0.0.1:9981";

    // Start signaling server in background
    let server_addr = signal_addr.to_string();
    tokio::spawn(async move {
        run_signaling_server(&server_addr).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let signal_url = format!("ws://{}", signal_addr);
    let room = "test-p2p-room";
    let ice_servers = IceServerConfig::default_config();

    // Spawn two peers
    let peer_a = {
        let url = signal_url.clone();
        let room = room.to_string();
        let servers = ice_servers.clone();
        tokio::spawn(async move { run_peer(&url, &room, &servers, "Peer-A").await })
    };

    // Small delay so Peer-A joins first
    tokio::time::sleep(Duration::from_millis(200)).await;

    let peer_b = {
        let url = signal_url.clone();
        let room = room.to_string();
        let servers = ice_servers.clone();
        tokio::spawn(async move { run_peer(&url, &room, &servers, "Peer-B").await })
    };

    let a_ok = peer_a.await.unwrap_or(false);
    let b_ok = peer_b.await.unwrap_or(false);

    log::info!(
        "\n=== P2P Connection Test {} ===",
        if a_ok && b_ok { "PASSED" } else { "FAILED" }
    );
    log::info!(
        "  Peer-A: {}",
        if a_ok { "✓ PASS" } else { "✗ FAIL" }
    );
    log::info!(
        "  Peer-B: {}",
        if b_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_peer(
    signal_url: &str,
    room: &str,
    ice_servers: &[IceServerConfig],
    name: &str,
) -> bool {
    log::info!("[{}] Connecting via connect_p2p...", name);

    let mut transport = match ego_transport::transport::connect_p2p(signal_url, room, ice_servers)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            log::error!("[{}] ✗ connect_p2p failed: {:?}", name, e);
            return false;
        }
    };

    log::info!("[{}] ✓ P2P connection established!", name);

    // Exchange messages
    for i in 1..=3 {
        let msg = format!("Hello from {}, message #{}", name, i);
        log::info!("[{}] Sending: {}", name, msg);

        if let Err(e) = transport.send(msg.as_bytes()).await {
            log::error!("[{}] ✗ Send error: {:?}", name, e);
            return false;
        }

        let mut buf = [0u8; 1024];
        match transport.recv(&mut buf).await {
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]);
                log::info!("[{}] ✓ Received: {}", name, response);
            }
            Err(e) => {
                log::error!("[{}] ✗ Recv error: {:?}", name, e);
                return false;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    log::info!("[{}] ✓ Test complete!", name);
    true
}

// ─── Inline signaling server (same as signaling_server.rs) ───────────────────

#[cfg(not(target_arch = "wasm32"))]
struct Room {
    peer_a_tx: tokio::sync::mpsc::UnboundedSender<String>,
    peer_b_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

#[cfg(not(target_arch = "wasm32"))]
type RoomMap = Arc<Mutex<HashMap<String, Room>>>;

#[cfg(not(target_arch = "wasm32"))]
async fn run_signaling_server(addr: &str) {
    let rooms: RoomMap = Arc::new(Mutex::new(HashMap::new()));
    let listener = AutoDetectListener::bind(addr)
        .await
        .expect("Failed to bind signaling server");

    ServerBuilder::new(listener)
        .concurrent()
        .run(move |transport| {
            let rooms = rooms.clone();
            async move {
                handle_server_peer(transport, rooms).await.ok();
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
    let n = transport.recv(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf[..n]);
    let join_msg = SignalingMessage::deserialize(&text)
        .ok_or_else(|| TransportError::Protocol("Expected JOIN".to_string()))?;

    if join_msg.kind != SignalingKind::Join {
        return Err(TransportError::Protocol("Expected JOIN".to_string()));
    }

    let room_name = join_msg.room.clone();
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
        } else {
            map.insert(room_name.clone(), Room { peer_a_tx: my_tx, peer_b_tx: None });
            role = PeerRole::Offerer;
        }
    }

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

// ─── Platform stubs ──────────────────────────────────────────────────────────

#[cfg(target_arch = "wasm32")]
fn main() {
    panic!("test_p2p is native-only");
}