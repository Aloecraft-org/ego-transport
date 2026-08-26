//! Embeddable signaling hub for peer-to-peer connection establishment.
//!
//! `SignalingHub` manages signaling rooms and relays SDP/ICE messages between
//! peers. It is designed to be embedded inside `AutoDetectListener` so that
//! any server automatically acts as a signaling server for WebRTC connections.
//!
//! ## How it integrates with AutoDetectListener
//!
//! When `AutoDetectListener` accepts a connection, it peeks at the first
//! application-level message. If it starts with `"JOIN:"`, the connection is
//! a signaling peer — the hub takes ownership and handles it. Otherwise the
//! connection is returned to `ServerBuilder` as a normal `Box<dyn Transport>`.
//!
//! ```text
//!   Incoming connection
//!         │
//!    ┌────▼────┐
//!    │ TCP/WS  │  (AutoDetect: transport-level detection)
//!    │ detect  │
//!    └────┬────┘
//!         │
//!    ┌────▼────┐
//!    │ First   │  (Protocol-level detection)
//!    │ message │
//!    └────┬────┘
//!         │
//!    ┌────┴────────────────┐
//!    │                     │
//!    ▼                     ▼
//!  "JOIN:..."          anything else
//!    │                     │
//!    ▼                     ▼
//!  SignalingHub        ServerBuilder
//!  (relay task)        (app handler)
//! ```
//!
//! ## Platform support
//!
//! `SignalingHub` is platform-agnostic — it only depends on the `Transport`
//! trait and standard library types. It works on native and WASI P2.
//! On WASI (sequential accept), signaling peers are handled inline before
//! returning to accept the next connection. On native (concurrent), each
//! signaling peer gets its own spawned task.
//!
//! ## Extracting it from the standalone signaling_server.rs
//!
//! The standalone `signaling_server` binary becomes a thin wrapper:
//!
//! ```no_run
//! # #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
//! # mod example {
//! # async fn run() -> Result<(), ego_transport::transport::TransportError> {
//!
//! use ego_transport::platform::server::{ServerBuilder, AutoDetectListener};
//! use ego_transport::transport::signaling_hub::SignalingHub;
//!
//! let hub = SignalingHub::new();
//! let listener = AutoDetectListener::bind("0.0.0.0:9995").await?
//!     .with_signaling(hub);
//!
//! // All connections are signaling — ServerBuilder handler is never called
//! ServerBuilder::new(listener)
//!     .concurrent()
//!     .run(|_| async { /* never reached for signaling-only server */ })
//!     .await?;
//!
//! # Ok(())
//! # }
//! # }
//! ```

use crate::transport::rtc_signaling::{PeerRole, SignalingKind, SignalingMessage};
use crate::transport::{Transport, TransportError};
use std::collections::HashMap;
use std::sync::Arc;

// ─── Platform-appropriate Mutex ──────────────────────────────────────────────
//
// On native + WASI P2 we use tokio::sync::Mutex (async-aware).
// On browser this module is not used (browser has no server listener).

// #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use tokio::sync::Mutex;

// ─── Room ────────────────────────────────────────────────────────────────────

/// A signaling room with up to two peers.
struct Room {
    /// Channel to send messages to the first peer (the offerer).
    peer_a_tx: tokio::sync::mpsc::UnboundedSender<String>,
    /// Channel to send messages to the second peer (the answerer).
    /// `None` until the second peer joins.
    peer_b_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

// ─── SignalingHub ────────────────────────────────────────────────────────────

/// A shared signaling room manager.
///
/// Thread-safe (wrapped in `Arc<Mutex<...>>` internally). Clone is cheap.
///
/// Create one and pass it to `AutoDetectListener::with_signaling()` to
/// enable embedded signaling. Multiple listeners can share the same hub.
#[derive(Clone)]
pub struct SignalingHub {
    rooms: Arc<Mutex<HashMap<String, Room>>>,
}

impl Default for SignalingHub {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalingHub {
    /// Create a new empty hub.
    pub fn new() -> Self {
        Self {
            rooms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check if a message is a JOIN request (protocol detection).
    ///
    /// This is called by `AutoDetectListener` after receiving the first
    /// application-level message to decide whether to route the connection
    /// to the hub or to the application handler.
    pub fn is_signaling_message(data: &[u8]) -> bool {
        // JOIN messages start with "JOIN:" — we check the first 5 bytes
        data.len() >= 5 && &data[..5] == b"JOIN:"
    }

    /// Handle a signaling peer.
    ///
    /// Takes ownership of the transport and the first message (which has
    /// already been received by AutoDetectListener for protocol detection).
    /// Manages the peer's lifecycle: room join, relay, cleanup.
    ///
    /// This function runs for the lifetime of the signaling connection.
    /// On native it should be spawned as a task. On WASI (sequential) it
    /// runs inline — which means the server can't accept new connections
    /// while a signaling peer is connected. For WASI, consider using a
    /// separate signaling port or accepting that signaling is sequential.
    pub async fn handle_peer(
        &self,
        mut transport: Box<dyn Transport>,
        first_message: &[u8],
    ) -> Result<(), TransportError> {
        // Parse the JOIN message (already validated by is_signaling_message)
        let text = String::from_utf8_lossy(first_message);
        let join_msg = SignalingMessage::deserialize(&text)
            .ok_or_else(|| TransportError::Protocol(format!("Invalid JOIN message: {}", text)))?;

        if join_msg.kind != SignalingKind::Join {
            let err = SignalingMessage::error("", "First message must be JOIN");
            let mut wire = err.serialize();
            wire.push('\n');
            transport.send(wire.as_bytes()).await.ok();
            return Err(TransportError::Protocol("Expected JOIN".to_string()));
        }

        let room_name = join_msg.room.clone();
        log::info!("[SignalingHub] Peer joining room '{}'", room_name);

        // Register in room
        let (my_tx, mut my_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let role;

        {
            let mut map = self.rooms.lock().await;

            if let Some(room) = map.get_mut(&room_name) {
                if room.peer_b_tx.is_some() {
                    let err = SignalingMessage::error(&room_name, "Room is full");
                    let mut wire = err.serialize();
                    wire.push('\n');
                    transport.send(wire.as_bytes()).await.ok();
                    return Err(TransportError::Protocol("Room full".to_string()));
                }

                // Second peer (answerer)
                room.peer_b_tx = Some(my_tx);
                role = PeerRole::Answerer;

                // Notify both
                let ready_a = SignalingMessage::ready(&room_name, PeerRole::Offerer);
                let mut wire_a = ready_a.serialize();
                wire_a.push('\n');
                room.peer_a_tx.send(wire_a).ok();
                let ready_b = SignalingMessage::ready(&room_name, PeerRole::Answerer);
                let mut wire_b = ready_b.serialize();
                wire_b.push('\n');
                transport.send(wire_b.as_bytes()).await?;

                log::info!(
                    "[SignalingHub] Room '{}' ready — two peers matched",
                    room_name
                );
            } else {
                // First peer (offerer)
                map.insert(
                    room_name.clone(),
                    Room {
                        peer_a_tx: my_tx,
                        peer_b_tx: None,
                    },
                );
                role = PeerRole::Offerer;
                log::info!(
                    "[SignalingHub] Room '{}' created — waiting for second peer",
                    room_name
                );
            }
        }

        // Relay loop
        let relay_result = self
            .relay_loop(&mut transport, &mut my_rx, &room_name, role)
            .await;

        // Cleanup
        self.cleanup_peer(&room_name, role).await;

        relay_result
    }

    /// Relay messages between the peer's transport and the room channel.
    async fn relay_loop(
        &self,
        transport: &mut Box<dyn Transport>,
        my_rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
        room_name: &str,
        role: PeerRole,
    ) -> Result<(), TransportError> {
        let mut buf = [0u8; 65536];

        loop {
            tokio::select! {
                // Message from this peer → relay to the other peer
                result = transport.recv(&mut buf) => {
                    let n = result?;
                    let mut text = String::from_utf8_lossy(&buf[..n]).to_string();
                    // Ensure newline termination for framing consistency.
                    // Clients using TransportSignalingChannel already send \n,
                    // but normalize here so relay is safe regardless.
                    if !text.ends_with('\n') {
                        text.push('\n');
                    }

                    let map = self.rooms.lock().await;
                    if let Some(room) = map.get(room_name) {
                        let other_tx = match role {
                            PeerRole::Offerer => room.peer_b_tx.as_ref(),
                            PeerRole::Answerer => Some(&room.peer_a_tx),
                        };
                        if let Some(tx) = other_tx
                            && tx.send(text).is_err() {
                                log::debug!("[SignalingHub] Other peer disconnected");
                                return Ok(());
                            }
                    }
                }

                // Message from the other peer → forward to this peer's transport
                result = my_rx.recv() => {
                    match result {
                        Some(msg) => {
                            // msg already has \n from the relay path above
                            transport.send(msg.as_bytes()).await?;
                        }
                        None => {
                            // Channel closed — other peer's handler exited
                            log::debug!("[SignalingHub] Channel closed for {:?}", role);
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Clean up after a peer disconnects.
    async fn cleanup_peer(&self, room_name: &str, role: PeerRole) {
        let mut map = self.rooms.lock().await;

        if let Some(room) = map.get(room_name) {
            // Notify the other peer
            let peer_left = SignalingMessage::peer_left(room_name);
            let mut msg = peer_left.serialize();
            msg.push('\n');

            match role {
                PeerRole::Offerer => {
                    if let Some(tx) = &room.peer_b_tx {
                        tx.send(msg).ok();
                    }
                }
                PeerRole::Answerer => {
                    room.peer_a_tx.send(msg).ok();
                }
            }

            // Remove room if both peers are gone
            let a_closed = room.peer_a_tx.is_closed();
            let b_closed = room.peer_b_tx.as_ref().is_none_or(|tx| tx.is_closed());

            if a_closed && b_closed {
                map.remove(room_name);
                log::info!("[SignalingHub] Room '{}' removed (empty)", room_name);
            }
        }
    }
}
