//! WASI WebRTC-compatible transport via signaling relay.
//!
//! WASI peers don't have a WebRTC stack. Instead, they participate in the
//! signaling protocol using manually constructed SDP and ICE candidates, then
//! use the signaling server's WebSocket connection as the data transport.
//!
//! From the perspective of `connect_p2p()`, this returns a `Box<dyn Transport>`
//! just like the browser and native versions — the caller doesn't know (or care)
//! that data is being relayed rather than flowing directly.
//!
//! ## How it works
//!
//! 1. Connect to signaling server via existing `ws_wasi` transport
//! 2. Join room, exchange SDP/ICE using `SdpBuilder` and `IceCandidate::from_addr()`
//! 3. Instead of establishing a direct WebRTC data channel, keep the signaling
//!    WebSocket open and use it as the data transport
//! 4. Application data is wrapped in signaling messages with a `DATA` kind
//!    (or sent raw if the signaling server supports relay mode)
//!
//! ## Limitations
//!
//! - All data flows through the signaling server (no direct P2P)
//! - No TURN — the signaling server IS the relay
//! - Higher latency than direct WebRTC
//! - Signaling server must stay running for the duration of the connection
//!
//! ## Future
//!
//! When WASI gets UDP socket support and a WebRTC library compiles to
//! `wasm32-wasip2`, this can be upgraded to direct P2P like native/browser.

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::transport::rtc_signaling::{
    IceCandidate, IceServerConfig, CandidateProtocol, PeerRole,
    SignalingKind, SignalingMessage, SdpBuilder,
};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::transport::{Transport, TransportError};

// ─── RtcWasi ─────────────────────────────────────────────────────────────────

/// A relay-based "RTC" transport for WASI peers.
///
/// Wraps an existing WebSocket transport to the signaling server. After the
/// signaling handshake, application data is exchanged as signaling messages
/// with the peer, relayed through the server.
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct RtcWasi {
    /// The WebSocket transport to the signaling server (kept open for relay).
    transport: Box<dyn Transport>,
    /// The room we're in.
    room: String,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl RtcWasi {
    /// Connect to a remote peer through a signaling server.
    ///
    /// Same interface as `RtcBrowser::connect()` and `RtcNative::connect()`.
    /// The `ice_servers` parameter is accepted for API compatibility but
    /// ignored — WASI peers relay through the signaling server.
    pub async fn connect(
        signaling_url: &str,
        room: &str,
        _ice_servers: &[IceServerConfig],
    ) -> Result<Self, TransportError> {
        log::info!("[RTC WASI] Starting connection to room '{}'", room);

        // ── Step 1: Connect to signaling server ──────────────────────────

        let mut transport = crate::transport::connect(signaling_url).await?;
        log::info!("[RTC WASI] ✓ Connected to signaling server");

        // ── Step 2: Join room and get role ───────────────────────────────

        let join_msg = SignalingMessage::join(room);
        transport.send(join_msg.serialize().as_bytes()).await?;

        let role = loop {
            let mut buf = [0u8; 4096];
            let n = transport.recv(&mut buf).await?;
            let text = String::from_utf8_lossy(&buf[..n]);
            if let Some(msg) = SignalingMessage::deserialize(&text) {
                match msg.kind {
                    SignalingKind::Ready => {
                        let r = PeerRole::from_str(&msg.payload).ok_or_else(|| {
                            TransportError::Protocol(format!("Bad role: {}", msg.payload))
                        })?;
                        break r;
                    }
                    SignalingKind::Error => {
                        return Err(TransportError::Protocol(msg.payload));
                    }
                    _ => {}
                }
            }
        };

        log::info!("[RTC WASI] Assigned role: {:?}", role);

        // ── Step 3: Construct and exchange SDP/ICE ───────────────────────
        //
        // We build minimal SDP and a host ICE candidate from our known
        // address. The peer (browser or native) will see these during its
        // signaling phase. Since we're relay-only, the ICE candidate is
        // informational — the actual data flows through the signaling server.

        let sdp_builder = SdpBuilder::new();

        // Build a candidate from whatever address info we can determine.
        // For relay-only connections this is informational, not functional.
        let local_candidate = IceCandidate::from_addr(
            "0.0.0.0:0",
            CandidateProtocol::Tcp,
        );

        match role {
            PeerRole::Offerer => {
                // Send offer
                let sdp = sdp_builder.build_offer();
                let offer = SignalingMessage::offer(room, &sdp);
                transport.send(offer.serialize().as_bytes()).await?;
                log::info!("[RTC WASI] Sent SDP offer");

                // Send ICE candidate (if we have one)
                if let Some(candidate) = &local_candidate {
                    let ice_msg = SignalingMessage::ice(room, candidate);
                    transport.send(ice_msg.serialize().as_bytes()).await?;
                }
                let done = SignalingMessage::ice_done(room);
                transport.send(done.serialize().as_bytes()).await?;

                // Wait for answer
                let mut got_answer = false;
                loop {
                    let mut buf = [0u8; 65536];
                    let n = transport.recv(&mut buf).await?;
                    let text = String::from_utf8_lossy(&buf[..n]);
                    if let Some(msg) = SignalingMessage::deserialize(&text) {
                        match msg.kind {
                            SignalingKind::Answer => {
                                log::info!("[RTC WASI] ✓ Received SDP answer");
                                got_answer = true;
                            }
                            SignalingKind::IceDone => {
                                if got_answer { break; }
                            }
                            SignalingKind::Ice => {
                                log::debug!("[RTC WASI] Received ICE candidate (noted)");
                            }
                            SignalingKind::PeerLeft => {
                                return Err(TransportError::Protocol("Peer left".to_string()));
                            }
                            _ => {}
                        }
                    }
                }
            }
            PeerRole::Answerer => {
                // Wait for offer
                loop {
                    let mut buf = [0u8; 65536];
                    let n = transport.recv(&mut buf).await?;
                    let text = String::from_utf8_lossy(&buf[..n]);
                    if let Some(msg) = SignalingMessage::deserialize(&text) {
                        match msg.kind {
                            SignalingKind::Offer => {
                                log::info!("[RTC WASI] ✓ Received SDP offer");
                                break;
                            }
                            SignalingKind::Ice => {
                                log::debug!("[RTC WASI] Received ICE candidate (noted)");
                            }
                            SignalingKind::PeerLeft => {
                                return Err(TransportError::Protocol("Peer left".to_string()));
                            }
                            _ => {}
                        }
                    }
                }

                // Send answer
                let sdp = sdp_builder.build_answer();
                let answer = SignalingMessage::answer(room, &sdp);
                transport.send(answer.serialize().as_bytes()).await?;
                log::info!("[RTC WASI] Sent SDP answer");

                // Send ICE
                if let Some(candidate) = &local_candidate {
                    let ice_msg = SignalingMessage::ice(room, candidate);
                    transport.send(ice_msg.serialize().as_bytes()).await?;
                }
                let done = SignalingMessage::ice_done(room);
                transport.send(done.serialize().as_bytes()).await?;

                // Drain remaining ICE from peer
                // Use a short timeout approach — once IceDone arrives, we're done
                loop {
                    let mut buf = [0u8; 65536];
                    let n = transport.recv(&mut buf).await?;
                    let text = String::from_utf8_lossy(&buf[..n]);
                    if let Some(msg) = SignalingMessage::deserialize(&text) {
                        match msg.kind {
                            SignalingKind::IceDone => break,
                            SignalingKind::Ice => {}
                            _ => {}
                        }
                    }
                }
            }
        }

        log::info!("[RTC WASI] ✓ Signaling complete — using relay mode");

        Ok(Self {
            transport,
            room: room.to_string(),
        })
    }
}

// ─── Transport Implementation ────────────────────────────────────────────────
//
// Application data is wrapped in a signaling message so the signaling server
// relays it to the peer. We use a simple "DATA:room:base64payload" format
// that extends the existing signaling protocol.
//
// The peer (browser or native) needs to recognize DATA messages and unwrap
// them. For browser↔browser or native↔native connections this doesn't apply
// since they use direct WebRTC data channels. DATA messages are only produced
// and consumed when a WASI peer is involved.

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
#[async_trait]
impl Transport for RtcWasi {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        // Encode data as base64 to safely embed in the text-based signaling format.
        // This avoids issues with binary data containing colons or newlines.
        let encoded = base64_encode(data);
        let msg = format!("DATA:{}:{}", self.room, encoded);
        self.transport.send(msg.as_bytes()).await
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        loop {
            let mut recv_buf = [0u8; 65536];
            let n = self.transport.recv(&mut recv_buf).await?;
            let text = String::from_utf8_lossy(&recv_buf[..n]);

            // Check for DATA message
            if text.starts_with("DATA:") {
                // Parse: DATA:room:base64payload
                let rest = &text[5..];
                if let Some(colon_pos) = rest.find(':') {
                    let payload_b64 = &rest[colon_pos + 1..];
                    if let Some(decoded) = base64_decode(payload_b64) {
                        let copy_len = decoded.len().min(buf.len());
                        buf[..copy_len].copy_from_slice(&decoded[..copy_len]);
                        return Ok(copy_len);
                    }
                }
            }

            // Check for signaling messages (PeerLeft, Error)
            if let Some(msg) = SignalingMessage::deserialize(&text) {
                match msg.kind {
                    SignalingKind::PeerLeft => {
                        return Err(TransportError::Closed);
                    }
                    SignalingKind::Error => {
                        return Err(TransportError::Protocol(msg.payload));
                    }
                    _ => {
                        // Other signaling messages during data phase — skip
                        log::debug!("[RTC WASI] Ignoring {:?} during data phase", msg.kind);
                        continue;
                    }
                }
            }

            // Unparseable — skip
            log::debug!("[RTC WASI] Ignoring unparseable message during data phase");
        }
    }
}

// ─── Base64 (minimal, no-dependency) ─────────────────────────────────────────
//
// We avoid pulling in the `base64` crate for this single use case.
// These are standard RFC 4648 base64 encode/decode.

const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(B64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(B64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(B64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(B64_CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.trim_end_matches('=');
    let mut result = Vec::with_capacity(s.len() * 3 / 4);

    let decode_char = |c: u8| -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };

    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = decode_char(bytes[i])?;
        let b1 = if i + 1 < bytes.len() { decode_char(bytes[i + 1])? } else { 0 };
        let b2 = if i + 2 < bytes.len() { decode_char(bytes[i + 2])? } else { 0 };
        let b3 = if i + 3 < bytes.len() { decode_char(bytes[i + 3])? } else { 0 };

        let triple = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;

        result.push(((triple >> 16) & 0xFF) as u8);
        if i + 2 < bytes.len() {
            result.push(((triple >> 8) & 0xFF) as u8);
        }
        if i + 3 < bytes.len() {
            result.push((triple & 0xFF) as u8);
        }

        i += 4;
    }

    Some(result)
}