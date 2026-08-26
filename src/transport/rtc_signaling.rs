//! Shared signaling types for WebRTC peer connection establishment.
//!
//! These types define the message protocol exchanged through a signaling server
//! to establish direct peer-to-peer connections. All platforms (native, WASI,
//! browser) serialize and deserialize these same types, ensuring the signaling
//! server can be completely platform-agnostic.
//!
//! ## Connection Flow
//!
//! ```text
//!   Client A                Signaling Server              Client B
//!      │                          │                          │
//!      │──── Join(room) ─────────►│                          │
//!      │                          │◄──── Join(room) ─────────│
//!      │◄─── Ready(offerer) ──────│───── Ready(answerer) ───►│
//!      │                          │                          │
//!      │──── Offer(sdp) ─────────►│───── Offer(sdp) ────────►│
//!      │                          │◄──── Answer(sdp) ────────│
//!      │◄─── Answer(sdp) ────────│                          │
//!      │                          │                          │
//!      │──── Ice(candidate) ─────►│───── Ice(candidate) ────►│
//!      │◄─── Ice(candidate) ──────│◄──── Ice(candidate) ─────│
//!      │                          │                          │
//!      │◄─────────── Direct P2P (or TURN relay) ────────────►│
//! ```
//!
//! ## WASI Peers
//!
//! WASI peers don't have a WebRTC stack. They construct minimal SDP and ICE
//! candidates from their transport address info using `IceCandidate::from_addr()`
//! and `SdpFields::for_data_channel()`, then serialize them through the same
//! signaling flow. The browser peer's ICE agent handles the actual connectivity
//! check.

use std::fmt;

// ─── Wire Format ─────────────────────────────────────────────────────────────
//
// Messages are serialized as newline-delimited text for simplicity and
// debuggability. Binary framing adds nothing here — signaling messages are
// small and infrequent.
//
// Format: "KIND:ROOM:PAYLOAD\n"
//
// KIND is a single uppercase word. ROOM is an opaque string (no colons).
// PAYLOAD is kind-specific (SDP string, JSON ICE candidate, or empty).

/// A signaling message exchanged between peers through the signaling server.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalingMessage {
    pub kind: SignalingKind,
    pub room: String,
    pub payload: String,
}

/// The type of signaling message.
#[derive(Debug, Clone, PartialEq)]
pub enum SignalingKind {
    /// Client requests to join a room. Payload is empty.
    Join,

    /// Server tells both peers the room is ready. Payload is the assigned role:
    /// `"offerer"` or `"answerer"`. The offerer creates the SDP offer.
    Ready,

    /// SDP offer from the offerer. Payload is the full SDP string.
    Offer,

    /// SDP answer from the answerer. Payload is the full SDP string.
    Answer,

    /// ICE candidate. Payload is a JSON-serialized `IceCandidate`.
    Ice,

    /// ICE gathering is complete. Payload is empty.
    /// Sent by each peer when they have no more candidates to send.
    IceDone,

    /// Peer disconnected from signaling. Sent by the server.
    PeerLeft,

    /// Error from the server. Payload is a human-readable error message.
    Error,
}

impl fmt::Display for SignalingKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalingKind::Join => write!(f, "JOIN"),
            SignalingKind::Ready => write!(f, "READY"),
            SignalingKind::Offer => write!(f, "OFFER"),
            SignalingKind::Answer => write!(f, "ANSWER"),
            SignalingKind::Ice => write!(f, "ICE"),
            SignalingKind::IceDone => write!(f, "ICE_DONE"),
            SignalingKind::PeerLeft => write!(f, "PEER_LEFT"),
            SignalingKind::Error => write!(f, "ERROR"),
        }
    }
}

impl SignalingKind {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "JOIN" => Some(Self::Join),
            "READY" => Some(Self::Ready),
            "OFFER" => Some(Self::Offer),
            "ANSWER" => Some(Self::Answer),
            "ICE" => Some(Self::Ice),
            "ICE_DONE" => Some(Self::IceDone),
            "PEER_LEFT" => Some(Self::PeerLeft),
            "ERROR" => Some(Self::Error),
            _ => None,
        }
    }
}

// ─── Serialization ───────────────────────────────────────────────────────────

impl SignalingMessage {
    /// Create a join message.
    pub fn join(room: &str) -> Self {
        Self {
            kind: SignalingKind::Join,
            room: room.to_string(),
            payload: String::new(),
        }
    }

    /// Create a ready message with the assigned role.
    pub fn ready(room: &str, role: PeerRole) -> Self {
        Self {
            kind: SignalingKind::Ready,
            room: room.to_string(),
            payload: role.to_string(),
        }
    }

    /// Create an SDP offer message.
    pub fn offer(room: &str, sdp: &str) -> Self {
        Self {
            kind: SignalingKind::Offer,
            room: room.to_string(),
            payload: sdp.to_string(),
        }
    }

    /// Create an SDP answer message.
    pub fn answer(room: &str, sdp: &str) -> Self {
        Self {
            kind: SignalingKind::Answer,
            room: room.to_string(),
            payload: sdp.to_string(),
        }
    }

    /// Create an ICE candidate message.
    pub fn ice(room: &str, candidate: &IceCandidate) -> Self {
        Self {
            kind: SignalingKind::Ice,
            room: room.to_string(),
            payload: candidate.serialize(),
        }
    }

    /// Create an ICE-done message.
    pub fn ice_done(room: &str) -> Self {
        Self {
            kind: SignalingKind::IceDone,
            room: room.to_string(),
            payload: String::new(),
        }
    }

    /// Create a peer-left message.
    pub fn peer_left(room: &str) -> Self {
        Self {
            kind: SignalingKind::PeerLeft,
            room: room.to_string(),
            payload: String::new(),
        }
    }

    /// Create an error message.
    pub fn error(room: &str, msg: &str) -> Self {
        Self {
            kind: SignalingKind::Error,
            room: room.to_string(),
            payload: msg.to_string(),
        }
    }

    /// Serialize to wire format: "KIND:ROOM:PAYLOAD"
    pub fn serialize(&self) -> String {
        format!("{}:{}:{}", self.kind, self.room, self.payload)
    }

    /// Deserialize from wire format.
    pub fn deserialize(s: &str) -> Option<Self> {
        // Only trim the leading/trailing whitespace from KIND and ROOM,
        // NOT the payload — SDP requires \r\n line endings and trim()
        // would strip the trailing \r\n from the last SDP line.
        let first_colon = s.find(':')?;
        let kind_str = s[..first_colon].trim();
        let rest = &s[first_colon + 1..];

        let second_colon = rest.find(':')?;
        let room = rest[..second_colon].trim();
        let payload = &rest[second_colon + 1..];

        let kind = SignalingKind::from_str(kind_str)?;

        Some(Self {
            kind,
            room: room.to_string(),
            payload: payload.to_string(),
        })
    }
}

// ─── Peer Role ───────────────────────────────────────────────────────────────

/// Role assigned by the signaling server to determine who creates the offer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerRole {
    /// This peer should create and send the SDP offer.
    Offerer,
    /// This peer should wait for the offer and send the SDP answer.
    Answerer,
}

impl fmt::Display for PeerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerRole::Offerer => write!(f, "offerer"),
            PeerRole::Answerer => write!(f, "answerer"),
        }
    }
}

impl PeerRole {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.trim() {
            "offerer" => Some(Self::Offerer),
            "answerer" => Some(Self::Answerer),
            _ => None,
        }
    }
}

// ─── ICE Candidate ───────────────────────────────────────────────────────────
//
// A minimal ICE candidate representation. Browser peers produce these from
// RTCIceCandidate events. WASI peers construct them from their known address.
//
// Serialized as "candidate|sdpMid|sdpMLineIndex" for compactness.
// The `candidate` field is the SDP candidate attribute string, e.g.:
//   "candidate:1 1 udp 2130706431 192.168.1.5 12345 typ host"

/// A single ICE candidate for peer connectivity.
#[derive(Debug, Clone, PartialEq)]
pub struct IceCandidate {
    /// The SDP candidate attribute string (the `a=candidate:...` line without
    /// the `a=` prefix).
    pub candidate: String,
    /// The SDP media description ID this candidate is associated with.
    pub sdp_mid: String,
    /// The zero-based index of the media description.
    pub sdp_mline_index: u16,
}

impl IceCandidate {
    /// Create an ICE candidate from browser-style fields.
    pub fn new(candidate: &str, sdp_mid: &str, sdp_mline_index: u16) -> Self {
        Self {
            candidate: candidate.to_string(),
            sdp_mid: sdp_mid.to_string(),
            sdp_mline_index,
        }
    }

    /// Construct a host candidate from a known address.
    ///
    /// Used by WASI peers that don't have a WebRTC stack but know their
    /// transport address. The generated candidate string follows RFC 8839
    /// format so the browser's ICE agent can parse it.
    ///
    /// `addr` should be "ip:port", e.g., "192.168.1.5:12345".
    /// `component` is 1 for RTP (data channel uses 1).
    pub fn from_addr(addr: &str, protocol: CandidateProtocol) -> Option<Self> {
        let (ip, port_str) = addr.rsplit_once(':')?;
        let port: u16 = port_str.parse().ok()?;

        // Priority calculation per RFC 8445 §5.1.2.1:
        //   priority = (2^24 * type_pref) + (2^8 * local_pref) + (256 - component_id)
        // type_pref: host=126, srflx=100, relay=0
        // For a host candidate with component 1:
        let priority: u32 = (126 << 24) + (65535 << 8) + 255;

        let proto = match protocol {
            CandidateProtocol::Udp => "udp",
            CandidateProtocol::Tcp => "tcp",
        };

        let candidate = format!(
            "candidate:1 1 {} {} {} {} typ host",
            proto, priority, ip, port
        );

        Some(Self {
            candidate,
            sdp_mid: "0".to_string(),
            sdp_mline_index: 0,
        })
    }

    /// Serialize to wire format: "candidate|sdpMid|sdpMLineIndex"
    pub fn serialize(&self) -> String {
        format!("{}|{}|{}", self.candidate, self.sdp_mid, self.sdp_mline_index)
    }

    /// Deserialize from wire format.
    pub fn deserialize(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.rsplitn(3, '|').collect();
        if parts.len() != 3 {
            return None;
        }
        // rsplitn reverses: [mline_index, sdp_mid, candidate]
        let sdp_mline_index: u16 = parts[0].parse().ok()?;
        let sdp_mid = parts[1].to_string();
        let candidate = parts[2].to_string();

        Some(Self {
            candidate,
            sdp_mid,
            sdp_mline_index,
        })
    }
}

/// Transport protocol for ICE candidates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CandidateProtocol {
    Udp,
    Tcp,
}

// ─── Minimal SDP Construction ────────────────────────────────────────────────
//
// WASI peers need to construct an SDP offer/answer without a WebRTC stack.
// For data-channel-only connections, the SDP is small and well-defined.

/// Helper for constructing minimal SDP strings for data-channel-only connections.
///
/// Browser peers get their SDP from `RTCPeerConnection.createOffer()` /
/// `.createAnswer()`. WASI peers use this to build a compatible SDP manually.
pub struct SdpBuilder {
    /// ICE ufrag (username fragment). Must be at least 4 characters.
    pub ice_ufrag: String,
    /// ICE password. Must be at least 22 characters.
    pub ice_pwd: String,
    /// DTLS fingerprint (SHA-256). In production this comes from the certificate.
    /// For WASI peers connecting through a relay, this can be a placeholder.
    pub fingerprint: String,
}

impl SdpBuilder {
    /// Create a builder with random ICE credentials.
    pub fn new() -> Self {
        Self {
            ice_ufrag: generate_ice_ufrag(),
            ice_pwd: generate_ice_pwd(),
            fingerprint: "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00".to_string(),
        }
    }

    /// Set the DTLS fingerprint (SHA-256 hex with colons).
    pub fn with_fingerprint(mut self, fp: &str) -> Self {
        self.fingerprint = fp.to_string();
        self
    }

    /// Build an SDP offer for a data-channel-only connection.
    pub fn build_offer(&self) -> String {
        self.build_sdp("actpass")
    }

    /// Build an SDP answer for a data-channel-only connection.
    pub fn build_answer(&self) -> String {
        self.build_sdp("active")
    }

    fn build_sdp(&self, setup_role: &str) -> String {
        // Minimal SDP for a data channel per RFC 8832 + RFC 8841.
        // This is the bare minimum that a browser's RTCPeerConnection will accept.
        format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 0.0.0.0\r\n\
             s=-\r\n\
             t=0 0\r\n\
             a=group:BUNDLE 0\r\n\
             a=msid-semantic:WMS\r\n\
             m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n\
             c=IN IP4 0.0.0.0\r\n\
             a=ice-ufrag:{ufrag}\r\n\
             a=ice-pwd:{pwd}\r\n\
             a=fingerprint:sha-256 {fp}\r\n\
             a=setup:{setup}\r\n\
             a=mid:0\r\n\
             a=sctp-port:5000\r\n\
             a=max-message-size:65536\r\n",
            ufrag = self.ice_ufrag,
            pwd = self.ice_pwd,
            fp = self.fingerprint,
            setup = setup_role,
        )
    }
}

// ─── Credential Generation ───────────────────────────────────────────────────
//
// ICE credentials need to be random but don't need to be cryptographically
// secure — they're for connection binding, not authentication.

/// Generate a random ICE ufrag (4+ alphanumeric characters).
fn generate_ice_ufrag() -> String {
    // Simple deterministic-enough approach using address of a stack variable
    // as entropy. In production, use a proper random source.
    let mut seed = 0u64;

    // Use the address of a stack variable as a quick entropy source.
    // This is NOT cryptographically secure, but ICE credentials don't need
    // to be — they're for binding, not authentication.
    let stack_addr = &seed as *const u64 as u64;
    seed = stack_addr.wrapping_mul(6364136223846793005).wrapping_add(1);

    // Mix in a timestamp if available
    #[cfg(not(target_arch = "wasm32"))]
    {
        if let Ok(dur) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            seed ^= dur.as_nanos() as u64;
        }
    }

    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        .chars()
        .collect();
    let mut result = String::with_capacity(8);
    for _ in 0..8 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (seed >> 33) as usize % chars.len();
        result.push(chars[idx]);
    }
    result
}

/// Generate a random ICE password (22+ alphanumeric characters).
fn generate_ice_pwd() -> String {
    let mut seed = 0u64;
    let stack_addr = &seed as *const u64 as u64;
    seed = stack_addr
        .wrapping_mul(6364136223846793005)
        .wrapping_add(3);

    #[cfg(not(target_arch = "wasm32"))]
    {
        if let Ok(dur) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            seed ^= dur.as_nanos() as u64;
        }
    }

    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        .chars()
        .collect();
    let mut result = String::with_capacity(24);
    for _ in 0..24 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (seed >> 33) as usize % chars.len();
        result.push(chars[idx]);
    }
    result
}

// ─── ICE Server Configuration ────────────────────────────────────────────────

/// Configuration for ICE servers (STUN and TURN).
///
/// Passed to the WebRTC stack on platforms that have one (browser, native).
/// WASI peers don't use this directly but may need the STUN server to discover
/// their server-reflexive address in a future implementation.
#[derive(Debug, Clone)]
pub struct IceServerConfig {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}

impl IceServerConfig {
    /// Google's public STUN server. Good enough for development and many
    /// production scenarios.
    pub fn google_stun() -> Self {
        Self {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            username: None,
            credential: None,
        }
    }

    /// A TURN server with credentials.
    pub fn turn(url: &str, username: &str, credential: &str) -> Self {
        Self {
            urls: vec![url.to_string()],
            username: Some(username.to_string()),
            credential: Some(credential.to_string()),
        }
    }

    /// Default config: Google STUN only. No TURN fallback.
    pub fn default_config() -> Vec<Self> {
        vec![Self::google_stun()]
    }
}

// ─── Signaling Channel ──────────────────────────────────────────────────────
//
// Trait for any channel that can exchange signaling messages between two peers.
// Implementations:
//   - WebSocket to a signaling server (current approach)
//   - Ego2 routed packets (for connecting through the overlay network)
//   - Any Box<dyn Transport> (generic fallback)

/// A channel for exchanging signaling messages between two peers.
///
/// This abstracts away *how* signaling messages are delivered. The WebRTC
/// connection setup code (`RtcBrowser`, `RtcNative`) only needs to send and
/// receive `SignalingMessage` values — it doesn't care whether they travel
/// over a dedicated WebSocket, through Ego2 packet routing, or any other
/// mechanism.
///
/// ## Implementations
///
/// - **WebSocket**: The default. Connect to a signaling server that relays
///   messages between peers in the same room.
/// - **Ego2 packets**: Embed signaling messages as payloads in routed packets.
///   This allows peers to establish WebRTC connections through the existing
///   overlay network without needing a separate signaling server.
/// - **Transport wrapper**: Any `Box<dyn Transport>` can be wrapped as a
///   `SignalingChannel` by serializing/deserializing `SignalingMessage` as text.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait::async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
pub trait SignalingChannel {
    /// Send a signaling message to the remote peer.
    async fn send_signal(&mut self, msg: &SignalingMessage) -> Result<(), crate::transport::TransportError>;

    /// Receive the next signaling message from the remote peer.
    /// Returns None if the channel is closed.
    async fn recv_signal(&mut self) -> Result<SignalingMessage, crate::transport::TransportError>;
}

/// Wraps any `Box<dyn Transport>` as a `SignalingChannel`.
///
/// Uses newline-delimited framing: each message is serialized as
/// `"KIND:ROOM:PAYLOAD\n"`. This handles TCP coalescing where multiple
/// messages may arrive in a single `recv()` call — the buffer is split
/// on `\n` boundaries and partial messages are preserved across calls.
///
/// WebSocket transports also benefit: while WS frames are message-bounded,
/// the consistent framing means all code paths behave identically.
pub struct TransportSignalingChannel {
    transport: Box<dyn crate::transport::Transport>,
    /// Buffered bytes from previous recv() calls that haven't been
    /// parsed into a complete message yet.
    pending: String,
}

impl TransportSignalingChannel {
    pub fn new(transport: Box<dyn crate::transport::Transport>) -> Self {
        Self { transport, pending: String::new() }
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait::async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
impl SignalingChannel for TransportSignalingChannel {
    async fn send_signal(&mut self, msg: &SignalingMessage) -> Result<(), crate::transport::TransportError> {
        let mut wire = msg.serialize();
        wire.push('\n');
        self.transport.send(wire.as_bytes()).await
    }

    async fn recv_signal(&mut self) -> Result<SignalingMessage, crate::transport::TransportError> {
        loop {
            // Check if we already have a complete line buffered
            if let Some(newline_pos) = self.pending.find('\n') {
                let line = self.pending[..newline_pos].to_string();
                self.pending = self.pending[newline_pos + 1..].to_string();
                if let Some(msg) = SignalingMessage::deserialize(&line) {
                    return Ok(msg);
                }
                // Unparseable line — skip and try next
                continue;
            }

            // Need more data from transport
            let mut buf = [0u8; 65536];
            let n = self.transport.recv(&mut buf).await?;
            self.pending.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }
}

// ─── Signaling Client Helper ─────────────────────────────────────────────────
//
// Thin helper that manages the signaling handshake over an existing Transport
// connection to the signaling server. Used by all platforms.

/// Manages the signaling handshake over an existing transport connection.
///
/// This is transport-agnostic — it works over any `Box<dyn Transport>` that
/// connects to the signaling server (WebSocket from browser, WebSocket from
/// WASI, TCP or WebSocket from native).
///
/// Uses newline-delimited framing, consistent with `TransportSignalingChannel`
/// and `SignalingHub`.
pub struct SignalingClient {
    room: String,
    role: Option<PeerRole>,
    /// Buffered bytes from previous recv() calls.
    pending: String,
}

impl SignalingClient {
    /// Create a new signaling client for the given room.
    pub fn new(room: &str) -> Self {
        Self {
            room: room.to_string(),
            role: None,
            pending: String::new(),
        }
    }

    /// The room this client is in.
    pub fn room(&self) -> &str {
        &self.room
    }

    /// The role assigned by the server (available after `join_and_wait`).
    pub fn role(&self) -> Option<PeerRole> {
        self.role
    }

    /// Send the join message and wait for the Ready signal.
    ///
    /// Returns the assigned `PeerRole`. The caller should then:
    /// - Offerer: create and send an SDP offer
    /// - Answerer: wait for the SDP offer, then send an answer
    pub async fn join_and_wait(
        &mut self,
        transport: &mut Box<dyn crate::transport::Transport>,
    ) -> Result<PeerRole, crate::transport::TransportError> {
        // Send join with newline delimiter
        let mut join_wire = SignalingMessage::join(&self.room).serialize();
        join_wire.push('\n');
        transport.send(join_wire.as_bytes()).await?;

        // Wait for Ready — using buffered recv for framing consistency
        loop {
            let msg = self.recv_one(transport).await?;
            match msg.kind {
                SignalingKind::Ready => {
                    let role = PeerRole::from_str(&msg.payload).ok_or_else(|| {
                        crate::transport::TransportError::Protocol(format!(
                            "Invalid role in Ready: {}",
                            msg.payload
                        ))
                    })?;
                    self.role = Some(role);
                    return Ok(role);
                }
                SignalingKind::Error => {
                    return Err(crate::transport::TransportError::Protocol(msg.payload));
                }
                _ => {
                    log::debug!(
                        "[Signaling] Unexpected message before Ready: {:?}",
                        msg.kind
                    );
                }
            }
        }
    }

    /// Send a signaling message through the transport.
    pub async fn send_message(
        &self,
        transport: &mut Box<dyn crate::transport::Transport>,
        msg: &SignalingMessage,
    ) -> Result<(), crate::transport::TransportError> {
        let mut wire = msg.serialize();
        wire.push('\n');
        transport.send(wire.as_bytes()).await
    }

    /// Receive the next signaling message from the transport.
    pub async fn recv_message(
        &mut self,
        transport: &mut Box<dyn crate::transport::Transport>,
    ) -> Result<SignalingMessage, crate::transport::TransportError> {
        self.recv_one(transport).await
    }

    /// Internal buffered receive: splits on newline boundaries.
    async fn recv_one(
        &mut self,
        transport: &mut Box<dyn crate::transport::Transport>,
    ) -> Result<SignalingMessage, crate::transport::TransportError> {
        loop {
            if let Some(newline_pos) = self.pending.find('\n') {
                let line = self.pending[..newline_pos].to_string();
                self.pending = self.pending[newline_pos + 1..].to_string();
                if let Some(msg) = SignalingMessage::deserialize(&line) {
                    return Ok(msg);
                }
                continue;
            }

            let mut buf = [0u8; 65536];
            let n = transport.recv(&mut buf).await?;
            self.pending.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signaling_message_roundtrip() {
        let msg = SignalingMessage::join("my-room");
        let serialized = msg.serialize();
        assert_eq!(serialized, "JOIN:my-room:");
        let deserialized = SignalingMessage::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, msg);
    }

    #[test]
    fn test_offer_with_colons_in_payload() {
        // SDP contains colons — make sure they survive serialization
        let sdp = "v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\na=fingerprint:sha-256 AA:BB:CC\r\n";
        let msg = SignalingMessage::offer("room1", sdp);
        let serialized = msg.serialize();
        let deserialized = SignalingMessage::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.payload, sdp);
        assert_eq!(deserialized.kind, SignalingKind::Offer);
        assert_eq!(deserialized.room, "room1");
    }

    #[test]
    fn test_ice_candidate_roundtrip() {
        let candidate = IceCandidate::new(
            "candidate:1 1 udp 2130706431 192.168.1.5 12345 typ host",
            "0",
            0,
        );
        let serialized = candidate.serialize();
        let deserialized = IceCandidate::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, candidate);
    }

    #[test]
    fn test_ice_candidate_from_addr() {
        let candidate =
            IceCandidate::from_addr("192.168.1.5:12345", CandidateProtocol::Udp).unwrap();
        assert!(candidate.candidate.contains("192.168.1.5"));
        assert!(candidate.candidate.contains("12345"));
        assert!(candidate.candidate.contains("udp"));
        assert!(candidate.candidate.contains("typ host"));
    }

    #[test]
    fn test_sdp_builder_offer() {
        let sdp = SdpBuilder::new().build_offer();
        assert!(sdp.contains("v=0"));
        assert!(sdp.contains("a=setup:actpass"));
        assert!(sdp.contains("webrtc-datachannel"));
        assert!(sdp.contains("a=ice-ufrag:"));
        assert!(sdp.contains("a=ice-pwd:"));
    }

    #[test]
    fn test_sdp_builder_answer() {
        let sdp = SdpBuilder::new().build_answer();
        assert!(sdp.contains("a=setup:active"));
    }

    #[test]
    fn test_ready_message_roles() {
        let msg = SignalingMessage::ready("room1", PeerRole::Offerer);
        let serialized = msg.serialize();
        let deserialized = SignalingMessage::deserialize(&serialized).unwrap();
        let role = PeerRole::from_str(&deserialized.payload).unwrap();
        assert_eq!(role, PeerRole::Offerer);
    }

    #[test]
    fn test_peer_role_display_parse() {
        assert_eq!(PeerRole::Offerer.to_string(), "offerer");
        assert_eq!(PeerRole::Answerer.to_string(), "answerer");
        assert_eq!(PeerRole::from_str("offerer"), Some(PeerRole::Offerer));
        assert_eq!(PeerRole::from_str("answerer"), Some(PeerRole::Answerer));
        assert_eq!(PeerRole::from_str("invalid"), None);
    }
}