//! STUN binding (RFC 5389): what does the outside world see this socket as?
//!
//! Hole punching needs one fact before anything else can happen: the
//! *server-reflexive* address a NAT assigns to a local socket. A STUN binding
//! request asks a server "what source address did this datagram arrive from?",
//! and the answer is the address a peer must aim at. Asking two servers from
//! the *same* socket answers the follow-up question — whether the NAT reuses
//! one mapping for every destination ([`NatMapping::EndpointIndependent`],
//! punchable) or mints a fresh one per destination
//! ([`NatMapping::EndpointDependent`], not punchable, relay required).
//!
//! This module implements only the binding request/response subset, by hand
//! and without a STUN dependency. That is a deliberate contrast with the `ssh`
//! scheme, which leans on a maintained protocol implementation: there, key
//! exchange and rekey are stateful cryptographic machinery where an
//! implementation bug is a vulnerability. Here there is no cryptographic
//! state at all — a 20-byte header, a type-length-value walk, and an XOR. The
//! one security-relevant property, that an off-path attacker cannot forge a
//! response, comes from the 96-bit random transaction id, which is generated
//! from the system CSPRNG and checked on every reply along with the source
//! address.
//!
//! Keeping the codec dependency-free also keeps it platform-neutral: it
//! compiles everywhere, even where no socket exists to use it on. The socket
//! side is gated — see [`probe`] and [`StunServer`], which refuse with a
//! typed [`StunError::Unsupported`] on platforms that have no UDP.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use thiserror::Error;

/// The RFC 5389 magic cookie, present in every conforming STUN message.
pub const MAGIC_COOKIE: u32 = 0x2112_A442;
const MAGIC_COOKIE_BYTES: [u8; 4] = MAGIC_COOKIE.to_be_bytes();

/// Every STUN message begins with a fixed 20-byte header.
pub const HEADER_LEN: usize = 20;
const TXID_LEN: usize = 12;

/// The IANA-assigned default STUN port, used when a server address omits one.
pub const DEFAULT_STUN_PORT: u16 = 3478;

const TYPE_BINDING_REQUEST: u16 = 0x0001;
const TYPE_BINDING_SUCCESS: u16 = 0x0101;
const TYPE_BINDING_ERROR: u16 = 0x0111;

const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const ATTR_ERROR_CODE: u16 = 0x0009;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;

const FAMILY_IPV4: u8 = 0x01;
const FAMILY_IPV6: u8 = 0x02;

/// Typed STUN failures. Distinct outcomes are distinct variants so a consumer
/// can tell "this platform has no UDP" from "that server never answered" from
/// "the reply was not for us".
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StunError {
    /// No UDP on this platform, so no STUN. Named at call time, never a stub.
    #[error("STUN cannot {operation} on {platform}: {reason}")]
    Unsupported {
        platform: &'static str,
        operation: &'static str,
        reason: &'static str,
    },

    /// The bytes are not a well-formed STUN message.
    #[error("malformed STUN message: {0}")]
    Malformed(&'static str),

    /// A reply arrived carrying someone else's transaction id — a stray
    /// datagram or a spoofing attempt, either way not our answer.
    #[error("STUN reply did not match the request's transaction id")]
    TransactionMismatch,

    /// A success response with no address attribute in it.
    #[error("STUN success response carried no mapped address")]
    NoMappedAddress,

    /// The server refused, with its own error code.
    #[error("STUN server returned error {code}: {reason}")]
    ServerError { code: u16, reason: String },

    /// The server never answered within the retransmission budget.
    #[error("no STUN response from {server} after {attempts} attempt(s)")]
    Timeout { server: String, attempts: usize },

    /// The server address could not be resolved.
    #[error("could not resolve STUN server address '{0}'")]
    Resolve(String),

    /// Socket-level failure (stringified: `io::Error` is not `Clone`).
    #[error("STUN I/O error: {0}")]
    Io(String),

    /// Mapping detection compares the mapping seen by two or more servers.
    #[error("mapping detection needs at least 2 STUN servers, got {0}")]
    NotEnoughServers(usize),
}

/// The 96-bit transaction id that ties a response to its request.
///
/// This is the anti-spoofing mechanism: an off-path attacker who cannot see
/// the request cannot guess the id, so a forged reply is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId([u8; TXID_LEN]);

impl TransactionId {
    pub const fn from_bytes(bytes: [u8; TXID_LEN]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; TXID_LEN] {
        &self.0
    }

    /// A fresh id from the system CSPRNG.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn random() -> Self {
        use rand::Rng;
        let mut bytes = [0u8; TXID_LEN];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }
}

/// A decoded STUN message. Only the binding method is modelled; anything else
/// is reported as [`StunError::Malformed`] rather than half-understood.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StunMessage {
    BindingRequest {
        txid: TransactionId,
    },
    BindingSuccess {
        txid: TransactionId,
        /// The source address the server observed — the reflexive address.
        mapped: SocketAddr,
    },
    BindingError {
        txid: TransactionId,
        code: u16,
        reason: String,
    },
}

impl StunMessage {
    pub fn txid(&self) -> TransactionId {
        match self {
            StunMessage::BindingRequest { txid }
            | StunMessage::BindingSuccess { txid, .. }
            | StunMessage::BindingError { txid, .. } => *txid,
        }
    }
}

/// Encode a binding request. No attributes: the request is the header alone.
pub fn encode_binding_request(txid: &TransactionId) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[..2].copy_from_slice(&TYPE_BINDING_REQUEST.to_be_bytes());
    // Attribute length stays 0.
    out[4..8].copy_from_slice(&MAGIC_COOKIE_BYTES);
    out[8..].copy_from_slice(txid.as_bytes());
    out
}

/// Encode a binding success response reporting `mapped` as the peer's
/// reflexive address, in the XOR form every modern client expects.
pub fn encode_binding_success(txid: &TransactionId, mapped: SocketAddr) -> Vec<u8> {
    let value = encode_xor_address(mapped, txid);
    let mut out = Vec::with_capacity(HEADER_LEN + 4 + value.len());
    out.extend_from_slice(&TYPE_BINDING_SUCCESS.to_be_bytes());
    out.extend_from_slice(&((value.len() + 4) as u16).to_be_bytes());
    out.extend_from_slice(&MAGIC_COOKIE_BYTES);
    out.extend_from_slice(txid.as_bytes());
    out.extend_from_slice(&ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(&value);
    out
}

/// Decode a STUN binding message.
///
/// Unknown attributes are skipped rather than rejected: the transaction id is
/// what authenticates a reply, and tolerating extra attributes (`SOFTWARE`,
/// `FINGERPRINT`, and the rest) is what keeps this interoperable with servers
/// in the wild.
pub fn decode(buf: &[u8]) -> Result<StunMessage, StunError> {
    if buf.len() < HEADER_LEN {
        return Err(StunError::Malformed("shorter than the 20-byte header"));
    }
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    if msg_type & 0xC000 != 0 {
        return Err(StunError::Malformed("leading two bits are not zero"));
    }
    let attr_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    if !attr_len.is_multiple_of(4) {
        return Err(StunError::Malformed(
            "attribute length is not a multiple of 4",
        ));
    }
    if u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) != MAGIC_COOKIE {
        return Err(StunError::Malformed("wrong magic cookie"));
    }
    if buf.len() < HEADER_LEN + attr_len {
        return Err(StunError::Malformed("truncated attributes"));
    }
    let mut id = [0u8; TXID_LEN];
    id.copy_from_slice(&buf[8..HEADER_LEN]);
    let txid = TransactionId::from_bytes(id);

    match msg_type {
        TYPE_BINDING_REQUEST => Ok(StunMessage::BindingRequest { txid }),
        TYPE_BINDING_SUCCESS => {
            let mut mapped = None;
            for (attr, value) in Attributes::new(&buf[HEADER_LEN..HEADER_LEN + attr_len]) {
                match attr {
                    ATTR_XOR_MAPPED_ADDRESS => {
                        mapped = Some(decode_xor_address(value, &txid)?);
                        break;
                    }
                    // Legacy form, kept as a fallback for older servers.
                    ATTR_MAPPED_ADDRESS if mapped.is_none() => {
                        mapped = Some(decode_plain_address(value)?);
                    }
                    _ => {}
                }
            }
            Ok(StunMessage::BindingSuccess {
                txid,
                mapped: mapped.ok_or(StunError::NoMappedAddress)?,
            })
        }
        TYPE_BINDING_ERROR => {
            let mut code = 0u16;
            let mut reason = String::new();
            for (attr, value) in Attributes::new(&buf[HEADER_LEN..HEADER_LEN + attr_len]) {
                if attr == ATTR_ERROR_CODE && value.len() >= 4 {
                    code = u16::from(value[2] & 0x07) * 100 + u16::from(value[3]);
                    reason = String::from_utf8_lossy(&value[4..]).into_owned();
                    break;
                }
            }
            Ok(StunMessage::BindingError { txid, code, reason })
        }
        _ => Err(StunError::Malformed("not a binding request or response")),
    }
}

/// Walks the type-length-value attribute region, skipping the padding that
/// aligns each value to a 4-byte boundary. Stops at the first malformed
/// header rather than reading past it.
struct Attributes<'a> {
    rest: &'a [u8],
}

impl<'a> Attributes<'a> {
    fn new(region: &'a [u8]) -> Self {
        Self { rest: region }
    }
}

impl<'a> Iterator for Attributes<'a> {
    type Item = (u16, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.len() < 4 {
            return None;
        }
        let attr = u16::from_be_bytes([self.rest[0], self.rest[1]]);
        let len = u16::from_be_bytes([self.rest[2], self.rest[3]]) as usize;
        if self.rest.len() < 4 + len {
            self.rest = &[];
            return None;
        }
        let value = &self.rest[4..4 + len];
        let advance = (4 + len + 3) & !3;
        self.rest = self.rest.get(advance..).unwrap_or(&[]);
        Some((attr, value))
    }
}

fn encode_xor_address(addr: SocketAddr, txid: &TransactionId) -> Vec<u8> {
    let port = addr.port() ^ (MAGIC_COOKIE >> 16) as u16;
    match addr.ip() {
        IpAddr::V4(ip) => {
            let mut out = Vec::with_capacity(8);
            out.extend_from_slice(&[0, FAMILY_IPV4]);
            out.extend_from_slice(&port.to_be_bytes());
            for (octet, key) in ip.octets().iter().zip(&MAGIC_COOKIE_BYTES) {
                out.push(octet ^ key);
            }
            out
        }
        IpAddr::V6(ip) => {
            let mut out = Vec::with_capacity(20);
            out.extend_from_slice(&[0, FAMILY_IPV6]);
            out.extend_from_slice(&port.to_be_bytes());
            for (octet, key) in ip.octets().iter().zip(ipv6_key(txid)) {
                out.push(octet ^ key);
            }
            out
        }
    }
}

fn decode_xor_address(value: &[u8], txid: &TransactionId) -> Result<SocketAddr, StunError> {
    if value.len() < 4 {
        return Err(StunError::Malformed("address attribute too short"));
    }
    let port = u16::from_be_bytes([value[2], value[3]]) ^ (MAGIC_COOKIE >> 16) as u16;
    match value[1] {
        FAMILY_IPV4 => {
            if value.len() < 8 {
                return Err(StunError::Malformed("IPv4 address attribute too short"));
            }
            let mut octets = [0u8; 4];
            for ((out, xored), key) in octets.iter_mut().zip(&value[4..8]).zip(&MAGIC_COOKIE_BYTES)
            {
                *out = xored ^ key;
            }
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port))
        }
        FAMILY_IPV6 => {
            if value.len() < 20 {
                return Err(StunError::Malformed("IPv6 address attribute too short"));
            }
            let key = ipv6_key(txid);
            let mut octets = [0u8; 16];
            for ((out, xored), k) in octets.iter_mut().zip(&value[4..20]).zip(&key) {
                *out = xored ^ k;
            }
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        _ => Err(StunError::Malformed("unknown address family")),
    }
}

fn decode_plain_address(value: &[u8]) -> Result<SocketAddr, StunError> {
    if value.len() < 4 {
        return Err(StunError::Malformed("address attribute too short"));
    }
    let port = u16::from_be_bytes([value[2], value[3]]);
    match value[1] {
        FAMILY_IPV4 if value.len() >= 8 => {
            let mut octets = [0u8; 4];
            octets.copy_from_slice(&value[4..8]);
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port))
        }
        FAMILY_IPV6 if value.len() >= 20 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&value[4..20]);
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        FAMILY_IPV4 | FAMILY_IPV6 => Err(StunError::Malformed("address attribute too short")),
        _ => Err(StunError::Malformed("unknown address family")),
    }
}

/// IPv6 addresses are XOR'd with the cookie followed by the transaction id.
fn ipv6_key(txid: &TransactionId) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..4].copy_from_slice(&MAGIC_COOKIE_BYTES);
    key[4..].copy_from_slice(txid.as_bytes());
    key
}

// ---------------------------------------------------------------------------
// Probe results and configuration (platform-neutral)
// ---------------------------------------------------------------------------

/// What one binding exchange learned about a socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StunProbe {
    /// The server that answered.
    pub server: SocketAddr,
    /// The local address of the socket the request went out on.
    pub local: SocketAddr,
    /// The address that server saw — what a peer must aim at to reach this
    /// socket, assuming an endpoint-independent mapping.
    pub reflexive: SocketAddr,
    /// Round-trip time of the exchange, in milliseconds.
    pub rtt_ms: u64,
}

impl StunProbe {
    /// Whether a NAT rewrote the address at all.
    pub fn is_natted(&self) -> bool {
        self.local != self.reflexive
    }
}

/// Retransmission and binding parameters for a probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeConfig {
    /// How many times to send before giving up.
    pub attempts: usize,
    /// Wait before the first retransmission; doubled on each retry, as
    /// RFC 5389 prescribes.
    pub initial_timeout: Duration,
    /// Local address to bind. Binding a concrete address (rather than the
    /// wildcard) is what lets [`NatMapping::Open`] be distinguished from
    /// [`NatMapping::EndpointIndependent`].
    pub bind_addr: SocketAddr,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            attempts: 3,
            initial_timeout: Duration::from_millis(500),
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }
}

/// How a NAT assigns mappings — the fact that decides whether hole punching
/// can work at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatMapping {
    /// The socket's own address is what the world sees: no NAT in the path.
    Open,
    /// One mapping is reused for every destination, so the address learned
    /// from a STUN server is the address a peer can reach. Punchable.
    EndpointIndependent,
    /// A fresh mapping per destination ("symmetric" NAT). What a STUN server
    /// reports says nothing about what a peer would see, so punching cannot
    /// work and traffic has to be relayed.
    EndpointDependent,
}

impl NatMapping {
    /// Whether a direct hole-punched path is worth attempting.
    pub fn hole_punching_viable(self) -> bool {
        !matches!(self, NatMapping::EndpointDependent)
    }
}

/// The result of comparing what several servers saw of one socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingReport {
    pub mapping: NatMapping,
    /// The local address every probe went out on.
    pub local: SocketAddr,
    /// One entry per server, in the order supplied.
    pub probes: Vec<StunProbe>,
}

impl MappingReport {
    /// The reflexive address to advertise to peers. Meaningful only when the
    /// mapping is endpoint-independent; `None` under a symmetric NAT, where
    /// no single address would be right.
    pub fn reflexive(&self) -> Option<SocketAddr> {
        match self.mapping {
            NatMapping::EndpointDependent => None,
            _ => self.probes.first().map(|p| p.reflexive),
        }
    }
}

// ---------------------------------------------------------------------------
// Platform entry points
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
pub use crate::platform::stun_native::{StunServer, StunServerMetrics, StunServerSnapshot};

/// Ask one STUN server what it sees, with default retransmission settings.
///
/// The server may be given as `host:port`, as a `stun:`/`stuns:` URL, or as a
/// bare host (in which case [`DEFAULT_STUN_PORT`] is used). IPv6 literals must
/// be bracketed.
pub async fn probe(server: &str) -> Result<StunProbe, StunError> {
    probe_with(server, &ProbeConfig::default()).await
}

/// Ask one STUN server what it sees, with explicit retransmission settings.
#[allow(unused_variables)] // consumed only by the native implementation
pub async fn probe_with(server: &str, config: &ProbeConfig) -> Result<StunProbe, StunError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::platform::stun_native::probe_with(server, config).await
    }
    #[cfg(target_arch = "wasm32")]
    {
        Err(unsupported("probe"))
    }
}

/// Ask two or more servers what they see of the *same* socket, and classify
/// the NAT mapping from whether their answers agree.
///
/// Using one socket for every probe is the whole point: two sockets would
/// have different mappings under any NAT, and the comparison would be
/// meaningless.
#[allow(unused_variables)] // consumed only by the native implementation
pub async fn detect_mapping(
    servers: &[&str],
    config: &ProbeConfig,
) -> Result<MappingReport, StunError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::platform::stun_native::detect_mapping(servers, config).await
    }
    #[cfg(target_arch = "wasm32")]
    {
        if servers.len() < 2 {
            return Err(StunError::NotEnoughServers(servers.len()));
        }
        Err(unsupported("detect mapping"))
    }
}

#[cfg(target_arch = "wasm32")]
fn unsupported(operation: &'static str) -> StunError {
    #[cfg(target_env = "p2")]
    {
        StunError::Unsupported {
            platform: "wasm32-wasip2",
            operation,
            reason: "UDP sockets are not wired up on this target yet",
        }
    }
    #[cfg(not(target_env = "p2"))]
    {
        StunError::Unsupported {
            platform: "browser",
            operation,
            reason: "the browser sandbox has no UDP sockets; ICE runs inside RtcPeerConnection instead",
        }
    }
}

/// Normalize a server address: strip a `stun:`/`stuns:` scheme and supply the
/// default port when none was given.
pub fn normalize_server(server: &str) -> String {
    let bare = server
        .strip_prefix("stun:")
        .or_else(|| server.strip_prefix("stuns:"))
        .unwrap_or(server);
    let has_port = if bare.starts_with('[') {
        bare.rsplit_once("]:").is_some()
    } else {
        bare.contains(':')
    };
    if has_port {
        bare.to_string()
    } else {
        format!("{bare}:{DEFAULT_STUN_PORT}")
    }
}
