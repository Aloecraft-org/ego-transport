//! How a connection actually reaches its peer.
//!
//! A peer-to-peer connection can end up on very different paths for the same
//! API call. ICE may find a direct route on the local network, punch through
//! NATs on both sides, or give up on both and fall back to relaying every
//! byte through a TURN server. Those outcomes have wildly different latency
//! and cost, and from the outside they look identical: the connection works
//! either way.
//!
//! This module is the vocabulary for telling them apart.
//! [`Transport::path`](crate::transport::Transport::path) reports what a live
//! connection settled on, so a consumer can prefer punched paths, alert on an
//! unexpected relay, or simply record which rung of the ladder it landed on.
//! As everywhere else in this crate, the answer is data — acting on it is the
//! consumer's business.
//!
//! The path is not fixed for the life of a connection: ICE can re-nominate a
//! different candidate pair, so this is worth re-reading rather than sampling
//! once at connect time.

/// What kind of address one end of the path is using, in ICE's terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandidateKind {
    /// An address on the machine's own interface: no NAT in between.
    Host,
    /// An address a NAT assigned, learned from a STUN server — the address
    /// hole punching aims at.
    ServerReflexive,
    /// A NAT-assigned address learned from the peer's own traffic rather than
    /// from a STUN server. Still a punched path.
    PeerReflexive,
    /// A TURN relay's address: traffic is being forwarded, not sent directly.
    Relayed,
    /// Not reported, or not recognized.
    Unknown,
}

impl CandidateKind {
    /// Parse the candidate type as it appears in ICE/SDP and in browser stats
    /// (`host`, `srflx`, `prflx`, `relay`).
    pub fn from_ice_str(s: &str) -> Self {
        match s {
            "host" => CandidateKind::Host,
            "srflx" => CandidateKind::ServerReflexive,
            "prflx" => CandidateKind::PeerReflexive,
            "relay" => CandidateKind::Relayed,
            _ => CandidateKind::Unknown,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            CandidateKind::Host => "host",
            CandidateKind::ServerReflexive => "srflx",
            CandidateKind::PeerReflexive => "prflx",
            CandidateKind::Relayed => "relay",
            CandidateKind::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for CandidateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the candidate pair adds up to: the fact a consumer usually wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathKind {
    /// Both ends on host addresses — same network, nothing in between.
    Direct,
    /// At least one end reached through a NAT mapping: hole punching worked
    /// and traffic still flows peer to peer.
    Punched,
    /// Traffic is being forwarded by a relay. It works, but every byte costs
    /// the relay's bandwidth and takes the long way around.
    Relayed,
    /// No path yet (ICE is still connecting), or this transport has nothing
    /// to report.
    Unknown,
}

impl PathKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            PathKind::Direct => "direct",
            PathKind::Punched => "punched",
            PathKind::Relayed => "relayed",
            PathKind::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for PathKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The path a live connection is taking, as far as it can be observed.
#[derive(Debug, Clone, PartialEq)]
pub struct PathInfo {
    pub kind: PathKind,
    /// The local end's candidate type.
    pub local: CandidateKind,
    /// The remote end's candidate type.
    pub remote: CandidateKind,
    /// The local address in use, when reported.
    pub local_addr: Option<String>,
    /// The remote address in use, when reported.
    pub remote_addr: Option<String>,
    /// Most recent round-trip time over this path, in milliseconds, when the
    /// stack reports one.
    pub rtt_ms: Option<f64>,
}

impl PathInfo {
    /// Classify a path from its two candidate types, deriving [`PathKind`].
    ///
    /// A relay on *either* end means every byte is relayed, so that wins over
    /// anything else. An unknown end means the classification cannot be
    /// trusted, which is reported as unknown rather than guessed.
    pub fn from_candidates(local: CandidateKind, remote: CandidateKind) -> Self {
        let kind = match (local, remote) {
            (CandidateKind::Relayed, _) | (_, CandidateKind::Relayed) => PathKind::Relayed,
            (CandidateKind::Unknown, _) | (_, CandidateKind::Unknown) => PathKind::Unknown,
            (CandidateKind::Host, CandidateKind::Host) => PathKind::Direct,
            _ => PathKind::Punched,
        };
        Self {
            kind,
            local,
            remote,
            local_addr: None,
            remote_addr: None,
            rtt_ms: None,
        }
    }

    /// Nothing to report: ICE has not settled, or this transport has no path
    /// to describe.
    pub fn unknown() -> Self {
        Self {
            kind: PathKind::Unknown,
            local: CandidateKind::Unknown,
            remote: CandidateKind::Unknown,
            local_addr: None,
            remote_addr: None,
            rtt_ms: None,
        }
    }

    /// A path that is relayed by construction rather than by ICE's choice —
    /// a transport that forwards through a server as its whole design.
    pub fn relayed() -> Self {
        Self {
            kind: PathKind::Relayed,
            local: CandidateKind::Relayed,
            remote: CandidateKind::Relayed,
            local_addr: None,
            remote_addr: None,
            rtt_ms: None,
        }
    }

    pub fn with_addrs(mut self, local: Option<String>, remote: Option<String>) -> Self {
        self.local_addr = local;
        self.remote_addr = remote;
        self
    }

    pub fn with_rtt_ms(mut self, rtt_ms: Option<f64>) -> Self {
        self.rtt_ms = rtt_ms;
        self
    }

    /// Whether a relay is carrying this traffic.
    pub fn is_relayed(&self) -> bool {
        matches!(self.kind, PathKind::Relayed)
    }

    /// Whether traffic flows straight between the peers, punched or not.
    pub fn is_peer_to_peer(&self) -> bool {
        matches!(self.kind, PathKind::Direct | PathKind::Punched)
    }
}

impl std::fmt::Display for PathInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} <-> {}", self.kind, self.local, self.remote)?;
        if let Some(rtt) = self.rtt_ms {
            write!(f, ", rtt {rtt:.1}ms")?;
        }
        f.write_str(")")
    }
}
