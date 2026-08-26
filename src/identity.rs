//! Peer identity as reported by a transport.
//!
//! Some schemes authenticate the remote end as part of connection setup (SSH
//! host keys and client keys, TLS peer certificates); others carry no identity
//! at all (bare TCP). This module is the typed surface through which a
//! transport reports whatever identity the scheme produced — verbatim, with no
//! policy attached. Deciding what an identity is *allowed to do* is the
//! consumer's business; ego-transport only reports it.

/// A public-key identity surfaced by a scheme's handshake.
///
/// The key material is kept in wire form (OpenSSH public key encoding) so it
/// can cross platform boundaries without dragging scheme-specific crates into
/// every build target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdentity {
    /// Algorithm name as negotiated on the wire (e.g. `ssh-ed25519`).
    pub algorithm: String,
    /// SHA-256 fingerprint in the OpenSSH presentation (`SHA256:<base64>`).
    pub fingerprint_sha256: String,
    /// The public key in OpenSSH wire encoding, verbatim.
    pub public_key: Vec<u8>,
}

impl std::fmt::Display for KeyIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.algorithm, self.fingerprint_sha256)
    }
}

/// The identity of the remote end of a connection, as far as the scheme that
/// produced the connection can attest to one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerIdentity {
    /// The scheme carries no identity (bare TCP, plain WebSocket).
    Anonymous,
    /// A public key proven during the handshake, plus the user name the peer
    /// authenticated as where the scheme has one (SSH client auth).
    Key {
        key: KeyIdentity,
        /// User name presented during authentication, if the scheme has one.
        user: Option<String>,
    },
}

impl PeerIdentity {
    /// The fingerprint of the peer's key, when the scheme produced one.
    pub fn fingerprint(&self) -> Option<&str> {
        match self {
            PeerIdentity::Anonymous => None,
            PeerIdentity::Key { key, .. } => Some(&key.fingerprint_sha256),
        }
    }
}
