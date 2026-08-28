//! Named schemes, endpoint addresses, and the per-platform support table.
//!
//! An endpoint reference is a location-independent name — `scheme://authority[/path]`
//! — resolved at dial or bind time. It names a resolver, not a live
//! connection, so it can be stored, serialized, and re-resolved on a
//! different machine later.
//!
//! Every scheme is gated per platform *here*, in one table, and an
//! unavailable scheme is a named, typed refusal
//! ([`TransportError::SchemeUnavailable`]) at parse/dial/bind time — never a
//! stub that half-works at runtime.

use crate::transport::{Transport, TransportError};

/// The platform this build is running on, as used in refusal messages.
pub const fn platform_name() -> &'static str {
    #[cfg(not(target_arch = "wasm32"))]
    {
        "native"
    }
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    {
        "wasm32-wasip2"
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        "browser"
    }
}

/// The connection-oriented schemes this crate owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Scheme {
    /// Plain TCP byte streams.
    Tcp,
    /// WebSocket, dial side.
    Wssc,
    /// WebSocket, listen side.
    Wssd,
    /// WebRTC data channels (the browser's only peer-to-peer path).
    Webrtc,
    /// SSH: authenticated, multiplexed channels (PTY and named subsystems).
    Ssh,
}

impl Scheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            Scheme::Tcp => "tcp",
            Scheme::Wssc => "wssc",
            Scheme::Wssd => "wssd",
            Scheme::Webrtc => "webrtc",
            Scheme::Ssh => "ssh",
        }
    }

    /// Parse a scheme name. `ws`/`wss` are accepted as aliases for `wssc`.
    pub fn parse(s: &str) -> Result<Self, TransportError> {
        match s {
            "tcp" => Ok(Scheme::Tcp),
            "wssc" | "ws" | "wss" => Ok(Scheme::Wssc),
            "wssd" => Ok(Scheme::Wssd),
            "webrtc" => Ok(Scheme::Webrtc),
            "ssh" => Ok(Scheme::Ssh),
            other => Err(TransportError::Protocol(format!(
                "unknown scheme '{other}' (known: tcp, wssc, wssd, webrtc, ssh)"
            ))),
        }
    }

    /// What this scheme can do on the current platform.
    pub const fn support(self) -> SchemeSupport {
        #[cfg(not(target_arch = "wasm32"))]
        {
            const DIAL_SIDE: &str = "wssd is the listen-side scheme; dial with wssc";
            const LISTEN_SIDE: &str = "wssc is the dial-side scheme; listen with wssd";
            const PEER_SCHEME: &str = "webrtc is peer-to-peer; both sides dial through signaling";
            match self {
                Scheme::Tcp => SchemeSupport::both(),
                Scheme::Wssc => SchemeSupport::dial_only(LISTEN_SIDE),
                Scheme::Wssd => SchemeSupport::listen_only(DIAL_SIDE),
                Scheme::Webrtc => SchemeSupport::dial_only(PEER_SCHEME),
                Scheme::Ssh => SchemeSupport::both(),
            }
        }
        #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
        {
            const DIAL_SIDE: &str = "wssd is the listen-side scheme; dial with wssc";
            const LISTEN_SIDE: &str = "wssc is the dial-side scheme; listen with wssd";
            const LATER: &str = "not yet implemented on wasm32-wasip2";
            match self {
                Scheme::Tcp => SchemeSupport::both(),
                Scheme::Wssc => SchemeSupport::dial_only(LISTEN_SIDE),
                Scheme::Wssd => SchemeSupport::listen_only(DIAL_SIDE),
                Scheme::Webrtc => SchemeSupport::none(LATER, LATER),
                Scheme::Ssh => SchemeSupport::none(LATER, LATER),
            }
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            const NO_BROWSER_SOCKETS: &str = "the browser sandbox has no raw sockets";
            const NO_BROWSER_LISTEN: &str = "browsers cannot accept incoming connections";
            const DIAL_SIDE: &str = "wssd is the listen-side scheme; dial with wssc";
            match self {
                Scheme::Tcp => SchemeSupport::none(NO_BROWSER_SOCKETS, NO_BROWSER_LISTEN),
                Scheme::Wssc => SchemeSupport::dial_only(NO_BROWSER_LISTEN),
                Scheme::Wssd => SchemeSupport::none(DIAL_SIDE, NO_BROWSER_LISTEN),
                Scheme::Webrtc => SchemeSupport::dial_only(NO_BROWSER_LISTEN),
                Scheme::Ssh => SchemeSupport::none(NO_BROWSER_SOCKETS, NO_BROWSER_LISTEN),
            }
        }
    }

    /// Typed refusal unless this scheme can dial on the current platform.
    pub fn require_dial(self) -> Result<(), TransportError> {
        match self.support().dial {
            Availability::Available => Ok(()),
            Availability::Unavailable { reason } => Err(TransportError::SchemeUnavailable {
                scheme: self.as_str(),
                platform: platform_name(),
                operation: "dial",
                reason,
            }),
        }
    }

    /// Typed refusal unless this scheme can listen on the current platform.
    pub fn require_listen(self) -> Result<(), TransportError> {
        match self.support().listen {
            Availability::Available => Ok(()),
            Availability::Unavailable { reason } => Err(TransportError::SchemeUnavailable {
                scheme: self.as_str(),
                platform: platform_name(),
                operation: "listen",
                reason,
            }),
        }
    }
}

impl std::fmt::Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether an operation is available on this platform, and if not, why —
/// named at configuration time, never discovered as a broken stub later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    Available,
    Unavailable { reason: &'static str },
}

impl Availability {
    pub const fn is_available(self) -> bool {
        matches!(self, Availability::Available)
    }
}

/// A scheme's dial/listen availability on the current platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemeSupport {
    pub dial: Availability,
    pub listen: Availability,
}

// Which constructors are used depends on the target's arm of the support
// table, so each platform sees a different subset as "dead".
#[allow(dead_code)]
impl SchemeSupport {
    const fn both() -> Self {
        Self {
            dial: Availability::Available,
            listen: Availability::Available,
        }
    }
    const fn dial_only(listen_reason: &'static str) -> Self {
        Self {
            dial: Availability::Available,
            listen: Availability::Unavailable {
                reason: listen_reason,
            },
        }
    }
    const fn listen_only(dial_reason: &'static str) -> Self {
        Self {
            dial: Availability::Unavailable {
                reason: dial_reason,
            },
            listen: Availability::Available,
        }
    }
    const fn none(dial_reason: &'static str, listen_reason: &'static str) -> Self {
        Self {
            dial: Availability::Unavailable {
                reason: dial_reason,
            },
            listen: Availability::Unavailable {
                reason: listen_reason,
            },
        }
    }
}

/// A parsed `scheme://authority[/path]` endpoint reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    pub scheme: Scheme,
    /// `host:port` (or whatever the scheme's rendezvous name is, for webrtc).
    pub authority: String,
    /// Path portion including the leading `/`, when present.
    pub path: Option<String>,
    /// Whether the endpoint asked for TLS (`wss://`).
    ///
    /// Recorded rather than discarded: dropping it would turn `wss://` into
    /// a plaintext connection, which is the one failure mode a transport
    /// must never have. TLS is not implemented yet, so dialing a secure
    /// endpoint is refused — see `docs/tls.md`.
    pub secure: bool,
}

impl Endpoint {
    /// Parse `scheme://authority[/path]`.
    pub fn parse(s: &str) -> Result<Self, TransportError> {
        let (scheme_str, rest) = s.split_once("://").ok_or_else(|| {
            TransportError::Protocol(format!(
                "endpoint '{s}' is not of the form scheme://authority[/path]"
            ))
        })?;
        let scheme = Scheme::parse(scheme_str)?;
        let secure = matches!(scheme_str, "wss" | "wsss");
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], Some(rest[i..].to_string())),
            None => (rest, None),
        };
        if authority.is_empty() {
            return Err(TransportError::Protocol(format!(
                "endpoint '{s}' has an empty authority"
            )));
        }
        Ok(Endpoint {
            scheme,
            authority: authority.to_string(),
            path,
            secure,
        })
    }

    /// Dial this endpoint on the current platform, for schemes that need no
    /// scheme-specific configuration (tcp, wssc).
    ///
    /// Schemes that require configuration to dial — credentials for `ssh`,
    /// signaling for `webrtc` — are refused here with a typed error naming
    /// the API to use instead; they cannot be dialed from a bare address.
    pub async fn dial(&self) -> Result<Box<dyn Transport>, TransportError> {
        self.scheme.require_dial()?;
        // TLS is not implemented yet (docs/tls.md). Refuse by name rather
        // than quietly dialing `ws://` instead: a scheme that promises
        // encryption must never hand back a plaintext connection.
        if self.secure {
            return Err(TransportError::SchemeUnavailable {
                scheme: "wss",
                platform: platform_name(),
                operation: "dial",
                reason: "TLS is not implemented yet; see docs/tls.md. \
                         Use ws:// explicitly if plaintext is acceptable",
            });
        }
        match self.scheme {
            Scheme::Tcp => crate::transport::connect(&self.authority).await,
            Scheme::Wssc => {
                let url = format!(
                    "{}://{}{}",
                    if self.secure { "wss" } else { "ws" },
                    self.authority,
                    self.path.as_deref().unwrap_or("")
                );
                crate::transport::connect(&url).await
            }
            Scheme::Ssh => Err(TransportError::SchemeNeedsConfig {
                scheme: "ssh",
                detail: "ssh dialing needs credentials and host verification; use the ssh client API",
            }),
            Scheme::Webrtc => Err(TransportError::SchemeNeedsConfig {
                scheme: "webrtc",
                detail: "webrtc dialing needs a signaling channel; use the p2p connect API",
            }),
            Scheme::Wssd => unreachable!("require_dial refuses listen-side schemes"),
        }
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scheme = if self.secure && self.scheme == Scheme::Wssc {
            "wss"
        } else {
            self.scheme.as_str()
        };
        write!(
            f,
            "{}://{}{}",
            scheme,
            self.authority,
            self.path.as_deref().unwrap_or("")
        )
    }
}
