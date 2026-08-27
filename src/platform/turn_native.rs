//! The `turn` relay: the last rung of the NAT-traversal ladder.
//!
//! [`crate::stun`] answers "what address can a peer aim at?" and, when the
//! NAT hands out a fresh mapping per destination, answers it with "none —
//! punching cannot work here". This module is what happens next: a TURN
//! server relays traffic for peers that could not reach each other directly.
//!
//! Running one matters most for browsers. A browser peer's ICE stack will
//! use a TURN server if it is given one and will otherwise simply fail behind
//! a symmetric NAT; the signaling-hub relay this crate already has rescues
//! only its own WASI path, not WebRTC. With a TURN server of our own the
//! whole ladder is self-hosted: discovery through [`crate::stun`], a direct
//! hole-punched path when the mapping allows it, and this relay when it does
//! not — no third-party service anywhere in the chain.
//!
//! The protocol comes from the maintained [`turn`] implementation rather than
//! from scratch. TURN carries HMAC authentication, allocation lifecycles,
//! permissions and channel framing — stateful machinery where an
//! implementation bug means an open relay. That is the same reasoning that
//! put the `ssh` scheme on russh, and the opposite of the STUN binding codec,
//! which is a header, a walk and an XOR.
//!
//! What stays with the consumer is policy. This module will not run an
//! unauthenticated relay — [`TurnServer::bind`] refuses one with a typed
//! error — but *who* gets credentials is not its business:
//! [`TurnCredentials::Verifier`] hands that decision back, exactly as the SSH
//! server hands back what an authenticated principal may do. Relay selection,
//! failover and credential issuance policy stay outside as well.
//!
//! Buffering is bounded, as everywhere else in this crate: allocations are
//! capped by [`TurnServerConfig::max_allocations`], a request past the cap is
//! refused rather than queued, and the refusal is visible in
//! [`TurnMetrics`].

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use thiserror::Error;
use tokio::net::UdpSocket;
use turn::auth::{AuthHandler, LongTermAuthHandler, generate_auth_key};
use turn::relay::RelayAddressGenerator;
use turn::relay::relay_static::RelayAddressGeneratorStatic;
use turn::server::Server;
use turn::server::config::{ConnConfig, ServerConfig};
use webrtc_util::Conn;
use webrtc_util::vnet::net::Net;

/// Typed TURN failures.
#[derive(Debug, Error)]
pub enum TurnError {
    /// A relay with no way to authenticate anyone is an open relay, so
    /// binding one is refused outright rather than left to configuration.
    #[error(
        "refusing to bind a TURN server with no credentials: an unauthenticated relay is an open relay"
    )]
    NoCredentials,

    /// The allocation cap was reached; the request was refused, not queued.
    #[error("TURN allocation refused: the server is at its cap of {max} allocations")]
    QuotaExceeded { max: usize },

    /// Protocol or server-side failure from the underlying implementation.
    #[error("TURN server error: {0}")]
    Protocol(String),

    /// Socket-level failure.
    #[error("TURN I/O error: {0}")]
    Io(String),
}

fn turn_err(e: turn::Error) -> TurnError {
    TurnError::Protocol(e.to_string())
}

fn io_err(e: std::io::Error) -> TurnError {
    TurnError::Io(e.to_string())
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// A consumer-supplied admission check: given the offered username, the realm
/// and the client's source address, return that user's password to admit them
/// or `None` to refuse.
pub type CredentialVerifier = Arc<dyn Fn(&str, &str, SocketAddr) -> Option<String> + Send + Sync>;

/// How the server decides whether a client may allocate a relay.
///
/// There is deliberately no "open" variant: every path here requires the
/// client to prove knowledge of a password, because relay capacity that
/// anyone may claim is bandwidth theft waiting to happen.
#[derive(Clone)]
pub enum TurnCredentials {
    /// Fixed username/password pairs. Suitable for a closed deployment; an
    /// empty set is refused at bind time.
    Static(HashMap<String, String>),

    /// Time-limited credentials derived from a shared secret — the scheme
    /// coturn calls "REST". The username is an expiry timestamp and the
    /// password is its HMAC, so a coordinator holding the secret can mint
    /// short-lived grants (see [`ephemeral_credentials`]) without the server
    /// keeping any per-user state.
    Ephemeral { shared_secret: String },

    /// The consumer decides — see [`CredentialVerifier`]. This is where an
    /// identity or capability model of the consumer's own plugs in; the
    /// server never learns what it means.
    Verifier(CredentialVerifier),
}

impl TurnCredentials {
    /// Convenience for the common static case.
    pub fn static_user(username: &str, password: &str) -> Self {
        let mut map = HashMap::new();
        map.insert(username.to_string(), password.to_string());
        TurnCredentials::Static(map)
    }
}

// Hand-written so secrets never reach a log through a derived `Debug`.
impl std::fmt::Debug for TurnCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnCredentials::Static(map) => write!(f, "Static({} user(s))", map.len()),
            TurnCredentials::Ephemeral { .. } => {
                f.write_str("Ephemeral { shared_secret: <redacted> }")
            }
            TurnCredentials::Verifier(_) => f.write_str("Verifier(..)"),
        }
    }
}

/// Mint a time-limited username/password pair valid for `ttl`, to hand to a
/// peer alongside the relay's URL (see
/// [`IceServerConfig::turn`](crate::transport::rtc_signaling::IceServerConfig::turn)).
///
/// The pair verifies against a server configured with
/// [`TurnCredentials::Ephemeral`] holding the same secret. Nothing is stored
/// on either side: the username *is* the expiry.
pub fn ephemeral_credentials(
    shared_secret: &str,
    ttl: Duration,
) -> Result<(String, String), TurnError> {
    turn::auth::generate_long_term_credentials(shared_secret, ttl).map_err(turn_err)
}

/// Resolved form of [`TurnCredentials`], built once at bind time.
enum ResolvedAuth {
    Static(HashMap<String, String>),
    Ephemeral(LongTermAuthHandler),
    Verifier(CredentialVerifier),
}

struct CredentialAuth {
    auth: ResolvedAuth,
    metrics: Arc<TurnMetrics>,
}

impl AuthHandler for CredentialAuth {
    fn auth_handle(
        &self,
        username: &str,
        realm: &str,
        src_addr: SocketAddr,
    ) -> Result<Vec<u8>, turn::Error> {
        let password = match &self.auth {
            ResolvedAuth::Static(map) => map.get(username).cloned(),
            // Expiry and HMAC checking stay in the maintained implementation.
            ResolvedAuth::Ephemeral(handler) => {
                let key = handler.auth_handle(username, realm, src_addr);
                return match key {
                    Ok(key) => {
                        self.metrics.auth_ok.fetch_add(1, Ordering::Relaxed);
                        Ok(key)
                    }
                    Err(e) => {
                        self.metrics.auth_refused.fetch_add(1, Ordering::Relaxed);
                        Err(e)
                    }
                };
            }
            ResolvedAuth::Verifier(verify) => verify(username, realm, src_addr),
        };

        match password {
            Some(password) => {
                self.metrics.auth_ok.fetch_add(1, Ordering::Relaxed);
                Ok(generate_auth_key(username, realm, &password))
            }
            None => {
                self.metrics.auth_refused.fetch_add(1, Ordering::Relaxed);
                Err(turn::Error::ErrNoSuchUser)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Quota
// ---------------------------------------------------------------------------

/// Wraps the relay-address generator so the allocation cap is enforced at the
/// one moment an allocation is actually created.
///
/// This is the right seam for the cap: refusing during authentication would
/// also refuse the refreshes that keep *existing* allocations alive, so a
/// full server would tear down the sessions it was already carrying.
struct QuotaRelayGenerator {
    inner: RelayAddressGeneratorStatic,
    max_allocations: usize,
    metrics: Arc<TurnMetrics>,
}

#[async_trait::async_trait]
impl RelayAddressGenerator for QuotaRelayGenerator {
    fn validate(&self) -> Result<(), turn::Error> {
        self.inner.validate()
    }

    async fn allocate_conn(
        &self,
        use_ipv4: bool,
        requested_port: u16,
    ) -> Result<(Arc<dyn Conn + Send + Sync>, SocketAddr), turn::Error> {
        // Claim a slot before allocating, so concurrent requests cannot both
        // squeeze past the cap.
        let mut live = self.metrics.live_allocations.load(Ordering::Acquire);
        loop {
            if live >= self.max_allocations {
                self.metrics
                    .allocations_refused
                    .fetch_add(1, Ordering::Relaxed);
                log::warn!(
                    "[turn] allocation refused: at cap of {} allocations",
                    self.max_allocations
                );
                return Err(turn::Error::Other(format!(
                    "allocation refused: server is at its cap of {} allocations",
                    self.max_allocations
                )));
            }
            match self.metrics.live_allocations.compare_exchange_weak(
                live,
                live + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => live = actual,
            }
        }

        match self.inner.allocate_conn(use_ipv4, requested_port).await {
            Ok(allocated) => {
                self.metrics
                    .allocations_granted
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.touch();
                Ok(allocated)
            }
            Err(e) => {
                // Hand the slot back: nothing was allocated.
                self.metrics.live_allocations.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters for a running [`TurnServer`], pollable at any time — the same
/// "expose data, not policy" surface as [`crate::flow::ConnectionMetrics`].
#[derive(Debug, Default)]
pub struct TurnMetrics {
    live_allocations: AtomicUsize,
    allocations_granted: AtomicU64,
    allocations_refused: AtomicU64,
    allocations_closed: AtomicU64,
    relayed_bytes_closed: AtomicU64,
    auth_ok: AtomicU64,
    auth_refused: AtomicU64,
    last_activity_ms: AtomicU64,
    max_allocations: AtomicUsize,
}

impl TurnMetrics {
    fn touch(&self) {
        self.last_activity_ms
            .store(crate::flow::now_millis(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TurnSnapshot {
        TurnSnapshot {
            live_allocations: self.live_allocations.load(Ordering::Relaxed),
            max_allocations: self.max_allocations.load(Ordering::Relaxed),
            allocations_granted: self.allocations_granted.load(Ordering::Relaxed),
            allocations_refused: self.allocations_refused.load(Ordering::Relaxed),
            allocations_closed: self.allocations_closed.load(Ordering::Relaxed),
            relayed_bytes_closed: self.relayed_bytes_closed.load(Ordering::Relaxed),
            auth_ok: self.auth_ok.load(Ordering::Relaxed),
            auth_refused: self.auth_refused.load(Ordering::Relaxed),
            last_activity_ms: self.last_activity_ms.load(Ordering::Relaxed),
        }
    }
}

/// The pollable view of [`TurnMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnSnapshot {
    /// Allocations currently held open.
    pub live_allocations: usize,
    /// The configured cap.
    pub max_allocations: usize,
    pub allocations_granted: u64,
    /// Allocation requests refused because the cap was reached.
    pub allocations_refused: u64,
    pub allocations_closed: u64,
    /// Bytes relayed by allocations that have since closed. For traffic on
    /// *live* allocations, see [`TurnServer::allocations`].
    pub relayed_bytes_closed: u64,
    pub auth_ok: u64,
    pub auth_refused: u64,
    /// [`crate::flow::now_millis`] stamp of the last allocation activity.
    pub last_activity_ms: u64,
}

impl TurnSnapshot {
    /// How full the relay is, in `[0, 1]` — the same saturation shape the
    /// rest of the crate reports.
    pub fn saturation(&self) -> f64 {
        if self.max_allocations == 0 {
            0.0
        } else {
            self.live_allocations as f64 / self.max_allocations as f64
        }
    }
}

/// One live allocation, as reported by [`TurnServer::allocations`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationSnapshot {
    /// The username that authenticated this allocation — the principal the
    /// consumer's own model can map back to whoever it represents.
    pub username: String,
    /// The relay address peers send to.
    pub relay_addr: SocketAddr,
    /// The client that holds the allocation.
    pub client_addr: SocketAddr,
    /// Bytes relayed so far.
    pub relayed_bytes: u64,
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Configuration for a [`TurnServer`].
#[derive(Debug, Clone)]
pub struct TurnServerConfig {
    /// Address to listen on, e.g. `"0.0.0.0:3478"`.
    pub listen_addr: String,
    /// The address peers will be told to send to. This must be the address
    /// clients can actually reach — on a host behind a NAT, its public IP,
    /// not the bound one.
    pub relay_address: IpAddr,
    /// Local address the relay sockets themselves bind to.
    pub relay_bind_ip: String,
    /// The authentication realm, echoed to clients.
    pub realm: String,
    pub credentials: TurnCredentials,
    /// Hard cap on simultaneous allocations. Requests past it are refused,
    /// never queued — this crate does not buffer without bound, and a relay
    /// is a buffering machine.
    pub max_allocations: usize,
    /// Lifetime of a channel binding.
    pub channel_bind_timeout: Duration,
}

impl TurnServerConfig {
    /// A config with the given credentials and sane defaults, listening on
    /// all interfaces and relaying via `relay_address`.
    pub fn new(relay_address: IpAddr, credentials: TurnCredentials) -> Self {
        Self {
            listen_addr: "0.0.0.0:3478".to_string(),
            relay_address,
            relay_bind_ip: "0.0.0.0".to_string(),
            realm: "ego".to_string(),
            credentials,
            max_allocations: 256,
            channel_bind_timeout: Duration::from_secs(600),
        }
    }
}

/// A TURN relay server.
///
/// Peers that cannot punch a direct path allocate a relay here and reach each
/// other through it. Handing the relay's URL and a credential pair to a peer
/// is all its ICE stack needs — see
/// [`IceServerConfig::turn`](crate::transport::rtc_signaling::IceServerConfig::turn).
pub struct TurnServer {
    inner: Server,
    local_addr: SocketAddr,
    relay_address: IpAddr,
    realm: String,
    metrics: Arc<TurnMetrics>,
}

impl TurnServer {
    /// Bind and start the relay.
    ///
    /// Refuses with [`TurnError::NoCredentials`] if the configuration admits
    /// nobody — an open relay is not something this will start by accident.
    pub async fn bind(config: TurnServerConfig) -> Result<Self, TurnError> {
        let auth = match &config.credentials {
            TurnCredentials::Static(map) if map.is_empty() => {
                return Err(TurnError::NoCredentials);
            }
            TurnCredentials::Ephemeral { shared_secret } if shared_secret.is_empty() => {
                return Err(TurnError::NoCredentials);
            }
            TurnCredentials::Static(map) => ResolvedAuth::Static(map.clone()),
            TurnCredentials::Ephemeral { shared_secret } => {
                ResolvedAuth::Ephemeral(LongTermAuthHandler::new(shared_secret.clone()))
            }
            TurnCredentials::Verifier(verify) => ResolvedAuth::Verifier(verify.clone()),
        };

        let metrics = Arc::new(TurnMetrics::default());
        metrics
            .max_allocations
            .store(config.max_allocations, Ordering::Relaxed);

        let socket = UdpSocket::bind(&config.listen_addr).await.map_err(io_err)?;
        let local_addr = socket.local_addr().map_err(io_err)?;

        // Allocation closes arrive here, releasing quota slots and carrying
        // the final byte count for each allocation.
        let (close_tx, mut close_rx) = tokio::sync::mpsc::channel(64);
        let close_metrics = metrics.clone();
        tokio::spawn(async move {
            while let Some(info) = close_rx.recv().await {
                let info: turn::allocation::AllocationInfo = info;
                close_metrics
                    .live_allocations
                    .fetch_sub(1, Ordering::AcqRel);
                close_metrics
                    .allocations_closed
                    .fetch_add(1, Ordering::Relaxed);
                close_metrics
                    .relayed_bytes_closed
                    .fetch_add(info.relayed_bytes as u64, Ordering::Relaxed);
                close_metrics.touch();
            }
        });

        let relay_generator = QuotaRelayGenerator {
            inner: RelayAddressGeneratorStatic {
                relay_address: config.relay_address,
                address: config.relay_bind_ip.clone(),
                net: Arc::new(Net::new(None)),
            },
            max_allocations: config.max_allocations,
            metrics: metrics.clone(),
        };

        let inner = Server::new(ServerConfig {
            conn_configs: vec![ConnConfig {
                conn: Arc::new(socket),
                relay_addr_generator: Box::new(relay_generator),
            }],
            realm: config.realm.clone(),
            auth_handler: Arc::new(CredentialAuth {
                auth,
                metrics: metrics.clone(),
            }),
            channel_bind_timeout: config.channel_bind_timeout,
            alloc_close_notify: Some(close_tx),
        })
        .await
        .map_err(turn_err)?;

        log::info!(
            "[turn] relay listening on {local_addr}, handing out {}, cap {} allocations",
            config.relay_address,
            config.max_allocations
        );

        Ok(Self {
            inner,
            local_addr,
            relay_address: config.relay_address,
            realm: config.realm,
            metrics,
        })
    }

    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The address handed to clients as their relay.
    pub fn relay_address(&self) -> IpAddr {
        self.relay_address
    }

    /// The authentication realm clients must use.
    pub fn realm(&self) -> &str {
        &self.realm
    }

    /// A `turn:` URL for this server, ready for
    /// [`IceServerConfig::turn`](crate::transport::rtc_signaling::IceServerConfig::turn).
    pub fn turn_url(&self) -> String {
        format!("turn:{}:{}", self.relay_address, self.local_addr.port())
    }

    /// The shared metrics handle. Clone it out to wherever health is polled.
    pub fn metrics(&self) -> Arc<TurnMetrics> {
        self.metrics.clone()
    }

    /// Every allocation currently open, with its principal and traffic so far.
    pub async fn allocations(&self) -> Result<Vec<AllocationSnapshot>, TurnError> {
        let info = self
            .inner
            .get_allocations_info(None)
            .await
            .map_err(turn_err)?;
        Ok(info
            .into_iter()
            .map(|(five_tuple, info)| AllocationSnapshot {
                username: info.username,
                relay_addr: info.relay_addr,
                client_addr: five_tuple.src_addr,
                relayed_bytes: info.relayed_bytes as u64,
            })
            .collect())
    }

    /// Drop every allocation held by `username`.
    ///
    /// The mechanism for revocation; deciding *when* to revoke belongs to
    /// whoever issued the credential.
    pub async fn revoke(&self, username: &str) -> Result<(), TurnError> {
        self.inner
            .delete_allocations_by_username(username.to_string())
            .await
            .map_err(turn_err)
    }

    /// Stop the server and release its allocations.
    pub async fn close(&self) -> Result<(), TurnError> {
        self.inner.close().await.map_err(turn_err)
    }
}
