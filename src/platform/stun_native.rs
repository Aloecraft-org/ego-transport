//! Native UDP side of the `stun` module: the binding probe and the binding
//! server.
//!
//! The codec lives in [`crate::stun`] and is platform-neutral; everything here
//! needs a real UDP socket, so it is native-only. Consumers should call
//! [`crate::stun::probe`] and friends rather than reaching in here — those
//! entry points dispatch to this module on native and produce a typed refusal
//! elsewhere.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::stun::{
    MappingReport, NatMapping, ProbeConfig, StunError, StunMessage, StunProbe, TransactionId,
    decode, encode_binding_request, encode_binding_success, normalize_server,
};

/// STUN messages are small; anything larger than a typical MTU is not one.
const MAX_DATAGRAM: usize = 1500;

fn io_err(e: std::io::Error) -> StunError {
    StunError::Io(e.to_string())
}

/// Resolve a server address, accepting `host:port`, a `stun:`/`stuns:` URL, or
/// a bare host.
async fn resolve(server: &str) -> Result<SocketAddr, StunError> {
    let normalized = normalize_server(server);
    tokio::net::lookup_host(&normalized)
        .await
        .map_err(|_| StunError::Resolve(server.to_string()))?
        .next()
        .ok_or_else(|| StunError::Resolve(server.to_string()))
}

/// Probe one server from a socket bound per `config`.
pub async fn probe_with(server: &str, config: &ProbeConfig) -> Result<StunProbe, StunError> {
    let socket = UdpSocket::bind(config.bind_addr).await.map_err(io_err)?;
    probe_from(&socket, server, config).await
}

/// Probe one server using a socket the caller owns.
///
/// This is the form hole punching wants: a reflexive address is a property of
/// *one socket*, so the socket that learns its mapping must be the same socket
/// that later sends to the peer. Binding a second socket would learn a mapping
/// that no longer applies.
pub async fn probe_from(
    socket: &UdpSocket,
    server: &str,
    config: &ProbeConfig,
) -> Result<StunProbe, StunError> {
    let server_addr = resolve(server).await?;
    let local = socket.local_addr().map_err(io_err)?;

    let txid = TransactionId::random();
    let request = encode_binding_request(&txid);
    let attempts = config.attempts.max(1);
    let mut wait = config.initial_timeout;
    let mut buf = [0u8; MAX_DATAGRAM];

    for _ in 0..attempts {
        socket
            .send_to(&request, server_addr)
            .await
            .map_err(io_err)?;
        let sent = Instant::now();
        let deadline = sent + wait;

        // Keep reading until this attempt's deadline: a datagram that is not
        // our answer (stray traffic, a late reply to an earlier attempt) must
        // not consume the attempt.
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (n, from) = match timeout(remaining, socket.recv_from(&mut buf)).await {
                Err(_) => break,
                Ok(Ok(x)) => x,
                Ok(Err(e)) => return Err(io_err(e)),
            };
            // Only the server we asked can answer us.
            if from != server_addr {
                continue;
            }
            match decode(&buf[..n]) {
                Ok(StunMessage::BindingSuccess { txid: got, mapped }) if got == txid => {
                    return Ok(StunProbe {
                        server: server_addr,
                        local,
                        reflexive: mapped,
                        rtt_ms: sent.elapsed().as_millis() as u64,
                    });
                }
                Ok(StunMessage::BindingError {
                    txid: got,
                    code,
                    reason,
                }) if got == txid => {
                    return Err(StunError::ServerError { code, reason });
                }
                // Someone else's transaction, or not a STUN message at all.
                _ => continue,
            }
        }
        wait *= 2;
    }

    Err(StunError::Timeout {
        server: server.to_string(),
        attempts,
    })
}

/// Probe several servers from one socket and classify the NAT mapping.
pub async fn detect_mapping(
    servers: &[&str],
    config: &ProbeConfig,
) -> Result<MappingReport, StunError> {
    if servers.len() < 2 {
        return Err(StunError::NotEnoughServers(servers.len()));
    }
    let socket = UdpSocket::bind(config.bind_addr).await.map_err(io_err)?;
    let local = socket.local_addr().map_err(io_err)?;

    let mut probes = Vec::with_capacity(servers.len());
    for server in servers {
        probes.push(probe_from(&socket, server, config).await?);
    }

    let first = probes[0].reflexive;
    let agree = probes.iter().all(|p| p.reflexive == first);
    let mapping = if !agree {
        NatMapping::EndpointDependent
    } else if first == local {
        NatMapping::Open
    } else {
        // A wildcard bind cannot be compared against a reflexive address, so
        // an unNATted socket bound to 0.0.0.0 lands here rather than in
        // `Open`. Both are punchable, so the distinction is descriptive only.
        NatMapping::EndpointIndependent
    };

    Ok(MappingReport {
        mapping,
        local,
        probes,
    })
}

// ---------------------------------------------------------------------------
// Binding server
// ---------------------------------------------------------------------------

/// Counters for a running [`StunServer`], pollable at any time.
#[derive(Debug, Default)]
pub struct StunServerMetrics {
    requests: AtomicU64,
    responses: AtomicU64,
    dropped: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    last_activity_ms: AtomicU64,
}

impl StunServerMetrics {
    pub fn snapshot(&self) -> StunServerSnapshot {
        StunServerSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            responses: self.responses.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            last_activity_ms: self.last_activity_ms.load(Ordering::Relaxed),
        }
    }

    fn touch(&self) {
        self.last_activity_ms
            .store(crate::flow::now_millis(), Ordering::Relaxed);
    }
}

/// The pollable view of [`StunServerMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StunServerSnapshot {
    /// Well-formed binding requests received.
    pub requests: u64,
    /// Success responses sent.
    pub responses: u64,
    /// Datagrams dropped without a reply because they were not binding
    /// requests.
    pub dropped: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// [`crate::flow::now_millis`] stamp of the last datagram handled.
    pub last_activity_ms: u64,
}

/// A STUN binding server: answers "what address did this datagram come from?"
///
/// Answering binding requests is stateless, so a node can run one alongside
/// whatever else it does and let peers discover their own reflexive addresses
/// without depending on a third-party STUN service.
///
/// Datagrams that are not well-formed binding requests are dropped in silence
/// rather than answered with an error — an unconditional reply would make the
/// server a reflector for spoofed traffic. Rate limiting, if a deployment
/// wants it, is the consumer's policy to apply.
pub struct StunServer {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    metrics: Arc<StunServerMetrics>,
}

impl StunServer {
    /// Bind a UDP socket for the server. Port 0 picks a free port, which
    /// [`local_addr`](Self::local_addr) then reports.
    pub async fn bind(addr: &str) -> Result<Self, StunError> {
        let socket = UdpSocket::bind(addr).await.map_err(io_err)?;
        let local_addr = socket.local_addr().map_err(io_err)?;
        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
            metrics: Arc::new(StunServerMetrics::default()),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The shared metrics handle. Clone it out to wherever health is polled.
    pub fn metrics(&self) -> Arc<StunServerMetrics> {
        self.metrics.clone()
    }

    /// Serve until the socket fails. Returns only on error.
    pub async fn run(&self) -> Result<(), StunError> {
        serve(self.socket.clone(), self.metrics.clone()).await
    }

    /// Serve in the background.
    pub fn spawn(&self) -> tokio::task::JoinHandle<()> {
        let socket = self.socket.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(socket, metrics).await {
                log::warn!("[stun] server stopped: {e}");
            }
        })
    }
}

async fn serve(socket: Arc<UdpSocket>, metrics: Arc<StunServerMetrics>) -> Result<(), StunError> {
    let mut buf = [0u8; MAX_DATAGRAM];
    loop {
        let (n, from) = socket.recv_from(&mut buf).await.map_err(io_err)?;
        metrics.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
        metrics.touch();

        match decode(&buf[..n]) {
            Ok(StunMessage::BindingRequest { txid }) => {
                metrics.requests.fetch_add(1, Ordering::Relaxed);
                let response = encode_binding_success(&txid, from);
                match socket.send_to(&response, from).await {
                    Ok(sent) => {
                        metrics.responses.fetch_add(1, Ordering::Relaxed);
                        metrics.bytes_out.fetch_add(sent as u64, Ordering::Relaxed);
                    }
                    Err(e) => log::debug!("[stun] could not answer {from}: {e}"),
                }
            }
            // Anything else — junk, a response, an unsupported method — is
            // dropped without a reply.
            _ => {
                metrics.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
