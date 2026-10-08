//! Native side of the `stun` module: the binding probe over UDP and TCP,
//! mapping and filtering detection, and the binding server.
//!
//! The codec lives in [`crate::stun`] and is platform-neutral; everything here
//! needs a real socket, so it is native-only. Consumers should call
//! [`crate::stun::probe`] and friends rather than reaching in here — those
//! entry points dispatch to this module on native and produce a typed refusal
//! elsewhere.
//!
//! Entry points: [`probe_with`], [`probe_from`], [`probe_tcp`],
//! [`detect_mapping`], [`detect_filtering`], and [`StunServer`]
//! ([`StunServer::bind`], [`StunServer::bind_with`], [`StunServer::run`],
//! [`StunServer::spawn`], [`StunServer::answer_for`]).
//!
//! How the server answers a UDP binding request, by its CHANGE-REQUEST
//! (RFC 5780), is decided in one place, `route_change`:
//!
//! | CHANGE-REQUEST        | Answered by                                     |
//! |-----------------------|-------------------------------------------------|
//! | absent or no flags    | the socket it arrived on                        |
//! | port only             | the other local socket, if an alternate port is bound |
//! | IP (with or without port) | the [`ChangeHandler`], if one is set; it forwards to the peer server, which calls [`StunServer::answer_for`] |
//! | anything else         | a 420 from the socket it arrived on             |
//!
//! Over TCP a CHANGE-REQUEST with any flag set is always a 420: a TCP
//! answer can only come back on the connection it was asked on.

use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use crate::stun::{
    ATTR_CHANGE_REQUEST, ChangeRequest, ERROR_BAD_REQUEST, ERROR_UNKNOWN_ATTRIBUTE,
    FilteringReport, HEADER_LEN, MappingReport, NatFiltering, NatMapping, ProbeConfig,
    ResponseAttributes, Rfc5780Attributes, StunError, StunMessage, StunProbe, TransactionId,
    decode, decode_rfc5780, encode_binding_error, encode_binding_request_with,
    encode_binding_success_with, message_len, normalize_server,
};

/// STUN messages are small; anything larger than a typical MTU is not one.
const MAX_DATAGRAM: usize = 1500;
/// The largest message read off a TCP connection. A binding message is never
/// this long, so a header claiming more closes the connection.
const MAX_TCP_MESSAGE: usize = MAX_DATAGRAM;
/// How long a TCP client may stay silent before the server closes its
/// connection. A binding exchange takes one round trip; this only bounds
/// clients that connect and say nothing.
const TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Open TCP connections the server holds at once. Connections past this are
/// accepted and closed straight away, and counted as dropped.
const MAX_TCP_CONNECTIONS: usize = 256;
/// Pause after a failed accept, so a persistent failure (out of file
/// descriptors) does not spin.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);
/// Tries at finding a port free for both UDP and TCP when the server binds
/// port 0.
const SHARED_PORT_ATTEMPTS: usize = 16;

fn io_err(e: std::io::Error) -> StunError {
    StunError::Io(e.to_string())
}

// depth: address resolution and family matching

/// Resolve a server address, accepting `host:port`, a `stun:`/`stuns:` URL, or
/// a bare host. With `family` given, an address of the same family as it is
/// preferred: a socket can only send to its own family.
async fn resolve(server: &str, family: Option<SocketAddr>) -> Result<SocketAddr, StunError> {
    let normalized = normalize_server(server);
    let found: Vec<SocketAddr> = tokio::net::lookup_host(&normalized)
        .await
        .map_err(|_| StunError::Resolve(server.to_string()))?
        .collect();
    let same_family = family.and_then(|f| found.iter().find(|a| a.is_ipv6() == f.is_ipv6()));
    same_family
        .or(found.first())
        .copied()
        .ok_or_else(|| StunError::Resolve(server.to_string()))
}

/// The family `bind` pins down, if it pins one: a concrete address does, a
/// wildcard leaves the choice to the server's address.
fn pinned_family(bind: SocketAddr) -> Option<SocketAddr> {
    (!bind.ip().is_unspecified()).then_some(bind)
}

/// The address to bind for talking to `server`: `bind` itself, unless it is a
/// wildcard of the other family, in which case the wildcard of the server's
/// family on the same port. The default [`ProbeConfig`] binds `0.0.0.0`,
/// which could not reach an IPv6 server otherwise.
fn bind_for(bind: SocketAddr, server: SocketAddr) -> SocketAddr {
    if bind.ip().is_unspecified() && bind.is_ipv6() != server.is_ipv6() {
        let ip = if server.is_ipv6() {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        };
        SocketAddr::new(ip, bind.port())
    } else {
        bind
    }
}

/// A client address as the server reports it: an IPv4 client reaching a
/// dual-stack IPv6 socket arrives as `::ffff:a.b.c.d` and is reported as
/// `a.b.c.d`, which is what the client compares against.
fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

/// The form of `client` that a socket bound at `local` can send to: an IPv4
/// client becomes IPv4-mapped on an IPv6 socket.
fn reachable_from(local: SocketAddr, client: SocketAddr) -> SocketAddr {
    match (local, client.ip()) {
        (SocketAddr::V6(_), IpAddr::V4(v4)) => {
            SocketAddr::new(IpAddr::V6(v4.to_ipv6_mapped()), client.port())
        }
        _ => client,
    }
}

/// The wait budget of a probe over TCP, where nothing is retransmitted: the
/// sum of the UDP probe's doubling waits.
fn tcp_budget(config: &ProbeConfig) -> Duration {
    let attempts = config.attempts.clamp(1, 16) as u32;
    config.initial_timeout * ((1u32 << attempts) - 1)
}

// depth: UDP client

/// Probe one server from a socket bound per `config`.
pub async fn probe_with(server: &str, config: &ProbeConfig) -> Result<StunProbe, StunError> {
    let server_addr = resolve(server, pinned_family(config.bind_addr)).await?;
    let socket = UdpSocket::bind(bind_for(config.bind_addr, server_addr))
        .await
        .map_err(io_err)?;
    probe_resolved(&socket, server_addr, server, config).await
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
    let local = socket.local_addr().map_err(io_err)?;
    let server_addr = resolve(server, Some(local)).await?;
    probe_resolved(socket, server_addr, server, config).await
}

async fn probe_resolved(
    socket: &UdpSocket,
    server_addr: SocketAddr,
    server: &str,
    config: &ProbeConfig,
) -> Result<StunProbe, StunError> {
    let local = socket.local_addr().map_err(io_err)?;
    let answer = transact(socket, server_addr, server, ChangeRequest::NONE, config).await?;
    Ok(StunProbe {
        server: server_addr,
        local,
        reflexive: answer.mapped,
        rtt_ms: answer.rtt_ms,
    })
}

/// One binding success, with where it came from.
struct Answer {
    from: SocketAddr,
    mapped: SocketAddr,
    extra: Rfc5780Attributes,
    rtt_ms: u64,
}

/// One binding transaction over UDP, with RFC 5389 retransmission.
///
/// A plain request is answered only by the server it was sent to. A change
/// request is answered from somewhere else by design, so any source is
/// accepted; the 96-bit transaction id is still what authenticates the reply.
async fn transact(
    socket: &UdpSocket,
    server_addr: SocketAddr,
    server: &str,
    change: ChangeRequest,
    config: &ProbeConfig,
) -> Result<Answer, StunError> {
    let txid = TransactionId::random();
    let request = encode_binding_request_with(&txid, change);
    let any_source = !change.is_none();
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
            if !any_source && from != server_addr {
                continue;
            }
            match decode(&buf[..n]) {
                Ok(StunMessage::BindingSuccess { txid: got, mapped }) if got == txid => {
                    return Ok(Answer {
                        from,
                        mapped,
                        extra: decode_rfc5780(&buf[..n]).unwrap_or_default(),
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
    // The first server picks the family; the rest are resolved to match it.
    let first_addr = resolve(servers[0], pinned_family(config.bind_addr)).await?;
    let socket = UdpSocket::bind(bind_for(config.bind_addr, first_addr))
        .await
        .map_err(io_err)?;
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

/// Classify filtering against one RFC 5780 server. See
/// [`crate::stun::detect_filtering`].
pub async fn detect_filtering(
    server: &str,
    config: &ProbeConfig,
) -> Result<FilteringReport, StunError> {
    let server_addr = resolve(server, pinned_family(config.bind_addr)).await?;
    let socket = UdpSocket::bind(bind_for(config.bind_addr, server_addr))
        .await
        .map_err(io_err)?;
    let local = socket.local_addr().map_err(io_err)?;

    // Test I: the mapping, and where the other address is.
    let plain = transact(&socket, server_addr, server, ChangeRequest::NONE, config).await?;
    let other_address = plain
        .extra
        .other_address
        .ok_or(StunError::NoOtherAddress(server_addr))?;

    let mut report = FilteringReport {
        filtering: NatFiltering::EndpointIndependent,
        server: server_addr,
        local,
        mapped: plain.mapped,
        other_address,
        change_ip_reply: None,
        change_port_reply: None,
    };

    // Test II: an answer from an address this socket never sent to.
    match transact(
        &socket,
        server_addr,
        server,
        ChangeRequest::IP_AND_PORT,
        config,
    )
    .await
    {
        Ok(answer) if answer.from.ip() == server_addr.ip() => {
            return Err(StunError::ChangeIgnored { from: answer.from });
        }
        Ok(answer) => {
            report.change_ip_reply = Some(answer.from);
            return Ok(report);
        }
        Err(StunError::Timeout { .. }) => {}
        Err(e) => return Err(e),
    }

    // Test III: an answer from the same address, another port.
    report.filtering =
        match transact(&socket, server_addr, server, ChangeRequest::PORT, config).await {
            Ok(answer) if answer.from == server_addr => {
                return Err(StunError::ChangeIgnored { from: answer.from });
            }
            Ok(answer) => {
                report.change_port_reply = Some(answer.from);
                NatFiltering::AddressDependent
            }
            Err(StunError::Timeout { .. }) => NatFiltering::AddressAndPortDependent,
            Err(StunError::ServerError { code, .. }) if code == ERROR_UNKNOWN_ATTRIBUTE => {
                NatFiltering::AddressDependentOrStricter
            }
            Err(e) => return Err(e),
        };
    Ok(report)
}

// depth: TCP client and framing

/// Probe one server over TCP. See [`crate::stun::probe_tcp`].
pub async fn probe_tcp(server: &str, config: &ProbeConfig) -> Result<StunProbe, StunError> {
    let server_addr = resolve(server, pinned_family(config.bind_addr)).await?;
    let bind = bind_for(config.bind_addr, server_addr);
    let socket = if bind.is_ipv6() {
        TcpSocket::new_v6()
    } else {
        TcpSocket::new_v4()
    }
    .map_err(io_err)?;
    socket.bind(bind).map_err(io_err)?;

    let deadline = Instant::now() + tcp_budget(config);
    let timed_out = || StunError::Timeout {
        server: server.to_string(),
        attempts: 1,
    };
    let mut stream = timeout(
        deadline.saturating_duration_since(Instant::now()),
        socket.connect(server_addr),
    )
    .await
    .map_err(|_| timed_out())?
    .map_err(io_err)?;
    let local = stream.local_addr().map_err(io_err)?;

    let txid = TransactionId::random();
    let sent = Instant::now();
    stream
        .write_all(&encode_binding_request_with(&txid, ChangeRequest::NONE))
        .await
        .map_err(io_err)?;

    let mut buf = Vec::new();
    let read = timeout(
        deadline.saturating_duration_since(Instant::now()),
        read_message(&mut stream, &mut buf),
    )
    .await
    .map_err(|_| timed_out())??;
    let Some(n) = read else {
        return Err(StunError::Io(
            "the server closed the connection without answering".into(),
        ));
    };
    match decode(&buf[..n])? {
        StunMessage::BindingSuccess { txid: got, mapped } if got == txid => Ok(StunProbe {
            server: server_addr,
            local,
            reflexive: mapped,
            rtt_ms: sent.elapsed().as_millis() as u64,
        }),
        StunMessage::BindingError {
            txid: got,
            code,
            reason,
        } if got == txid => Err(StunError::ServerError { code, reason }),
        // Only one request is outstanding on this connection, so anything
        // else is the server misbehaving.
        _ => Err(StunError::TransactionMismatch),
    }
}

/// Read one STUN message off a stream into `buf`, framed by its own header
/// (RFC 5389 §7.2.2). `Ok(None)` when the stream ends.
async fn read_message<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
) -> Result<Option<usize>, StunError> {
    buf.resize(HEADER_LEN, 0);
    match stream.read_exact(&mut buf[..HEADER_LEN]).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(io_err(e)),
    }
    let len = message_len(&buf[..HEADER_LEN])?;
    if len > MAX_TCP_MESSAGE {
        return Err(StunError::Malformed(
            "longer than any binding message over TCP",
        ));
    }
    buf.resize(len, 0);
    stream
        .read_exact(&mut buf[HEADER_LEN..])
        .await
        .map_err(io_err)?;
    Ok(Some(len))
}

// ---------------------------------------------------------------------------
// Binding server
// ---------------------------------------------------------------------------

/// Counters for a running [`StunServer`], pollable at any time.
#[derive(Debug, Default)]
pub struct StunServerMetrics {
    requests: AtomicU64,
    responses: AtomicU64,
    errors: AtomicU64,
    change_requests: AtomicU64,
    tcp_connections: AtomicU64,
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
            errors: self.errors.load(Ordering::Relaxed),
            change_requests: self.change_requests.load(Ordering::Relaxed),
            tcp_connections: self.tcp_connections.load(Ordering::Relaxed),
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
    /// Well-formed binding requests received, over UDP and TCP.
    pub requests: u64,
    /// Success responses sent, including those sent by
    /// [`StunServer::answer_for`].
    pub responses: u64,
    /// Error responses sent (400 for a malformed CHANGE-REQUEST, 420 for one
    /// this server cannot carry out).
    pub errors: u64,
    /// Requests carrying a CHANGE-REQUEST with a flag set.
    pub change_requests: u64,
    /// TCP connections accepted and served.
    pub tcp_connections: u64,
    /// Datagrams or TCP messages dropped without a reply because they were
    /// not binding requests, and TCP connections closed at once because
    /// [`MAX_TCP_CONNECTIONS`] were already open.
    pub dropped: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// [`crate::flow::now_millis`] stamp of the last datagram handled.
    pub last_activity_ms: u64,
}

/// The RFC 5780 and TCP parts of a [`StunServer`]. The default is the plain
/// RFC 5389 UDP server that [`StunServer::bind`] gives.
#[derive(Clone, Default)]
pub struct StunServerOptions {
    /// Also serve STUN over TCP (RFC 5389 §7.2.2) on the same port as UDP.
    pub tcp: bool,
    /// Bind a second UDP socket on the same IP and this port (0 for any free
    /// port), which answers change-port requests. Without it a change-port
    /// request gets a 420.
    pub alternate_port: Option<u16>,
    /// The IP clients reach this server at, reported in RESPONSE-ORIGIN with
    /// the answering socket's port. Defaults to the bound IP when that is
    /// concrete. A server bound to a wildcard leaves RESPONSE-ORIGIN out
    /// unless this is set, since it cannot know which of its addresses a
    /// client used.
    pub public_ip: Option<IpAddr>,
    /// The other server of an RFC 5780 pair, advertised as OTHER-ADDRESS in
    /// every success response. Clients need it before they can test
    /// filtering.
    pub other_address: Option<SocketAddr>,
    /// Where change-IP requests go. Without one they get a 420.
    pub change_handler: Option<Arc<dyn ChangeHandler>>,
}

/// A change-IP request this server cannot answer itself: the answer must come
/// from the other server of the pair, which the handler asks to call
/// [`StunServer::answer_for`] with this event's fields.
///
/// The handler runs on the serving loop, so it must not block: hand the event
/// to a task. Whether to forward it at all (rate limits, authentication of
/// the link between the servers) is the handler's policy; one it drops simply
/// goes unanswered, which a client reads the same as a filtering NAT.
pub trait ChangeHandler: Send + Sync + 'static {
    fn change_requested(&self, event: ChangeRequestEvent);
}

impl<F> ChangeHandler for F
where
    F: Fn(ChangeRequestEvent) + Send + Sync + 'static,
{
    fn change_requested(&self, event: ChangeRequestEvent) {
        self(event)
    }
}

/// What the other server needs to answer a change request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeRequestEvent {
    pub txid: TransactionId,
    /// The client's address as this server observed it. The answer goes here
    /// and nowhere else: the client never chooses the address.
    pub client: SocketAddr,
    pub change: ChangeRequest,
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
    inner: Arc<Inner>,
}

struct Inner {
    primary: UdpSocket,
    local_addr: SocketAddr,
    alternate: Option<(UdpSocket, SocketAddr)>,
    tcp: Option<(TcpListener, SocketAddr)>,
    tcp_slots: Arc<Semaphore>,
    public_ip: Option<IpAddr>,
    other_address: Option<SocketAddr>,
    change_handler: Option<Arc<dyn ChangeHandler>>,
    metrics: Arc<StunServerMetrics>,
}

/// Which of the server's UDP sockets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Socket {
    Primary,
    Alternate,
}

/// What to do with a UDP binding request, by its CHANGE-REQUEST. The table in
/// this module's docs, as code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// Answer from this socket.
    Answer(Socket),
    /// Hand to the change handler.
    Handler,
    /// Refuse with this error code.
    Refuse(u16),
}

fn route_change(
    arrived_on: Socket,
    change: Result<ChangeRequest, StunError>,
    has_alternate: bool,
    has_handler: bool,
) -> Route {
    let Ok(change) = change else {
        return Route::Refuse(ERROR_BAD_REQUEST);
    };
    match (change.change_ip, change.change_port) {
        (false, false) => Route::Answer(arrived_on),
        (false, true) if has_alternate => Route::Answer(match arrived_on {
            Socket::Primary => Socket::Alternate,
            Socket::Alternate => Socket::Primary,
        }),
        (true, _) if has_handler => Route::Handler,
        _ => Route::Refuse(ERROR_UNKNOWN_ATTRIBUTE),
    }
}

impl StunServer {
    /// Bind a UDP socket for the server. Port 0 picks a free port, which
    /// [`local_addr`](Self::local_addr) then reports.
    pub async fn bind(addr: &str) -> Result<Self, StunError> {
        Self::bind_with(addr, StunServerOptions::default()).await
    }

    /// Bind with TCP and the RFC 5780 parts. With `options.tcp`, the TCP
    /// listener takes the same port as the UDP socket; port 0 picks one free
    /// for both.
    pub async fn bind_with(addr: &str, options: StunServerOptions) -> Result<Self, StunError> {
        let (primary, tcp) = bind_shared(addr, options.tcp).await?;
        let local_addr = primary.local_addr().map_err(io_err)?;
        let alternate = match options.alternate_port {
            Some(port) => {
                let socket = UdpSocket::bind(SocketAddr::new(local_addr.ip(), port))
                    .await
                    .map_err(io_err)?;
                let addr = socket.local_addr().map_err(io_err)?;
                Some((socket, addr))
            }
            None => None,
        };
        let tcp = match tcp {
            Some(listener) => {
                let addr = listener.local_addr().map_err(io_err)?;
                Some((listener, addr))
            }
            None => None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                primary,
                local_addr,
                alternate,
                tcp,
                tcp_slots: Arc::new(Semaphore::new(MAX_TCP_CONNECTIONS)),
                public_ip: options.public_ip,
                other_address: options.other_address,
                change_handler: options.change_handler,
                metrics: Arc::new(StunServerMetrics::default()),
            }),
        })
    }

    /// The UDP socket's address.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// The TCP listener's address, when TCP is served. Same port as UDP.
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.inner.tcp.as_ref().map(|(_, addr)| *addr)
    }

    /// The alternate UDP socket's address, when one is bound.
    pub fn alternate_addr(&self) -> Option<SocketAddr> {
        self.inner.alternate.as_ref().map(|(_, addr)| *addr)
    }

    /// The shared metrics handle. Clone it out to wherever health is polled.
    pub fn metrics(&self) -> Arc<StunServerMetrics> {
        self.inner.metrics.clone()
    }

    /// Answer a change request another server handed off: send a binding
    /// success for `txid` to `client`, from the alternate socket when
    /// `change.change_port` is set and one is bound, else from the primary.
    /// Returns the address the answer went out from.
    ///
    /// This is the second half of a [`ChangeHandler`]: the first server of a
    /// pair receives the request, and this one, at the other IP, answers it.
    /// `client` must be the address the first server observed, never one a
    /// client named.
    pub async fn answer_for(
        &self,
        txid: TransactionId,
        client: SocketAddr,
        change: ChangeRequest,
    ) -> Result<SocketAddr, StunError> {
        let from = if change.change_port && self.inner.alternate.is_some() {
            Socket::Alternate
        } else {
            Socket::Primary
        };
        self.inner.answer(from, txid, client).await?;
        Ok(self.inner.addr(from))
    }

    /// Serve until a socket fails. Returns only on error.
    pub async fn run(&self) -> Result<(), StunError> {
        serve(self.inner.clone()).await
    }

    /// Serve in the background.
    pub fn spawn(&self) -> tokio::task::JoinHandle<()> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(inner).await {
                log::warn!("[stun] server stopped: {e}");
            }
        })
    }
}

// depth: binding the sockets

/// Bind the UDP socket and, when asked, a TCP listener on the same port.
async fn bind_shared(addr: &str, tcp: bool) -> Result<(UdpSocket, Option<TcpListener>), StunError> {
    if !tcp {
        return Ok((UdpSocket::bind(addr).await.map_err(io_err)?, None));
    }
    let target = tokio::net::lookup_host(addr)
        .await
        .map_err(io_err)?
        .next()
        .ok_or_else(|| StunError::Resolve(addr.to_string()))?;
    for _ in 0..SHARED_PORT_ATTEMPTS {
        let udp = UdpSocket::bind(target).await.map_err(io_err)?;
        let local = udp.local_addr().map_err(io_err)?;
        match TcpListener::bind(local).await {
            Ok(listener) => return Ok((udp, Some(listener))),
            // The free UDP port is taken for TCP: try another.
            Err(e) if target.port() == 0 && e.kind() == ErrorKind::AddrInUse => continue,
            Err(e) => return Err(io_err(e)),
        }
    }
    Err(StunError::Io(format!(
        "no port free for both UDP and TCP on {target} after {SHARED_PORT_ATTEMPTS} tries"
    )))
}

// depth: serving

impl Inner {
    fn socket(&self, which: Socket) -> &UdpSocket {
        match (which, &self.alternate) {
            (Socket::Alternate, Some((socket, _))) => socket,
            _ => &self.primary,
        }
    }

    fn addr(&self, which: Socket) -> SocketAddr {
        match (which, &self.alternate) {
            (Socket::Alternate, Some((_, addr))) => *addr,
            _ => self.local_addr,
        }
    }

    /// RESPONSE-ORIGIN for an answer sent from `local`.
    fn origin(&self, local: SocketAddr) -> Option<SocketAddr> {
        let ip = self
            .public_ip
            .or_else(|| (!local.ip().is_unspecified()).then(|| local.ip()))?;
        Some(SocketAddr::new(ip, local.port()))
    }

    fn success(&self, txid: &TransactionId, client: SocketAddr, local: SocketAddr) -> Vec<u8> {
        let extra = ResponseAttributes {
            response_origin: self.origin(local),
            other_address: self.other_address,
        };
        encode_binding_success_with(txid, client, &extra)
    }

    fn refusal(&self, txid: &TransactionId, code: u16) -> Vec<u8> {
        if code == ERROR_UNKNOWN_ATTRIBUTE {
            encode_binding_error(txid, code, "Unknown Attribute", &[ATTR_CHANGE_REQUEST])
        } else {
            encode_binding_error(txid, code, "Bad Request", &[])
        }
    }

    async fn answer(
        &self,
        from: Socket,
        txid: TransactionId,
        client: SocketAddr,
    ) -> Result<(), StunError> {
        let local = self.addr(from);
        let response = self.success(&txid, client, local);
        self.send(from, &response, client, true).await
    }

    async fn send(
        &self,
        from: Socket,
        message: &[u8],
        client: SocketAddr,
        success: bool,
    ) -> Result<(), StunError> {
        let to = reachable_from(self.addr(from), client);
        let sent = self
            .socket(from)
            .send_to(message, to)
            .await
            .map_err(io_err)?;
        let counter = if success {
            &self.metrics.responses
        } else {
            &self.metrics.errors
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_out
            .fetch_add(sent as u64, Ordering::Relaxed);
        Ok(())
    }
}

/// Run every loop the server has; the first to fail ends the server.
async fn serve(inner: Arc<Inner>) -> Result<(), StunError> {
    let mut loops: Vec<BoxFuture<'static, Result<(), StunError>>> =
        vec![serve_udp(inner.clone(), Socket::Primary).boxed()];
    if inner.alternate.is_some() {
        loops.push(serve_udp(inner.clone(), Socket::Alternate).boxed());
    }
    if inner.tcp.is_some() {
        loops.push(serve_tcp(inner.clone()).boxed());
    }
    futures::future::select_all(loops).await.0
}

async fn serve_udp(inner: Arc<Inner>, which: Socket) -> Result<(), StunError> {
    let metrics = &inner.metrics;
    let mut buf = [0u8; MAX_DATAGRAM];
    loop {
        let (n, from) = inner
            .socket(which)
            .recv_from(&mut buf)
            .await
            .map_err(io_err)?;
        metrics.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
        metrics.touch();

        // Anything but a binding request — junk, a response, an unsupported
        // method — is dropped without a reply.
        let Ok(StunMessage::BindingRequest { txid }) = decode(&buf[..n]) else {
            metrics.dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        metrics.requests.fetch_add(1, Ordering::Relaxed);
        let change = decode_rfc5780(&buf[..n]).map(|a| a.change_request.unwrap_or_default());
        if !matches!(change, Ok(c) if c.is_none()) {
            metrics.change_requests.fetch_add(1, Ordering::Relaxed);
        }
        let client = canonical(from);

        let route = route_change(
            which,
            change.clone(),
            inner.alternate.is_some(),
            inner.change_handler.is_some(),
        );
        let result = match route {
            Route::Answer(socket) => inner.answer(socket, txid, client).await,
            Route::Handler => {
                if let (Some(handler), Ok(change)) = (&inner.change_handler, change) {
                    handler.change_requested(ChangeRequestEvent {
                        txid,
                        client,
                        change,
                    });
                }
                Ok(())
            }
            Route::Refuse(code) => {
                let refusal = inner.refusal(&txid, code);
                inner.send(which, &refusal, client, false).await
            }
        };
        if let Err(e) = result {
            log::debug!("[stun] could not answer {from}: {e}");
        }
    }
}

async fn serve_tcp(inner: Arc<Inner>) -> Result<(), StunError> {
    let Some((listener, _)) = &inner.tcp else {
        return Ok(());
    };
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            // A failed accept is about that connection (reset before accept)
            // or about the process (out of descriptors); neither is a reason
            // to stop serving the others.
            Err(e) => {
                log::debug!("[stun] accept failed: {e}");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let Ok(permit) = inner.tcp_slots.clone().try_acquire_owned() else {
            inner.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        inner
            .metrics
            .tcp_connections
            .fetch_add(1, Ordering::Relaxed);
        tokio::spawn(serve_tcp_connection(
            inner.clone(),
            stream,
            canonical(peer),
            permit,
        ));
    }
}

/// Answer binding requests on one connection until the client closes it, goes
/// idle, or sends something that is not a binding request (the framing cannot
/// be trusted after that).
async fn serve_tcp_connection(
    inner: Arc<Inner>,
    mut stream: TcpStream,
    client: SocketAddr,
    _permit: OwnedSemaphorePermit,
) {
    let metrics = &inner.metrics;
    let Ok(local) = stream.local_addr() else {
        return;
    };
    let mut buf = Vec::new();
    loop {
        let n = match timeout(TCP_IDLE_TIMEOUT, read_message(&mut stream, &mut buf)).await {
            Ok(Ok(Some(n))) => n,
            Ok(Err(StunError::Malformed(_))) => {
                metrics.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
            _ => return,
        };
        metrics.bytes_in.fetch_add(n as u64, Ordering::Relaxed);
        metrics.touch();

        let Ok(StunMessage::BindingRequest { txid }) = decode(&buf[..n]) else {
            metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        metrics.requests.fetch_add(1, Ordering::Relaxed);
        let (response, counter) = match decode_rfc5780(&buf[..n]) {
            Err(_) => (inner.refusal(&txid, ERROR_BAD_REQUEST), &metrics.errors),
            Ok(Rfc5780Attributes {
                change_request: Some(change),
                ..
            }) if !change.is_none() => {
                metrics.change_requests.fetch_add(1, Ordering::Relaxed);
                (
                    inner.refusal(&txid, ERROR_UNKNOWN_ATTRIBUTE),
                    &metrics.errors,
                )
            }
            Ok(_) => (inner.success(&txid, client, local), &metrics.responses),
        };
        if stream.write_all(&response).await.is_err() {
            return;
        }
        counter.fetch_add(1, Ordering::Relaxed);
        metrics
            .bytes_out
            .fetch_add(response.len() as u64, Ordering::Relaxed);
    }
}
