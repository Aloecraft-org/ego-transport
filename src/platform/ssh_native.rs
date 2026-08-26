//! The `ssh` scheme: authenticated, multiplexed channels over SSH.
//!
//! Both directions are built on [russh] for the protocol state machines
//! (KEX, rekey, channels) with RustCrypto key material underneath — the
//! transport is deliberately *not* hand-rolled from primitives.
//!
//! Deliberate constraints, enforced here rather than left to configuration:
//!
//! - **Public-key auth only.** No passwords, no keyboard-interactive.
//! - **Modern suite only**: ed25519 host and client keys, curve25519 key
//!   exchange, chacha20-poly1305. There is no legacy-algorithm table to
//!   maintain and no downgrade path to audit.
//! - Host-key verification on the client side is explicit: the caller
//!   supplies the keys or fingerprints it trusts, or explicitly opts in to
//!   accepting any (trust-on-first-use is the *caller's* decision, never the
//!   default).
//!
//! The server surfaces two channel kinds to the consumer: interactive PTY
//! channels (with window-size changes) and named subsystem channels, which
//! carry length-prefixed frames via [`crate::framing`]. Each connection
//! reports the authenticated client key verbatim as its
//! [`PeerIdentity`] — what that principal may do is the consumer's decision.
//! The server's own host-key fingerprint is exposed as the node identity
//! primitive.

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use russh::keys::{HashAlg, PrivateKeyWithHashAlg};
use russh::{Channel, ChannelMsg, Preferred};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::identity::{KeyIdentity, PeerIdentity};
use crate::transport::{Transport, TransportError};

// Re-exported so consumers can build configs without depending on russh.
pub use russh::keys::ssh_key::Algorithm;
pub use russh::keys::{PrivateKey, PublicKey};

/// Typed SSH failures. Distinct outcomes are distinct variants so consumers
/// can react to *why* a connection was refused, not parse strings.
#[derive(Debug, Error)]
pub enum SshError {
    /// Protocol, I/O, or handshake failure below the authentication layer.
    #[error("ssh protocol failure: {0}")]
    Protocol(String),

    /// The server rejected our public-key authentication.
    #[error("server rejected public-key authentication for user '{user}'")]
    AuthRejected { user: String },

    /// The server's host key did not match the supplied trust set.
    #[error("host key verification failed{}", match offered {
        Some(k) => format!(" (server offered {k})"),
        None => String::new(),
    })]
    HostKeyMismatch { offered: Option<KeyIdentity> },

    /// The channel (or the session under it) is gone.
    #[error("ssh channel closed")]
    ChannelClosed,

    /// The remote side refused a channel request (pty, shell, subsystem, exec).
    #[error("remote side refused {kind} request")]
    RequestRefused { kind: &'static str },

    /// An accept-side queue was full; the connection or channel was refused
    /// rather than buffered without bound.
    #[error("accept backlog full")]
    BacklogFull,
}

impl From<russh::Error> for SshError {
    fn from(e: russh::Error) -> Self {
        match e {
            russh::Error::UnknownKey => SshError::HostKeyMismatch { offered: None },
            russh::Error::NotAuthenticated => SshError::AuthRejected {
                user: String::new(),
            },
            other => SshError::Protocol(other.to_string()),
        }
    }
}

fn ssh_err(e: russh::Error) -> TransportError {
    TransportError::Ssh(SshError::from(e))
}

/// The one algorithm suite this scheme speaks.
fn modern_preferred() -> Preferred {
    Preferred {
        kex: Cow::Borrowed(&[russh::kex::CURVE25519]),
        key: Cow::Borrowed(&[Algorithm::Ed25519]),
        host_key_certificates: Cow::Borrowed(&[]),
        cipher: Cow::Borrowed(&[russh::cipher::CHACHA20_POLY1305]),
        mac: Preferred::DEFAULT.mac,
        compression: Cow::Borrowed(&[russh::compression::NONE]),
    }
}

/// [`KeyIdentity`] for a public key, verbatim from the wire encoding.
pub fn key_identity(key: &PublicKey) -> KeyIdentity {
    KeyIdentity {
        algorithm: key.algorithm().to_string(),
        fingerprint_sha256: key.fingerprint(HashAlg::Sha256).to_string(),
        public_key: key.to_bytes().unwrap_or_default(),
    }
}

/// Generate a fresh ed25519 private key (host or client).
pub fn generate_ed25519() -> PrivateKey {
    PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)
        .expect("ed25519 key generation cannot fail")
}

/// Parse an OpenSSH-format private key (optionally passphrase-protected).
pub fn private_key_from_openssh(
    pem: &str,
    passphrase: Option<&str>,
) -> Result<PrivateKey, TransportError> {
    let key = PrivateKey::from_openssh(pem)
        .map_err(|e| TransportError::Ssh(SshError::Protocol(format!("bad private key: {e}"))))?;
    match (key.is_encrypted(), passphrase) {
        (false, _) => Ok(key),
        (true, Some(p)) => key
            .decrypt(p)
            .map_err(|e| TransportError::Ssh(SshError::Protocol(format!("bad passphrase: {e}")))),
        (true, None) => Err(TransportError::Ssh(SshError::Protocol(
            "private key is encrypted and no passphrase was supplied".into(),
        ))),
    }
}

/// Parse an OpenSSH-format public key (`ssh-ed25519 AAAA... comment`).
pub fn public_key_from_openssh(s: &str) -> Result<PublicKey, TransportError> {
    PublicKey::from_openssh(s)
        .map_err(|e| TransportError::Ssh(SshError::Protocol(format!("bad public key: {e}"))))
}

// ---------------------------------------------------------------------------
// Channel kinds and events (shared between client and server wrappers)
// ---------------------------------------------------------------------------

/// Parameters of a PTY request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyParams {
    pub term: String,
    pub cols: u32,
    pub rows: u32,
    pub pix_width: u32,
    pub pix_height: u32,
}

/// What the remote side asked this channel to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshChannelKind {
    /// Interactive terminal (pty-req, usually followed by shell).
    Pty(PtyParams),
    /// Shell without a PTY.
    Shell,
    /// One-shot command execution.
    Exec(Vec<u8>),
    /// A named subsystem; carries framed messages (see [`crate::framing`]).
    Subsystem(String),
}

/// Lifecycle and data events on an SSH channel. Everything is surfaced —
/// open, data, resize, eof, close — never swallowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshChannelEvent {
    Data(Vec<u8>),
    /// stderr stream (client side of exec/shell channels).
    ExtendedData(Vec<u8>),
    /// The peer's terminal was resized (PTY channels).
    WindowChange {
        cols: u32,
        rows: u32,
        pix_width: u32,
        pix_height: u32,
    },
    /// Remote command exit status (client side).
    ExitStatus(u32),
    Eof,
    Closed,
}

// Shared plumbing for both channel directions: pull events, buffer partial
// reads, queue resize notifications seen while a caller was doing byte reads.
// The message type parameter differs per side, so this is a macro rather than
// a generic.
macro_rules! channel_common {
    ($name:ident, $msg:ty) => {
        pub struct $name {
            channel: Channel<$msg>,
            kind: SshChannelKind,
            /// Bytes from the last Data event not yet consumed by `recv`.
            pending: Vec<u8>,
            /// Resize events observed while the caller was reading bytes.
            resizes: std::collections::VecDeque<(u32, u32, u32, u32)>,
            closed: bool,
        }

        impl $name {
            /// What the remote side asked this channel to be.
            pub fn kind(&self) -> &SshChannelKind {
                &self.kind
            }

            /// Next channel event. Every lifecycle transition is an event;
            /// nothing is swallowed.
            pub async fn next_event(&mut self) -> SshChannelEvent {
                if let Some((cols, rows, pw, ph)) = self.resizes.pop_front() {
                    return SshChannelEvent::WindowChange {
                        cols,
                        rows,
                        pix_width: pw,
                        pix_height: ph,
                    };
                }
                if self.closed {
                    return SshChannelEvent::Closed;
                }
                loop {
                    match self.channel.wait().await {
                        None => {
                            self.closed = true;
                            return SshChannelEvent::Closed;
                        }
                        Some(ChannelMsg::Data { data }) => {
                            return SshChannelEvent::Data(data.to_vec());
                        }
                        Some(ChannelMsg::ExtendedData { data, .. }) => {
                            return SshChannelEvent::ExtendedData(data.to_vec());
                        }
                        Some(ChannelMsg::WindowChange {
                            col_width,
                            row_height,
                            pix_width,
                            pix_height,
                        }) => {
                            return SshChannelEvent::WindowChange {
                                cols: col_width,
                                rows: row_height,
                                pix_width,
                                pix_height,
                            };
                        }
                        Some(ChannelMsg::ExitStatus { exit_status }) => {
                            return SshChannelEvent::ExitStatus(exit_status);
                        }
                        Some(ChannelMsg::Eof) => return SshChannelEvent::Eof,
                        Some(ChannelMsg::Close) => {
                            self.closed = true;
                            return SshChannelEvent::Closed;
                        }
                        // Late request/reply chatter (shell after pty, success
                        // acks, env) carries no payload for the consumer.
                        Some(_) => continue,
                    }
                }
            }

            /// Resize events that arrived while `recv` was delivering bytes.
            pub fn pop_resize(&mut self) -> Option<(u32, u32, u32, u32)> {
                self.resizes.pop_front()
            }

            /// Half-close: no more data from this side.
            pub async fn send_eof(&mut self) -> Result<(), TransportError> {
                self.channel.eof().await.map_err(ssh_err)
            }

            pub async fn close(&mut self) -> Result<(), TransportError> {
                self.channel.close().await.map_err(ssh_err)
            }
        }

        #[async_trait::async_trait]
        impl Transport for $name {
            async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
                self.channel.data(data).await.map_err(ssh_err)
            }

            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
                loop {
                    if !self.pending.is_empty() {
                        let n = self.pending.len().min(buf.len());
                        buf[..n].copy_from_slice(&self.pending[..n]);
                        self.pending.drain(..n);
                        return Ok(n);
                    }
                    match self.next_event().await {
                        SshChannelEvent::Data(d) | SshChannelEvent::ExtendedData(d) => {
                            self.pending = d;
                        }
                        SshChannelEvent::WindowChange {
                            cols,
                            rows,
                            pix_width,
                            pix_height,
                        } => {
                            // Kept for pop_resize(); a byte reader isn't the
                            // audience for terminal geometry.
                            self.resizes.push_back((cols, rows, pix_width, pix_height));
                        }
                        SshChannelEvent::ExitStatus(_) => continue,
                        SshChannelEvent::Eof | SshChannelEvent::Closed => {
                            return Err(TransportError::Closed);
                        }
                    }
                }
            }
        }
    };
}

channel_common!(SshServerChannel, russh::server::Msg);
channel_common!(SshClientChannel, russh::client::Msg);

impl SshClientChannel {
    /// Report a local terminal resize to the remote PTY.
    pub async fn resize(&mut self, cols: u32, rows: u32) -> Result<(), TransportError> {
        self.channel
            .window_change(cols, rows, 0, 0)
            .await
            .map_err(ssh_err)
    }
}

impl SshServerChannel {
    /// Report the command/session exit status (PTY and exec channels).
    pub async fn exit_status(&mut self, status: u32) -> Result<(), TransportError> {
        self.channel.exit_status(status).await.map_err(ssh_err)
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Which client keys may authenticate. The signature is always verified by
/// the protocol layer first; this only decides whether a *proven* key is
/// admitted.
#[derive(Clone)]
pub enum ClientAuthorization {
    /// Admit any key that proves ownership, and surface it as the
    /// connection's principal — the consumer maps principals to what they
    /// may do.
    AnyProvenKey,
    /// Admit only these public keys.
    Keys(Vec<PublicKey>),
}

impl ClientAuthorization {
    fn admits(&self, key: &PublicKey) -> bool {
        match self {
            ClientAuthorization::AnyProvenKey => true,
            ClientAuthorization::Keys(keys) => keys.iter().any(|k| k.key_data() == key.key_data()),
        }
    }
}

/// Server-side configuration for the `ssh` scheme.
pub struct SshServerConfig {
    /// The host key; its fingerprint is this node's identity primitive.
    pub host_key: PrivateKey,
    pub authorization: ClientAuthorization,
    /// Authenticated connections queued for `accept` before further
    /// connections are refused (bounded, observable — never an unbounded
    /// queue).
    pub connection_backlog: usize,
    /// Opened channels queued per connection before further opens are
    /// refused.
    pub channel_backlog: usize,
    pub inactivity_timeout: Option<Duration>,
    /// Constant time taken to reject a failed authentication attempt.
    pub auth_rejection_time: Duration,
}

impl SshServerConfig {
    pub fn new(host_key: PrivateKey) -> Self {
        Self {
            host_key,
            authorization: ClientAuthorization::AnyProvenKey,
            connection_backlog: 64,
            channel_backlog: 32,
            inactivity_timeout: Some(Duration::from_secs(3600)),
            auth_rejection_time: Duration::from_secs(1),
        }
    }
}

struct AcceptEntry {
    connection: SshServerConnection,
}

/// A bound `ssh` listener: accepts TCP connections, runs the SSH handshake
/// and public-key authentication, and yields fully authenticated connections.
pub struct SshListener {
    local_addr: SocketAddr,
    host_identity: KeyIdentity,
    conn_rx: Mutex<mpsc::Receiver<AcceptEntry>>,
    acceptor: tokio::task::JoinHandle<()>,
}

impl SshListener {
    pub async fn bind(addr: &str, config: SshServerConfig) -> Result<Self, TransportError> {
        let host_identity = key_identity(&config.host_key.public_key().clone());

        let mut russh_config = russh::server::Config::default();
        russh_config.methods = russh::MethodSet::empty();
        russh_config.methods.push(russh::MethodKind::PublicKey);
        russh_config.keys = vec![config.host_key];
        russh_config.preferred = modern_preferred();
        russh_config.inactivity_timeout = config.inactivity_timeout;
        russh_config.auth_rejection_time = config.auth_rejection_time;
        let russh_config = Arc::new(russh_config);

        let tcp = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(TransportError::Io)?;
        let local_addr = tcp.local_addr().map_err(TransportError::Io)?;

        let (conn_tx, conn_rx) = mpsc::channel(config.connection_backlog);
        let authorization = config.authorization;
        let channel_backlog = config.channel_backlog;

        let acceptor = tokio::spawn(async move {
            loop {
                let (socket, peer) = match tcp.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        log::warn!("[ssh] accept failed: {e}");
                        continue;
                    }
                };
                let handler = ServerHandler {
                    authorization: authorization.clone(),
                    remote_addr: Some(peer),
                    conn_tx: conn_tx.clone(),
                    channel_backlog,
                    principal: None,
                    chan_tx: None,
                };
                let config = russh_config.clone();
                tokio::spawn(async move {
                    match russh::server::run_stream(config, socket, handler).await {
                        Ok(session) => {
                            if let Err(e) = session.await {
                                log::debug!("[ssh] session from {peer} ended: {e}");
                            }
                        }
                        Err(e) => log::debug!("[ssh] handshake with {peer} failed: {e}"),
                    }
                });
            }
        });

        Ok(Self {
            local_addr,
            host_identity,
            conn_rx: Mutex::new(conn_rx),
            acceptor,
        })
    }

    /// The next authenticated connection. Connections that never complete
    /// authentication never show up here.
    pub async fn accept(&self) -> Result<SshServerConnection, TransportError> {
        self.conn_rx
            .lock()
            .await
            .recv()
            .await
            .map(|e| e.connection)
            .ok_or(TransportError::Closed)
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// This node's identity primitive: the host key, fingerprint included.
    pub fn host_identity(&self) -> &KeyIdentity {
        &self.host_identity
    }
}

impl Drop for SshListener {
    fn drop(&mut self) {
        self.acceptor.abort();
    }
}

/// One authenticated inbound SSH connection.
pub struct SshServerConnection {
    identity: PeerIdentity,
    remote_addr: Option<SocketAddr>,
    channels: mpsc::Receiver<Channel<russh::server::Msg>>,
}

impl SshServerConnection {
    /// The authenticated client key (and user name), verbatim.
    pub fn identity(&self) -> &PeerIdentity {
        &self.identity
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.remote_addr
    }

    /// The next channel the client opens, classified by its first request
    /// (PTY, shell, exec, or subsystem). Returns `Err(Closed)` when the
    /// connection is gone.
    pub async fn next_channel(&mut self) -> Result<SshServerChannel, TransportError> {
        'next: loop {
            let mut channel = self.channels.recv().await.ok_or(TransportError::Closed)?;
            loop {
                match channel.wait().await {
                    None => continue 'next,
                    Some(ChannelMsg::RequestPty {
                        term,
                        col_width,
                        row_height,
                        pix_width,
                        pix_height,
                        ..
                    }) => {
                        return Ok(SshServerChannel {
                            channel,
                            kind: SshChannelKind::Pty(PtyParams {
                                term,
                                cols: col_width,
                                rows: row_height,
                                pix_width,
                                pix_height,
                            }),
                            pending: Vec::new(),
                            resizes: Default::default(),
                            closed: false,
                        });
                    }
                    Some(ChannelMsg::RequestShell { .. }) => {
                        return Ok(SshServerChannel {
                            channel,
                            kind: SshChannelKind::Shell,
                            pending: Vec::new(),
                            resizes: Default::default(),
                            closed: false,
                        });
                    }
                    Some(ChannelMsg::Exec { command, .. }) => {
                        return Ok(SshServerChannel {
                            channel,
                            kind: SshChannelKind::Exec(command),
                            pending: Vec::new(),
                            resizes: Default::default(),
                            closed: false,
                        });
                    }
                    Some(ChannelMsg::RequestSubsystem { name, .. }) => {
                        return Ok(SshServerChannel {
                            channel,
                            kind: SshChannelKind::Subsystem(name),
                            pending: Vec::new(),
                            resizes: Default::default(),
                            closed: false,
                        });
                    }
                    // Open confirmation and other pre-request chatter.
                    Some(_) => continue,
                }
            }
        }
    }
}

struct ServerHandler {
    authorization: ClientAuthorization,
    remote_addr: Option<SocketAddr>,
    conn_tx: mpsc::Sender<AcceptEntry>,
    channel_backlog: usize,
    principal: Option<PeerIdentity>,
    chan_tx: Option<mpsc::Sender<Channel<russh::server::Msg>>>,
}

impl russh::server::Handler for ServerHandler {
    type Error = russh::Error;

    async fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &PublicKey,
    ) -> Result<russh::server::Auth, Self::Error> {
        // Signature verified by the protocol layer before this is called; we
        // only decide admission and record the principal.
        if self.authorization.admits(public_key) {
            self.principal = Some(PeerIdentity::Key {
                key: key_identity(public_key),
                user: Some(user.to_string()),
            });
            Ok(russh::server::Auth::Accept)
        } else {
            Ok(russh::server::Auth::reject())
        }
    }

    async fn auth_succeeded(
        &mut self,
        _session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        let identity = self.principal.clone().unwrap_or(PeerIdentity::Anonymous);
        let (chan_tx, chan_rx) = mpsc::channel(self.channel_backlog);
        self.chan_tx = Some(chan_tx);
        let entry = AcceptEntry {
            connection: SshServerConnection {
                identity,
                remote_addr: self.remote_addr,
                channels: chan_rx,
            },
        };
        // Bounded handoff: a full accept queue refuses the connection
        // observably instead of buffering it.
        self.conn_tx
            .try_send(entry)
            .map_err(|_| russh::Error::Disconnect)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<russh::server::Msg>,
        reply: russh::server::ChannelOpenHandle,
        _session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        match &self.chan_tx {
            Some(tx) => match tx.try_send(channel) {
                Ok(()) => reply.accept().await,
                // Bounded per-connection channel queue: refuse, don't buffer.
                Err(_) => {
                    reply
                        .reject(russh::ChannelOpenFailure::ResourceShortage)
                        .await
                }
            },
            None => {
                reply
                    .reject(russh::ChannelOpenFailure::AdministrativelyProhibited)
                    .await
            }
        }
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: russh::ChannelId,
        _term: &str,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel)
    }

    async fn shell_request(
        &mut self,
        channel: russh::ChannelId,
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel)
    }

    async fn exec_request(
        &mut self,
        channel: russh::ChannelId,
        _data: &[u8],
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel)
    }

    async fn subsystem_request(
        &mut self,
        channel: russh::ChannelId,
        _name: &str,
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel)
    }

    async fn window_change_request(
        &mut self,
        channel: russh::ChannelId,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel)
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// How the client decides whether to trust the server's host key.
///
/// There is deliberately no implicit trust-on-first-use: accepting an unknown
/// key is an explicit caller decision ([`HostKeyVerification::AcceptAny`]),
/// typically paired with recording the surfaced identity for next time.
#[derive(Clone)]
pub enum HostKeyVerification {
    /// Trust exactly these host keys.
    Keys(Vec<PublicKey>),
    /// Trust host keys with these SHA-256 fingerprints
    /// (`SHA256:<base64>` presentation).
    Fingerprints(Vec<String>),
    /// Accept whatever key the server offers. Explicit opt-in only; the
    /// offered key is still surfaced so the caller can record it.
    AcceptAny,
}

impl HostKeyVerification {
    fn admits(&self, key: &PublicKey) -> bool {
        match self {
            HostKeyVerification::AcceptAny => true,
            HostKeyVerification::Keys(keys) => keys.iter().any(|k| k.key_data() == key.key_data()),
            HostKeyVerification::Fingerprints(fps) => {
                let fp = key.fingerprint(HashAlg::Sha256).to_string();
                fps.iter().any(|f| *f == fp)
            }
        }
    }
}

/// Client-side configuration for the `ssh` scheme.
pub struct SshClientConfig {
    pub user: String,
    /// The client key; its public half becomes this connection's principal
    /// on the server side.
    pub key: PrivateKey,
    pub host_verification: HostKeyVerification,
    pub inactivity_timeout: Option<Duration>,
}

struct ClientHandler {
    verification: HostKeyVerification,
    observed_tx: Option<oneshot::Sender<KeyIdentity>>,
    observed: Arc<std::sync::Mutex<Option<KeyIdentity>>>,
}

impl russh::client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let key = match server_key {
            russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } => key.clone(),
            // We never advertise certificate host-key algorithms, so a
            // certificate here is out of suite: refuse it.
            russh::keys::PublicKeyOrCertificate::Certificate(_) => return Ok(false),
        };
        let identity = key_identity(&key);
        *self.observed.lock().unwrap() = Some(identity.clone());
        if let Some(tx) = self.observed_tx.take() {
            let _ = tx.send(identity);
        }
        Ok(self.verification.admits(&key))
    }
}

/// An authenticated outbound SSH connection.
pub struct SshClientConnection {
    handle: russh::client::Handle<ClientHandler>,
    host_identity: KeyIdentity,
}

impl SshClientConnection {
    /// Dial `host:port`, verify the host key against the supplied trust set,
    /// and authenticate with the supplied key. Every failure mode is a typed
    /// error: [`SshError::HostKeyMismatch`] names the key the server offered,
    /// [`SshError::AuthRejected`] names the refused user.
    pub async fn connect(addr: &str, config: SshClientConfig) -> Result<Self, TransportError> {
        let mut russh_config = russh::client::Config::default();
        russh_config.preferred = modern_preferred();
        russh_config.inactivity_timeout = config.inactivity_timeout;
        let russh_config = Arc::new(russh_config);

        let observed = Arc::new(std::sync::Mutex::new(None));
        let handler = ClientHandler {
            verification: config.host_verification,
            observed_tx: None,
            observed: observed.clone(),
        };

        let mut handle = russh::client::connect(russh_config, addr, handler)
            .await
            .map_err(|e| match e {
                russh::Error::UnknownKey => TransportError::Ssh(SshError::HostKeyMismatch {
                    offered: observed.lock().unwrap().clone(),
                }),
                other => ssh_err(other),
            })?;

        let auth = handle
            .authenticate_publickey(
                config.user.clone(),
                PrivateKeyWithHashAlg::new(Arc::new(config.key), None),
            )
            .await
            .map_err(ssh_err)?;
        if !auth.success() {
            return Err(TransportError::Ssh(SshError::AuthRejected {
                user: config.user,
            }));
        }

        let host_identity = observed.lock().unwrap().clone().ok_or_else(|| {
            TransportError::Ssh(SshError::Protocol("host key never offered".into()))
        })?;

        Ok(Self {
            handle,
            host_identity,
        })
    }

    /// The server's host key — the remote node's identity primitive.
    pub fn host_identity(&self) -> &KeyIdentity {
        &self.host_identity
    }

    /// Open a named subsystem channel (framed-message transport).
    pub async fn open_subsystem(&self, name: &str) -> Result<SshClientChannel, TransportError> {
        let mut channel = self.handle.channel_open_session().await.map_err(ssh_err)?;
        channel
            .request_subsystem(true, name)
            .await
            .map_err(ssh_err)?;
        wait_for_reply(&mut channel, "subsystem").await?;
        Ok(SshClientChannel {
            channel,
            kind: SshChannelKind::Subsystem(name.to_string()),
            pending: Vec::new(),
            resizes: Default::default(),
            closed: false,
        })
    }

    /// Open an interactive PTY channel (pty-req followed by shell).
    pub async fn open_pty(
        &self,
        term: &str,
        cols: u32,
        rows: u32,
    ) -> Result<SshClientChannel, TransportError> {
        let mut channel = self.handle.channel_open_session().await.map_err(ssh_err)?;
        channel
            .request_pty(true, term, cols, rows, 0, 0, &[])
            .await
            .map_err(ssh_err)?;
        wait_for_reply(&mut channel, "pty").await?;
        channel.request_shell(true).await.map_err(ssh_err)?;
        wait_for_reply(&mut channel, "shell").await?;
        Ok(SshClientChannel {
            channel,
            kind: SshChannelKind::Pty(PtyParams {
                term: term.to_string(),
                cols,
                rows,
                pix_width: 0,
                pix_height: 0,
            }),
            pending: Vec::new(),
            resizes: Default::default(),
            closed: false,
        })
    }

    /// Run a single remote command.
    pub async fn open_exec(&self, command: &[u8]) -> Result<SshClientChannel, TransportError> {
        let mut channel = self.handle.channel_open_session().await.map_err(ssh_err)?;
        channel.exec(true, command).await.map_err(ssh_err)?;
        wait_for_reply(&mut channel, "exec").await?;
        Ok(SshClientChannel {
            channel,
            kind: SshChannelKind::Exec(command.to_vec()),
            pending: Vec::new(),
            resizes: Default::default(),
            closed: false,
        })
    }

    /// Close the whole connection.
    pub async fn disconnect(&self) -> Result<(), TransportError> {
        self.handle
            .disconnect(russh::Disconnect::ByApplication, "", "en")
            .await
            .map_err(ssh_err)
    }
}

/// Await the server's success/failure reply to a want-reply channel request.
async fn wait_for_reply(
    channel: &mut Channel<russh::client::Msg>,
    kind: &'static str,
) -> Result<(), TransportError> {
    loop {
        match channel.wait().await {
            None => return Err(TransportError::Ssh(SshError::ChannelClosed)),
            Some(ChannelMsg::Success) => return Ok(()),
            Some(ChannelMsg::Failure) => {
                return Err(TransportError::Ssh(SshError::RequestRefused { kind }));
            }
            Some(_) => continue,
        }
    }
}
