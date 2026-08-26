// server.rs

//! Platform-aware server implementations.
//!
//! This module provides a unified server interface that adapts to platform
//! capabilities:
//!
//! - **Native**: Concurrent connection handling via tokio::spawn
//! - **WASI P2**: Sequential connection handling (one at a time)
//!
//! ## AutoDetectListener
//!
//! `AutoDetectListener` accepts TCP connections and automatically detects whether
//! each one is a raw TCP connection or a WebSocket upgrade request — returning the
//! appropriate `Box<dyn Transport>` either way.
//!
//! ## Embedded Signaling
//!
//! `AutoDetectListener` can optionally embed a `SignalingHub` that handles WebRTC
//! signaling peers. When enabled, connections whose first message is `JOIN:...`
//! are routed to the signaling hub (which manages rooms and relays SDP/ICE
//! messages). Application connections pass through to `ServerBuilder` as normal.
//!
//! ```no_run
//! use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
//! use ego_transport::transport::signaling_hub::SignalingHub;
//!
//! async fn run() {
//!     let hub = SignalingHub::new();
//!     let listener = AutoDetectListener::bind("0.0.0.0:9990").await.unwrap()
//!         .with_signaling(hub);
//!
//!     ServerBuilder::new(listener)
//!         .concurrent()
//!         .run(|mut transport| async move {
//!             // Only non-signaling connections reach this handler.
//!             // Signaling peers are handled by the embedded hub.
//!             let mut buf = [0u8; 1024];
//!             while let Ok(n) = transport.recv(&mut buf).await {
//!                 transport.send(&buf[..n]).await.ok();
//!             }
//!         })
//!         .await
//!         .expect("Server error");
//! }
//! ```
//!
//! For a signaling-only server (no application handler), the handler can
//! be a no-op — all connections are signaling peers.

use crate::transport::signaling_hub::SignalingHub;
use crate::transport::{Transport, TransportError};
use std::future::Future;

// Platform-specific imports
// Available on native AND WASI P2 (tokio::spawn works on both)
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use tokio::task::spawn as tokio_spawn;

// Conditionally require Send trait bound
// Native + WASI P2: require Send (tokio::spawn needs it)
// Browser: no Send (single-threaded, no spawn)
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait MaybeSend: Send {}
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send> MaybeSend for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait MaybeSend {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T> MaybeSend for T {}

/// Trait for types that can accept connections
#[async_trait::async_trait]
pub trait Listener {
    /// Accept a new connection
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError>;
}

/// One accepted connection with everything the consumer needs to route it:
/// the stream itself, whatever identity the scheme attested to, and the
/// remote address when the platform can report one.
pub struct Accepted {
    pub transport: Box<dyn Transport>,
    pub identity: crate::identity::PeerIdentity,
    pub remote_addr: Option<String>,
}

/// Listeners that surface identity and remote address with each accept.
///
/// Schemes without a handshake identity (tcp, plain ws) report
/// [`PeerIdentity::Anonymous`](crate::identity::PeerIdentity::Anonymous);
/// authenticated schemes report the peer's proven key. Identity is reported
/// verbatim — mapping it to permissions is the consumer's job.
#[async_trait::async_trait]
pub trait IdentifiedListener {
    async fn accept_identified(&self) -> Result<Accepted, TransportError>;
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl IdentifiedListener for crate::platform::tcp_native::TcpListenerNative {
    async fn accept_identified(&self) -> Result<Accepted, TransportError> {
        let stream = self.accept_std().await?;
        let remote_addr = stream.peer_addr().ok().map(|a| a.to_string());
        Ok(Accepted {
            transport: Box::new(crate::platform::tcp_native::TcpStreamNative { inner: stream }),
            identity: crate::identity::PeerIdentity::Anonymous,
            remote_addr,
        })
    }
}

/// Builder for creating a server with platform-appropriate concurrency
pub struct ServerBuilder<L> {
    listener: L,
    mode: ServerMode,
}

/// Server execution mode
pub enum ServerMode {
    /// Handle connections one at a time (available on all platforms)
    Sequential,

    /// Spawn a task for each connection (native and WASI P2)
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    Concurrent,
}

impl<L: Listener> ServerBuilder<L> {
    /// Create a new server with platform-default mode
    ///
    /// - Native: Concurrent by default
    /// - WASI P2: Sequential (only option)
    pub fn new(listener: L) -> Self {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let default_mode = ServerMode::Concurrent;

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let default_mode = ServerMode::Sequential;
        Self {
            listener,
            mode: default_mode,
        }
    }

    /// Use sequential mode (handle one connection at a time)
    pub fn sequential(mut self) -> Self {
        self.mode = ServerMode::Sequential;
        self
    }

    /// Use concurrent mode (spawn a task per connection)
    ///
    /// Only available on native and threaded WASI builds.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub fn concurrent(mut self) -> Self {
        self.mode = ServerMode::Concurrent;
        self
    }

    /// Run the server with the given connection handler
    ///
    /// The handler closure is called for each accepted connection.
    /// Connections routed to an embedded SignalingHub (if configured)
    /// never reach this handler.
    pub async fn run<F, Fut>(self, handler: F) -> Result<(), TransportError>
    where
        F: Fn(Box<dyn Transport>) -> Fut + Clone + MaybeSend + 'static,
        Fut: Future<Output = ()> + MaybeSend + 'static,
    {
        match self.mode {
            ServerMode::Sequential => {
                log::info!("Server running in SEQUENTIAL mode");
                loop {
                    match self.listener.accept().await {
                        Ok(transport) => {
                            log::debug!("Connection accepted, handling sequentially");
                            handler(transport).await;
                            log::debug!("Connection complete, ready for next");
                        }
                        Err(e) => {
                            log::error!("Accept error: {:?}", e);
                            return Err(e);
                        }
                    }
                }
            }

            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            ServerMode::Concurrent => {
                log::info!("Server running in CONCURRENT mode");
                loop {
                    match self.listener.accept().await {
                        Ok(transport) => {
                            log::debug!("Connection accepted, spawning handler");
                            let handler = handler.clone();
                            tokio_spawn(async move {
                                handler(transport).await;
                            });
                        }
                        Err(e) => {
                            log::error!("Accept error: {:?}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }
    }
}

// =====================================================================
// AutoDetectListener
// =====================================================================

/// Number of bytes to inspect for protocol detection.
const DETECT_PREFIX_LEN: usize = 4;

/// The byte sequence that identifies a WebSocket upgrade request.
const WS_HANDSHAKE_PREFIX: &[u8; DETECT_PREFIX_LEN] = b"GET ";

// --- Platform-specific imports ---

#[cfg(not(target_arch = "wasm32"))]
use crate::platform::tcp_native::{TcpListenerNative, TcpStreamNative};
#[cfg(not(target_arch = "wasm32"))]
use crate::platform::ws_native::WebSocketNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::tcp_wasi::TcpListenerWasi;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::ws_wasi::WebSocketWasi;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::transport::BufferedTransport;

/// A listener that auto-detects TCP vs WebSocket on a single port,
/// with optional embedded signaling for WebRTC peer connections.
///
/// ### Protocol detection
///
/// Two-stage detection:
/// 1. **Transport level**: TCP (`not "GET "`) vs WebSocket (`"GET "`)
/// 2. **Application level**: Signaling (`"JOIN:"` first message) vs app data
///
/// ### Protocol filtering
///
/// By default both TCP and WebSocket are accepted. Use `.tcp_only()` or
/// `.ws_only()` to restrict.
///
/// ### Embedded signaling
///
/// Call `.with_signaling(hub)` to enable. Connections whose first message
/// is a `JOIN:` signaling message are handled by the hub and never reach
/// `ServerBuilder`'s handler.
pub struct AutoDetectListener {
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    inner: TcpListenerWasi,
    #[cfg(not(target_arch = "wasm32"))]
    inner: TcpListenerNative,
    allow_ws: bool,
    allow_tcp: bool,
    signaling: Option<SignalingHub>,
    /// Detection tasks send classified transports here.
    #[cfg(not(target_arch = "wasm32"))]
    detect_tx: tokio::sync::mpsc::Sender<Result<Box<dyn Transport>, ()>>,
    /// Receiver for completed detections. Uses tokio::sync::Mutex so
    /// accept(&self) can recv() across await points without &mut self.
    #[cfg(not(target_arch = "wasm32"))]
    detect_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Result<Box<dyn Transport>, ()>>>,
}

// ===== Native implementation =====

#[cfg(not(target_arch = "wasm32"))]
impl AutoDetectListener {
    /// Bind to an address, accepting both TCP and WebSocket by default.
    pub async fn bind(addr: &str) -> Result<Self, TransportError> {
        let inner = TcpListenerNative::bind(addr)?;
        #[cfg(not(target_arch = "wasm32"))]
        let (detect_tx, detect_rx) = tokio::sync::mpsc::channel(32);
        Ok(Self {
            inner,
            allow_ws: true,
            allow_tcp: true,
            signaling: None,
            #[cfg(not(target_arch = "wasm32"))]
            detect_tx,
            #[cfg(not(target_arch = "wasm32"))]
            detect_rx: tokio::sync::Mutex::new(detect_rx),
        })
    }

    /// Restrict to TCP connections only.
    pub fn tcp_only(mut self) -> Self {
        self.allow_ws = false;
        self
    }

    /// Restrict to WebSocket connections only.
    pub fn ws_only(mut self) -> Self {
        self.allow_tcp = false;
        self
    }

    /// Enable embedded signaling. Connections whose first message is `JOIN:`
    /// are handled by the hub and never reach the application handler.
    ///
    /// The hub is shared (cloneable) — multiple listeners or servers can
    /// share the same signaling rooms.
    pub fn with_signaling(mut self, hub: SignalingHub) -> Self {
        self.signaling = Some(hub);
        self
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl Listener for AutoDetectListener {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError> {
        let tx = self.detect_tx.clone();
        let mut rx = self.detect_rx.lock().await;

        loop {
            tokio::select! {
                // Accept new raw connections and spawn detection tasks
                accept_result = self.inner.accept_std() => {
                    let stream = accept_result?;
                    let allow_ws = self.allow_ws;
                    let allow_tcp = self.allow_tcp;
                    let signaling = self.signaling.clone();
                    let tx = tx.clone();

                    tokio_spawn(async move {
                        let result = detect_and_classify(
                            stream, allow_ws, allow_tcp, signaling,
                        ).await;
                        let _ = tx.send(result).await;
                    });
                }

                // Completed detection — return app transports, skip handled ones
                Some(result) = rx.recv() => {
                    match result {
                        Ok(transport) => return Ok(transport),
                        Err(()) => continue,
                    }
                }
            }
        }
    }
}

/// Classify a raw TCP connection: detect protocol, handle signaling, return transport.
///
/// Returns `Ok(transport)` for app connections that should be returned to the caller.
/// Returns `Err(())` for connections handled internally (signaling) or that failed.
#[cfg(not(target_arch = "wasm32"))]
async fn detect_and_classify(
    stream: std::net::TcpStream,
    allow_ws: bool,
    allow_tcp: bool,
    signaling: Option<SignalingHub>,
) -> Result<Box<dyn Transport>, ()> {
    use std::io::ErrorKind;

    let mut peek_buf = [0u8; DETECT_PREFIX_LEN];
    let peeked = loop {
        match stream.peek(&mut peek_buf) {
            Ok(n) => break n,
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(e) => {
                log::debug!("[AutoDetect] Peek error: {:?}", e);
                return Err(());
            }
        }
    };

    if peeked == 0 {
        return Err(());
    }

    let is_websocket =
        peeked >= DETECT_PREFIX_LEN && &peek_buf[..DETECT_PREFIX_LEN] == WS_HANDSHAKE_PREFIX;

    if is_websocket {
        if !allow_ws {
            return Err(());
        }

        let tokio_stream = match tokio::net::TcpStream::from_std(stream) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("[AutoDetect] Stream conversion failed: {:?}", e);
                return Err(());
            }
        };
        let mut ws = match WebSocketNative::accept(tokio_stream).await {
            Ok(ws) => ws,
            Err(e) => {
                log::warn!("[AutoDetect] WebSocket handshake failed: {:?}", e);
                return Err(());
            }
        };

        if let Some(hub) = signaling {
            let mut first_buf = [0u8; 4096];
            match tokio::time::timeout(std::time::Duration::from_secs(5), ws.recv(&mut first_buf))
                .await
            {
                Ok(Ok(n)) => {
                    if SignalingHub::is_signaling_message(&first_buf[..n]) {
                        log::info!("[AutoDetect] Routing WS to SignalingHub");
                        let first_msg = first_buf[..n].to_vec();
                        tokio_spawn(async move {
                            let transport: Box<dyn Transport> = Box::new(ws);
                            if let Err(e) = hub.handle_peer(transport, &first_msg).await {
                                log::debug!("[SignalingHub] Peer handler ended: {:?}", e);
                            }
                        });
                        return Err(()); // Handled internally
                    } else {
                        let prefix = first_buf[..n].to_vec();
                        return Ok(Box::new(crate::transport::BufferedTransport::new(
                            prefix,
                            Box::new(ws),
                        )));
                    }
                }
                Ok(Err(_)) => return Err(()),
                Err(_) => return Ok(Box::new(ws)),
            }
        } else {
            return Ok(Box::new(ws));
        }
    } else {
        if !allow_tcp {
            return Err(());
        }

        if let Some(hub) = signaling {
            let mut sig_peek = [0u8; 5];
            let sig_peeked = loop {
                match stream.peek(&mut sig_peek) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        tokio::task::yield_now().await;
                    }
                    Err(_) => break 0,
                }
            };

            if sig_peeked >= 5 && SignalingHub::is_signaling_message(&sig_peek[..sig_peeked]) {
                log::info!("[AutoDetect] TCP signaling connection");
                let mut tcp = TcpStreamNative { inner: stream };
                let mut first_buf = [0u8; 4096];
                match tcp.recv(&mut first_buf).await {
                    Ok(n) => {
                        let first_msg = first_buf[..n].to_vec();
                        tokio_spawn(async move {
                            let transport: Box<dyn Transport> = Box::new(tcp);
                            if let Err(e) = hub.handle_peer(transport, &first_msg).await {
                                log::debug!("[SignalingHub] Peer handler ended: {:?}", e);
                            }
                        });
                        return Err(());
                    }
                    Err(_) => return Err(()),
                }
            }
        }

        return Ok(Box::new(TcpStreamNative { inner: stream }));
    }
}
// ===== WASI implementation =====

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl AutoDetectListener {
    /// Bind to an address, accepting both TCP and WebSocket by default.
    pub async fn bind(addr: &str) -> Result<Self, TransportError> {
        let inner = TcpListenerWasi::bind(addr).await?;
        Ok(Self {
            inner,
            allow_tcp: true,
            allow_ws: true,
            signaling: None,
        })
    }

    /// Restrict to TCP connections only.
    pub fn tcp_only(mut self) -> Self {
        self.allow_ws = false;
        self
    }

    /// Restrict to WebSocket connections only.
    pub fn ws_only(mut self) -> Self {
        self.allow_tcp = false;
        self
    }

    /// Enable embedded signaling.
    pub fn with_signaling(mut self, hub: SignalingHub) -> Self {
        self.signaling = Some(hub);
        self
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
#[async_trait::async_trait]
impl Listener for AutoDetectListener {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError> {
        loop {
            let mut stream = self.inner.accept().await?;

            // --- Protocol detection via consuming read + prefix replay ---
            let mut detect_buf = [0u8; DETECT_PREFIX_LEN];
            let mut read = 0;
            while read < DETECT_PREFIX_LEN {
                match stream.recv(&mut detect_buf[read..]).await {
                    Ok(0) => break,
                    Ok(n) => read += n,
                    Err(TransportError::Closed) => break,
                    Err(e) => return Err(e),
                }
            }

            if read == 0 {
                log::debug!("[AutoDetect] Connection closed during detection");
                continue;
            }

            let is_websocket = read >= DETECT_PREFIX_LEN
                && &detect_buf[..DETECT_PREFIX_LEN] == WS_HANDSHAKE_PREFIX;

            let prefix = detect_buf[..read].to_vec();

            if is_websocket {
                if !self.allow_ws {
                    log::warn!("[AutoDetect] Rejected WebSocket (not enabled)");
                    continue;
                }
                let mut ws = WebSocketWasi::accept_with_prefix(stream, prefix).await?;

                // Application-level signaling detection for WASI WebSocket
                if let Some(hub) = &self.signaling {
                    let mut first_buf = [0u8; 4096];
                    match ws.recv(&mut first_buf).await {
                        Ok(n) => {
                            if SignalingHub::is_signaling_message(&first_buf[..n]) {
                                log::info!("[AutoDetect] WASI: routing to SignalingHub");
                                let transport: Box<dyn Transport> = Box::new(ws);
                                // WASI is sequential — handle inline
                                hub.handle_peer(transport, &first_buf[..n]).await.ok();
                                continue;
                            } else {
                                // Not signaling — return with buffered prefix
                                let msg_prefix = first_buf[..n].to_vec();
                                return Ok(Box::new(BufferedTransport::new(
                                    msg_prefix,
                                    Box::new(ws),
                                )));
                            }
                        }
                        Err(_) => continue,
                    }
                } else {
                    return Ok(Box::new(ws));
                }
            } else {
                if !self.allow_tcp {
                    log::warn!("[AutoDetect] Rejected TCP (not enabled)");
                    continue;
                }

                // Check for signaling on TCP
                if let Some(hub) = &self.signaling {
                    // We already consumed `prefix` bytes. Check if they start
                    // with "JOIN" (4 bytes of the 5-byte "JOIN:" prefix).
                    if read >= 4 && &prefix[..4] == b"JOIN" {
                        // Read one more byte to confirm the colon
                        let mut colon = [0u8; 1];
                        match stream.recv(&mut colon).await {
                            Ok(1) if colon[0] == b':' => {
                                // It's a JOIN message. Read the rest.
                                let mut rest_buf = [0u8; 4096];
                                let mut full_msg = prefix.clone();
                                full_msg.push(b':');
                                match stream.recv(&mut rest_buf).await {
                                    Ok(n) => {
                                        full_msg.extend_from_slice(&rest_buf[..n]);
                                    }
                                    Err(_) => {}
                                }
                                log::info!("[AutoDetect] WASI TCP: routing to SignalingHub");
                                let transport: Box<dyn Transport> = Box::new(stream);
                                // Handle inline (WASI sequential)
                                hub.handle_peer(transport, &full_msg).await.ok();
                                continue;
                            }
                            Ok(n) => {
                                // Not a colon — reconstruct prefix and return
                                let mut full_prefix = prefix;
                                full_prefix.extend_from_slice(&colon[..n]);
                                return Ok(Box::new(BufferedTransport::new(
                                    full_prefix,
                                    Box::new(stream),
                                )));
                            }
                            Err(_) => continue,
                        }
                    }
                }

                return Ok(Box::new(BufferedTransport::new(prefix, Box::new(stream))));
            }
        }
    }
}
