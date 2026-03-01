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
//! appropriate `Box<dyn Transport>` either way. It implements the `Listener` trait,
//! so it works with `ServerBuilder` unchanged:
//!
//! ```no_run
//! use aloeclient::platform::server::{AutoDetectListener, ServerBuilder};
//!
//! async fn run() {
//!     let listener = AutoDetectListener::bind("0.0.0.0:9990").await.unwrap();
//!
//!     ServerBuilder::new(listener)
//!         .concurrent()
//!         .run(|mut transport| async move {
//!             // transport is TCP or WebSocket — handler is the same either way
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
    ///
    /// This is useful for:
    /// - Testing
    /// - Debugging
    /// - Resource-constrained environments
    /// - When connection order matters
    pub fn sequential(mut self) -> Self {
        self.mode = ServerMode::Sequential;
        self
    }

    /// Use concurrent mode (spawn a task per connection)
    ///
    /// Only available on native and threaded WASI builds.
    /// Each connection is handled in its own tokio task.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub fn concurrent(mut self) -> Self {
        self.mode = ServerMode::Concurrent;
        self
    }

    /// Run the server with the given connection handler
    ///
    /// The handler closure is called for each accepted connection.
    /// Behavior depends on the mode:
    ///
    /// - **Sequential**: Handler is awaited before accepting next connection
    /// - **Concurrent**: Handler is spawned and next connection accepted immediately
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
//
// A Listener that accepts TCP connections and sniffs the first bytes to
// determine whether each connection is raw TCP or a WebSocket upgrade.
//
// Detection strategy differs by platform:
//   - Native: std::net::TcpStream::peek() — non-consuming, so the TCP path
//     has zero overhead (no buffering, no round-trip conversions).
//   - WASI:   InputStream has no peek. We read 4 bytes (consuming) and route
//             via prefix buffers: BufferedTransport for TCP, WasiSyncStream
//             with_prefix for WebSocket (so tungstenite sees the full request).
//
// Both platforms loop internally on rejected connections (tcp_only / ws_only
// guards), so ServerBuilder never sees a rejection as an error — it just
// sees the next valid connection.
// =====================================================================

/// Number of bytes to inspect for protocol detection.
/// WebSocket upgrade requests are HTTP, which always begins with a method name.
/// "GET " (4 bytes) is sufficient to distinguish from raw TCP in practice.
const DETECT_PREFIX_LEN: usize = 4;

/// The byte sequence that identifies a WebSocket upgrade request.
/// All HTTP/1.x requests begin with a method; WebSocket upgrades are always GET.
const WS_HANDSHAKE_PREFIX: &[u8; DETECT_PREFIX_LEN] = b"GET ";

// --- Platform-specific imports for AutoDetectListener ---

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

/// A listener that auto-detects TCP vs WebSocket on a single port.
///
/// Implements `Listener`, so it works directly with `ServerBuilder` — including
/// concurrent mode on native. The detection happens inside `accept()` before the
/// transport is handed off, so it is invisible to the concurrency model.
///
/// ### Protocol filtering
///
/// By default both TCP and WebSocket are accepted. Use `.tcp_only()` or
/// `.ws_only()` to restrict. Connections of a disallowed type are closed
/// silently (with a log warning) and the listener loops back to accept the
/// next connection — the restriction never surfaces as an error to `ServerBuilder`.
pub struct AutoDetectListener {
    #[cfg(not(target_arch = "wasm32"))]
    inner: TcpListenerNative,

    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    inner: TcpListenerWasi,

    /// Accept raw TCP connections
    allow_tcp: bool,
    /// Accept WebSocket upgrade requests
    allow_ws: bool,
}

// ===== Native implementation =====

#[cfg(not(target_arch = "wasm32"))]
impl AutoDetectListener {
    /// Bind to an address, accepting both TCP and WebSocket by default.
    ///
    /// This is `async` for cross-platform compatibility — on native it returns
    /// immediately; on WASI the bind itself is async.
    pub async fn bind(addr: &str) -> Result<Self, TransportError> {
        let inner = TcpListenerNative::bind(addr)?;
        Ok(Self {
            inner,
            allow_tcp: true,
            allow_ws: true,
        })
    }

    /// Restrict to TCP connections only. WebSocket upgrades will be rejected
    /// (connection closed, warning logged).
    pub fn tcp_only(mut self) -> Self {
        self.allow_ws = false;
        self
    }

    /// Restrict to WebSocket connections only. Raw TCP connections will be
    /// rejected (connection closed, warning logged).
    pub fn ws_only(mut self) -> Self {
        self.allow_tcp = false;
        self
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl Listener for AutoDetectListener {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError> {
        use std::io::ErrorKind;

        loop {
            // Accept a raw TCP connection via the shared primitive.
            let stream = self.inner.accept_std().await?;

            // --- Protocol detection via non-consuming peek ---
            //
            // std::net::TcpStream::peek() does not advance the read position.
            // On WouldBlock (no data yet) we yield and retry — same pattern as
            // recv() elsewhere in this crate.
            let mut peek_buf = [0u8; DETECT_PREFIX_LEN];
            let peeked = loop {
                match stream.peek(&mut peek_buf) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        tokio::task::yield_now().await;
                    }
                    Err(e) => return Err(TransportError::Io(e)),
                }
            };

            // EOF before any data — connection is already dead.
            if peeked == 0 {
                log::debug!("[AutoDetect] Connection closed during protocol detection");
                continue; // stream dropped, accept next
            }

            let is_websocket = peeked >= DETECT_PREFIX_LEN
                && &peek_buf[..DETECT_PREFIX_LEN] == WS_HANDSHAKE_PREFIX;

            if is_websocket {
                if !self.allow_ws {
                    log::warn!(
                        "[AutoDetect] Rejected WebSocket connection (WebSocket not enabled)"
                    );
                    continue; // stream dropped → connection closed
                }
                // Convert to tokio stream for the tungstenite upgrade.
                // peek() was non-consuming so all bytes are still available.
                let tokio_stream =
                    tokio::net::TcpStream::from_std(stream).map_err(TransportError::Io)?;
                let ws = WebSocketNative::accept(tokio_stream).await?;
                return Ok(Box::new(ws));
            } else {
                if !self.allow_tcp {
                    log::warn!("[AutoDetect] Rejected TCP connection (TCP not enabled)");
                    continue; // stream dropped → connection closed
                }
                // Wrap directly as TCP — peek was non-consuming, all bytes
                // remain available for the handler's first recv().
                return Ok(Box::new(TcpStreamNative { inner: stream }));
            }
        }
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
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
#[async_trait::async_trait]
impl Listener for AutoDetectListener {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError> {
        loop {
            // Accept a TCP connection. On WASI we get a TcpStreamWasi directly.
            let mut stream = self.inner.accept().await?;

            // --- Protocol detection via consuming read + prefix replay ---
            //
            // WASI InputStream has no peek(). We read up to DETECT_PREFIX_LEN
            // bytes (consuming them from the stream) and then replay them via
            // prefix buffers so the handler or tungstenite handshake sees the
            // complete data.
            let mut detect_buf = [0u8; DETECT_PREFIX_LEN];
            let mut read = 0;
            while read < DETECT_PREFIX_LEN {
                match stream.recv(&mut detect_buf[read..]).await {
                    Ok(0) => break, // EOF
                    Ok(n) => read += n,
                    Err(TransportError::Closed) => break,
                    Err(e) => return Err(e),
                }
            }

            // EOF before any data — connection is already dead.
            if read == 0 {
                log::debug!("[AutoDetect] Connection closed during protocol detection");
                continue; // stream dropped, accept next
            }

            let is_websocket = read >= DETECT_PREFIX_LEN
                && &detect_buf[..DETECT_PREFIX_LEN] == WS_HANDSHAKE_PREFIX;

            // The bytes we consumed — must be replayed regardless of which path we take.
            let prefix = detect_buf[..read].to_vec();

            if is_websocket {
                if !self.allow_ws {
                    log::warn!(
                        "[AutoDetect] Rejected WebSocket connection (WebSocket not enabled)"
                    );
                    continue; // stream dropped → connection closed
                }
                // Replay prefix through WasiSyncStream so tungstenite sees
                // the full HTTP upgrade request during handshake.
                let ws = WebSocketWasi::accept_with_prefix(stream, prefix).await?;
                return Ok(Box::new(ws));
            } else {
                if !self.allow_tcp {
                    log::warn!("[AutoDetect] Rejected TCP connection (TCP not enabled)");
                    continue; // stream dropped → connection closed
                }
                // Wrap in BufferedTransport so the handler's first recv()
                // delivers the detection bytes before reading from the stream.
                return Ok(Box::new(BufferedTransport::new(prefix, Box::new(stream))));
            }
        }
    }
}
