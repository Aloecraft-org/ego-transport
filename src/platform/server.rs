//! Platform-aware server implementations.
//!
//! This module provides a unified server interface that adapts to platform
//! capabilities:
//!
//! - **Native**: Concurrent connection handling via tokio::spawn
//! - **WASI P2**: Sequential connection handling (one at a time)
//!
//! # Example
//!
//! ```no_run
//! use aloeclient::server::ServerBuilder;
//!
//! async fn run_server(listener: impl Listener) {
//!     ServerBuilder::new(listener)
//!         .run(|mut transport| async move {
//!             // Handle connection
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
//!

use crate::transport::{Transport, TransportError};
use std::future::Future;

// Platform-specific imports
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::spawn as tokio_spawn;

// Conditionally require Send trait bound
#[cfg(not(all(target_arch = "wasm32", target_env = "p2")))]
pub trait MaybeSend: Send {}
#[cfg(not(all(target_arch = "wasm32", target_env = "p2")))]
impl<T: Send> MaybeSend for T {}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub trait MaybeSend {}
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
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

    /// Spawn a task for each connection (native and threaded WASI only)
    #[cfg(not(target_arch = "wasm32"))]
    Concurrent,
}

impl<L: Listener> ServerBuilder<L> {
    /// Create a new server with platform-default mode
    ///
    /// - Native: Concurrent by default
    /// - WASI P2: Sequential (only option)
    pub fn new(listener: L) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let default_mode = ServerMode::Concurrent;

        #[cfg(target_arch = "wasm32")]
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
    #[cfg(not(target_arch = "wasm32"))]
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

            #[cfg(not(target_arch = "wasm32"))]
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
