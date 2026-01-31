//! Buffered transport wrapper for protocol auto-detection.
//!
//! When detecting the protocol of an incoming connection (e.g., TCP vs WebSocket),
//! some bytes may be consumed during detection. `BufferedTransport` replays those
//! bytes to the handler before delegating to the underlying transport.
//!
//! On native platforms this wrapper is not needed (tokio's `peek()` is non-consuming),
//! but on WASI where streams have no peek support, the detection bytes are read and
//! must be replayed. This type is the mechanism for that replay.

use crate::transport::{Transport, TransportError};

/// A transport that delivers buffered prefix bytes before delegating to an inner transport.
///
/// The prefix is drained on successive `recv()` calls. Once the prefix is exhausted,
/// all reads go directly to the inner transport. `send()` always delegates immediately.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub struct BufferedTransport {
    /// Bytes consumed during protocol detection, to be replayed to the handler.
    prefix: Vec<u8>,
    /// The underlying transport (e.g., a TcpStreamWasi after detection).
    inner: Box<dyn Transport>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl BufferedTransport {
    /// Create a buffered transport that replays `prefix` before reading from `inner`.
    pub fn new(prefix: Vec<u8>, inner: Box<dyn Transport>) -> Self {
        Self { prefix, inner }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use async_trait::async_trait;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
impl Transport for BufferedTransport {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.inner.send(data).await
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        let mut filled = 0;

        // Drain prefix bytes first. If the caller's buffer is smaller than the
        // remaining prefix, we deliver a partial chunk and retain the rest.
        if !self.prefix.is_empty() {
            let n = self.prefix.len().min(buf.len());
            buf[..n].copy_from_slice(&self.prefix[..n]);
            self.prefix.drain(..n);
            filled = n;
        }

        // If the buffer isn't full, read from the inner transport to coalesce.
        //
        // This keeps BufferedTransport transparent to the handler: on native,
        // peek() is non-consuming so the first recv() returns all available bytes
        // as one contiguous chunk. Without coalescing here, WASI would split the
        // same data into two recv() results (prefix, then stream), which changes
        // observable behavior for any handler that echoes per-recv.
        //
        // If the inner read fails but we already have prefix bytes, we return
        // those — the error will surface on the caller's next recv().
        if filled < buf.len() {
            match self.inner.recv(&mut buf[filled..]).await {
                Ok(n) => filled += n,
                Err(e) => {
                    if filled == 0 {
                        return Err(e);
                    }
                    // Prefix bytes are ready to return. Error deferred to next call.
                }
            }
        }

        Ok(filled)
    }
}