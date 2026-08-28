//! Delivering whole messages into caller-sized buffers without losing the tail.
//!
//! Message-oriented transports (WebSocket, WebRTC data channels) receive a
//! complete message at a time, but [`Transport::recv`](crate::transport::Transport::recv)
//! hands the caller a fixed buffer that may be smaller than the message. The
//! obvious `copy_from_slice(&data[..buf.len()])` silently drops the rest,
//! which is not a short read — it is data loss, and it corrupts anything
//! that treats the transport as a byte stream. A length-prefixed reader, for
//! instance, will take the *next* message's first bytes as a length prefix
//! and desync into garbage rather than failing.
//!
//! [`MessageBuffer`] holds whatever did not fit and returns it on the
//! following `recv`, so a large message arrives across several reads with
//! every byte intact and in order.

/// Retains the unread tail of a message between `recv` calls.
#[derive(Debug, Default)]
pub(crate) struct MessageBuffer {
    pending: Vec<u8>,
}

impl MessageBuffer {
    /// Whether bytes from an earlier message are still owed to the caller.
    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Copy as much of the retained tail as fits, returning how many bytes
    /// were delivered.
    pub(crate) fn drain_into(&mut self, buf: &mut [u8]) -> usize {
        let n = self.pending.len().min(buf.len());
        buf[..n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        n
    }

    /// Deliver a freshly received message, retaining whatever does not fit.
    ///
    /// Only call this with no tail outstanding — drain that first, or the
    /// retained bytes would be delivered out of order.
    pub(crate) fn deliver(&mut self, data: &[u8], buf: &mut [u8]) -> usize {
        debug_assert!(
            self.pending.is_empty(),
            "a retained tail must be drained before a new message is delivered"
        );
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        if n < data.len() {
            self.pending.extend_from_slice(&data[n..]);
        }
        n
    }
}
