//! Length-prefixed framing over any [`Transport`].
//!
//! Consumers that exchange discrete messages (msgpack maps, for instance) over
//! byte streams need one framing convention, implemented once — not per
//! scheme. A frame on the wire is a 4-byte big-endian length followed by that
//! many payload bytes. The payload is opaque to this module.
//!
//! The same helper works over every scheme: wrap whatever the scheme handed
//! you in [`FramedTransport`] and exchange whole frames.

use crate::transport::{Transport, TransportError};

/// Default cap on a single frame's payload. Frames above the configured cap
/// are refused (typed, on both send and receive) rather than buffered — an
/// oversized length prefix must never translate into an unbounded allocation.
pub const DEFAULT_MAX_FRAME: usize = 16 * 1024 * 1024;

const HEADER_LEN: usize = 4;
const READ_CHUNK: usize = 64 * 1024;

/// Encode one frame (header + payload) into a fresh buffer.
///
/// Fails with [`TransportError::FrameTooLarge`] when the payload exceeds
/// `max_frame`.
pub fn encode_frame(payload: &[u8], max_frame: usize) -> Result<Vec<u8>, TransportError> {
    if payload.len() > max_frame {
        return Err(TransportError::FrameTooLarge {
            len: payload.len(),
            max: max_frame,
        });
    }
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Try to decode one frame from the front of `buf`.
///
/// Returns `Ok(Some((payload, consumed)))` when a complete frame is present,
/// `Ok(None)` when more bytes are needed, and
/// [`TransportError::FrameTooLarge`] when the header announces a payload
/// beyond `max_frame`.
pub fn decode_frame(
    buf: &[u8],
    max_frame: usize,
) -> Result<Option<(Vec<u8>, usize)>, TransportError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    let mut header = [0u8; HEADER_LEN];
    header.copy_from_slice(&buf[..HEADER_LEN]);
    let len = u32::from_be_bytes(header) as usize;
    if len > max_frame {
        return Err(TransportError::FrameTooLarge {
            len,
            max: max_frame,
        });
    }
    if buf.len() < HEADER_LEN + len {
        return Ok(None);
    }
    Ok(Some((
        buf[HEADER_LEN..HEADER_LEN + len].to_vec(),
        HEADER_LEN + len,
    )))
}

/// A [`Transport`] wrapper that exchanges whole length-prefixed frames.
pub struct FramedTransport<T: Transport> {
    inner: T,
    max_frame: usize,
    rbuf: Vec<u8>,
}

impl<T: Transport> FramedTransport<T> {
    /// Wrap a transport with the default frame-size cap.
    pub fn new(inner: T) -> Self {
        Self::with_max_frame(inner, DEFAULT_MAX_FRAME)
    }

    /// Wrap a transport with an explicit frame-size cap.
    pub fn with_max_frame(inner: T, max_frame: usize) -> Self {
        Self {
            inner,
            max_frame,
            rbuf: Vec::new(),
        }
    }

    /// Send one frame. The payload travels as a single header-prefixed write,
    /// so on message-oriented schemes (WebSocket) one frame is one message.
    pub async fn send_frame(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        let wire = encode_frame(payload, self.max_frame)?;
        self.inner.send(&wire).await
    }

    /// Receive one complete frame, reading from the underlying transport as
    /// needed. Bytes past the frame boundary are retained for the next call.
    pub async fn recv_frame(&mut self) -> Result<Vec<u8>, TransportError> {
        loop {
            if let Some((payload, consumed)) = decode_frame(&self.rbuf, self.max_frame)? {
                self.rbuf.drain(..consumed);
                return Ok(payload);
            }
            let mut chunk = [0u8; READ_CHUNK];
            let n = self.inner.recv(&mut chunk).await?;
            if n == 0 {
                return Err(TransportError::Closed);
            }
            self.rbuf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Bytes buffered beyond the last returned frame.
    pub fn buffered(&self) -> usize {
        self.rbuf.len()
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Unwrap, discarding any partially buffered frame.
    pub fn into_inner(self) -> T {
        self.inner
    }
}
