//! Sync adapter for WASI async streams

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::tcp_wasi::TcpStreamWasi;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use std::io::{self, Read, Write};

/// Sync wrapper around async TcpStreamWasi.
///
/// Implements `std::io::Read` and `std::io::Write` so that synchronous libraries
/// (like `tungstenite`) can operate over WASI streams.
///
/// An optional `prefix` buffer supports protocol auto-detection: when initial bytes
/// are consumed during detection, they are stored here and delivered to the reader
/// before any bytes are read from the underlying stream.
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct WasiSyncStream {
    inner: TcpStreamWasi,
    /// Bytes to deliver before reading from the inner stream.
    /// Populated by `with_prefix()` when bytes were consumed during protocol detection.
    /// Empty in the normal (non-detection) case.
    prefix: Vec<u8>,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl WasiSyncStream {
    /// Create a sync stream with no prefix (standard usage).
    pub fn new(stream: TcpStreamWasi) -> Self {
        Self {
            inner: stream,
            prefix: Vec::new(),
        }
    }

    /// Create a sync stream that delivers `prefix` bytes before reading from the
    /// inner stream. Used by `AutoDetectListener` on WASI: the detection bytes
    /// (e.g., "GET ") are consumed during protocol sniffing and must be replayed
    /// so that tungstenite sees the complete HTTP upgrade request.
    pub fn with_prefix(stream: TcpStreamWasi, prefix: Vec<u8>) -> Self {
        Self {
            inner: stream,
            prefix,
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl Read for WasiSyncStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Drain prefix bytes first. If the caller's buffer is smaller than the
        // remaining prefix we deliver a partial chunk and retain the rest.
        if !self.prefix.is_empty() {
            let n = self.prefix.len().min(buf.len());
            buf[..n].copy_from_slice(&self.prefix[..n]);
            self.prefix.drain(..n);
            return Ok(n);
        }

        // Prefix fully drained — read directly from the inner WASI stream.
        match self.inner.input.read(buf.len() as u64) {
            Ok(data) => {
                if data.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "No data available",
                    ));
                }
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
            Err(e) => {
                use wasip2::io::streams::StreamError;
                match e {
                    StreamError::Closed => {
                        Ok(0) // EOF
                    }
                    StreamError::LastOperationFailed(err) => {
                        let error_str = err.to_debug_string();
                        if error_str.contains("would-block") {
                            Err(io::Error::new(io::ErrorKind::WouldBlock, error_str))
                        } else {
                            Err(io::Error::other(format!(
                                "WASI stream error: {}",
                                error_str
                            )))
                        }
                    }
                }
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl Write for WasiSyncStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        loop {
            match self.inner.output.write(buf) {
                Ok(()) => {
                    return Ok(buf.len());
                }
                Err(e) => {
                    use wasip2::io::streams::StreamError;
                    match e {
                        StreamError::Closed => {
                            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Stream closed"));
                        }
                        StreamError::LastOperationFailed(err) => {
                            let error_str = err.to_debug_string();
                            if error_str.contains("would-block") {
                                std::thread::yield_now();
                                continue;
                            } else {
                                return Err(io::Error::other(format!(
                                    "WASI stream error: {}",
                                    error_str
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            match self.inner.output.blocking_flush() {
                Ok(_) => return Ok(()),
                Err(e) => {
                    use wasip2::io::streams::StreamError;
                    match e {
                        StreamError::Closed => {
                            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Stream closed"));
                        }
                        StreamError::LastOperationFailed(err) => {
                            let error_str = err.to_debug_string();
                            if error_str.contains("would-block") {
                                std::thread::yield_now();
                                continue;
                            } else {
                                // Ignore flush errors
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }
}
