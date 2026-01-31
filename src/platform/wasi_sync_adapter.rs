//! Sync adapter for WASI async streams

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::tcp_wasi::TcpStreamWasi;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use std::io::{self, Read, Write};

/// Sync wrapper around async TcpStreamWasi
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct WasiSyncStream {
    // Remove Rc<RefCell<>> - just own it directly
    inner: TcpStreamWasi,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl WasiSyncStream {
    pub fn new(stream: TcpStreamWasi) -> Self {
        Self { inner: stream }
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl Read for WasiSyncStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Direct access, no borrow_mut needed
        loop {
            match self.inner.input.read(buf.len() as u64) {
                Ok(data) => {
                    if data.is_empty() {
                        std::thread::yield_now();
                        continue;
                    }
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    return Ok(n);
                }
                Err(e) => {
                    use wasip2::io::streams::StreamError;
                    match e {
                        StreamError::Closed => {
                            return Ok(0); // EOF
                        }
                        StreamError::LastOperationFailed(err) => {
                            let error_str = err.to_debug_string();
                            if error_str.contains("would-block") {
                                std::thread::yield_now();
                                continue;
                            } else {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("WASI stream error: {}", error_str)
                                ));
                            }
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
                            return Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "Stream closed"
                            ));
                        }
                        StreamError::LastOperationFailed(err) => {
                            let error_str = err.to_debug_string();
                            if error_str.contains("would-block") {
                                std::thread::yield_now();
                                continue;
                            } else {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("WASI stream error: {}", error_str)
                                ));
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
                            return Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "Stream closed"
                            ));
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