// src/transport/bridge.rs

use super::Transport;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Adapts AloeCraft Transport to AsyncRead + AsyncWrite
pub struct TransportBridge {
    inner: Box<dyn Transport>,
    read_buffer: Vec<u8>,
    read_pos: usize,
    read_len: usize,
}

impl TransportBridge {
    pub fn new(transport: Box<dyn Transport>) -> Self {
        Self {
            inner: transport,
            read_buffer: vec![0u8; 8192],
            read_pos: 0,
            read_len: 0,
        }
    }
}

impl AsyncRead for TransportBridge {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // 1. If we have buffered data, deliver it
        if self.read_pos < self.read_len {
            let available = self.read_len - self.read_pos;
            let to_copy = available.min(buf.remaining());
            buf.put_slice(&self.read_buffer[self.read_pos..self.read_pos + to_copy]);
            self.read_pos += to_copy;

            if self.read_pos >= self.read_len {
                self.read_pos = 0;
                self.read_len = 0;
            }

            return Poll::Ready(Ok(()));
        }

        let this = self.get_mut();

        let n = {
            let inner = &mut this.inner;
            let temp_buf = &mut this.read_buffer[..];

            let fut = inner.recv(temp_buf);
            tokio::pin!(fut);

            match fut.poll(cx) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Transport error: {:?}", e),
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        };
        if n == 0 {
            return Poll::Ready(Ok(())); // EOF
        }

        let to_copy = n.min(buf.remaining());
        buf.put_slice(&this.read_buffer[..to_copy]);

        if to_copy < n {
            this.read_len = n;
            this.read_pos = to_copy;
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TransportBridge {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let inner = &mut self.inner;
        let data = buf.to_vec();
        let len = data.len();

        let fut = inner.send(&data);
        tokio::pin!(fut);

        match fut.poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(len)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Transport error: {:?}", e),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
