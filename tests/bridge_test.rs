// tests/bridge_test.rs

mod common;
use common::{async_test, test};

use aloeclient::transport::{Transport, TransportBridge};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Mock transport for testing
struct MockTransport {
    read_data: Vec<Vec<u8>>,
    read_pos: usize,
    written: Vec<Vec<u8>>,
}

impl MockTransport {
    fn new(responses: Vec<Vec<u8>>) -> Self {
        Self {
            read_data: responses,
            read_pos: 0,
            written: vec![],
        }
    }
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    async_trait::async_trait
)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
impl Transport for MockTransport {
    async fn send(&mut self, data: &[u8]) -> Result<(), aloeclient::transport::TransportError> {
        self.written.push(data.to_vec());
        Ok(())
    }

    async fn recv(
        &mut self,
        buf: &mut [u8],
    ) -> Result<usize, aloeclient::transport::TransportError> {
        if self.read_pos >= self.read_data.len() {
            return Ok(0); // EOF
        }

        let data = &self.read_data[self.read_pos];
        self.read_pos += 1;

        let len = data.len().min(buf.len());
        buf[..len].copy_from_slice(&data[..len]);
        Ok(len)
    }
}

#[async_test]
async fn test_bridge_read() {
    let mock = MockTransport::new(vec![b"Hello".to_vec(), b" ".to_vec(), b"World".to_vec()]);

    let mut bridge = TransportBridge::new(Box::new(mock));

    let mut buf = [0u8; 11];
    bridge.read_exact(&mut buf).await.unwrap();

    assert_eq!(&buf, b"Hello World");
}

#[async_test]
async fn test_bridge_write() {
    let mock = MockTransport::new(vec![]);
    let mut bridge = TransportBridge::new(Box::new(mock));

    bridge.write_all(b"Test message").await.unwrap();

    // Can't easily verify without exposing inner, but at least it doesn't panic
}

#[async_test]
async fn test_bridge_partial_reads() {
    // Test buffering when Transport returns less than requested
    let mock = MockTransport::new(vec![vec![1, 2, 3], vec![4, 5]]);

    let mut bridge = TransportBridge::new(Box::new(mock));

    // Read 5 bytes total across 2 transport reads
    let mut buf = [0u8; 5];
    bridge.read_exact(&mut buf).await.unwrap();

    assert_eq!(&buf, &[1, 2, 3, 4, 5]);
}

#[async_test]
async fn test_bridge_read_single_chunk() {
    let mock = MockTransport::new(vec![b"Hello World".to_vec()]);

    let mut bridge = TransportBridge::new(Box::new(mock));

    let mut buf = [0u8; 11];
    bridge.read_exact(&mut buf).await.unwrap();

    assert_eq!(&buf, b"Hello World");
}

#[async_test]
async fn test_bridge_read_multiple_chunks() {
    let mock = MockTransport::new(vec![b"Hello".to_vec(), b" ".to_vec(), b"World".to_vec()]);

    let mut bridge = TransportBridge::new(Box::new(mock));

    let mut buf = [0u8; 11];
    bridge.read_exact(&mut buf).await.unwrap();

    assert_eq!(&buf, b"Hello World");
}

#[async_test]
async fn test_bridge_read_partial() {
    // Transport returns more than we ask for initially
    let mock = MockTransport::new(vec![b"Hello World Extra".to_vec()]);

    let mut bridge = TransportBridge::new(Box::new(mock));

    // Read only first 5 bytes
    let mut buf1 = [0u8; 5];
    bridge.read_exact(&mut buf1).await.unwrap();
    assert_eq!(&buf1, b"Hello");

    // Read next 6 bytes (should come from buffer)
    let mut buf2 = [0u8; 6];
    bridge.read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b" World");
}

#[async_test]
async fn test_bridge_eof() {
    let mock = MockTransport::new(vec![]); // No data

    let mut bridge = TransportBridge::new(Box::new(mock));

    let mut buf = [0u8; 10];
    let n = bridge.read(&mut buf).await.unwrap();

    assert_eq!(n, 0); // Should indicate EOF
}

#[async_test]
async fn test_bridge_small_reads() {
    // Simulate many small reads
    let mock = MockTransport::new(vec![vec![1], vec![2], vec![3], vec![4], vec![5]]);

    let mut bridge = TransportBridge::new(Box::new(mock));

    let mut buf = [0u8; 5];
    bridge.read_exact(&mut buf).await.unwrap();

    assert_eq!(&buf, &[1, 2, 3, 4, 5]);
}
