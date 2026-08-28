//! A message larger than the caller's buffer must arrive whole.
//!
//! Message-oriented transports (WebSocket, WebRTC data channels) receive a
//! complete message at a time, but `recv` hands back a fixed buffer. Copying
//! only what fits and dropping the rest is not a short read — it is silent
//! data loss, and it desyncs anything that treats the transport as a byte
//! stream, `FramedTransport` included.
//!
//! These tests read with buffers deliberately smaller than the messages sent.

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use ego_transport::framing::FramedTransport;
use ego_transport::platform::server::{AutoDetectListener, Listener};
use ego_transport::platform::ws_native::WebSocketNative;
use ego_transport::transport::{Transport, TransportError};

/// A payload that is not a repeating byte, so a mis-ordered or dropped
/// chunk cannot pass an equality check by luck.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Echo every message back, reading with a small buffer so the server side
/// exercises reassembly too.
async fn spawn_ws_echo() -> String {
    let listener = AutoDetectListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok(mut transport) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = transport.recv(&mut buf).await {
                    if n == 0 || transport.send(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn a_websocket_message_survives_a_smaller_read_buffer() {
    let url = spawn_ws_echo().await;
    let mut ws = WebSocketNative::connect(&url).await.unwrap();

    // One 200 KB message read through a 1 KB window.
    let sent = payload(200 * 1024);
    ws.send(&sent).await.unwrap();

    let mut received = Vec::new();
    let mut buf = [0u8; 1024];
    while received.len() < sent.len() {
        let n = tokio::time::timeout(Duration::from_secs(10), ws.recv(&mut buf))
            .await
            .expect("timed out reassembling the message")
            .unwrap();
        assert_ne!(n, 0, "recv returned 0 with bytes still outstanding");
        received.extend_from_slice(&buf[..n]);
    }

    assert_eq!(received.len(), sent.len(), "message was truncated");
    assert_eq!(received, sent, "message was corrupted or reordered");
}

#[tokio::test]
async fn message_boundaries_do_not_bleed_into_each_other() {
    let url = spawn_ws_echo().await;
    let mut ws = WebSocketNative::connect(&url).await.unwrap();

    // A large message followed by a small one: the small one must not be
    // consumed while the large one's tail is still outstanding.
    let big = payload(100 * 1024);
    let small = b"after the big one".to_vec();
    ws.send(&big).await.unwrap();
    ws.send(&small).await.unwrap();

    let mut received = Vec::new();
    let mut buf = [0u8; 8192];
    while received.len() < big.len() + small.len() {
        let n = tokio::time::timeout(Duration::from_secs(10), ws.recv(&mut buf))
            .await
            .expect("timed out")
            .unwrap();
        received.extend_from_slice(&buf[..n]);
    }

    assert_eq!(&received[..big.len()], &big[..], "first message corrupted");
    assert_eq!(
        &received[big.len()..],
        &small[..],
        "second message corrupted"
    );
}

#[tokio::test]
async fn framed_msgpack_sized_payloads_round_trip_over_a_websocket() {
    let url = spawn_ws_echo().await;
    let ws = WebSocketNative::connect(&url).await.unwrap();
    let mut framed = FramedTransport::new(ws);

    // FramedTransport reads in 64 KB chunks while allowing frames up to
    // 16 MB, so any frame past 64 KB is exactly the case that used to lose
    // its tail and desync the stream.
    for size in [1024, 64 * 1024, 200 * 1024, 1024 * 1024] {
        let sent = payload(size);
        framed.send_frame(&sent).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(20), framed.recv_frame())
            .await
            .unwrap_or_else(|_| panic!("timed out on a {size}-byte frame"))
            .unwrap_or_else(|e| panic!("a {size}-byte frame failed: {e}"));
        assert_eq!(got.len(), sent.len(), "{size}-byte frame changed length");
        assert_eq!(got, sent, "{size}-byte frame came back corrupted");
    }

    // The stream is still in sync afterwards.
    framed.send_frame(b"still aligned").await.unwrap();
    assert_eq!(framed.recv_frame().await.unwrap(), b"still aligned");
}

#[tokio::test]
async fn a_zero_length_buffer_does_not_consume_a_message() -> Result<(), TransportError> {
    let url = spawn_ws_echo().await;
    let mut ws = WebSocketNative::connect(&url).await.unwrap();

    let sent = payload(4096);
    ws.send(&sent).await?;

    let mut received = Vec::new();
    let mut buf = [0u8; 512];
    while received.len() < sent.len() {
        let n = tokio::time::timeout(Duration::from_secs(10), ws.recv(&mut buf))
            .await
            .expect("timed out")?;
        received.extend_from_slice(&buf[..n]);
    }
    assert_eq!(received, sent);
    Ok(())
}
