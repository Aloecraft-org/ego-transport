//! Native tests for the scheme table, endpoint parsing, framing helper, and
//! the bounded inbound buffer.

#![cfg(not(target_arch = "wasm32"))]

use ego_transport::endpoint::{Availability, Endpoint, Scheme};
use ego_transport::flow::{InboundBuffer, PushOutcome};
use ego_transport::framing::{FramedTransport, decode_frame, encode_frame};
use ego_transport::transport::{Transport, TransportError};

// ---------------------------------------------------------------------------
// In-memory transport pair for exercising helpers without sockets
// ---------------------------------------------------------------------------

struct PipeTransport {
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    pending: Vec<u8>,
}

fn pipe_pair() -> (PipeTransport, PipeTransport) {
    let (atx, arx) = tokio::sync::mpsc::channel(64);
    let (btx, brx) = tokio::sync::mpsc::channel(64);
    (
        PipeTransport {
            tx: atx,
            rx: brx,
            pending: Vec::new(),
        },
        PipeTransport {
            tx: btx,
            rx: arx,
            pending: Vec::new(),
        },
    )
}

#[async_trait::async_trait]
impl Transport for PipeTransport {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.tx
            .send(data.to_vec())
            .await
            .map_err(|_| TransportError::Closed)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        if self.pending.is_empty() {
            self.pending = self.rx.recv().await.ok_or(TransportError::Closed)?;
        }
        let n = self.pending.len().min(buf.len());
        buf[..n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Endpoint / scheme table
// ---------------------------------------------------------------------------

#[test]
fn endpoint_parses_scheme_authority_path() {
    let e = Endpoint::parse("wssc://example.net:9000/session").unwrap();
    assert_eq!(e.scheme, Scheme::Wssc);
    assert_eq!(e.authority, "example.net:9000");
    assert_eq!(e.path.as_deref(), Some("/session"));
    assert_eq!(e.to_string(), "wssc://example.net:9000/session");

    let e = Endpoint::parse("tcp://127.0.0.1:9999").unwrap();
    assert_eq!(e.scheme, Scheme::Tcp);
    assert_eq!(e.path, None);
}

#[test]
fn endpoint_rejects_malformed_addresses() {
    assert!(Endpoint::parse("no-scheme-here").is_err());
    assert!(Endpoint::parse("bogus://host:1").is_err());
    assert!(Endpoint::parse("tcp://").is_err());
}

#[test]
fn scheme_support_table_native() {
    assert!(Scheme::Tcp.support().dial.is_available());
    assert!(Scheme::Tcp.support().listen.is_available());
    assert!(Scheme::Ssh.support().dial.is_available());
    assert!(Scheme::Ssh.support().listen.is_available());
    assert!(Scheme::Wssc.support().dial.is_available());
    // Listen-side/dial-side splits are named refusals, not stubs.
    assert!(matches!(
        Scheme::Wssc.support().listen,
        Availability::Unavailable { .. }
    ));
    assert!(matches!(
        Scheme::Wssd.support().dial,
        Availability::Unavailable { .. }
    ));
}

#[test]
fn unavailable_scheme_is_a_typed_refusal() {
    let err = Scheme::Wssd.require_dial().unwrap_err();
    match err {
        TransportError::SchemeUnavailable {
            scheme,
            platform,
            operation,
            ..
        } => {
            assert_eq!(scheme, "wssd");
            assert_eq!(platform, "native");
            assert_eq!(operation, "dial");
        }
        other => panic!("expected SchemeUnavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn credentialed_schemes_refuse_bare_dial() {
    let e = Endpoint::parse("ssh://127.0.0.1:22").unwrap();
    let err = match e.dial().await {
        Ok(_) => panic!("expected a typed refusal"),
        Err(e) => e,
    };
    match err {
        TransportError::SchemeNeedsConfig { scheme: "ssh", .. } => {}
        other => panic!("expected SchemeNeedsConfig, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn frames_round_trip_across_a_pipe() {
    let (a, b) = pipe_pair();
    let mut fa = FramedTransport::new(a);
    let mut fb = FramedTransport::new(b);

    // Several frames of varied size, including empty.
    let payloads: Vec<Vec<u8>> = vec![vec![], b"hello".to_vec(), vec![0xAB; 200_000]];
    for p in &payloads {
        fa.send_frame(p).await.unwrap();
    }
    for p in &payloads {
        assert_eq!(&fb.recv_frame().await.unwrap(), p);
    }

    // And back the other way.
    fb.send_frame(b"pong").await.unwrap();
    assert_eq!(fa.recv_frame().await.unwrap(), b"pong");
}

#[tokio::test]
async fn oversized_frames_are_refused_not_buffered() {
    let (a, _b) = pipe_pair();
    let mut fa = FramedTransport::with_max_frame(a, 16);
    match fa.send_frame(&[0u8; 17]).await.unwrap_err() {
        TransportError::FrameTooLarge { len: 17, max: 16 } => {}
        other => panic!("expected FrameTooLarge, got {other:?}"),
    }

    // A hostile header announcing a huge frame is refused at decode time.
    let wire = encode_frame(&[0u8; 32], 1024).unwrap();
    match decode_frame(&wire, 16).unwrap_err() {
        TransportError::FrameTooLarge { len: 32, max: 16 } => {}
        other => panic!("expected FrameTooLarge, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Bounded inbound buffer
// ---------------------------------------------------------------------------

#[test]
fn inbound_buffer_full_is_observable_and_returns_the_message() {
    let mut buf = InboundBuffer::new(2, 1024);
    assert_eq!(buf.try_push(b"one".to_vec()), PushOutcome::Accepted);
    assert_eq!(buf.try_push(b"two".to_vec()), PushOutcome::Accepted);
    match buf.try_push(b"three".to_vec()) {
        PushOutcome::Full(msg) => assert_eq!(msg, b"three"),
        PushOutcome::Accepted => panic!("expected Full"),
    }

    let m = buf.metrics().snapshot();
    assert_eq!(m.queue_depth, 2);
    assert_eq!(m.queue_capacity, 2);
    assert_eq!(m.rejected, 1);
    assert!((m.saturation() - 1.0).abs() < f64::EPSILON);

    assert_eq!(buf.try_pop().unwrap(), b"one");
    assert_eq!(buf.try_push(b"three".to_vec()), PushOutcome::Accepted);
}

#[test]
fn inbound_buffer_byte_cap_binds_independently() {
    let mut buf = InboundBuffer::new(100, 10);
    assert_eq!(buf.try_push(vec![0u8; 8]), PushOutcome::Accepted);
    assert!(buf.would_refuse(3));
    assert!(matches!(buf.try_push(vec![0u8; 3]), PushOutcome::Full(_)));
    buf.try_pop().unwrap();
    assert_eq!(buf.try_push(vec![0u8; 3]), PushOutcome::Accepted);
    assert_eq!(buf.bytes(), 3);
}
