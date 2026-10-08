//! Tests for STUN over TCP and the RFC 5780 parts of the `stun` module:
//! CHANGE-REQUEST, RESPONSE-ORIGIN and OTHER-ADDRESS against hand-computed
//! wire bytes, the TCP client and server on loopback, the server's change
//! routing, and filtering detection. IPv6 runs on `::1` where the host has
//! it, and says so when it does not.

#![cfg(not(target_arch = "wasm32"))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ego_transport::stun::{
    ChangeRequest, ChangeRequestEvent, ERROR_UNKNOWN_ATTRIBUTE, HEADER_LEN, MAGIC_COOKIE,
    NatFiltering, ProbeConfig, ResponseAttributes, Rfc5780Attributes, StunError, StunMessage,
    StunServer, StunServerOptions, TransactionId, decode, decode_rfc5780, detect_filtering,
    encode_binding_error, encode_binding_request, encode_binding_request_with,
    encode_binding_success, encode_binding_success_with, message_len, probe_tcp, probe_with,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TXID: TransactionId = TransactionId::from_bytes([
    0xB7, 0xE7, 0xA7, 0x01, 0xBC, 0x34, 0xD6, 0x86, 0xFA, 0x87, 0xDF, 0xAE,
]);

fn config(bind: &str) -> ProbeConfig {
    ProbeConfig {
        attempts: 2,
        initial_timeout: Duration::from_millis(150),
        bind_addr: bind.parse().unwrap(),
    }
}

/// Whether this host can bind IPv6 loopback. CI runners can; some
/// containers cannot, and a test that needs it says it skipped.
async fn has_ipv6_loopback() -> bool {
    let ok = tokio::net::UdpSocket::bind("[::1]:0").await.is_ok();
    if !ok {
        eprintln!("skipped: no IPv6 loopback on this host");
    }
    ok
}

fn tcp_options() -> StunServerOptions {
    StunServerOptions {
        tcp: true,
        ..StunServerOptions::default()
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

#[test]
fn change_request_matches_hand_computed_bytes() {
    let wire = encode_binding_request_with(&TXID, ChangeRequest::IP_AND_PORT);
    assert_eq!(wire.len(), HEADER_LEN + 8);
    assert_eq!(&wire[..2], &[0x00, 0x01]);
    assert_eq!(&wire[2..4], &[0x00, 0x08]);
    // CHANGE-REQUEST, length 4, change IP (0x04) | change port (0x02).
    assert_eq!(
        &wire[HEADER_LEN..],
        &[0x00, 0x03, 0x00, 0x04, 0, 0, 0, 0x06]
    );

    // Still an ordinary binding request to `decode`.
    assert!(matches!(
        decode(&wire).unwrap(),
        StunMessage::BindingRequest { txid } if txid == TXID
    ));
    let extra = decode_rfc5780(&wire).unwrap();
    assert_eq!(extra.change_request, Some(ChangeRequest::IP_AND_PORT));

    let port_only = encode_binding_request_with(&TXID, ChangeRequest::PORT);
    assert_eq!(&port_only[HEADER_LEN + 4..], &[0, 0, 0, 0x02]);
    assert_eq!(
        decode_rfc5780(&port_only).unwrap().change_request,
        Some(ChangeRequest::PORT)
    );
}

#[test]
fn no_change_is_the_plain_request() {
    assert_eq!(
        encode_binding_request_with(&TXID, ChangeRequest::NONE),
        encode_binding_request(&TXID).to_vec()
    );
    assert_eq!(
        decode_rfc5780(&encode_binding_request(&TXID)).unwrap(),
        Rfc5780Attributes::default()
    );
}

#[test]
fn response_origin_and_other_address_match_hand_computed_bytes() {
    let mapped: SocketAddr = "192.0.2.1:32853".parse().unwrap();
    let origin: SocketAddr = "192.0.2.9:3478".parse().unwrap();
    let other: SocketAddr = "[2001:db8::1]:3479".parse().unwrap();
    let wire = encode_binding_success_with(
        &TXID,
        mapped,
        &ResponseAttributes {
            response_origin: Some(origin),
            other_address: Some(other),
        },
    );

    // XOR-MAPPED-ADDRESS (4 + 8), RESPONSE-ORIGIN (4 + 8), OTHER-ADDRESS
    // (4 + 20).
    assert_eq!(u16::from_be_bytes([wire[2], wire[3]]), 48);
    let origin_at = HEADER_LEN + 12;
    assert_eq!(
        &wire[origin_at..origin_at + 12],
        // 0x802B, length 8, IPv4, port 3478 (0x0D96) and the address in the
        // clear: these two attributes are not XOR'd.
        &[0x80, 0x2B, 0x00, 0x08, 0x00, 0x01, 0x0D, 0x96, 192, 0, 2, 9]
    );
    let other_at = origin_at + 12;
    assert_eq!(
        &wire[other_at..other_at + 8],
        &[0x80, 0x2C, 0x00, 0x14, 0x00, 0x02, 0x0D, 0x97]
    );
    assert_eq!(
        &wire[other_at + 8..],
        &[
            0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01
        ]
    );

    // An RFC 5389-only decoder still finds the mapped address.
    assert!(matches!(
        decode(&wire).unwrap(),
        StunMessage::BindingSuccess { txid, mapped: m } if txid == TXID && m == mapped
    ));
    let extra = decode_rfc5780(&wire).unwrap();
    assert_eq!(extra.response_origin, Some(origin));
    assert_eq!(extra.other_address, Some(other));
    assert_eq!(extra.change_request, None);
}

#[test]
fn success_without_extras_is_the_plain_success() {
    let mapped: SocketAddr = "[2001:db8::7]:40000".parse().unwrap();
    assert_eq!(
        encode_binding_success_with(&TXID, mapped, &ResponseAttributes::default()),
        encode_binding_success(&TXID, mapped)
    );
}

#[test]
fn error_420_carries_unknown_attributes() {
    let wire = encode_binding_error(&TXID, 420, "Unknown Attribute", &[0x0003]);
    assert_eq!(&wire[..2], &[0x01, 0x11]);
    // ERROR-CODE: 4 + (4 + 17 bytes of reason, padded to 24); then
    // UNKNOWN-ATTRIBUTES: 4 + (2, padded to 4).
    assert_eq!(u16::from_be_bytes([wire[2], wire[3]]), 36);
    assert_eq!(
        &wire[HEADER_LEN..HEADER_LEN + 8],
        &[0x00, 0x09, 0x00, 21, 0, 0, 4, 20]
    );
    let unknown_at = HEADER_LEN + 4 + 24;
    assert_eq!(
        &wire[unknown_at..],
        &[0x00, 0x0A, 0x00, 0x02, 0x00, 0x03, 0, 0]
    );

    match decode(&wire).unwrap() {
        StunMessage::BindingError { txid, code, reason } => {
            assert_eq!(txid, TXID);
            assert_eq!(code, 420);
            assert_eq!(reason, "Unknown Attribute");
        }
        other => panic!("expected an error response, got {other:?}"),
    }
}

#[test]
fn message_len_frames_from_the_header() {
    let wire = encode_binding_request_with(&TXID, ChangeRequest::PORT);
    assert_eq!(message_len(&wire[..HEADER_LEN]).unwrap(), wire.len());
    assert!(matches!(
        message_len(&wire[..HEADER_LEN - 1]),
        Err(StunError::Malformed(_))
    ));
    let mut bad_cookie = wire.clone();
    bad_cookie[4] ^= 0xFF;
    assert!(matches!(
        message_len(&bad_cookie),
        Err(StunError::Malformed(_))
    ));
}

#[test]
fn a_change_request_of_the_wrong_length_is_malformed() {
    let mut wire = Vec::new();
    wire.extend_from_slice(&0x0001u16.to_be_bytes());
    wire.extend_from_slice(&8u16.to_be_bytes());
    wire.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    wire.extend_from_slice(TXID.as_bytes());
    wire.extend_from_slice(&[0x00, 0x03, 0x00, 0x02, 0, 0x06, 0, 0]);
    assert!(matches!(
        decode_rfc5780(&wire),
        Err(StunError::Malformed(_))
    ));
}

// ---------------------------------------------------------------------------
// TCP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcp_server_reports_the_connection_it_sees() {
    let server = StunServer::bind_with("127.0.0.1:0", tcp_options())
        .await
        .unwrap();
    let tcp_addr = server.tcp_addr().unwrap();
    // One port, two protocols.
    assert_eq!(tcp_addr, server.local_addr());
    let metrics = server.metrics();
    server.spawn();

    let result = probe_tcp(&tcp_addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap();
    assert_eq!(result.reflexive, result.local);
    assert_eq!(result.server, tcp_addr);

    // UDP still answers on the same port.
    let udp = probe_with(&tcp_addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap();
    assert_eq!(udp.reflexive, udp.local);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.requests, 2);
    assert_eq!(snapshot.responses, 2);
    assert_eq!(snapshot.tcp_connections, 1);
}

#[tokio::test]
async fn tcp_answers_back_to_back_requests_on_one_connection() {
    let server = StunServer::bind_with("127.0.0.1:0", tcp_options())
        .await
        .unwrap();
    let addr = server.tcp_addr().unwrap();
    server.spawn();

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let local = stream.local_addr().unwrap();
    let first = TransactionId::from_bytes([1; 12]);
    let second = TransactionId::from_bytes([2; 12]);
    // Both requests in one write: the server must frame them by header.
    let mut both = encode_binding_request(&first).to_vec();
    both.extend_from_slice(&encode_binding_request(&second));
    stream.write_all(&both).await.unwrap();

    for expected in [first, second] {
        let mut header = [0u8; HEADER_LEN];
        stream.read_exact(&mut header).await.unwrap();
        let mut message = header.to_vec();
        message.resize(message_len(&header).unwrap(), 0);
        stream.read_exact(&mut message[HEADER_LEN..]).await.unwrap();
        match decode(&message).unwrap() {
            StunMessage::BindingSuccess { txid, mapped } => {
                assert_eq!(txid, expected);
                assert_eq!(mapped, local);
            }
            other => panic!("expected a success response, got {other:?}"),
        }
        assert_eq!(
            decode_rfc5780(&message).unwrap().response_origin,
            Some(addr)
        );
    }
}

#[tokio::test]
async fn tcp_refuses_a_change_request() {
    let server = StunServer::bind_with("127.0.0.1:0", tcp_options())
        .await
        .unwrap();
    let addr = server.tcp_addr().unwrap();
    server.spawn();

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&encode_binding_request_with(&TXID, ChangeRequest::PORT))
        .await
        .unwrap();
    let mut reply = vec![0u8; 512];
    let n = stream.read(&mut reply).await.unwrap();
    match decode(&reply[..n]).unwrap() {
        StunMessage::BindingError { code, .. } => assert_eq!(code, ERROR_UNKNOWN_ATTRIBUTE),
        other => panic!("expected a 420, got {other:?}"),
    }
}

#[tokio::test]
async fn tcp_closes_on_junk() {
    let server = StunServer::bind_with("127.0.0.1:0", tcp_options())
        .await
        .unwrap();
    let addr = server.tcp_addr().unwrap();
    let metrics = server.metrics();
    server.spawn();

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    // Longer than a STUN header, so the server gets as far as checking it.
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("server should close the connection")
        .unwrap_or(0);
    assert_eq!(n, 0, "server should close without answering");
    assert_eq!(metrics.snapshot().dropped, 1);
}

#[tokio::test]
async fn tcp_probe_against_a_closed_port_fails() {
    // Port 1 on loopback refuses.
    let err = probe_tcp("127.0.0.1:1", &config("127.0.0.1:0"))
        .await
        .unwrap_err();
    assert!(matches!(err, StunError::Io(_)), "got {err:?}");
}

// ---------------------------------------------------------------------------
// IPv6
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipv6_udp_and_tcp_round_trip() {
    if !has_ipv6_loopback().await {
        return;
    }
    let server = StunServer::bind_with("[::1]:0", tcp_options())
        .await
        .unwrap();
    let addr = server.local_addr().to_string();
    server.spawn();

    // The default config binds 0.0.0.0; the probe must pick the server's
    // family on its own.
    let udp = probe_with(&addr, &ProbeConfig::default()).await.unwrap();
    assert!(udp.reflexive.is_ipv6());
    assert_eq!(udp.reflexive.port(), udp.local.port());

    let tcp = probe_tcp(&addr, &ProbeConfig::default()).await.unwrap();
    assert!(tcp.reflexive.is_ipv6());
    assert_eq!(tcp.reflexive, tcp.local);
}

#[cfg(not(windows))] // Windows sockets are IPv6-only by default
#[tokio::test]
async fn a_dual_stack_server_reports_ipv4_clients_as_ipv4() {
    if !has_ipv6_loopback().await {
        return;
    }
    let server = StunServer::bind_with("[::]:0", tcp_options())
        .await
        .unwrap();
    let port = server.local_addr().port();
    server.spawn();

    let target = format!("127.0.0.1:{port}");
    let udp = probe_with(&target, &config("127.0.0.1:0")).await.unwrap();
    assert_eq!(udp.reflexive, udp.local);
    let tcp = probe_tcp(&target, &config("127.0.0.1:0")).await.unwrap();
    assert_eq!(tcp.reflexive, tcp.local);
}

// ---------------------------------------------------------------------------
// Change routing and filtering
// ---------------------------------------------------------------------------

/// A handler that only records what it was handed: as if the link to the
/// peer server were down, so change-IP requests go unanswered.
fn recording_handler() -> (
    Arc<dyn ego_transport::stun::ChangeHandler>,
    Arc<std::sync::Mutex<Vec<ChangeRequestEvent>>>,
) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    let handler = move |event: ChangeRequestEvent| log.lock().unwrap().push(event);
    (Arc::new(handler), seen)
}

#[tokio::test]
async fn the_server_advertises_its_other_address() {
    let other: SocketAddr = "192.0.2.2:3478".parse().unwrap();
    let server = StunServer::bind_with(
        "127.0.0.1:0",
        StunServerOptions {
            other_address: Some(other),
            ..StunServerOptions::default()
        },
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    server.spawn();

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client
        .send_to(&encode_binding_request(&TXID), addr)
        .await
        .unwrap();
    let mut buf = [0u8; 512];
    let (n, _) = client.recv_from(&mut buf).await.unwrap();
    let extra = decode_rfc5780(&buf[..n]).unwrap();
    assert_eq!(extra.other_address, Some(other));
    assert_eq!(extra.response_origin, Some(addr));
}

#[tokio::test]
async fn a_wildcard_server_leaves_response_origin_out_unless_told() {
    let server = StunServer::bind("0.0.0.0:0").await.unwrap();
    let port = server.local_addr().port();
    server.spawn();
    let told = StunServer::bind_with(
        "0.0.0.0:0",
        StunServerOptions {
            public_ip: Some("198.51.100.4".parse().unwrap()),
            ..StunServerOptions::default()
        },
    )
    .await
    .unwrap();
    let told_port = told.local_addr().port();
    told.spawn();

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut buf = [0u8; 512];
    client
        .send_to(&encode_binding_request(&TXID), ("127.0.0.1", port))
        .await
        .unwrap();
    let (n, _) = client.recv_from(&mut buf).await.unwrap();
    assert_eq!(decode_rfc5780(&buf[..n]).unwrap().response_origin, None);

    client
        .send_to(&encode_binding_request(&TXID), ("127.0.0.1", told_port))
        .await
        .unwrap();
    let (n, _) = client.recv_from(&mut buf).await.unwrap();
    assert_eq!(
        decode_rfc5780(&buf[..n]).unwrap().response_origin,
        Some(SocketAddr::new("198.51.100.4".parse().unwrap(), told_port))
    );
}

#[tokio::test]
async fn a_change_request_without_a_handler_gets_420() {
    let server = StunServer::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();
    let metrics = server.metrics();
    server.spawn();

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for change in [ChangeRequest::IP_AND_PORT, ChangeRequest::PORT] {
        client
            .send_to(&encode_binding_request_with(&TXID, change), addr)
            .await
            .unwrap();
        let mut buf = [0u8; 512];
        let (n, from) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(from, addr);
        match decode(&buf[..n]).unwrap() {
            StunMessage::BindingError { code, .. } => assert_eq!(code, 420),
            other => panic!("expected a 420, got {other:?}"),
        }
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.change_requests, 2);
    assert_eq!(snapshot.errors, 2);
    assert_eq!(snapshot.responses, 0);
}

#[tokio::test]
async fn change_port_is_answered_from_the_alternate_socket() {
    let server = StunServer::bind_with(
        "127.0.0.1:0",
        StunServerOptions {
            alternate_port: Some(0),
            ..StunServerOptions::default()
        },
    )
    .await
    .unwrap();
    let primary = server.local_addr();
    let alternate = server.alternate_addr().unwrap();
    assert_ne!(primary.port(), alternate.port());
    server.spawn();

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut buf = [0u8; 512];
    for (to, expect_from) in [(primary, alternate), (alternate, primary)] {
        client
            .send_to(&encode_binding_request_with(&TXID, ChangeRequest::PORT), to)
            .await
            .unwrap();
        let (n, from) = client.recv_from(&mut buf).await.unwrap();
        assert_eq!(from, expect_from);
        assert_eq!(
            decode_rfc5780(&buf[..n]).unwrap().response_origin,
            Some(expect_from)
        );
    }
}

#[tokio::test]
async fn change_ip_goes_to_the_handler_with_the_observed_client() {
    let (handler, seen) = recording_handler();
    let server = StunServer::bind_with(
        "127.0.0.1:0",
        StunServerOptions {
            change_handler: Some(handler),
            ..StunServerOptions::default()
        },
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    server.spawn();

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client
        .send_to(
            &encode_binding_request_with(&TXID, ChangeRequest::IP_AND_PORT),
            addr,
        )
        .await
        .unwrap();
    // The server itself stays silent; the answer is the peer's to send.
    let mut buf = [0u8; 512];
    let reply = tokio::time::timeout(Duration::from_millis(250), client.recv_from(&mut buf)).await;
    assert!(
        reply.is_err(),
        "server should not answer a handed-off request"
    );

    let events = seen.lock().unwrap().clone();
    assert_eq!(
        events,
        vec![ChangeRequestEvent {
            txid: TXID,
            client: client.local_addr().unwrap(),
            change: ChangeRequest::IP_AND_PORT,
        }]
    );
}

#[tokio::test]
async fn filtering_needs_other_address() {
    let server = StunServer::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr();
    server.spawn();
    let err = detect_filtering(&addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StunError::NoOtherAddress(a) if a == addr),
        "got {err:?}"
    );
}

/// A server whose change-IP requests vanish (the handler drops them), as a
/// filtering NAT would make them.
async fn server_dropping_change_ip(alternate_port: Option<u16>) -> StunServer {
    let (handler, _) = recording_handler();
    StunServer::bind_with(
        "127.0.0.1:0",
        StunServerOptions {
            alternate_port,
            other_address: Some("192.0.2.2:3478".parse().unwrap()),
            change_handler: Some(handler),
            ..StunServerOptions::default()
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn filtering_is_address_dependent_when_only_the_port_change_arrives() {
    let server = server_dropping_change_ip(Some(0)).await;
    let addr = server.local_addr();
    let alternate = server.alternate_addr().unwrap();
    server.spawn();

    let report = detect_filtering(&addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap();
    assert_eq!(report.filtering, NatFiltering::AddressDependent);
    assert_eq!(report.change_ip_reply, None);
    assert_eq!(report.change_port_reply, Some(alternate));
    assert_eq!(report.mapped, report.local);
}

#[tokio::test]
async fn filtering_is_undetermined_without_an_alternate_port() {
    let server = server_dropping_change_ip(None).await;
    let addr = server.local_addr();
    server.spawn();

    let report = detect_filtering(&addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap();
    assert_eq!(report.filtering, NatFiltering::AddressDependentOrStricter);
    assert_eq!(report.change_port_reply, None);
}

/// The two-gate deployment in miniature: gate A hands change-IP requests to
/// gate B, which answers with `answer_for`. Needs a second loopback IP, which
/// Linux has across 127.0.0.0/8 and macOS does not.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn filtering_is_endpoint_independent_when_the_peer_gate_answers() {
    let gate_b = Arc::new(StunServer::bind("127.0.0.2:0").await.unwrap());
    let b_addr = gate_b.local_addr();
    gate_b.spawn();

    let peer = gate_b.clone();
    let forward = move |event: ChangeRequestEvent| {
        let peer = peer.clone();
        tokio::spawn(async move {
            peer.answer_for(event.txid, event.client, event.change)
                .await
                .unwrap();
        });
    };
    let gate_a = StunServer::bind_with(
        "127.0.0.1:0",
        StunServerOptions {
            other_address: Some(b_addr),
            change_handler: Some(Arc::new(forward)),
            ..StunServerOptions::default()
        },
    )
    .await
    .unwrap();
    let a_addr = gate_a.local_addr();
    gate_a.spawn();

    let report = detect_filtering(&a_addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap();
    assert_eq!(report.filtering, NatFiltering::EndpointIndependent);
    assert_eq!(report.other_address, b_addr);
    assert_eq!(report.change_ip_reply, Some(b_addr));
    assert_eq!(report.change_port_reply, None);
    assert_eq!(gate_b.metrics().snapshot().responses, 1);
}

#[tokio::test]
async fn a_server_that_ignores_change_ip_is_caught() {
    // A "peer" that is really the same gate: the answer comes back from the
    // address the request went to, which says nothing about filtering.
    let slot: Arc<std::sync::OnceLock<Arc<StunServer>>> = Arc::new(std::sync::OnceLock::new());
    let me = slot.clone();
    let forward = move |event: ChangeRequestEvent| {
        let me = me.get().unwrap().clone();
        tokio::spawn(async move {
            me.answer_for(event.txid, event.client, ChangeRequest::NONE)
                .await
                .unwrap();
        });
    };
    let server = Arc::new(
        StunServer::bind_with(
            "127.0.0.1:0",
            StunServerOptions {
                other_address: Some("192.0.2.2:3478".parse().unwrap()),
                change_handler: Some(Arc::new(forward)),
                ..StunServerOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let _ = slot.set(server.clone());
    let addr = server.local_addr();
    server.spawn();

    let err = detect_filtering(&addr.to_string(), &config("127.0.0.1:0"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StunError::ChangeIgnored { from } if from == addr),
        "got {err:?}"
    );
}
