//! Tests for the `stun` module: the binding codec (against hand-computed
//! wire bytes, not just its own output) and live probe/server round-trips
//! over loopback.

#![cfg(not(target_arch = "wasm32"))]

use std::net::SocketAddr;
use std::time::Duration;

use ego_transport::stun::{
    HEADER_LEN, MAGIC_COOKIE, NatMapping, ProbeConfig, StunError, StunMessage, StunServer,
    TransactionId, decode, encode_binding_request, encode_binding_success, normalize_server, probe,
    probe_with,
};

const TXID: TransactionId = TransactionId::from_bytes([
    0xB7, 0xE7, 0xA7, 0x01, 0xBC, 0x34, 0xD6, 0x86, 0xFA, 0x87, 0xDF, 0xAE,
]);

/// 192.0.2.1:32853 in XOR-MAPPED-ADDRESS form, computed by hand:
/// port 0x8055 ^ 0x2112 = 0xA147, and 192.0.2.1 (C0 00 02 01) XOR the magic
/// cookie (21 12 A4 42) = E1 12 A6 43.
const XOR_VALUE_192_0_2_1: [u8; 8] = [0x00, 0x01, 0xA1, 0x47, 0xE1, 0x12, 0xA6, 0x43];

fn sample_addr() -> SocketAddr {
    "192.0.2.1:32853".parse().unwrap()
}

fn probe_config(bind: &str) -> ProbeConfig {
    ProbeConfig {
        attempts: 3,
        initial_timeout: Duration::from_millis(250),
        bind_addr: bind.parse().unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

#[test]
fn binding_request_round_trips() {
    let wire = encode_binding_request(&TXID);
    assert_eq!(wire.len(), HEADER_LEN);
    assert_eq!(u16::from_be_bytes([wire[0], wire[1]]), 0x0001);
    // No attributes on a bare binding request.
    assert_eq!(u16::from_be_bytes([wire[2], wire[3]]), 0);
    assert_eq!(
        u32::from_be_bytes([wire[4], wire[5], wire[6], wire[7]]),
        MAGIC_COOKIE
    );

    match decode(&wire).unwrap() {
        StunMessage::BindingRequest { txid } => assert_eq!(txid, TXID),
        other => panic!("expected a binding request, got {other:?}"),
    }
}

#[test]
fn xor_mapped_address_matches_hand_computed_bytes() {
    let wire = encode_binding_success(&TXID, sample_addr());

    // Header, then one 8-byte XOR-MAPPED-ADDRESS attribute.
    assert_eq!(u16::from_be_bytes([wire[0], wire[1]]), 0x0101);
    assert_eq!(u16::from_be_bytes([wire[2], wire[3]]) as usize, 12);
    assert_eq!(
        u16::from_be_bytes([wire[HEADER_LEN], wire[HEADER_LEN + 1]]),
        0x0020
    );
    assert_eq!(&wire[HEADER_LEN + 4..], &XOR_VALUE_192_0_2_1);

    match decode(&wire).unwrap() {
        StunMessage::BindingSuccess { txid, mapped } => {
            assert_eq!(txid, TXID);
            assert_eq!(mapped, sample_addr());
        }
        other => panic!("expected a success response, got {other:?}"),
    }
}

#[test]
fn ipv6_mapped_address_round_trips() {
    let addr: SocketAddr = "[2001:db8::1]:9000".parse().unwrap();
    let wire = encode_binding_success(&TXID, addr);
    match decode(&wire).unwrap() {
        StunMessage::BindingSuccess { mapped, .. } => assert_eq!(mapped, addr),
        other => panic!("expected a success response, got {other:?}"),
    }

    // The IPv6 XOR key includes the transaction id, so decoding under a
    // different id must not yield the same address.
    let mut forged = wire.clone();
    forged[8] ^= 0xFF;
    match decode(&forged).unwrap() {
        StunMessage::BindingSuccess { mapped, .. } => assert_ne!(mapped, addr),
        other => panic!("expected a success response, got {other:?}"),
    }
}

#[test]
fn unknown_attributes_are_skipped_with_correct_padding() {
    // SOFTWARE ("ego", 3 bytes + 1 byte of padding) ahead of the address, so
    // finding the address at all requires walking the padding correctly.
    let mut wire = Vec::new();
    wire.extend_from_slice(&0x0101u16.to_be_bytes());
    wire.extend_from_slice(&20u16.to_be_bytes());
    wire.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    wire.extend_from_slice(TXID.as_bytes());
    wire.extend_from_slice(&0x8022u16.to_be_bytes()); // SOFTWARE
    wire.extend_from_slice(&3u16.to_be_bytes());
    wire.extend_from_slice(b"ego");
    wire.push(0); // padding to a 4-byte boundary
    wire.extend_from_slice(&0x0020u16.to_be_bytes()); // XOR-MAPPED-ADDRESS
    wire.extend_from_slice(&8u16.to_be_bytes());
    wire.extend_from_slice(&XOR_VALUE_192_0_2_1);

    match decode(&wire).unwrap() {
        StunMessage::BindingSuccess { mapped, .. } => assert_eq!(mapped, sample_addr()),
        other => panic!("expected a success response, got {other:?}"),
    }
}

#[test]
fn legacy_mapped_address_is_accepted() {
    let mut wire = Vec::new();
    wire.extend_from_slice(&0x0101u16.to_be_bytes());
    wire.extend_from_slice(&12u16.to_be_bytes());
    wire.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    wire.extend_from_slice(TXID.as_bytes());
    wire.extend_from_slice(&0x0001u16.to_be_bytes()); // MAPPED-ADDRESS
    wire.extend_from_slice(&8u16.to_be_bytes());
    wire.extend_from_slice(&[0x00, 0x01]);
    wire.extend_from_slice(&32853u16.to_be_bytes()); // not XOR'd
    wire.extend_from_slice(&[192, 0, 2, 1]);

    match decode(&wire).unwrap() {
        StunMessage::BindingSuccess { mapped, .. } => assert_eq!(mapped, sample_addr()),
        other => panic!("expected a success response, got {other:?}"),
    }
}

#[test]
fn malformed_messages_are_rejected() {
    assert!(matches!(
        decode(&[0u8; 8]),
        Err(StunError::Malformed("shorter than the 20-byte header"))
    ));

    // Wrong magic cookie.
    let mut wire = encode_binding_request(&TXID);
    wire[4] ^= 0xFF;
    assert!(matches!(
        decode(&wire),
        Err(StunError::Malformed("wrong magic cookie"))
    ));

    // Leading two bits set.
    let mut wire = encode_binding_request(&TXID);
    wire[0] |= 0xC0;
    assert!(matches!(
        decode(&wire),
        Err(StunError::Malformed("leading two bits are not zero"))
    ));

    // Attribute length that is not a multiple of 4.
    let mut wire = encode_binding_request(&TXID).to_vec();
    wire[3] = 5;
    assert!(matches!(
        decode(&wire),
        Err(StunError::Malformed(
            "attribute length is not a multiple of 4"
        ))
    ));

    // Announced attributes that are not actually present.
    let mut wire = encode_binding_request(&TXID).to_vec();
    wire[3] = 8;
    assert!(matches!(
        decode(&wire),
        Err(StunError::Malformed("truncated attributes"))
    ));

    // A success response with no address in it.
    let mut wire = encode_binding_request(&TXID).to_vec();
    wire[0..2].copy_from_slice(&0x0101u16.to_be_bytes());
    assert!(matches!(decode(&wire), Err(StunError::NoMappedAddress)));
}

#[test]
fn server_addresses_are_normalized() {
    assert_eq!(
        normalize_server("stun:example.net:19302"),
        "example.net:19302"
    );
    assert_eq!(normalize_server("stuns:example.net"), "example.net:3478");
    assert_eq!(normalize_server("example.net"), "example.net:3478");
    assert_eq!(normalize_server("192.0.2.1:3478"), "192.0.2.1:3478");
    assert_eq!(normalize_server("[2001:db8::1]:3478"), "[2001:db8::1]:3478");
    assert_eq!(normalize_server("[2001:db8::1]"), "[2001:db8::1]:3478");
}

// ---------------------------------------------------------------------------
// Live probe and server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_reports_the_address_it_sees() {
    let server = StunServer::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr();
    let metrics = server.metrics();
    server.spawn();

    let result = probe_with(&server_addr.to_string(), &probe_config("127.0.0.1:0"))
        .await
        .unwrap();

    // On loopback there is no NAT, so the address the server saw is exactly
    // the socket's own address.
    assert_eq!(result.reflexive, result.local);
    assert!(!result.is_natted());
    assert_eq!(result.server, server_addr);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.requests, 1);
    assert_eq!(snapshot.responses, 1);
    assert_eq!(snapshot.dropped, 0);
    assert!(snapshot.bytes_in >= HEADER_LEN as u64);
    assert!(snapshot.bytes_out > 0);
}

#[tokio::test]
async fn stun_urls_are_accepted_as_server_addresses() {
    let server = StunServer::bind("127.0.0.1:0").await.unwrap();
    let url = format!("stun:{}", server.local_addr());
    server.spawn();

    let result = probe_with(&url, &probe_config("127.0.0.1:0"))
        .await
        .unwrap();
    assert_eq!(result.reflexive, result.local);
}

#[tokio::test]
async fn agreeing_servers_mean_a_punchable_mapping() {
    let a = StunServer::bind("127.0.0.1:0").await.unwrap();
    let b = StunServer::bind("127.0.0.1:0").await.unwrap();
    let (addr_a, addr_b) = (a.local_addr().to_string(), b.local_addr().to_string());
    a.spawn();
    b.spawn();

    let report =
        ego_transport::stun::detect_mapping(&[&addr_a, &addr_b], &probe_config("127.0.0.1:0"))
            .await
            .unwrap();

    // Both servers see the same socket at the same address, and with a
    // concrete local bind that address is the socket's own: no NAT at all.
    assert_eq!(report.mapping, NatMapping::Open);
    assert!(report.mapping.hole_punching_viable());
    assert_eq!(report.probes.len(), 2);
    assert_eq!(report.reflexive(), Some(report.local));
}

#[tokio::test]
async fn mapping_detection_needs_two_servers() {
    let err = ego_transport::stun::detect_mapping(&["127.0.0.1:3478"], &ProbeConfig::default())
        .await
        .unwrap_err();
    assert!(matches!(err, StunError::NotEnoughServers(1)));
}

#[test]
fn symmetric_mappings_advertise_no_reflexive_address() {
    // A symmetric NAT hands out a different mapping per destination, so no
    // single reflexive address is worth advertising and punching is off.
    assert!(!NatMapping::EndpointDependent.hole_punching_viable());
    assert!(NatMapping::EndpointIndependent.hole_punching_viable());
    assert!(NatMapping::Open.hole_punching_viable());
}

#[tokio::test]
async fn a_silent_server_times_out() {
    // Bind a socket and never answer on it.
    let dead = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();

    let config = ProbeConfig {
        attempts: 2,
        initial_timeout: Duration::from_millis(50),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
    };
    let err = probe_with(&dead_addr, &config).await.unwrap_err();
    match err {
        StunError::Timeout { attempts, .. } => assert_eq!(attempts, 2),
        other => panic!("expected a timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn the_server_never_answers_a_non_stun_datagram() {
    let server = StunServer::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr();
    let metrics = server.metrics();
    server.spawn();

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client
        .send_to(b"definitely not stun", server_addr)
        .await
        .unwrap();

    // Silence is the correct response: replying to junk would make the
    // server a reflector.
    let mut buf = [0u8; 512];
    let reply = tokio::time::timeout(Duration::from_millis(250), client.recv_from(&mut buf)).await;
    assert!(reply.is_err(), "server should not have answered");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.dropped, 1);
    assert_eq!(snapshot.responses, 0);
}

#[tokio::test]
async fn an_unresolvable_server_is_reported_as_such() {
    let err = probe("this-host-does-not-exist.invalid:3478")
        .await
        .unwrap_err();
    assert!(matches!(err, StunError::Resolve(_)), "got {err:?}");
}
