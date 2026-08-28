//! Tests for path observability: whether a live connection punched through
//! or is quietly paying relay latency.
//!
//! The relayed case is exercised against a real TURN server from this crate,
//! with ICE forced to relay-only, so it proves both halves at once — the path
//! is reported correctly *and* the relay actually carries WebRTC traffic.

#![cfg(not(target_arch = "wasm32"))]

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use ego_transport::path::{CandidateKind, PathKind};
use ego_transport::platform::server::AutoDetectListener;
use ego_transport::transport::rtc_signaling::{
    IceServerConfig, IceTransportPolicy, PeerRole, RtcOptions, SignalingMessage,
};
use ego_transport::transport::{Transport, connect_p2p_with};
use ego_transport::turn::{TurnCredentials, TurnServer, TurnServerConfig};

const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// A minimal two-peer signaling relay on an ephemeral port, returning its
/// ws:// URL.
///
/// Deliberately not the embedded `SignalingHub`: that path does not currently
/// deliver ICE candidates to real WebRTC peers (SDP arrives, candidates do
/// not, and ICE fails with no candidate pairs). This mirrors the relay in
/// `src/bin/test_p2p.rs`, which is the setup known to complete a native
/// peer-to-peer connection.
async fn signaling_server(room: &'static str) -> String {
    ego_platform::init();
    let listener = AutoDetectListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener
        .local_addr()
        .expect("listener should report its address");

    tokio::spawn(async move {
        use ego_transport::platform::server::Listener;

        // Pair the first two peers to arrive, then pump between them.
        let mut a = match listener.accept().await {
            Ok(t) => t,
            Err(_) => return,
        };
        let mut buf_a = [0u8; 65536];
        let mut buf_b = [0u8; 65536];
        if a.recv(&mut buf_a).await.is_err() {
            return; // JOIN from the first peer
        }
        let mut b = match listener.accept().await {
            Ok(t) => t,
            Err(_) => return,
        };
        if b.recv(&mut buf_b).await.is_err() {
            return; // JOIN from the second peer
        }

        let ready_a = SignalingMessage::ready(room, PeerRole::Offerer).serialize();
        let ready_b = SignalingMessage::ready(room, PeerRole::Answerer).serialize();
        if a.send(ready_a.as_bytes()).await.is_err() || b.send(ready_b.as_bytes()).await.is_err() {
            return;
        }

        // Everything after the handshake — offers, answers, ICE candidates —
        // is relayed verbatim in both directions.
        loop {
            tokio::select! {
                result = a.recv(&mut buf_a) => {
                    let Ok(n) = result else { break };
                    if b.send(&buf_a[..n]).await.is_err() { break }
                }
                result = b.recv(&mut buf_b) => {
                    let Ok(n) = result else { break };
                    if a.send(&buf_b[..n]).await.is_err() { break }
                }
            }
        }
    });

    format!("ws://{addr}")
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

#[test]
fn a_relay_on_either_end_makes_the_whole_path_relayed() {
    use ego_transport::path::PathInfo;

    // One relayed end is enough: every byte is forwarded either way.
    for (local, remote) in [
        (CandidateKind::Relayed, CandidateKind::Host),
        (CandidateKind::Host, CandidateKind::Relayed),
        (CandidateKind::Relayed, CandidateKind::Relayed),
        (CandidateKind::Relayed, CandidateKind::ServerReflexive),
    ] {
        let info = PathInfo::from_candidates(local, remote);
        assert_eq!(info.kind, PathKind::Relayed, "{local} <-> {remote}");
        assert!(info.is_relayed());
        assert!(!info.is_peer_to_peer());
    }

    // Two host candidates are a direct path; a reflexive one means a NAT was
    // punched, and both still flow peer to peer.
    let direct = PathInfo::from_candidates(CandidateKind::Host, CandidateKind::Host);
    assert_eq!(direct.kind, PathKind::Direct);
    assert!(direct.is_peer_to_peer());

    for (local, remote) in [
        (CandidateKind::ServerReflexive, CandidateKind::Host),
        (CandidateKind::Host, CandidateKind::PeerReflexive),
        (
            CandidateKind::ServerReflexive,
            CandidateKind::ServerReflexive,
        ),
    ] {
        let info = PathInfo::from_candidates(local, remote);
        assert_eq!(info.kind, PathKind::Punched, "{local} <-> {remote}");
        assert!(info.is_peer_to_peer());
        assert!(!info.is_relayed());
    }

    // An unreadable end is reported as unknown rather than guessed at.
    let unknown = PathInfo::from_candidates(CandidateKind::Host, CandidateKind::Unknown);
    assert_eq!(unknown.kind, PathKind::Unknown);
    assert!(!unknown.is_peer_to_peer());
    assert!(!unknown.is_relayed());
}

#[test]
fn candidate_kinds_parse_from_ice_names() {
    assert_eq!(CandidateKind::from_ice_str("host"), CandidateKind::Host);
    assert_eq!(
        CandidateKind::from_ice_str("srflx"),
        CandidateKind::ServerReflexive
    );
    assert_eq!(
        CandidateKind::from_ice_str("prflx"),
        CandidateKind::PeerReflexive
    );
    assert_eq!(CandidateKind::from_ice_str("relay"), CandidateKind::Relayed);
    assert_eq!(
        CandidateKind::from_ice_str("something-else"),
        CandidateKind::Unknown
    );
    // Round-trips through the ICE spelling.
    for kind in [
        CandidateKind::Host,
        CandidateKind::ServerReflexive,
        CandidateKind::PeerReflexive,
        CandidateKind::Relayed,
    ] {
        assert_eq!(CandidateKind::from_ice_str(kind.as_str()), kind);
    }
}

// ---------------------------------------------------------------------------
// Transports with no path to report
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_plain_connection_reports_no_path() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = listener.accept().await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    // TCP goes where it was dialed: there is no candidate pair, and saying
    // "direct" would be a guess about everything in between.
    let transport = ego_transport::transport::connect(&addr.to_string())
        .await
        .unwrap();
    assert!(transport.path().await.is_none());
}

// ---------------------------------------------------------------------------
// Live WebRTC paths
// ---------------------------------------------------------------------------

/// Verifies a peer-to-peer path end to end, with no relay configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_loopback_peers_report_a_direct_path() {
    let room = "path-direct";
    let url = signaling_server(room).await;

    let a: tokio::task::JoinHandle<Result<Box<dyn Transport>, _>> = {
        let url = url.clone();
        tokio::spawn(async move {
            connect_p2p_with(&url, room, &[], RtcOptions::default().including_loopback()).await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b: tokio::task::JoinHandle<Result<Box<dyn Transport>, _>> = {
        let url = url.clone();
        tokio::spawn(async move {
            connect_p2p_with(&url, room, &[], RtcOptions::default().including_loopback()).await
        })
    };

    let mut peer_a = a.await.unwrap().expect("peer A failed to connect");
    let mut peer_b = b.await.unwrap().expect("peer B failed to connect");

    // Confirm the connection actually carries data before judging its path.
    peer_a.send(b"ping").await.unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), peer_b.recv(&mut buf))
        .await
        .expect("peer B never received")
        .unwrap();
    assert_eq!(&buf[..n], b"ping");

    // No relay is configured, so whatever ICE picked must be peer-to-peer.
    // Which flavour it is depends on timing: a peer that receives a binding
    // request from an address it has not been told about learns it as
    // peer-reflexive, so `host <-> prflx` (Punched) is as legitimate an
    // outcome here as `host <-> host` (Direct). What matters is that no
    // relay is involved.
    let path = peer_a
        .path()
        .await
        .expect("a WebRTC transport must report a path");
    assert!(
        path.is_peer_to_peer(),
        "expected a peer-to-peer path, got {path}"
    );
    assert!(!path.is_relayed(), "no relay was configured, got {path}");
    assert!(
        matches!(path.kind, PathKind::Direct | PathKind::Punched),
        "unexpected path kind: {path}"
    );
    assert_ne!(path.local, CandidateKind::Relayed);
    assert_ne!(path.remote, CandidateKind::Relayed);
    assert!(path.local_addr.is_some(), "path should name its addresses");
    assert!(path.remote_addr.is_some());
}

/// The claim this whole feature exists to support: a connection that works
/// but is quietly relayed says so — verified against a real TURN server from
/// this crate, with ICE forced to relay-only. Doubles as the end-to-end proof
/// that the relay carries real WebRTC traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_only_connection_reports_a_relayed_path() {
    // A real relay from this crate, carrying real WebRTC traffic.
    let mut turn_config = TurnServerConfig::new(
        LOOPBACK,
        TurnCredentials::static_user("peer", "relay-password"),
    );
    turn_config.listen_addr = "127.0.0.1:0".to_string();
    turn_config.relay_bind_ip = "127.0.0.1".to_string();
    let turn = TurnServer::bind(turn_config).await.unwrap();
    let turn_metrics = turn.metrics();
    let ice = vec![IceServerConfig::turn(
        &turn.turn_url(),
        "peer",
        "relay-password",
    )];

    let room = "path-relayed";
    let url = signaling_server(room).await;

    // Relay-only: ICE may not use the direct route that plainly exists here.
    let a: tokio::task::JoinHandle<Result<Box<dyn Transport>, _>> = {
        let (url, ice) = (url.clone(), ice.clone());
        tokio::spawn(async move {
            connect_p2p_with(
                &url,
                room,
                &ice,
                RtcOptions::with_policy(IceTransportPolicy::RelayOnly).including_loopback(),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b: tokio::task::JoinHandle<Result<Box<dyn Transport>, _>> = {
        let (url, ice) = (url.clone(), ice.clone());
        tokio::spawn(async move {
            connect_p2p_with(
                &url,
                room,
                &ice,
                RtcOptions::with_policy(IceTransportPolicy::RelayOnly).including_loopback(),
            )
            .await
        })
    };

    let mut peer_a = a.await.unwrap().expect("peer A failed to connect");
    let mut peer_b = b.await.unwrap().expect("peer B failed to connect");

    peer_a.send(b"through the relay").await.unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), peer_b.recv(&mut buf))
        .await
        .expect("peer B never received")
        .unwrap();
    assert_eq!(&buf[..n], b"through the relay");

    // The whole point: a working connection that is quietly relayed says so.
    let path = peer_a
        .path()
        .await
        .expect("a WebRTC transport must report a path");
    assert_eq!(path.kind, PathKind::Relayed, "path was {path}");
    assert!(path.is_relayed());
    assert!(!path.is_peer_to_peer());
    assert_eq!(path.local, CandidateKind::Relayed);

    // And the relay confirms it was the one carrying the bytes.
    let snapshot = turn_metrics.snapshot();
    assert!(
        snapshot.allocations_granted > 0,
        "no allocation was made on the relay"
    );
    let allocations = turn.allocations().await.unwrap();
    assert!(
        allocations.iter().any(|a| a.relayed_bytes > 0),
        "the relay carried no traffic"
    );

    turn.close().await.unwrap();
}
