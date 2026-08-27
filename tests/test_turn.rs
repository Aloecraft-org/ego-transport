//! Tests for the `turn` relay server, driven by an independent TURN client
//! implementation rather than by our own code.
//!
//! The relay round-trip uses a plain UDP socket as the far peer: the client
//! sends through its allocation first, which installs the permission, and the
//! peer's reply comes back through the same allocation. That exercises
//! allocation, permissions and relaying end to end without depending on two
//! allocations racing to install permissions for each other.

#![cfg(not(target_arch = "wasm32"))]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ego_transport::transport::rtc_signaling::IceServerConfig;
use ego_transport::turn::{
    ClosedAllocation, TurnCredentials, TurnError, TurnServer, TurnServerConfig,
    credential_principal, ephemeral_credentials, ephemeral_credentials_for,
};
use tokio::net::UdpSocket;
use turn::client::{Client, ClientConfig};
use webrtc_util::Conn;

const REALM: &str = "ego-test";
const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn config(credentials: TurnCredentials) -> TurnServerConfig {
    TurnServerConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        relay_address: LOOPBACK,
        relay_bind_ip: "127.0.0.1".to_string(),
        realm: REALM.to_string(),
        credentials,
        max_allocations: 8,
        channel_bind_timeout: Duration::from_secs(60),
        on_allocation_closed: None,
    }
}

async fn turn_client(
    server_addr: SocketAddr,
    username: &str,
    password: &str,
) -> Result<Client, turn::Error> {
    let conn = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let client = Client::new(ClientConfig {
        stun_serv_addr: server_addr.to_string(),
        turn_serv_addr: server_addr.to_string(),
        username: username.to_string(),
        password: password.to_string(),
        realm: REALM.to_string(),
        software: String::new(),
        rto_in_ms: 200,
        conn,
        vnet: None,
    })
    .await?;
    client.listen().await?;
    Ok(client)
}

/// Assert that an allocation does not succeed, without waiting on the
/// client's full retry budget.
async fn assert_allocation_refused(client: &Client) {
    // An explicit refusal and a client still retrying against a server that
    // keeps saying no both mean "not allocated"; only success is a failure.
    if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(5), client.allocate()).await {
        panic!("allocation should have been refused");
    }
}

// ---------------------------------------------------------------------------
// Refusing to be an open relay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_server_with_no_credentials_refuses_to_bind() {
    let err = TurnServer::bind(config(TurnCredentials::Static(Default::default())))
        .await
        .err()
        .expect("an empty credential set must not bind");
    assert!(matches!(err, TurnError::NoCredentials), "got {err:?}");

    let err = TurnServer::bind(config(TurnCredentials::Ephemeral {
        shared_secret: String::new(),
    }))
    .await
    .err()
    .expect("an empty shared secret must not bind");
    assert!(matches!(err, TurnError::NoCredentials), "got {err:?}");
}

#[tokio::test]
async fn credentials_are_never_printed_by_debug() {
    let creds = TurnCredentials::Ephemeral {
        shared_secret: "hunter2-super-secret".to_string(),
    };
    let rendered = format!("{creds:?}");
    assert!(!rendered.contains("hunter2"), "secret leaked: {rendered}");

    let rendered = format!("{:?}", TurnCredentials::static_user("alice", "s3kr1t"));
    assert!(!rendered.contains("s3kr1t"), "password leaked: {rendered}");
}

// ---------------------------------------------------------------------------
// Relaying
// ---------------------------------------------------------------------------

#[tokio::test]
async fn traffic_relays_through_an_allocation() {
    let server = TurnServer::bind(config(TurnCredentials::static_user("alice", "pw")))
        .await
        .unwrap();
    let metrics = server.metrics();

    let client = turn_client(server.local_addr(), "alice", "pw")
        .await
        .unwrap();
    let relay = client.allocate().await.unwrap();
    let relay_addr = relay.local_addr().unwrap();

    // The far peer is an ordinary UDP socket that echoes what it receives.
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 256];
        while let Ok((n, from)) = peer.recv_from(&mut buf).await {
            let _ = peer.send_to(&buf[..n], from).await;
        }
    });

    // Sending through the allocation installs the permission for the peer,
    // so its reply is allowed back through.
    relay
        .send_to(b"through the relay", peer_addr)
        .await
        .unwrap();

    let mut buf = [0u8; 256];
    let (n, from) = tokio::time::timeout(Duration::from_secs(5), relay.recv_from(&mut buf))
        .await
        .expect("relayed reply timed out")
        .unwrap();
    assert_eq!(&buf[..n], b"through the relay");
    assert_eq!(from, peer_addr);

    // The relay address handed to peers is the configured one, not the
    // client's own address.
    assert_eq!(relay_addr.ip(), LOOPBACK);

    let allocations = server.allocations().await.unwrap();
    assert_eq!(allocations.len(), 1);
    assert_eq!(allocations[0].username, "alice");
    assert_eq!(allocations[0].relay_addr, relay_addr);
    assert!(allocations[0].relayed_bytes > 0);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.live_allocations, 1);
    assert_eq!(snapshot.allocations_granted, 1);
    assert_eq!(snapshot.allocations_refused, 0);
    assert!(snapshot.auth_ok > 0);
    assert!(snapshot.saturation() > 0.0);

    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn a_turn_url_is_ready_for_ice() {
    let server = TurnServer::bind(config(TurnCredentials::static_user("alice", "pw")))
        .await
        .unwrap();

    let url = server.turn_url();
    assert_eq!(
        url,
        format!("turn:127.0.0.1:{}", server.local_addr().port())
    );
    assert_eq!(server.realm(), REALM);

    // The URL drops straight into the WebRTC ICE configuration.
    let ice = IceServerConfig::turn(&url, "alice", "pw");
    assert_eq!(ice.urls, vec![url]);
    assert_eq!(ice.username.as_deref(), Some("alice"));

    server.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_wrong_password_is_refused() {
    let server = TurnServer::bind(config(TurnCredentials::static_user("alice", "pw")))
        .await
        .unwrap();
    let metrics = server.metrics();

    let client = turn_client(server.local_addr(), "alice", "not-the-password")
        .await
        .unwrap();
    assert_allocation_refused(&client).await;
    assert_eq!(metrics.snapshot().live_allocations, 0);

    // An unknown user is refused by our handler, which the counter records.
    let stranger = turn_client(server.local_addr(), "mallory", "pw")
        .await
        .unwrap();
    assert_allocation_refused(&stranger).await;
    assert!(metrics.snapshot().auth_refused > 0);
    assert_eq!(metrics.snapshot().allocations_granted, 0);

    server.close().await.unwrap();
}

#[tokio::test]
async fn ephemeral_credentials_are_accepted_and_forgeries_are_not() {
    let secret = "shared-secret-between-coordinator-and-relay";
    let server = TurnServer::bind(config(TurnCredentials::Ephemeral {
        shared_secret: secret.to_string(),
    }))
    .await
    .unwrap();
    let metrics = server.metrics();

    // A coordinator holding the secret mints a short-lived grant; the server
    // keeps no per-user state.
    let (username, password) = ephemeral_credentials(secret, Duration::from_secs(300)).unwrap();
    let client = turn_client(server.local_addr(), &username, &password)
        .await
        .unwrap();
    let relay = client.allocate().await.unwrap();
    assert_eq!(relay.local_addr().unwrap().ip(), LOOPBACK);
    assert_eq!(metrics.snapshot().allocations_granted, 1);
    client.close().await.unwrap();

    // The same username with a made-up password fails the HMAC check.
    let forger = turn_client(server.local_addr(), &username, "made-up")
        .await
        .unwrap();
    assert_allocation_refused(&forger).await;

    // A username that is not a timestamp at all is refused too.
    let nonsense = turn_client(server.local_addr(), "not-a-timestamp", "whatever")
        .await
        .unwrap();
    assert_allocation_refused(&nonsense).await;

    assert!(metrics.snapshot().auth_refused > 0);
    assert_eq!(metrics.snapshot().allocations_granted, 1);

    server.close().await.unwrap();
}

#[tokio::test]
async fn a_verifier_decides_who_may_relay() {
    let saw_expected_user = Arc::new(AtomicBool::new(false));
    let seen = saw_expected_user.clone();

    let credentials =
        TurnCredentials::Verifier(Arc::new(move |username, realm, src: SocketAddr| {
            // The consumer's own model decides; here, one name is admitted.
            assert_eq!(realm, REALM);
            assert!(src.ip().is_loopback());
            if username == "admitted" {
                seen.store(true, Ordering::SeqCst);
                Some("pw".to_string())
            } else {
                None
            }
        }));

    let server = TurnServer::bind(config(credentials)).await.unwrap();
    let metrics = server.metrics();

    let refused = turn_client(server.local_addr(), "rejected", "pw")
        .await
        .unwrap();
    assert_allocation_refused(&refused).await;

    let allowed = turn_client(server.local_addr(), "admitted", "pw")
        .await
        .unwrap();
    allowed.allocate().await.unwrap();

    assert!(saw_expected_user.load(Ordering::SeqCst));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.allocations_granted, 1);
    assert!(snapshot.auth_refused > 0);

    allowed.close().await.unwrap();
    server.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Quota and revocation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn allocations_past_the_cap_are_refused_not_queued() {
    let mut cfg = config(TurnCredentials::static_user("alice", "pw"));
    cfg.max_allocations = 1;
    let server = TurnServer::bind(cfg).await.unwrap();
    let metrics = server.metrics();

    let first = turn_client(server.local_addr(), "alice", "pw")
        .await
        .unwrap();
    first.allocate().await.unwrap();
    assert_eq!(metrics.snapshot().live_allocations, 1);
    assert_eq!(metrics.snapshot().saturation(), 1.0);

    // The cap is a refusal, not a wait: the second client is turned away
    // rather than parked until a slot frees.
    let second = turn_client(server.local_addr(), "alice", "pw")
        .await
        .unwrap();
    assert_allocation_refused(&second).await;

    let snapshot = metrics.snapshot();
    assert!(snapshot.allocations_refused > 0, "refusal was not recorded");
    assert_eq!(
        snapshot.live_allocations, 1,
        "a refusal must not leak a slot"
    );
    assert_eq!(snapshot.allocations_granted, 1);

    first.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn revoking_a_principal_drops_its_allocations() {
    let server = TurnServer::bind(config(TurnCredentials::static_user("alice", "pw")))
        .await
        .unwrap();
    let metrics = server.metrics();

    let client = turn_client(server.local_addr(), "alice", "pw")
        .await
        .unwrap();
    client.allocate().await.unwrap();
    assert_eq!(server.allocations().await.unwrap().len(), 1);

    server.revoke("alice").await.unwrap();

    // The close notification releases the quota slot as well.
    let mut released = false;
    for _ in 0..50 {
        if metrics.snapshot().live_allocations == 0 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(released, "revocation did not release the allocation slot");
    assert!(server.allocations().await.unwrap().is_empty());
    assert_eq!(metrics.snapshot().allocations_closed, 1);

    client.close().await.ok();
    server.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Credential format interop
// ---------------------------------------------------------------------------

/// Mint a credential the way coturn's `use-auth-secret` convention does —
/// `<expiry>:<user>`, HMAC-SHA1 over the whole username — without going
/// through our own minting code, so this checks interoperability rather than
/// self-consistency.
fn coturn_style_credential(secret: &str, ttl_secs: u64, user: &str) -> (String, String) {
    use base64::Engine;
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + ttl_secs;
    let username = format!("{expiry}:{user}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret.as_bytes());
    let tag = ring::hmac::sign(&key, username.as_bytes());
    let password = base64::engine::general_purpose::STANDARD.encode(tag.as_ref());
    (username, password)
}

#[tokio::test]
async fn coturn_style_usernames_with_a_principal_are_accepted() {
    let secret = "shared-with-whoever-mints-credentials";
    let server = TurnServer::bind(config(TurnCredentials::Ephemeral {
        shared_secret: secret.to_string(),
    }))
    .await
    .unwrap();

    // Minted the coturn way by an independent implementation, not by ours.
    let (username, password) = coturn_style_credential(secret, 300, "alice");
    let client = turn_client(server.local_addr(), &username, &password)
        .await
        .unwrap();
    client.allocate().await.unwrap();

    // The principal rides along on the allocation, no re-parsing needed.
    let allocations = server.allocations().await.unwrap();
    assert_eq!(allocations.len(), 1);
    assert_eq!(allocations[0].principal.as_deref(), Some("alice"));
    assert_eq!(allocations[0].username, username);

    client.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn both_username_forms_verify_against_one_server() {
    let secret = "one-secret-two-username-shapes";
    let server = TurnServer::bind(config(TurnCredentials::Ephemeral {
        shared_secret: secret.to_string(),
    }))
    .await
    .unwrap();
    let metrics = server.metrics();

    // The bare-expiry form, as implementations that only parse digits mint it.
    let (bare_user, bare_pw) = ephemeral_credentials(secret, Duration::from_secs(300)).unwrap();
    assert!(bare_user.parse::<u64>().is_ok(), "expected a bare expiry");
    assert_eq!(credential_principal(&bare_user), None);
    let bare = turn_client(server.local_addr(), &bare_user, &bare_pw)
        .await
        .unwrap();
    bare.allocate().await.unwrap();

    // The labelled form, which names its principal.
    let (labelled_user, labelled_pw) =
        ephemeral_credentials_for(secret, Duration::from_secs(300), "bob").unwrap();
    assert_eq!(credential_principal(&labelled_user), Some("bob"));
    let labelled = turn_client(server.local_addr(), &labelled_user, &labelled_pw)
        .await
        .unwrap();
    labelled.allocate().await.unwrap();

    assert_eq!(metrics.snapshot().allocations_granted, 2);

    bare.close().await.unwrap();
    labelled.close().await.unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn expired_and_forged_credentials_are_refused_in_both_forms() {
    let secret = "secret";
    let server = TurnServer::bind(config(TurnCredentials::Ephemeral {
        shared_secret: secret.to_string(),
    }))
    .await
    .unwrap();
    let metrics = server.metrics();

    // Expired, labelled form: a valid HMAC over an expiry in the past.
    let (expired_user, expired_pw) = coturn_style_credential_at(secret, -60, "alice");
    let expired = turn_client(server.local_addr(), &expired_user, &expired_pw)
        .await
        .unwrap();
    assert_allocation_refused(&expired).await;

    // Unexpired but with someone else's HMAC.
    let (user, _) = coturn_style_credential(secret, 300, "alice");
    let (_, other_pw) = coturn_style_credential("a-different-secret", 300, "alice");
    let forger = turn_client(server.local_addr(), &user, &other_pw)
        .await
        .unwrap();
    assert_allocation_refused(&forger).await;

    // A username with a principal but no parsable expiry.
    let nonsense = turn_client(server.local_addr(), "soon:alice", "whatever")
        .await
        .unwrap();
    assert_allocation_refused(&nonsense).await;

    assert_eq!(metrics.snapshot().allocations_granted, 0);
    assert!(metrics.snapshot().auth_refused > 0);
    server.close().await.unwrap();
}

/// Like `coturn_style_credential`, but with an expiry offset that may be in
/// the past.
fn coturn_style_credential_at(secret: &str, offset_secs: i64, user: &str) -> (String, String) {
    use base64::Engine;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let username = format!("{}:{}", now + offset_secs, user);
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret.as_bytes());
    let tag = ring::hmac::sign(&key, username.as_bytes());
    let password = base64::engine::general_purpose::STANDARD.encode(tag.as_ref());
    (username, password)
}

// ---------------------------------------------------------------------------
// Per-principal revocation and accounting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_principal_is_revoked_across_all_its_credentials() {
    let secret = "secret";
    let server = TurnServer::bind(config(TurnCredentials::Ephemeral {
        shared_secret: secret.to_string(),
    }))
    .await
    .unwrap();

    // The same user, holding two separately-minted credentials — different
    // usernames, because each carries its own expiry.
    let (u1, p1) = coturn_style_credential(secret, 300, "alice");
    let (u2, p2) = coturn_style_credential(secret, 600, "alice");
    assert_ne!(u1, u2);
    let (u3, p3) = coturn_style_credential(secret, 300, "bob");

    let a1 = turn_client(server.local_addr(), &u1, &p1).await.unwrap();
    a1.allocate().await.unwrap();
    let a2 = turn_client(server.local_addr(), &u2, &p2).await.unwrap();
    a2.allocate().await.unwrap();
    let b = turn_client(server.local_addr(), &u3, &p3).await.unwrap();
    b.allocate().await.unwrap();
    assert_eq!(server.allocations().await.unwrap().len(), 3);

    // One call cuts alice off entirely, and leaves bob alone.
    let revoked = server.revoke_principal("alice").await.unwrap();
    assert_eq!(revoked, 2);

    let remaining = server.allocations().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].principal.as_deref(), Some("bob"));

    a1.close().await.ok();
    a2.close().await.ok();
    b.close().await.ok();
    server.close().await.unwrap();
}

#[tokio::test]
async fn closing_allocations_report_their_bytes_for_attribution() {
    let closed: Arc<std::sync::Mutex<Vec<ClosedAllocation>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = closed.clone();

    let secret = "secret";
    let mut cfg = config(TurnCredentials::Ephemeral {
        shared_secret: secret.to_string(),
    });
    cfg.on_allocation_closed = Some(Arc::new(move |report: ClosedAllocation| {
        sink.lock().unwrap().push(report);
    }));
    let server = TurnServer::bind(cfg).await.unwrap();

    let (username, password) = coturn_style_credential(secret, 300, "alice");
    let client = turn_client(server.local_addr(), &username, &password)
        .await
        .unwrap();
    let relay = client.allocate().await.unwrap();

    // Relay some traffic so the allocation has bytes worth attributing.
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 256];
        while let Ok((n, from)) = peer.recv_from(&mut buf).await {
            let _ = peer.send_to(&buf[..n], from).await;
        }
    });
    relay.send_to(b"billable bytes", peer_addr).await.unwrap();
    let mut buf = [0u8; 256];
    tokio::time::timeout(Duration::from_secs(5), relay.recv_from(&mut buf))
        .await
        .expect("relayed reply timed out")
        .unwrap();

    // The allocation closes; its final byte count must not vanish with it.
    server.revoke_principal("alice").await.unwrap();

    let mut report = None;
    for _ in 0..50 {
        if let Some(first) = closed.lock().unwrap().first().cloned() {
            report = Some(first);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let report = report.expect("no closing report was delivered");
    assert_eq!(report.principal.as_deref(), Some("alice"));
    assert_eq!(report.username, username);
    assert!(
        report.relayed_bytes > 0,
        "closing report carried no byte count"
    );

    client.close().await.ok();
    server.close().await.unwrap();
}
