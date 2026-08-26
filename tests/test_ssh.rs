//! Native tests for the `ssh` scheme: subsystem frame round-trips, PTY
//! round-trips with resize, principal surfacing, and typed refusals for
//! wrong client keys and wrong host keys.

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use ego_transport::framing::FramedTransport;
use ego_transport::identity::PeerIdentity;
use ego_transport::ssh::{
    ClientAuthorization, HostKeyVerification, SshChannelEvent, SshChannelKind, SshClientConfig,
    SshClientConnection, SshError, SshListener, SshServerConfig, generate_ed25519, key_identity,
};
use ego_transport::transport::TransportError;

fn client_config(
    key: ego_transport::ssh::PrivateKey,
    host: HostKeyVerification,
) -> SshClientConfig {
    SshClientConfig {
        user: "tester".into(),
        key,
        host_verification: host,
        inactivity_timeout: Some(Duration::from_secs(30)),
    }
}

async fn bind_server(config: SshServerConfig) -> (SshListener, String) {
    let listener = SshListener::bind("127.0.0.1:0", config).await.unwrap();
    let addr = listener.local_addr().to_string();
    (listener, addr)
}

#[tokio::test]
async fn subsystem_frames_round_trip_and_principal_is_surfaced() {
    let host_key = generate_ed25519();
    let host_pub = host_key.public_key().clone();
    let client_key = generate_ed25519();
    let client_fp = key_identity(&client_key.public_key().clone()).fingerprint_sha256;

    let (listener, addr) = bind_server(SshServerConfig::new(host_key)).await;
    assert!(
        listener
            .host_identity()
            .fingerprint_sha256
            .starts_with("SHA256:")
    );

    let server = tokio::spawn(async move {
        let mut conn = listener.accept().await.unwrap();

        // The authenticated client key is the principal, surfaced verbatim.
        match conn.identity() {
            PeerIdentity::Key { key, user } => {
                assert_eq!(key.fingerprint_sha256, client_fp);
                assert_eq!(key.algorithm, "ssh-ed25519");
                assert_eq!(user.as_deref(), Some("tester"));
            }
            other => panic!("expected key identity, got {other:?}"),
        }
        assert!(conn.remote_addr().is_some());

        let channel = conn.next_channel().await.unwrap();
        assert_eq!(
            channel.kind(),
            &SshChannelKind::Subsystem("frames".to_string())
        );

        // Echo framed messages until the peer hangs up.
        let mut framed = FramedTransport::new(channel);
        loop {
            match framed.recv_frame().await {
                Ok(frame) => framed.send_frame(&frame).await.unwrap(),
                Err(_) => break,
            }
        }
    });

    let conn = SshClientConnection::connect(
        &addr,
        client_config(client_key, HostKeyVerification::Keys(vec![host_pub])),
    )
    .await
    .unwrap();
    assert!(
        conn.host_identity()
            .fingerprint_sha256
            .starts_with("SHA256:")
    );

    let channel = conn.open_subsystem("frames").await.unwrap();
    let mut framed = FramedTransport::new(channel);

    // Msgpack-shaped payloads are opaque bytes to the transport.
    for payload in [&b"\x82\xa3key\xa5value\xa1n\x01"[..], &[0u8; 70_000][..]] {
        framed.send_frame(payload).await.unwrap();
        assert_eq!(framed.recv_frame().await.unwrap(), payload);
    }

    conn.disconnect().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn pty_round_trip_with_resize_and_close() {
    let host_key = generate_ed25519();
    let host_fp = key_identity(&host_key.public_key().clone()).fingerprint_sha256;
    let (listener, addr) = bind_server(SshServerConfig::new(host_key)).await;

    let server = tokio::spawn(async move {
        let mut conn = listener.accept().await.unwrap();
        let mut channel = conn.next_channel().await.unwrap();

        match channel.kind().clone() {
            SshChannelKind::Pty(pty) => {
                assert_eq!(pty.term, "xterm-256color");
                assert_eq!((pty.cols, pty.rows), (80, 24));
            }
            other => panic!("expected pty channel, got {other:?}"),
        }

        // Shell-like echo loop: echo data, observe the resize, stop on close.
        let mut saw_resize = None;
        loop {
            match channel.next_event().await {
                SshChannelEvent::Data(d) => {
                    use ego_transport::transport::Transport;
                    channel.send(&d).await.unwrap();
                }
                SshChannelEvent::WindowChange { cols, rows, .. } => {
                    saw_resize = Some((cols, rows));
                }
                SshChannelEvent::Eof | SshChannelEvent::Closed => break,
                _ => {}
            }
        }
        saw_resize
    });

    // Fingerprint-based host verification is enough to connect.
    let conn = SshClientConnection::connect(
        &addr,
        client_config(
            generate_ed25519(),
            HostKeyVerification::Fingerprints(vec![host_fp]),
        ),
    )
    .await
    .unwrap();

    let mut channel = conn.open_pty("xterm-256color", 80, 24).await.unwrap();

    use ego_transport::transport::Transport;
    channel.send(b"say hello\r").await.unwrap();
    let mut buf = [0u8; 64];
    let mut echoed = Vec::new();
    while echoed.len() < b"say hello\r".len() {
        let n = channel.recv(&mut buf).await.unwrap();
        echoed.extend_from_slice(&buf[..n]);
    }
    assert_eq!(echoed, b"say hello\r");

    channel.resize(132, 43).await.unwrap();
    channel.close().await.unwrap();
    conn.disconnect().await.ok();

    let saw_resize = server.await.unwrap();
    assert_eq!(saw_resize, Some((132, 43)));
}

#[tokio::test]
async fn wrong_client_key_is_refused_typed() {
    let host_key = generate_ed25519();
    let host_pub = host_key.public_key().clone();
    let allowed = generate_ed25519();

    let mut config = SshServerConfig::new(host_key);
    config.authorization = ClientAuthorization::Keys(vec![allowed.public_key().clone()]);
    config.auth_rejection_time = Duration::from_millis(10);
    let (listener, addr) = bind_server(config).await;

    // A different (but valid) key must be rejected at the auth layer.
    let err = match SshClientConnection::connect(
        &addr,
        client_config(
            generate_ed25519(),
            HostKeyVerification::Keys(vec![host_pub.clone()]),
        ),
    )
    .await
    {
        Ok(_) => panic!("expected auth rejection"),
        Err(e) => e,
    };
    match err {
        TransportError::Ssh(SshError::AuthRejected { user }) => assert_eq!(user, "tester"),
        other => panic!("expected AuthRejected, got {other:?}"),
    }

    // The allowed key still gets in.
    SshClientConnection::connect(
        &addr,
        client_config(allowed, HostKeyVerification::Keys(vec![host_pub])),
    )
    .await
    .unwrap();
    drop(listener);
}

#[tokio::test]
async fn wrong_host_key_is_refused_typed_and_surfaces_offered_key() {
    let host_key = generate_ed25519();
    let real_host_fp = key_identity(&host_key.public_key().clone()).fingerprint_sha256;
    let (_listener, addr) = bind_server(SshServerConfig::new(host_key)).await;

    // Trusting a *different* host key must fail before authentication...
    let unrelated = generate_ed25519().public_key().clone();
    let err = match SshClientConnection::connect(
        &addr,
        client_config(
            generate_ed25519(),
            HostKeyVerification::Keys(vec![unrelated]),
        ),
    )
    .await
    {
        Ok(_) => panic!("expected host key mismatch"),
        Err(e) => e,
    };

    // ...with the offered key surfaced so the caller can record or report it.
    match err {
        TransportError::Ssh(SshError::HostKeyMismatch { offered }) => {
            assert_eq!(offered.unwrap().fingerprint_sha256, real_host_fp);
        }
        other => panic!("expected HostKeyMismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn accept_any_host_key_is_an_explicit_opt_in_that_still_surfaces_identity() {
    let host_key = generate_ed25519();
    let host_fp = key_identity(&host_key.public_key().clone()).fingerprint_sha256;
    let (_listener, addr) = bind_server(SshServerConfig::new(host_key)).await;

    let conn = SshClientConnection::connect(
        &addr,
        client_config(generate_ed25519(), HostKeyVerification::AcceptAny),
    )
    .await
    .unwrap();
    assert_eq!(conn.host_identity().fingerprint_sha256, host_fp);
}
