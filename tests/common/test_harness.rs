//! Test harness utilities for ego_transport integration tests.
//!
//! - `run_dumb_relay`: A raw TCP relay (no protocol detection). Native-only.
//! - `run_dumb_relay_chain`: Multiple chained relays for multi-hop tests.
//! - `run_signaling_hub_fixture`: AutoDetectListener + SignalingHub server.
//! - `run_echo_fixture`: AutoDetectListener + ServerBuilder echo handler.
//! - `run_signaling_test`: Orchestrator-driven two-peer signaling test.
//! - `collect_actor_events`: Generic orchestrator event collector.

use crate::common::test_actor::{SignalingRole, SignalingTestActor, SignalingTestEvent};
use ego_proc::OrchestrationStrategy;
use ego_proc::actor::Orchestrator;
use std::time::Duration;

// ─── Orchestrator helpers ────────────────────────────────────────────────────

/// Spawn two signaling actors into an orchestrator, wait for both to complete.
/// Returns (offerer_result, answerer_result).
pub async fn run_signaling_test(
    offerer_transport: Box<dyn ego_transport::transport::Transport>,
    answerer_transport: Box<dyn ego_transport::transport::Transport>,
    room: &str,
    timeout_secs: u64,
) -> (Option<SignalingTestEvent>, Option<SignalingTestEvent>) {
    let mut orch: Orchestrator<SignalingTestActor> =
        Orchestrator::new(OrchestrationStrategy::oneshot());

    let offerer = SignalingTestActor::new(SignalingRole::Offerer, room, offerer_transport);
    let answerer = SignalingTestActor::new(SignalingRole::Answerer, room, answerer_transport);

    orch.spawn(offerer);
    orch.spawn(answerer);

    let mut offerer_result: Option<SignalingTestEvent> = None;
    let mut answerer_result: Option<SignalingTestEvent> = None;

    let deadline = Duration::from_secs(timeout_secs);
    let _ = ego_platform::timeout(deadline, async {
        loop {
            orch.maintain().await;
            while let Some((_id, event)) = orch.recv_output() {
                match &event {
                    SignalingTestEvent::Complete { role, .. } => match role {
                        SignalingRole::Offerer => offerer_result = Some(event.clone()),
                        SignalingRole::Answerer => answerer_result = Some(event.clone()),
                    },
                    _ => {}
                }
            }
            if offerer_result.is_some() && answerer_result.is_some() {
                return;
            }
            ego_platform::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    (offerer_result, answerer_result)
}

/// Generic event collector: runs maintain loop and collects events until
/// `predicate` returns true or timeout expires.
pub async fn collect_actor_events<S: ego_proc::actor::ActorState + 'static>(
    orch: &mut Orchestrator<S>,
    timeout_secs: u64,
    mut predicate: impl FnMut(&S::O) -> bool,
) -> Vec<(uuid::Uuid, S::O)> {
    let mut events = Vec::new();
    let deadline = Duration::from_secs(timeout_secs);
    let _ = ego_platform::timeout(deadline, async {
        loop {
            orch.maintain().await;
            while let Some((id, event)) = orch.recv_output() {
                let done = predicate(&event);
                events.push((id, event));
                if done {
                    return;
                }
            }
            ego_platform::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    events
}

// ─── Dumb relay fixtures ─────────────────────────────────────────────────────

/// A dumb TCP relay: accepts exactly two connections on the given address and
/// forwards bytes bidirectionally. No protocol detection, no signaling hub.
/// Native-only. Runs until either side disconnects.
#[cfg(not(target_arch = "wasm32"))]
pub async fn run_dumb_relay(addr: &str) {
    use ego_transport::platform::server::Listener;
    use ego_transport::platform::tcp_native::TcpListenerNative;
    use ego_transport::transport::Transport;

    let listener = TcpListenerNative::bind(addr).expect("Failed to bind relay");
    log::info!("[DumbRelay] Listening on {}", addr);

    let mut conn_a = listener
        .accept()
        .await
        .expect("Failed to accept first connection");
    log::info!("[DumbRelay] First peer connected");

    let mut conn_b = listener
        .accept()
        .await
        .expect("Failed to accept second connection");
    log::info!("[DumbRelay] Second peer connected");

    let mut buf_ab = [0u8; 65536];
    let mut buf_ba = [0u8; 65536];

    log::info!("[DumbRelay] Relaying...");
    loop {
        tokio::select! {
            result = conn_a.recv(&mut buf_ab) => {
                match result {
                    Ok(n) => {
                        if conn_b.send(&buf_ab[..n]).await.is_err() { return; }
                    }
                    Err(_) => return,
                }
            }
            result = conn_b.recv(&mut buf_ba) => {
                match result {
                    Ok(n) => {
                        if conn_a.send(&buf_ba[..n]).await.is_err() { return; }
                    }
                    Err(_) => return,
                }
            }
        }
    }
}

/// Start a chain of dumb relays. Returns the addresses in order.
/// Each relay accepts two connections and forwards between them.
/// Callers must connect them in sequence: peer→relay[0], relay[0]→relay[1], ..., relay[n]→peer.
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_dumb_relay_chain(addrs: &[&str]) -> Vec<ego_platform::TaskHandle<()>> {
    addrs
        .iter()
        .map(|addr| {
            let addr = addr.to_string();
            ego_platform::spawn(async move {
                run_dumb_relay(&addr).await;
            })
        })
        .collect()
}

/// Connect two relays: opens a connection to `from_addr` (as that relay's
/// second peer) and a connection to `to_addr` (as that relay's first peer),
/// then forwards bytes between them. Runs until either side disconnects.
#[cfg(not(target_arch = "wasm32"))]
pub async fn run_bridge(from_addr: &str, to_addr: &str) {
    use ego_transport::transport::Transport;

    let mut conn_from = ego_transport::transport::connect(from_addr)
        .await
        .expect("Bridge: connect to source failed");
    log::info!("[Bridge] Connected to {}", from_addr);

    let mut conn_to = ego_transport::transport::connect(to_addr)
        .await
        .expect("Bridge: connect to destination failed");
    log::info!("[Bridge] Connected to {}", to_addr);

    let mut buf_ft = [0u8; 65536];
    let mut buf_tf = [0u8; 65536];

    loop {
        tokio::select! {
            r = conn_from.recv(&mut buf_ft) => {
                match r {
                    Ok(n) => { if conn_to.send(&buf_ft[..n]).await.is_err() { return; } }
                    Err(_) => return,
                }
            }
            r = conn_to.recv(&mut buf_tf) => {
                match r {
                    Ok(n) => { if conn_from.send(&buf_tf[..n]).await.is_err() { return; } }
                    Err(_) => return,
                }
            }
        }
    }
}

// ─── AutoDetectListener fixtures ─────────────────────────────────────────────

/// Start an AutoDetectListener with an embedded SignalingHub.
/// Signaling peers (JOIN: first message) are routed to the hub.
/// Non-signaling connections are passed to a no-op handler (dropped).
/// Returns a task handle. The hub is returned for inspection if needed.
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_signaling_hub_fixture(addr: &str) -> ego_platform::TaskHandle<()> {
    use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
    use ego_transport::transport::signaling_hub::SignalingHub;

    let addr = addr.to_string();
    ego_platform::spawn(async move {
        let hub = SignalingHub::new();
        let listener = AutoDetectListener::bind(&addr)
            .await
            .expect("Failed to bind signaling hub fixture");
        log::info!("[HubFixture] Listening on {} with SignalingHub", addr);

        ServerBuilder::new(listener.with_signaling(hub))
            .concurrent()
            .run(|_transport| async {
                // Non-signaling connections are silently accepted and dropped.
                // Tests that need echo behavior use spawn_echo_fixture instead.
            })
            .await
            .ok();
    })
}

/// Start an AutoDetectListener with SignalingHub + echo handler.
/// Signaling peers go to the hub; non-signaling connections get echoed.
/// Returns (task_handle, app_connection_counter).
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_echo_fixture(
    addr: &str,
) -> (
    ego_platform::TaskHandle<()>,
    std::sync::Arc<std::sync::atomic::AtomicU32>,
) {
    use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
    use ego_transport::transport::Transport;
    use ego_transport::transport::signaling_hub::SignalingHub;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    let addr = addr.to_string();

    let handle = ego_platform::spawn(async move {
        let hub = SignalingHub::new();
        let listener = AutoDetectListener::bind(&addr)
            .await
            .expect("Failed to bind echo fixture");
        log::info!(
            "[EchoFixture] Listening on {} with SignalingHub + echo",
            addr
        );

        ServerBuilder::new(listener.with_signaling(hub))
            .concurrent()
            .run(move |mut transport| {
                let count = counter_clone.clone();
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    log::info!("[EchoFixture] App connection received {:?}", count);
                    let mut buf = [0u8; 4096];
                    loop {
                        match transport.recv(&mut buf).await {
                            Ok(n) => {
                                transport.send(&buf[..n]).await.ok();
                            }
                            Err(_) => break,
                        }
                    }
                }
            })
            .await
            .ok();
    });

    (handle, counter)
}

// ─── Assertion helpers ───────────────────────────────────────────────────────

/// Assert a SignalingTestEvent is Complete with success=true
pub fn assert_signaling_success(event: &Option<SignalingTestEvent>, label: &str) {
    match event {
        Some(SignalingTestEvent::Complete {
            success, detail, ..
        }) => {
            assert!(*success, "{} failed: {}", label, detail);
            log::info!("[{}] ✓ {}", label, detail);
        }
        None => {
            panic!("{} timed out — no result", label);
        }
        _ => {
            panic!("{} unexpected event type", label);
        }
    }
}

/// Send data and verify echo response over a transport.
#[cfg(not(target_arch = "wasm32"))]
pub async fn assert_echo(
    transport: &mut Box<dyn ego_transport::transport::Transport>,
    msg: &[u8],
    label: &str,
) {
    use ego_transport::transport::Transport;

    log::debug!("[assert_echo]: {:?}", msg);
    transport
        .send(msg)
        .await
        .unwrap_or_else(|e| panic!("[{}] send failed: {:?}", label, e));

    let mut buf = [0u8; 4096];

    log::debug!("[assert_echo]: awaiting response... (transport.recv)");
    let n = transport
        .recv(&mut buf)
        .await
        .unwrap_or_else(|e| panic!("[{}] recv failed: {:?}", label, e));
    log::debug!("[assert_echo]: transport.recv returned!");

    assert_eq!(&buf[..n], msg, "[{}] echo mismatch", label);
    log::info!("[{}] ✓ Echo verified", label);
}
