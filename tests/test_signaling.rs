//! Integration test: signaling through AutoDetectListener + SignalingHub.
//!
//! This is the realistic production path:
//! 1. AutoDetectListener detects WebSocket upgrade
//! 2. First message is JOIN → routed to SignalingHub
//! 3. Hub manages room, assigns roles, relays signals
//! 4. Two SignalingTestActors (hub mode) complete the handshake
//!
//! IMPORTANT: Each peer must be connected AND its actor spawned before
//! connecting the next peer. AutoDetectListener's signaling detection
//! recv's the first message — if the actor hasn't sent JOIN yet,
//! AutoDetect blocks the accept loop (5s timeout on WS, peek-spin on TCP).
//!
//! Replaces: src/bin/test_signaling.rs

mod common;

use common::test_harness;

const SIGNALING_HUB_ADDR: &str = "127.0.0.1:19986";

/// Helper: connect one peer, spawn its actor, give it time to send JOIN,
/// then connect the next peer and spawn it. Collects results.
#[cfg(not(target_arch = "wasm32"))]
async fn run_hub_signaling_test(
    addr: &str,
    protocol: &str,
    room: &str,
) -> Vec<common::test_actor::SignalingTestEvent> {
    use crate::common::test_actor::{SignalingTestActor, SignalingTestEvent};
    use ego_proc::OrchestrationStrategy;
    use ego_proc::actor::Orchestrator;

    let mut orch: Orchestrator<SignalingTestActor> =
        Orchestrator::new(OrchestrationStrategy::oneshot());

    let url = match protocol {
        "ws" => format!("ws://{}", addr),
        _ => addr.to_string(),
    };

    // Connect both peers — AutoDetect now handles detection concurrently
    let transport_a = ego_transport::transport::connect(&url)
        .await
        .expect("Peer A: connect failed");
    let transport_b = ego_transport::transport::connect(&url)
        .await
        .expect("Peer B: connect failed");

    orch.spawn(SignalingTestActor::new_hub_mode(room, transport_a));
    orch.spawn(SignalingTestActor::new_hub_mode(room, transport_b));

    // Collect results
    let mut results: Vec<SignalingTestEvent> = Vec::new();
    let _ = ego_platform::timeout(std::time::Duration::from_secs(10), async {
        loop {
            orch.maintain().await;
            while let Some((_id, event)) = orch.recv_output() {
                results.push(event);
            }
            if results.len() >= 2 {
                return;
            }
            ego_platform::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;

    results
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_signaling_through_hub_websocket() {
    use crate::common::test_actor::{SignalingRole, SignalingTestEvent};

    ego_platform::init();

    let _fixture = test_harness::spawn_signaling_hub_fixture(SIGNALING_HUB_ADDR);
    ego_platform::sleep(std::time::Duration::from_millis(300)).await;

    let results = run_hub_signaling_test(SIGNALING_HUB_ADDR, "ws", "hub-ws-room").await;

    assert_eq!(
        results.len(),
        2,
        "Expected 2 completion events, got {}",
        results.len()
    );
    for event in &results {
        test_harness::assert_signaling_success(&Some(event.clone()), "hub WS peer");
    }

    let offerers = results
        .iter()
        .filter(|e| {
            matches!(
                e,
                SignalingTestEvent::Complete {
                    role: SignalingRole::Offerer,
                    ..
                }
            )
        })
        .count();
    let answerers = results
        .iter()
        .filter(|e| {
            matches!(
                e,
                SignalingTestEvent::Complete {
                    role: SignalingRole::Answerer,
                    ..
                }
            )
        })
        .count();
    assert_eq!(offerers, 1, "Expected 1 offerer");
    assert_eq!(answerers, 1, "Expected 1 answerer");
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_signaling_through_hub_tcp() {
    ego_platform::init();

    let addr = "127.0.0.1:19987";
    let _fixture = test_harness::spawn_signaling_hub_fixture(addr);
    ego_platform::sleep(std::time::Duration::from_millis(300)).await;

    let results = run_hub_signaling_test(addr, "tcp", "hub-tcp-room").await;

    assert_eq!(
        results.len(),
        2,
        "Expected 2 completion events, got {}",
        results.len()
    );
    for event in &results {
        test_harness::assert_signaling_success(&Some(event.clone()), "hub TCP peer");
    }
}
