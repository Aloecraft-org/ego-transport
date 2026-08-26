//! Integration test: signaling over routed transport.
//!
//! Topology: Z ──tcp──> Relay ──tcp──> C
//!
//! Z (offerer) and C (answerer) exchange signaling messages through a dumb
//! TCP relay, proving that signaling works over any Transport without a
//! dedicated signaling server.
//!
//! Uses the SignalingTestActor + Orchestrator pattern instead of manual
//! async functions, avoiding the AutoDetectListener deadlock.

mod common;

use common::TEST_RELAY_ADDR;
use common::test_harness;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_routed_signaling_through_relay() {
    ego_platform::init();

    // Start dumb relay in background
    let relay_handle = tokio::spawn(async { test_harness::run_dumb_relay(TEST_RELAY_ADDR).await });

    // Give the relay time to bind
    ego_platform::sleep(std::time::Duration::from_millis(200)).await;

    // Connect Z (offerer) first — relay accepts this as conn_a
    let z_transport = ego_transport::transport::connect(TEST_RELAY_ADDR)
        .await
        .expect("Z failed to connect to relay");

    // Small delay so relay accepts Z before C connects
    ego_platform::sleep(std::time::Duration::from_millis(100)).await;

    // Connect C (answerer) — relay accepts this as conn_b
    let c_transport = ego_transport::transport::connect(TEST_RELAY_ADDR)
        .await
        .expect("C failed to connect to relay");

    // Run the signaling test through the orchestrator
    let (offerer_result, answerer_result) =
        test_harness::run_signaling_test(z_transport, c_transport, "routed-test-room", 10).await;

    // Assert both peers completed successfully
    let offerer_ok = match &offerer_result {
        Some(common::test_actor::SignalingTestEvent::Complete {
            success, detail, ..
        }) => {
            log::info!("[Offerer] success={}, detail={}", success, detail);
            *success
        }
        None => {
            log::error!("[Offerer] Timed out — no result");
            false
        }
    };

    let answerer_ok = match &answerer_result {
        Some(common::test_actor::SignalingTestEvent::Complete {
            success, detail, ..
        }) => {
            log::info!("[Answerer] success={}, detail={}", success, detail);
            *success
        }
        None => {
            log::error!("[Answerer] Timed out — no result");
            false
        }
    };

    relay_handle.abort();

    assert!(offerer_ok, "Offerer failed");
    assert!(answerer_ok, "Answerer failed");
}
