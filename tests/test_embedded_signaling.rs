//! Integration test: AutoDetectListener with embedded SignalingHub.
//!
//! Tests that AutoDetectListener correctly routes:
//! 1. Signaling peers (JOIN: first message) → SignalingHub
//! 2. Regular TCP clients → application echo handler
//! 3. Regular WebSocket clients → application echo handler
//!
//! Replaces: src/bin/test_embedded_signaling.rs

mod common;

use common::test_harness;

const EMBEDDED_ADDR: &str = "127.0.0.1:19988";

/// Test 1: Signaling peers are routed to the hub and complete handshake.
/// App handler connection count should remain 0.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_embedded_signaling_peers_routed_to_hub() {
    use crate::common::test_actor::{SignalingTestActor, SignalingTestEvent};
    use ego_proc::actor::Orchestrator;
    use ego_proc::OrchestrationStrategy;

    ego_platform::init();

    let addr = EMBEDDED_ADDR;
    let (_fixture, app_counter) = test_harness::spawn_echo_fixture(addr);
    ego_platform::sleep(std::time::Duration::from_millis(300)).await;

    let ws_url = format!("ws://{}", addr);
    let room = "embedded-sig-room";

    let mut orch: Orchestrator<SignalingTestActor> =
        Orchestrator::new(OrchestrationStrategy::oneshot());

    let transport_a = ego_transport::transport::connect(&ws_url).await
        .expect("Signaling peer A: connect failed");
    let transport_b = ego_transport::transport::connect(&ws_url).await
        .expect("Signaling peer B: connect failed");

    orch.spawn(SignalingTestActor::new_hub_mode(room, transport_a));
    orch.spawn(SignalingTestActor::new_hub_mode(room, transport_b));

    let mut results: Vec<SignalingTestEvent> = Vec::new();

    let _ = ego_platform::timeout(std::time::Duration::from_secs(10), async {
        loop {
            orch.maintain().await;
            while let Some((_id, event)) = orch.recv_output() {
                results.push(event);
            }
            if results.len() >= 2 { return; }
            ego_platform::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await;

    assert_eq!(results.len(), 2, "Expected 2 signaling completions, got {}", results.len());
    for event in &results {
        test_harness::assert_signaling_success(&Some(event.clone()), "embedded signaling peer");
    }

    let count = app_counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(count, 0, "Signaling peers should not reach app handler, but count={}", count);

    log::info!("✓ Signaling peers routed to hub, app handler untouched");
}

/// Test 2: Regular TCP client passes through to echo handler.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_embedded_tcp_echo_passthrough() {
    use ego_transport::platform::tcp_native::TcpStreamNative;
    use ego_transport::transport::Transport;

    ego_platform::init();

    let addr = "127.0.0.1:19989";
    let (_fixture, app_counter) = test_harness::spawn_echo_fixture(addr);
    ego_platform::sleep(std::time::Duration::from_millis(300)).await;

    // Connect as raw TCP — first bytes are NOT "GET " (not WS) and NOT "JOIN:" (not signaling)
    let mut transport: Box<dyn Transport> = Box::new(
        TcpStreamNative::connect(addr).await.expect("TCP echo connect failed")
    );

    test_harness::assert_echo(&mut transport, b"Hello from TCP!", "TCP echo").await;

    let count = app_counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(count, 1, "Expected 1 app handler call for TCP, got {}", count);

    log::info!("✓ TCP client passed through to echo handler");
}

/// Test 3: Regular WebSocket client passes through to echo handler.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_embedded_ws_echo_passthrough() {
    use ego_transport::platform::ws_native::WebSocketNative;
    use ego_transport::transport::Transport;

    ego_platform::init();

    let addr = "127.0.0.1:19990";
    let (_fixture, app_counter) = test_harness::spawn_echo_fixture(addr);
    ego_platform::sleep(std::time::Duration::from_millis(300)).await;

    // Connect as WebSocket — AutoDetect sees "GET " prefix, upgrades to WS,
    // then first app message is NOT "JOIN:" so it passes through to handler
    let ws_url = format!("ws://{}", addr);
    let mut transport: Box<dyn Transport> = Box::new(
        WebSocketNative::connect(&ws_url).await.expect("WS echo connect failed")
    );

    test_harness::assert_echo(&mut transport, b"Hello from WebSocket!", "WS echo").await;

    let count = app_counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(count, 1, "Expected 1 app handler call for WS, got {}", count);

    log::info!("✓ WebSocket client passed through to echo handler");
}

/// Test 4: Mixed — signaling + echo on same port, verify counts.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn test_embedded_mixed_signaling_and_echo() {
    use crate::common::test_actor::{SignalingTestActor, SignalingTestEvent};
    use ego_transport::platform::tcp_native::TcpStreamNative;
    use ego_transport::platform::ws_native::WebSocketNative;
    use ego_transport::transport::Transport;
    use ego_proc::actor::Orchestrator;
    use ego_proc::OrchestrationStrategy;

    ego_platform::init();

    let addr = "127.0.0.1:19991";
    let (_fixture, app_counter) = test_harness::spawn_echo_fixture(addr);
    ego_platform::sleep(std::time::Duration::from_millis(300)).await;

    // Phase 1: Two signaling peers
    let ws_url = format!("ws://{}", addr);
    let room = "mixed-room";
    let mut orch: Orchestrator<SignalingTestActor> =
        Orchestrator::new(OrchestrationStrategy::oneshot());

    let sig_a = ego_transport::transport::connect(&ws_url).await.expect("sig A connect");
    let sig_b = ego_transport::transport::connect(&ws_url).await.expect("sig B connect");

    orch.spawn(SignalingTestActor::new_hub_mode(room, sig_a));
    orch.spawn(SignalingTestActor::new_hub_mode(room, sig_b));

    let mut sig_results: Vec<SignalingTestEvent> = Vec::new();
    let _ = ego_platform::timeout(std::time::Duration::from_secs(10), async {
        loop {
            orch.maintain().await;
            while let Some((_id, event)) = orch.recv_output() {
                sig_results.push(event);
            }
            if sig_results.len() >= 2 { return; }
            ego_platform::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await;

    assert_eq!(sig_results.len(), 2, "Expected 2 signaling completions");
    let count_after_signaling = app_counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(count_after_signaling, 0, "Signaling should not trigger app handler");

    // Phase 2: TCP echo
    let mut tcp: Box<dyn Transport> = Box::new(
        TcpStreamNative::connect(addr).await.expect("TCP connect")
    );
    test_harness::assert_echo(&mut tcp, b"tcp-mixed", "mixed TCP").await;

    // Phase 3: WS echo
    let mut ws: Box<dyn Transport> = Box::new(
        WebSocketNative::connect(&ws_url).await.expect("WS connect")
    );
    test_harness::assert_echo(&mut ws, b"ws-mixed", "mixed WS").await;

    let final_count = app_counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(final_count, 2, "Expected 2 app handler calls (TCP+WS), got {}", final_count);

    log::info!("✓ Mixed test: 2 signaling peers + TCP echo + WS echo on same port");
}
