// bin/test_auto_detect_wasi_server.rs
//
// Split-platform test for AutoDetectListener on WASI:
//   - Native binary: runs a TCP client, then a WebSocket client (sequentially,
//     matching the WASI server's sequential accept model).
//   - WASI binary:   runs an AutoDetectListener echo server that handles exactly
//     2 connections (one TCP, one WS) and exits.
//
// Run:
//   cargo build --bin test_auto_detect_wasi_server                          # native clients
//   cargo build --target wasm32-wasip2 --bin test_auto_detect_wasi_server   # WASI server
//   wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_auto_detect_wasi_server.wasm &
//   ./target/debug/test_auto_detect_wasi_server

// ═══════════════════════════════════════════════════════════════════════════════
// Native imports — client side
// ═══════════════════════════════════════════════════════════════════════════════
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::tcp_native::TcpStreamNative;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::ws_native::WebSocketNative;
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::transport::Transport;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

// ═══════════════════════════════════════════════════════════════════════════════
// WASI imports — server side
// ═══════════════════════════════════════════════════════════════════════════════
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::server::{AutoDetectListener, Listener};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::transport::Transport;

// ═══════════════════════════════════════════════════════════════════════════════
// NATIVE — sequential clients
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() {
    platform::init_logging();
    log::info!("=== Native Clients for WASI AutoDetect Server ===\n");

    let addr = "127.0.0.1:9992";

    // Give the WASI server time to bind and start listening.
    log::info!("[Clients] Waiting 1s for WASI server to start...");
    aloeplatform::sleep(Duration::from_secs(1)).await;

    // ─── Connection 1: TCP ───────────────────────────────────────────────────
    log::info!("\n[Clients] ── TCP client ──");
    let tcp_ok = run_tcp_client(addr).await;
    log::info!(
        "[Clients] TCP: {}",
        if tcp_ok { "✓ PASS" } else { "✗ FAIL" }
    );

    // Brief pause — WASI server is sequential, give it a moment to loop back
    // to accept after the TCP handler completes.
    aloeplatform::sleep(Duration::from_millis(500)).await;

    // ─── Connection 2: WebSocket ─────────────────────────────────────────────
    log::info!("\n[Clients] ── WebSocket client ──");
    let ws_ok = run_ws_client(&format!("ws://{}", addr)).await;
    log::info!("[Clients] WS: {}", if ws_ok { "✓ PASS" } else { "✗ FAIL" });

    // ─── Summary ─────────────────────────────────────────────────────────────
    log::info!(
        "\n=== WASI AutoDetect Server Test {} ===",
        if tcp_ok && ws_ok { "PASSED" } else { "FAILED" }
    );

    aloeplatform::sleep(Duration::from_secs(1)).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_tcp_client(addr: &str) -> bool {
    log::info!("[TCP Client] Connecting to {}", addr);

    let mut transport = match TcpStreamNative::connect(addr).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[TCP Client] ✗ Connect failed: {:?}", e);
            return false;
        }
    };
    log::info!("[TCP Client] ✓ Connected");

    for i in 1..=2 {
        let msg = format!("TCP hello from native #{}", i);
        if let Err(e) = transport.send(msg.as_bytes()).await {
            log::error!("[TCP Client] ✗ Send error: {:?}", e);
            return false;
        }
        log::info!("[TCP Client] Sent: {}", msg);

        let mut buf = [0u8; 1024];
        match transport.recv(&mut buf).await {
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]);
                log::info!("[TCP Client] ✓ Echo: {}", response);
                if response != msg {
                    log::error!("[TCP Client] ✗ Mismatch!");
                    return false;
                }
            }
            Err(e) => {
                log::error!("[TCP Client] ✗ Recv error: {:?}", e);
                return false;
            }
        }
        aloeplatform::sleep(Duration::from_millis(200)).await;
    }

    log::info!("[TCP Client] ✓ Complete");
    true
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_ws_client(url: &str) -> bool {
    log::info!("[WS Client] Connecting to {}", url);

    let mut transport = match WebSocketNative::connect(url).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[WS Client] ✗ Connect failed: {:?}", e);
            return false;
        }
    };
    log::info!("[WS Client] ✓ Connected");

    for i in 1..=2 {
        let msg = format!("WS hello from native #{}", i);
        if let Err(e) = transport.send(msg.as_bytes()).await {
            log::error!("[WS Client] ✗ Send error: {:?}", e);
            return false;
        }
        log::info!("[WS Client] Sent: {}", msg);

        let mut buf = [0u8; 1024];
        match transport.recv(&mut buf).await {
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]);
                log::info!("[WS Client] ✓ Echo: {}", response);
                if response != msg {
                    log::error!("[WS Client] ✗ Mismatch!");
                    return false;
                }
            }
            Err(e) => {
                log::error!("[WS Client] ✗ Recv error: {:?}", e);
                return false;
            }
        }
        aloeplatform::sleep(Duration::from_millis(200)).await;
    }

    log::info!("[WS Client] ✓ Complete");
    true
}

// ═══════════════════════════════════════════════════════════════════════════════
// WASI — AutoDetect echo server (handles exactly 2 connections then exits)
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run_wasi_server());
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
async fn run_wasi_server() {
    platform::init_logging();
    log::info!("=== WASI AutoDetect Server Test ===\n");

    let addr = "127.0.0.1:9992";
    log::info!("[WASI Server] Binding AutoDetect listener on {}", addr);

    let listener = AutoDetectListener::bind(addr)
        .await
        .expect("Failed to bind");

    log::info!("[WASI Server] ✓ Bound and listening\n");

    // Handle exactly 2 connections sequentially, then exit.
    // We use the Listener trait directly rather than ServerBuilder so we can
    // count connections and exit cleanly after the test completes.
    for conn_num in 1..=2 {
        log::info!("[WASI Server] Waiting for connection #{} ...", conn_num);

        match listener.accept().await {
            Ok(mut transport) => {
                log::info!(
                    "[WASI Server] ✓ Connection #{} accepted (auto-detected)",
                    conn_num
                );

                let mut buf = [0u8; 1024];
                let mut msg_count = 0;

                loop {
                    match transport.recv(&mut buf).await {
                        Ok(n) => {
                            msg_count += 1;
                            let data = String::from_utf8_lossy(&buf[..n]);
                            log::info!(
                                "[WASI Server] ✓ Conn #{} msg #{}: {}",
                                conn_num,
                                msg_count,
                                data
                            );

                            if let Err(e) = transport.send(&buf[..n]).await {
                                log::error!(
                                    "[WASI Server] ✗ Send error on conn #{}: {:?}",
                                    conn_num,
                                    e
                                );
                                break;
                            }
                            log::info!("[WASI Server] ✓ Echoed {} bytes", n);

                            // Each client sends 2 messages then disconnects
                            if msg_count >= 2 {
                                log::info!(
                                    "[WASI Server] Connection #{} handler complete",
                                    conn_num
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            log::info!("[WASI Server] Connection #{} ended: {:?}", conn_num, e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("[WASI Server] ✗ Accept error: {:?}", e);
                break;
            }
        }
    }

    log::info!("\n[WASI Server] ✓ Handled 2 connections, exiting");
}

// Browser stub
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn main() {
    panic!("test_auto_detect_wasi_server is not for browser");
}
