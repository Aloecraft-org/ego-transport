pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub mod ssh_native;
#[cfg(not(target_arch = "wasm32"))]
pub mod stun_native;
pub mod tcp_native;
pub mod tcp_wasi;
pub mod ws_browser;
pub mod ws_native;
pub mod ws_wasi;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod rtc_browser;
pub mod rtc_native;
pub mod rtc_wasi;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub mod wasi_sync_adapter;
