pub mod tcp_native;
pub mod tcp_wasi;
pub mod ws_browser;
pub mod ws_native;
pub mod ws_wasi;
pub mod sleep;
pub mod spawn;
pub mod server;
pub use sleep::yield_now;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub mod wasi_sync_adapter;  // Add this

/// Initialize Logging
pub fn init_logging() {
    crate::log_impl::init();
}
