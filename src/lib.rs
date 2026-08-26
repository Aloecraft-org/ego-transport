pub mod platform;
pub mod transport;

#[cfg(not(target_arch = "wasm32"))]
pub use platform::ws_native::WebSocketNative;

#[cfg(not(target_arch = "wasm32"))]
pub use platform::tcp_native::TcpStreamNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub use platform::tcp_wasi::TcpStreamWasi;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub use platform::ws_wasi::WebSocketWasi;
