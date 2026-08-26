pub mod test_actor;
pub mod test_harness;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use wasm_bindgen_test::wasm_bindgen_test;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use tokio::test as async_test;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use wasm_bindgen_test as async_test;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use wasm_bindgen_test as test;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use test;

/// Default relay address for test fixtures
pub const TEST_RELAY_ADDR: &str = "127.0.0.1:19983";
