use crate::transport::{Transport, TransportError};

#[cfg(not(target_arch = "wasm32"))]
use crate::platform::ws_native::WebSocketNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use crate::platform::ws_wasi::WebSocketWasi;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::platform::ws_browser::WebSocketBrowser;

pub async fn connect_ws(url: &str) -> Result<Box<dyn Transport>, TransportError> {
    #[cfg(not(target_arch = "wasm32"))]
    return Ok(Box::new(WebSocketNative::connect(url).await?));

    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    return Ok(Box::new(WebSocketWasi::connect(url).await?));

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    return Ok(Box::new(WebSocketBrowser::connect(url).await?));
}
