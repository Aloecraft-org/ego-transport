//! High-level peer-to-peer connection API.
//!
//! `connect_p2p()` is the single entry point for establishing a direct P2P
//! connection through a signaling server. It dispatches to the appropriate
//! platform implementation:
//!
//! - **Browser**: `RtcBrowser` (WebRTC via `web_sys`)
//! - **Native**: `RtcNative` (WebRTC via `webrtc-rs`)
//! - **WASI**: `RtcWasi` (relay through signaling server)
//!
//! ## Usage
//!
//! ```no_run
//! use ego_transport::transport::connect_p2p;
//! use ego_transport::transport::rtc_signaling::IceServerConfig;
//! use ego_transport::transport::Transport;
//!
//! async fn play_game() {
//!     let mut peer = connect_p2p(
//!         "ws://signal.example.com:9995",
//!         "game-room-42",
//!         &IceServerConfig::default_config(),
//!     ).await.unwrap();
//!
//!     // Use it like any other Transport — platform details are hidden
//!     peer.send(b"game state update").await.unwrap();
//!     let mut buf = [0u8; 4096];
//!     let n = peer.recv(&mut buf).await.unwrap();
//! }
//! ```

use crate::transport::rtc_signaling::IceServerConfig;
use crate::transport::{Transport, TransportError};

/// Connect to a peer through a signaling server.
///
/// Returns a `Box<dyn Transport>` that communicates with the remote peer.
/// The underlying implementation depends on the platform:
///
/// - **Browser**: Direct WebRTC data channel (STUN/TURN via browser ICE)
/// - **Native**: Direct WebRTC data channel (STUN/TURN via webrtc-rs ICE)
/// - **WASI**: Relayed through the signaling server's WebSocket
///
/// The signaling server URL should be a WebSocket URL (e.g., `ws://host:port`).
/// Both peers must join the same `room` name. The first peer to join becomes
/// the offerer; the second becomes the answerer.
///
/// `ice_servers` configures STUN/TURN servers for NAT traversal. Use
/// `IceServerConfig::default_config()` for Google's public STUN server.
/// WASI peers ignore this parameter (they relay through the signaling server).
pub async fn connect_p2p(
    signaling_url: &str,
    room: &str,
    ice_servers: &[IceServerConfig],
) -> Result<Box<dyn Transport>, TransportError> {
    // ── Browser ──────────────────────────────────────────────────────────
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        use crate::platform::rtc_browser::RtcBrowser;
        let rtc = RtcBrowser::connect(signaling_url, room, ice_servers).await?;
        return Ok(Box::new(rtc) as Box<dyn Transport>);
    }

    // ── Native ───────────────────────────────────────────────────────────
    #[cfg(not(target_arch = "wasm32"))]
    {
        use crate::platform::rtc_native::RtcNative;
        let rtc = RtcNative::connect(signaling_url, room, ice_servers).await?;
        return Ok(Box::new(rtc) as Box<dyn Transport>);
    }

    // ── WASI ─────────────────────────────────────────────────────────────
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    {
        use crate::platform::rtc_wasi::RtcWasi;
        let rtc = RtcWasi::connect(signaling_url, room, ice_servers).await?;
        return Ok(Box::new(rtc) as Box<dyn Transport>);
    }
}
