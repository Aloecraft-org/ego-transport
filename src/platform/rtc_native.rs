//! Native WebRTC transport via the `webrtc` crate (pure-Rust).
//!
//! Implements the `Transport` trait over a WebRTC data channel, using the
//! `webrtc-rs` stack for ICE/STUN/TURN. Connection establishment is performed
//! through a signaling server using the shared `rtc_signaling` message types.
//!
//! ## Usage
//!
//! ```no_run
//! # #[cfg(not(target_arch = "wasm32"))]
//! # mod example {
//! use ego_transport::platform::rtc_native::RtcNative;
//! use ego_transport::transport::rtc_signaling::IceServerConfig;
//! use ego_transport::transport::Transport;
//!
//! async fn connect_to_peer() {
//!     let mut rtc = RtcNative::connect(
//!         "ws://signal.example.com:9995",
//!         "my-game-room",
//!         &IceServerConfig::default_config(),
//!     ).await.unwrap();
//!
//!     rtc.send(b"hello peer").await.unwrap();
//!     let mut buf = [0u8; 1024];
//!     let n = rtc.recv(&mut buf).await.unwrap();
//! }
//! # }
//! ```
//!
//! ## Architecture
//!
//! Same flow as `rtc_browser.rs`:
//! 1. Connect to signaling server via WebSocket
//! 2. Join room, wait for Ready + role assignment
//! 3. Create RTCPeerConnection with ICE config
//! 4. Exchange SDP offer/answer and ICE candidates through signaling
//! 5. Data channel opens → return as Transport
//! 6. Signaling connection dropped
//!
//! ## Platform
//!
//! Native only (`not(target_arch = "wasm32")`). Requires the `webrtc` crate.

#[cfg(not(target_arch = "wasm32"))]
use crate::path::{CandidateKind, PathInfo};
#[cfg(not(target_arch = "wasm32"))]
use crate::transport::rtc_signaling::{
    IceCandidate, IceServerConfig, IceTransportPolicy, PeerRole, RtcOptions, SignalingKind,
    SignalingMessage,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::transport::{Transport, TransportError};

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::{Mutex, Notify, mpsc};

#[cfg(not(target_arch = "wasm32"))]
use webrtc::api::APIBuilder;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::api::interceptor_registry::register_default_interceptors;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::api::media_engine::MediaEngine;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::api::setting_engine::SettingEngine;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::data_channel::RTCDataChannel;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::data_channel::data_channel_message::DataChannelMessage;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::ice_transport::ice_server::RTCIceServer;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::interceptor::registry::Registry;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::peer_connection::RTCPeerConnection;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::peer_connection::configuration::RTCConfiguration;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
#[cfg(not(target_arch = "wasm32"))]
use webrtc::stats::StatsReportType;

// ─── Constants ───────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
const DATA_CHANNEL_LABEL: &str = "aloecraft";

// ─── RtcNative ───────────────────────────────────────────────────────────────

/// A WebRTC data channel wrapped as a `Transport`.
///
/// Created via `RtcNative::connect()`. Same interface as `RtcBrowser`.
#[cfg(not(target_arch = "wasm32"))]
pub struct RtcNative {
    /// Keeps the peer connection alive — dropping it tears down ICE and the
    /// data channel — and is also what `path()` queries for the selected
    /// candidate pair.
    pc: Arc<RTCPeerConnection>,
    /// The data channel for sending.
    dc: Arc<RTCDataChannel>,
    /// Receiver for incoming messages.
    rx: mpsc::Receiver<Vec<u8>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl RtcNative {
    /// Connect to a remote peer through a signaling server.
    ///
    /// Same interface as `RtcBrowser::connect()` — signaling URL, room name,
    /// ICE server config.
    pub async fn connect(
        signaling_url: &str,
        room: &str,
        ice_servers: &[IceServerConfig],
    ) -> Result<Self, TransportError> {
        Self::connect_with(signaling_url, room, ice_servers, RtcOptions::default()).await
    }

    /// Connect with explicit ICE options.
    ///
    /// [`IceTransportPolicy::RelayOnly`] forces the connection through a
    /// relay even when a direct path is available;
    /// [`RtcOptions::include_loopback_candidates`] allows peers that share a
    /// host to find each other.
    pub async fn connect_with(
        signaling_url: &str,
        room: &str,
        ice_servers: &[IceServerConfig],
        options: RtcOptions,
    ) -> Result<Self, TransportError> {
        log::info!("[RTC Native] Starting connection to room '{}'", room);

        // ── Step 1: Connect to signaling server ──────────────────────────

        let mut signal_transport = crate::transport::connect(signaling_url).await?;

        // ── Step 2: Join room and get role ───────────────────────────────

        let join_msg = SignalingMessage::join(room);
        signal_transport
            .send(join_msg.serialize().as_bytes())
            .await?;

        let role = loop {
            let mut buf = [0u8; 4096];
            let n = signal_transport.recv(&mut buf).await?;
            let text = String::from_utf8_lossy(&buf[..n]);
            if let Some(msg) = SignalingMessage::deserialize(&text) {
                match msg.kind {
                    SignalingKind::Ready => {
                        let r = PeerRole::from_str(&msg.payload).ok_or_else(|| {
                            TransportError::Protocol(format!("Bad role: {}", msg.payload))
                        })?;
                        break r;
                    }
                    SignalingKind::Error => {
                        return Err(TransportError::Protocol(msg.payload));
                    }
                    _ => {}
                }
            }
        };

        log::info!("[RTC Native] Assigned role: {:?}", role);

        // ── Step 3: Create RTCPeerConnection ─────────────────────────────

        let pc = create_peer_connection(ice_servers, options).await?;

        // ICE candidate channel — collected from on_ice_candidate callback,
        // sent through signaling
        let (ice_tx, mut ice_rx) = mpsc::channel::<IceCandidate>(64);

        // Set up ICE candidate callback
        let ice_tx_clone = ice_tx.clone();
        pc.on_ice_candidate(Box::new(move |candidate| {
            let ice_tx = ice_tx_clone.clone();
            Box::pin(async move {
                if let Some(c) = candidate {
                    let candidate_str = c.to_json().map(|j| j.candidate).unwrap_or_default();
                    if candidate_str.is_empty() {
                        return;
                    }
                    let sdp_mid = c
                        .to_json()
                        .map(|j| j.sdp_mid.unwrap_or_default())
                        .unwrap_or_default();
                    let sdp_mline_index = c
                        .to_json()
                        .map(|j| j.sdp_mline_index.unwrap_or(0))
                        .unwrap_or(0);

                    let ice = IceCandidate::new(&candidate_str, &sdp_mid, sdp_mline_index);
                    ice_tx.send(ice).await.ok();
                }
            })
        }));

        // ── Step 4: Create or wait for data channel ──────────────────────

        // Channel for incoming data channel messages
        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(256);
        let dc_open = Arc::new(Notify::new());

        let dc: Arc<RTCDataChannel>;

        if role == PeerRole::Offerer {
            // Offerer creates the data channel
            let channel = pc
                .create_data_channel(DATA_CHANNEL_LABEL, None)
                .await
                .map_err(|e| {
                    TransportError::Protocol(format!("create_data_channel failed: {}", e))
                })?;

            log::info!("[RTC Native] Created data channel '{}'", DATA_CHANNEL_LABEL);

            wire_data_channel_callbacks(&channel, data_tx.clone(), dc_open.clone()).await;
            dc = channel;

            // Do the signaling exchange
            exchange_signaling_offerer(&pc, &mut signal_transport, &mut ice_rx, room).await?;
        } else {
            // Answerer waits for data channel from offerer
            let dc_holder: Arc<Mutex<Option<Arc<RTCDataChannel>>>> = Arc::new(Mutex::new(None));
            let dc_holder_clone = dc_holder.clone();
            let data_tx_clone = data_tx.clone();
            let dc_open_clone = dc_open.clone();

            pc.on_data_channel(Box::new(move |channel| {
                let dc_holder = dc_holder_clone.clone();
                let data_tx = data_tx_clone.clone();
                let dc_open = dc_open_clone.clone();

                Box::pin(async move {
                    log::info!("[RTC Native] Received data channel: '{}'", channel.label());
                    wire_data_channel_callbacks(&channel, data_tx, dc_open).await;
                    *dc_holder.lock().await = Some(channel);
                })
            }));

            // Do the signaling exchange
            exchange_signaling_answerer(&pc, &mut signal_transport, &mut ice_rx, room).await?;

            // Wait for the data channel to arrive
            let mut attempts = 0;
            loop {
                if let Some(channel) = dc_holder.lock().await.take() {
                    dc = channel;
                    break;
                }
                attempts += 1;
                if attempts > 500 {
                    return Err(TransportError::Protocol(
                        "Data channel never arrived".to_string(),
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        // ── Step 5: Wait for data channel to open ────────────────────────

        // dc_open is notified by the on_open callback
        tokio::select! {
            _ = dc_open.notified() => {
                log::info!("[RTC Native] ✓ Data channel open");
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                return Err(TransportError::Protocol(
                    "Data channel open timeout".to_string(),
                ));
            }
        }

        // Signaling transport dropped — direct P2P from here
        let pc = Arc::new(pc);

        Ok(Self {
            pc,
            dc,
            rx: data_rx,
        })
    }
}

// ─── Transport Implementation ────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Transport for RtcNative {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.dc
            .send(&bytes::Bytes::copy_from_slice(data))
            .await
            .map_err(|e| TransportError::Protocol(format!("DataChannel send failed: {}", e)))?;
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        match self.rx.recv().await {
            Some(data) => {
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
            None => Err(TransportError::Closed),
        }
    }

    async fn path(&self) -> Option<PathInfo> {
        Some(self.path_info().await)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl RtcNative {
    /// What ICE settled on for this connection: a direct route, a punched
    /// one, or a relay.
    ///
    /// ICE can re-nominate a different candidate pair while a connection is
    /// live, so this reads the current pair each time rather than caching an
    /// answer from connect time. Before ICE settles it reports
    /// [`PathKind::Unknown`](crate::path::PathKind::Unknown).
    pub async fn path_info(&self) -> PathInfo {
        let pair = self
            .pc
            .sctp()
            .transport()
            .ice_transport()
            .get_selected_candidate_pair()
            .await;

        let Some(pair) = pair else {
            return PathInfo::unknown();
        };

        let info = PathInfo::from_candidates(
            candidate_kind(&pair.local.typ),
            candidate_kind(&pair.remote.typ),
        )
        .with_addrs(
            Some(format!("{}:{}", pair.local.address, pair.local.port)),
            Some(format!("{}:{}", pair.remote.address, pair.remote.port)),
        );

        info.with_rtt_ms(self.nominated_rtt_ms().await)
    }

    /// Round-trip time of the nominated candidate pair, from the stats
    /// report. Reported in seconds there; milliseconds is the unit the rest
    /// of this crate speaks.
    async fn nominated_rtt_ms(&self) -> Option<f64> {
        let stats = self.pc.get_stats().await;
        stats.reports.values().find_map(|report| match report {
            StatsReportType::CandidatePair(pair) if pair.nominated => {
                let rtt = pair.current_round_trip_time;
                // 0.0 is "not measured yet", not a zero-latency link.
                (rtt > 0.0).then_some(rtt * 1000.0)
            }
            _ => None,
        })
    }
}

/// Translate the webrtc crate's candidate type into this crate's vocabulary.
#[cfg(not(target_arch = "wasm32"))]
fn candidate_kind(typ: &RTCIceCandidateType) -> CandidateKind {
    match typ {
        RTCIceCandidateType::Host => CandidateKind::Host,
        RTCIceCandidateType::Srflx => CandidateKind::ServerReflexive,
        RTCIceCandidateType::Prflx => CandidateKind::PeerReflexive,
        RTCIceCandidateType::Relay => CandidateKind::Relayed,
        RTCIceCandidateType::Unspecified => CandidateKind::Unknown,
    }
}

// ─── Peer Connection Setup ───────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn create_peer_connection(
    ice_servers: &[IceServerConfig],
    options: RtcOptions,
) -> Result<RTCPeerConnection, TransportError> {
    let mut media_engine = MediaEngine::default();
    media_engine
        .register_default_codecs()
        .map_err(|e| TransportError::Protocol(format!("MediaEngine error: {}", e)))?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)
        .map_err(|e| TransportError::Protocol(format!("Interceptor error: {}", e)))?;

    let mut setting_engine = SettingEngine::default();
    if options.include_loopback_candidates {
        setting_engine.set_include_loopback_candidate(true);
    }

    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_setting_engine(setting_engine)
        .build();

    let config = RTCConfiguration {
        ice_servers: ice_servers
            .iter()
            .map(|s| RTCIceServer {
                urls: s.urls.clone(),
                username: s.username.clone().unwrap_or_default(),
                credential: s.credential.clone().unwrap_or_default(),
            })
            .collect(),
        ice_transport_policy: match options.policy {
            IceTransportPolicy::All => RTCIceTransportPolicy::All,
            IceTransportPolicy::RelayOnly => RTCIceTransportPolicy::Relay,
        },
        ..Default::default()
    };

    let pc = api
        .new_peer_connection(config)
        .await
        .map_err(|e| TransportError::Protocol(format!("PeerConnection error: {}", e)))?;

    log::info!(
        "[RTC Native] Created RTCPeerConnection with {} ICE servers",
        ice_servers.len()
    );

    Ok(pc)
}

// ─── Data Channel Callbacks ──────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn wire_data_channel_callbacks(
    dc: &Arc<RTCDataChannel>,
    data_tx: mpsc::Sender<Vec<u8>>,
    dc_open: Arc<Notify>,
) {
    let dc_open_clone = dc_open.clone();
    dc.on_open(Box::new(move || {
        let dc_open = dc_open_clone.clone();
        Box::pin(async move {
            log::info!("[RTC Native] Data channel opened");
            dc_open.notify_one();
        })
    }));

    dc.on_message(Box::new(move |msg: DataChannelMessage| {
        let tx = data_tx.clone();
        Box::pin(async move {
            tx.send(msg.data.to_vec()).await.ok();
        })
    }));

    dc.on_close(Box::new(|| {
        Box::pin(async {
            log::info!("[RTC Native] Data channel closed");
        })
    }));

    dc.on_error(Box::new(|e| {
        Box::pin(async move {
            log::error!("[RTC Native] Data channel error: {}", e);
        })
    }));
}

// ─── Signaling Exchange ──────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
async fn send_signal(
    transport: &mut Box<dyn Transport>,
    msg: &SignalingMessage,
) -> Result<(), TransportError> {
    transport.send(msg.serialize().as_bytes()).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn recv_signal(
    transport: &mut Box<dyn Transport>,
) -> Result<SignalingMessage, TransportError> {
    let mut buf = [0u8; 65536];
    loop {
        let n = transport.recv(&mut buf).await?;
        let text = String::from_utf8_lossy(&buf[..n]);
        if let Some(msg) = SignalingMessage::deserialize(&text) {
            return Ok(msg);
        }
        log::warn!("[RTC Native] Unparseable signaling message, retrying");
    }
}

/// Drain collected ICE candidates and send them through signaling.
#[cfg(not(target_arch = "wasm32"))]
async fn drain_ice(
    ice_rx: &mut mpsc::Receiver<IceCandidate>,
    transport: &mut Box<dyn Transport>,
    room: &str,
) -> Result<(), TransportError> {
    while let Ok(ice) = ice_rx.try_recv() {
        send_signal(transport, &SignalingMessage::ice(room, &ice)).await?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn exchange_signaling_offerer(
    pc: &RTCPeerConnection,
    signal: &mut Box<dyn Transport>,
    ice_rx: &mut mpsc::Receiver<IceCandidate>,
    room: &str,
) -> Result<(), TransportError> {
    // Create offer
    let offer = pc
        .create_offer(None)
        .await
        .map_err(|e| TransportError::Protocol(format!("create_offer failed: {}", e)))?;

    let offer_sdp = offer.sdp.clone();
    log::info!("[RTC Native] Created offer ({} bytes)", offer_sdp.len());

    pc.set_local_description(offer)
        .await
        .map_err(|e| TransportError::Protocol(format!("set_local_description failed: {}", e)))?;

    // Send offer
    send_signal(signal, &SignalingMessage::offer(room, &offer_sdp)).await?;

    // Drain early ICE candidates
    drain_ice(ice_rx, signal, room).await?;

    // Wait for answer + remote ICE
    let mut got_answer = false;
    loop {
        drain_ice(ice_rx, signal, room).await?;

        let msg = recv_signal(signal).await?;
        match msg.kind {
            SignalingKind::Answer => {
                log::info!("[RTC Native] Received answer ({} bytes)", msg.payload.len());
                let answer = RTCSessionDescription::answer(msg.payload)
                    .map_err(|e| TransportError::Protocol(format!("Bad answer SDP: {}", e)))?;
                pc.set_remote_description(answer).await.map_err(|e| {
                    TransportError::Protocol(format!("set_remote_description failed: {}", e))
                })?;
                got_answer = true;
            }
            SignalingKind::Ice => {
                if let Some(ice) = IceCandidate::deserialize(&msg.payload) {
                    let init = RTCIceCandidateInit {
                        candidate: ice.candidate,
                        sdp_mid: Some(ice.sdp_mid),
                        sdp_mline_index: Some(ice.sdp_mline_index),
                        ..Default::default()
                    };
                    pc.add_ice_candidate(init).await.ok();
                }
            }
            SignalingKind::IceDone => {
                log::info!("[RTC Native] Remote ICE gathering complete");
                if got_answer {
                    break;
                }
            }
            SignalingKind::PeerLeft => {
                return Err(TransportError::Protocol("Peer left".to_string()));
            }
            _ => {}
        }
    }

    send_signal(signal, &SignalingMessage::ice_done(room)).await?;
    drain_ice(ice_rx, signal, room).await?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn exchange_signaling_answerer(
    pc: &RTCPeerConnection,
    signal: &mut Box<dyn Transport>,
    ice_rx: &mut mpsc::Receiver<IceCandidate>,
    room: &str,
) -> Result<(), TransportError> {
    // Wait for offer
    let mut pending_ice: Vec<IceCandidate> = Vec::new();

    loop {
        let msg = recv_signal(signal).await?;
        match msg.kind {
            SignalingKind::Offer => {
                log::info!("[RTC Native] Received offer ({} bytes)", msg.payload.len());
                let offer = RTCSessionDescription::offer(msg.payload)
                    .map_err(|e| TransportError::Protocol(format!("Bad offer SDP: {}", e)))?;
                pc.set_remote_description(offer).await.map_err(|e| {
                    TransportError::Protocol(format!("set_remote_description failed: {}", e))
                })?;

                // Apply buffered ICE candidates
                for ice in pending_ice.drain(..) {
                    let init = RTCIceCandidateInit {
                        candidate: ice.candidate,
                        sdp_mid: Some(ice.sdp_mid),
                        sdp_mline_index: Some(ice.sdp_mline_index),
                        ..Default::default()
                    };
                    pc.add_ice_candidate(init).await.ok();
                }
                break;
            }
            SignalingKind::Ice => {
                if let Some(ice) = IceCandidate::deserialize(&msg.payload) {
                    pending_ice.push(ice);
                }
            }
            SignalingKind::PeerLeft => {
                return Err(TransportError::Protocol("Peer left".to_string()));
            }
            _ => {}
        }
    }

    // Create answer
    let answer = pc
        .create_answer(None)
        .await
        .map_err(|e| TransportError::Protocol(format!("create_answer failed: {}", e)))?;

    let answer_sdp = answer.sdp.clone();
    log::info!("[RTC Native] Created answer ({} bytes)", answer_sdp.len());

    pc.set_local_description(answer)
        .await
        .map_err(|e| TransportError::Protocol(format!("set_local_description failed: {}", e)))?;

    // Send answer
    send_signal(signal, &SignalingMessage::answer(room, &answer_sdp)).await?;
    drain_ice(ice_rx, signal, room).await?;

    // Continue receiving remote ICE until done
    loop {
        drain_ice(ice_rx, signal, room).await?;
        let msg = recv_signal(signal).await?;
        match msg.kind {
            SignalingKind::Ice => {
                if let Some(ice) = IceCandidate::deserialize(&msg.payload) {
                    let init = RTCIceCandidateInit {
                        candidate: ice.candidate,
                        sdp_mid: Some(ice.sdp_mid),
                        sdp_mline_index: Some(ice.sdp_mline_index),
                        ..Default::default()
                    };
                    pc.add_ice_candidate(init).await.ok();
                }
            }
            SignalingKind::IceDone => {
                log::info!("[RTC Native] Remote ICE gathering complete");
                break;
            }
            _ => {}
        }
    }

    send_signal(signal, &SignalingMessage::ice_done(room)).await?;
    drain_ice(ice_rx, signal, room).await?;

    Ok(())
}
