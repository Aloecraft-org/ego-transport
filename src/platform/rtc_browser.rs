//! Browser WebRTC transport via `web_sys::RtcPeerConnection`.
//!
//! Implements the `Transport` trait over a WebRTC data channel, using the
//! browser's built-in ICE/STUN/TURN stack. Connection establishment is
//! performed through a signaling server using the shared `rtc_signaling`
//! message types.
//!
//! ## Usage
//!
//! ```no_run
//! use ego_transport::platform::rtc_browser::RtcBrowser;
//! use ego_transport::transport::rtc_signaling::IceServerConfig;
//! use ego_transport::transport::Transport;
//!
//! async fn connect_to_peer() {
//!     let mut rtc = RtcBrowser::connect(
//!         "ws://signal.example.com:9995",  // signaling server
//!         "my-game-room",                   // room name
//!         &IceServerConfig::default_config(),
//!     ).await.unwrap();
//!
//!     // Now use it like any other Transport
//!     rtc.send(b"hello peer").await.unwrap();
//!     let mut buf = [0u8; 1024];
//!     let n = rtc.recv(&mut buf).await.unwrap();
//! }
//! ```
//!
//! ## Architecture
//!
//! The connect flow:
//!
//! 1. Open a WebSocket to the signaling server (reusing `WebSocketBrowser`)
//! 2. Send JOIN, wait for READY with our assigned role (offerer/answerer)
//! 3. Create `RTCPeerConnection` with ICE server config
//! 4. Create (offerer) or wait for (answerer) a data channel
//! 5. Exchange SDP offer/answer and ICE candidates through signaling
//! 6. Once the data channel opens, return the `RtcBrowser` as a `Transport`
//! 7. The signaling WebSocket is dropped — all further data flows over the
//!    direct P2P data channel
//!
//! ## Platform
//!
//! Browser-only (`wasm32-unknown-unknown`). Uses `web_sys` bindings to the
//! WebRTC API. Single-threaded (`?Send`), matching the browser execution model.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::transport::{Transport, TransportError};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::transport::rtc_signaling::{
    IceCandidate, IceServerConfig, PeerRole, SignalingKind, SignalingMessage,
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::{Array, Object, Reflect, JsString};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use std::cell::RefCell;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use std::rc::Rc;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use tokio::sync::mpsc;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::JsCast;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_sys::{
    MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcDataChannelInit,
    RtcIceCandidate, RtcIceCandidateInit, RtcPeerConnection, RtcPeerConnectionIceEvent,
    RtcSdpType, RtcSessionDescriptionInit,
};

// ─── Data Channel Label ──────────────────────────────────────────────────────

/// The label used for the WebRTC data channel. Both peers must agree on this.
const DATA_CHANNEL_LABEL: &str = "aloecraft";

// ─── RtcBrowser ──────────────────────────────────────────────────────────────

/// A WebRTC data channel wrapped as a `Transport`.
///
/// Created via `RtcBrowser::connect()`, which handles the full signaling
/// handshake. Once connected, `send()` and `recv()` operate over the direct
/// P2P data channel.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub struct RtcBrowser {
    /// The underlying RTCPeerConnection. Kept alive for the duration of the
    /// connection — dropping it tears down the ICE agent and data channel.
    _pc: RtcPeerConnection,

    /// The data channel used for application data.
    dc: RtcDataChannel,

    /// Receiver for incoming data channel messages.
    rx: mpsc::UnboundedReceiver<Vec<u8>>,

    /// Whether the data channel has been closed.
    closed: Rc<RefCell<bool>>,

    // ── prevent closures from being garbage collected ──
    _on_dc_message: Closure<dyn FnMut(MessageEvent)>,
    _on_dc_close: Closure<dyn FnMut(JsValue)>,
    _on_dc_error: Closure<dyn FnMut(JsValue)>,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl RtcBrowser {
    /// Connect to a remote peer through a signaling server.
    ///
    /// This performs the full WebRTC connection establishment:
    /// 1. Connects to the signaling server via WebSocket
    /// 2. Joins the specified room and waits for a peer
    /// 3. Exchanges SDP offer/answer and ICE candidates
    /// 4. Returns once the data channel is open and ready
    ///
    /// The signaling WebSocket is dropped after the connection is established.
    /// All subsequent data flows over the direct P2P data channel.
    pub async fn connect(
        signaling_url: &str,
        room: &str,
        ice_servers: &[IceServerConfig],
    ) -> Result<Self, TransportError> {
        log::info!("[RTC Browser] Starting connection to room '{}'", room);

        // ── Step 1: Connect to signaling server ──────────────────────────

        let mut signal = SignalingTransport::connect(signaling_url).await?;

        // ── Step 2: Join room and get role ───────────────────────────────

        log::info!("[RTC Browser] Sending JOIN for room '{}'", room);
        signal.send_msg(&SignalingMessage::join(room)).await?;
        log::info!("[RTC Browser] JOIN sent, waiting for READY...");

        let role = loop {
            let msg = signal.recv_msg().await?;
            match msg.kind {
                SignalingKind::Ready => {
                    let role = PeerRole::from_str(&msg.payload).ok_or_else(|| {
                        TransportError::Protocol(format!("Bad role: {}", msg.payload))
                    })?;
                    break role;
                }
                SignalingKind::Error => {
                    return Err(TransportError::Protocol(msg.payload));
                }
                _ => {
                    log::debug!("[RTC Browser] Ignoring pre-Ready message: {:?}", msg.kind);
                }
            }
        };

        log::info!("[RTC Browser] Assigned role: {:?}", role);

        // ── Step 3: Create RTCPeerConnection ─────────────────────────────

        let pc = create_peer_connection(ice_servers)?;

        // Channel for collecting ICE candidates to send through signaling
        let (ice_tx, mut ice_rx) = mpsc::unbounded_channel::<IceCandidate>();

        // Set up ICE candidate callback
        let ice_tx_clone = ice_tx.clone();
        let on_ice_candidate =
            Closure::wrap(Box::new(move |event: RtcPeerConnectionIceEvent| {
                if let Some(candidate) = event.candidate() {
                    let candidate_str = candidate.candidate();
                    if candidate_str.is_empty() {
                        // Empty candidate means ICE gathering is done
                        return;
                    }
                    let ice = IceCandidate::new(
                        &candidate_str,
                        &candidate.sdp_mid().unwrap_or_default(),
                        candidate.sdp_m_line_index().unwrap_or(0),
                    );
                    ice_tx_clone.send(ice).ok();
                }
            }) as Box<dyn FnMut(_)>);

        pc.set_onicecandidate(Some(on_ice_candidate.as_ref().unchecked_ref()));
        on_ice_candidate.forget(); // prevent GC — pc owns the callback lifetime

        // ── Step 4: Create or wait for data channel ──────────────────────

        // Channel to signal when the data channel is open and ready
        let dc_ready = Rc::new(RefCell::new(false));
        let (dc_data_tx, dc_data_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let dc_closed = Rc::new(RefCell::new(false));

        let dc: RtcDataChannel;
        let on_dc_message: Closure<dyn FnMut(MessageEvent)>;
        let on_dc_close: Closure<dyn FnMut(JsValue)>;
        let on_dc_error: Closure<dyn FnMut(JsValue)>;

        // Closure to hold on_datachannel for the answerer path
        // We need to keep this alive until the data channel is established
        let _on_datachannel: Option<Closure<dyn FnMut(RtcDataChannelEvent)>>;

        if role == PeerRole::Offerer {
            // Offerer creates the data channel
            let mut dc_init = RtcDataChannelInit::new();
            dc_init.ordered(true);

            dc = pc.create_data_channel_with_data_channel_dict(DATA_CHANNEL_LABEL, &dc_init);
            log::info!("[RTC Browser] Created data channel '{}'", DATA_CHANNEL_LABEL);

            let (on_msg, on_close, on_err) =
                wire_data_channel_callbacks(&dc, &dc_data_tx, &dc_closed, &dc_ready);
            on_dc_message = on_msg;
            on_dc_close = on_close;
            on_dc_error = on_err;
            _on_datachannel = None;
        } else {
            // Answerer waits for the data channel from the offerer.
            // We use a shared Rc<RefCell<Option<...>>> to capture the channel
            // when ondatachannel fires.
            let dc_holder: Rc<RefCell<Option<RtcDataChannel>>> = Rc::new(RefCell::new(None));
            let dc_holder_clone = dc_holder.clone();
            let dc_data_tx_clone = dc_data_tx.clone();
            let dc_closed_clone = dc_closed.clone();
            let dc_ready_clone = dc_ready.clone();

            // We need to store the callback closures somewhere they won't be dropped.
            // Since the data channel callbacks are set inside ondatachannel, we use
            // Rc<RefCell> to extract them.
            let callback_holder: Rc<
                RefCell<
                    Option<(
                        Closure<dyn FnMut(MessageEvent)>,
                        Closure<dyn FnMut(JsValue)>,
                        Closure<dyn FnMut(JsValue)>,
                    )>,
                >,
            > = Rc::new(RefCell::new(None));
            let callback_holder_clone = callback_holder.clone();

            let on_dc_event =
                Closure::wrap(Box::new(move |event: RtcDataChannelEvent| {
                    let channel = event.channel();
                    log::info!(
                        "[RTC Browser] Received data channel: '{}'",
                        channel.label()
                    );

                    let (on_msg, on_close, on_err) = wire_data_channel_callbacks(
                        &channel,
                        &dc_data_tx_clone,
                        &dc_closed_clone,
                        &dc_ready_clone,
                    );

                    // Store callbacks so they aren't dropped
                    *callback_holder_clone.borrow_mut() = Some((on_msg, on_close, on_err));
                    *dc_holder_clone.borrow_mut() = Some(channel);
                }) as Box<dyn FnMut(_)>);

            pc.set_ondatachannel(Some(on_dc_event.as_ref().unchecked_ref()));
            _on_datachannel = Some(on_dc_event);

            // We'll extract dc after signaling completes and the channel opens.
            // For now, do the signaling and then wait.

            // Perform signaling first (below), then extract the channel.
            // The channel will be set by ondatachannel during setRemoteDescription.

            // Do the SDP/ICE exchange as answerer
            exchange_signaling_answerer(&pc, &mut signal, room, &mut ice_rx).await?;

            // Wait for data channel to arrive
            let mut attempts = 0;
            while dc_holder.borrow().is_none() && attempts < 500 {
                ego_platform::sleep(std::time::Duration::from_millis(10)).await;
                attempts += 1;
            }

            let dc_opt = dc_holder.borrow_mut().take();
            dc = dc_opt.ok_or_else(|| {
                TransportError::Protocol("Data channel never arrived".to_string())
            })?;

            // Extract the stored callbacks
            let callbacks = callback_holder.borrow_mut().take();
            if let Some((msg_cb, close_cb, err_cb)) = callbacks {
                on_dc_message = msg_cb;
                on_dc_close = close_cb;
                on_dc_error = err_cb;
            } else {
                return Err(TransportError::Protocol(
                    "Data channel callbacks not set".to_string(),
                ));
            }

            // Wait for data channel to open
            let mut attempts = 0;
            while !*dc_ready.borrow() && attempts < 500 {
                ego_platform::sleep(std::time::Duration::from_millis(10)).await;
                attempts += 1;
            }

            if !*dc_ready.borrow() {
                return Err(TransportError::Protocol(
                    "Data channel did not open".to_string(),
                ));
            }

            log::info!("[RTC Browser] ✓ Data channel open (answerer path)");

            // Signal transport is dropped here — signaling is done
            return Ok(Self {
                _pc: pc,
                dc,
                rx: dc_data_rx,
                closed: dc_closed,
                _on_dc_message: on_dc_message,
                _on_dc_close: on_dc_close,
                _on_dc_error: on_dc_error,
            });
        }

        // ── Step 5: (Offerer) Exchange SDP and ICE ───────────────────────

        exchange_signaling_offerer(&pc, &mut signal, room, &mut ice_rx).await?;

        // ── Step 6: Wait for data channel to open ────────────────────────

        let mut attempts = 0;
        while !*dc_ready.borrow() && attempts < 500 {
            ego_platform::sleep(std::time::Duration::from_millis(10)).await;
            attempts += 1;

            // Drain and send any ICE candidates that arrived
            while let Ok(ice) = ice_rx.try_recv() {
                let msg = SignalingMessage::ice(room, &ice);
                signal.send_msg(&msg).await.ok();
            }
        }

        if !*dc_ready.borrow() {
            return Err(TransportError::Protocol(
                "Data channel did not open in time".to_string(),
            ));
        }

        log::info!("[RTC Browser] ✓ Data channel open (offerer path)");

        // Signaling transport dropped — direct P2P from here
        Ok(Self {
            _pc: pc,
            dc,
            rx: dc_data_rx,
            closed: dc_closed,
            _on_dc_message: on_dc_message,
            _on_dc_close: on_dc_close,
            _on_dc_error: on_dc_error,
        })
    }
}

// ─── Transport Implementation ────────────────────────────────────────────────

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[async_trait(?Send)]
impl Transport for RtcBrowser {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        if *self.closed.borrow() {
            return Err(TransportError::Closed);
        }

        // RtcDataChannel.send() accepts ArrayBuffer
        let array = js_sys::Uint8Array::new_with_length(data.len() as u32);
        array.copy_from(data);

        self.dc
            .send_with_array_buffer(&array.buffer())
            .map_err(|e| TransportError::Protocol(format!("DataChannel send failed: {:?}", e)))?;

        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        let mut attempts = 0;
        loop {
            match self.rx.try_recv() {
                Ok(data) => {
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    return Ok(n);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(TransportError::Closed);
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    if *self.closed.borrow() {
                        return Err(TransportError::Closed);
                    }
                    attempts += 1;
                    if attempts >= 10000 {
                        return Err(TransportError::Protocol(
                            "DataChannel receive timeout".to_string(),
                        ));
                    }
                    ego_platform::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
}

// ─── Signaling Helpers ───────────────────────────────────────────────────────
//
// These functions orchestrate the SDP/ICE exchange through the signaling
// WebSocket. They're separate from the Transport impl because they only
// run during connection setup, not during the data transfer phase.

/// Offerer signaling flow:
/// 1. Create offer → setLocalDescription
/// 2. Send offer through signaling
/// 3. Wait for answer → setRemoteDescription
/// 4. Exchange ICE candidates
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn exchange_signaling_offerer(
    pc: &RtcPeerConnection,
    signal: &mut SignalingTransport,
    room: &str,
    ice_rx: &mut mpsc::UnboundedReceiver<IceCandidate>,
) -> Result<(), TransportError> {
    // Create and set local offer
    let offer = wasm_bindgen_futures::JsFuture::from(pc.create_offer())
        .await
        .map_err(|e| TransportError::Protocol(format!("createOffer failed: {:?}", e)))?;

    let offer_sdp = Reflect::get(&offer, &JsValue::from_str("sdp"))
        .map_err(|e| TransportError::Protocol(format!("No sdp in offer: {:?}", e)))?
        .as_string()
        .ok_or_else(|| TransportError::Protocol("Offer SDP is not a string".to_string()))?;

    log::info!(
        "[RTC Browser] Created offer ({} bytes)",
        offer_sdp.len()
    );

    let mut offer_desc = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
    offer_desc.sdp(&offer_sdp);

    wasm_bindgen_futures::JsFuture::from(pc.set_local_description(&offer_desc))
        .await
        .map_err(|e| TransportError::Protocol(format!("setLocalDescription failed: {:?}", e)))?;

    // Send offer to peer through signaling
    signal
        .send_msg(&SignalingMessage::offer(room, &offer_sdp))
        .await?;

    // Send any ICE candidates that have already been collected
    while let Ok(ice) = ice_rx.try_recv() {
        signal
            .send_msg(&SignalingMessage::ice(room, &ice))
            .await?;
    }

    // Wait for answer and remote ICE candidates
    let mut got_answer = false;

    loop {
        // Drain local ICE candidates
        while let Ok(ice) = ice_rx.try_recv() {
            signal
                .send_msg(&SignalingMessage::ice(room, &ice))
                .await?;
        }

        let msg = signal.recv_msg().await?;
        match msg.kind {
            SignalingKind::Answer => {
                log::info!(
                    "[RTC Browser] Received answer ({} bytes)",
                    msg.payload.len()
                );

                let mut answer_desc = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
                answer_desc.sdp(&msg.payload);

                wasm_bindgen_futures::JsFuture::from(pc.set_remote_description(&answer_desc))
                    .await
                    .map_err(|e| {
                        TransportError::Protocol(format!(
                            "setRemoteDescription failed: {:?}",
                            e
                        ))
                    })?;

                got_answer = true;
            }
            SignalingKind::Ice => {
                if let Some(ice) = IceCandidate::deserialize(&msg.payload) {
                    add_ice_candidate(pc, &ice).await?;
                }
            }
            SignalingKind::IceDone => {
                log::info!("[RTC Browser] Remote ICE gathering complete");
                if got_answer {
                    break;
                }
            }
            SignalingKind::PeerLeft => {
                return Err(TransportError::Protocol(
                    "Peer left during signaling".to_string(),
                ));
            }
            _ => {
                log::debug!("[RTC Browser] Ignoring {:?} during offerer signaling", msg.kind);
            }
        }
    }

    // Send ICE done
    signal
        .send_msg(&SignalingMessage::ice_done(room))
        .await?;

    // Drain remaining ICE candidates
    while let Ok(ice) = ice_rx.try_recv() {
        signal
            .send_msg(&SignalingMessage::ice(room, &ice))
            .await?;
    }

    Ok(())
}

/// Answerer signaling flow:
/// 1. Wait for offer → setRemoteDescription
/// 2. Create answer → setLocalDescription
/// 3. Send answer through signaling
/// 4. Exchange ICE candidates
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn exchange_signaling_answerer(
    pc: &RtcPeerConnection,
    signal: &mut SignalingTransport,
    room: &str,
    ice_rx: &mut mpsc::UnboundedReceiver<IceCandidate>,
) -> Result<(), TransportError> {
    // Wait for offer
    let mut got_offer = false;
    let mut pending_ice: Vec<IceCandidate> = Vec::new();

    loop {
        let msg = signal.recv_msg().await?;
        match msg.kind {
            SignalingKind::Offer => {
                log::info!(
                    "[RTC Browser] Received offer ({} bytes)",
                    msg.payload.len()
                );

                let mut offer_desc = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
                offer_desc.sdp(&msg.payload);

                wasm_bindgen_futures::JsFuture::from(pc.set_remote_description(&offer_desc))
                    .await
                    .map_err(|e| {
                        TransportError::Protocol(format!(
                            "setRemoteDescription failed: {:?}",
                            e
                        ))
                    })?;

                // Apply any ICE candidates that arrived before the offer
                for ice in pending_ice.drain(..) {
                    add_ice_candidate(pc, &ice).await?;
                }

                got_offer = true;
                break;
            }
            SignalingKind::Ice => {
                // ICE candidates may arrive before the offer. Buffer them.
                if let Some(ice) = IceCandidate::deserialize(&msg.payload) {
                    if got_offer {
                        add_ice_candidate(pc, &ice).await?;
                    } else {
                        pending_ice.push(ice);
                    }
                }
            }
            SignalingKind::PeerLeft => {
                return Err(TransportError::Protocol(
                    "Peer left during signaling".to_string(),
                ));
            }
            _ => {
                log::debug!("[RTC Browser] Ignoring {:?} during answerer signaling", msg.kind);
            }
        }
    }

    // Create and set local answer
    let answer = wasm_bindgen_futures::JsFuture::from(pc.create_answer())
        .await
        .map_err(|e| TransportError::Protocol(format!("createAnswer failed: {:?}", e)))?;

    let answer_sdp = Reflect::get(&answer, &JsValue::from_str("sdp"))
        .map_err(|e| TransportError::Protocol(format!("No sdp in answer: {:?}", e)))?
        .as_string()
        .ok_or_else(|| TransportError::Protocol("Answer SDP is not a string".to_string()))?;

    log::info!(
        "[RTC Browser] Created answer ({} bytes)",
        answer_sdp.len()
    );

    let mut answer_desc = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
    answer_desc.sdp(&answer_sdp);

    wasm_bindgen_futures::JsFuture::from(pc.set_local_description(&answer_desc))
        .await
        .map_err(|e| TransportError::Protocol(format!("setLocalDescription failed: {:?}", e)))?;

    // Send answer to peer through signaling
    signal
        .send_msg(&SignalingMessage::answer(room, &answer_sdp))
        .await?;

    // Send any local ICE candidates
    while let Ok(ice) = ice_rx.try_recv() {
        signal
            .send_msg(&SignalingMessage::ice(room, &ice))
            .await?;
    }

    // Continue receiving remote ICE candidates until done
    loop {
        // Drain local ICE candidates
        while let Ok(ice) = ice_rx.try_recv() {
            signal
                .send_msg(&SignalingMessage::ice(room, &ice))
                .await?;
        }

        // Non-blocking check for more signaling messages.
        // We use a short timeout since the data channel may already be opening.
        let msg = signal.recv_msg().await;
        match msg {
            Ok(msg) => match msg.kind {
                SignalingKind::Ice => {
                    if let Some(ice) = IceCandidate::deserialize(&msg.payload) {
                        add_ice_candidate(pc, &ice).await?;
                    }
                }
                SignalingKind::IceDone => {
                    log::info!("[RTC Browser] Remote ICE gathering complete");
                    break;
                }
                _ => {
                    log::debug!(
                        "[RTC Browser] Ignoring {:?} during answerer ICE phase",
                        msg.kind
                    );
                }
            },
            Err(_) => break,
        }
    }

    // Send ICE done
    signal
        .send_msg(&SignalingMessage::ice_done(room))
        .await?;

    // Drain remaining
    while let Ok(ice) = ice_rx.try_recv() {
        signal
            .send_msg(&SignalingMessage::ice(room, &ice))
            .await?;
    }

    Ok(())
}

// ─── RTCPeerConnection Helpers ───────────────────────────────────────────────

/// Create an RTCPeerConnection with the given ICE server configuration.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn create_peer_connection(
    ice_servers: &[IceServerConfig],
) -> Result<RtcPeerConnection, TransportError> {
    let config = RtcConfiguration::new();

    // Build iceServers array
    let servers = Array::new();
    for server in ice_servers {
        let entry = Object::new();

        let urls = Array::new();
        for url in &server.urls {
            urls.push(&JsValue::from_str(url));
        }
        Reflect::set(&entry, &JsValue::from_str("urls"), &urls)
            .map_err(|e| TransportError::Protocol(format!("Failed to set urls: {:?}", e)))?;

        if let Some(username) = &server.username {
            Reflect::set(
                &entry,
                &JsValue::from_str("username"),
                &JsValue::from_str(username),
            )
            .ok();
        }
        if let Some(credential) = &server.credential {
            Reflect::set(
                &entry,
                &JsValue::from_str("credential"),
                &JsValue::from_str(credential),
            )
            .ok();
        }

        servers.push(&entry);
    }

    config.set_ice_servers(&servers);

    let pc = RtcPeerConnection::new_with_configuration(&config)
        .map_err(|e| TransportError::Protocol(format!("Failed to create RTCPeerConnection: {:?}", e)))?;

    log::info!("[RTC Browser] Created RTCPeerConnection with {} ICE servers", ice_servers.len());

    Ok(pc)
}

/// Add a remote ICE candidate to the peer connection.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn add_ice_candidate(
    pc: &RtcPeerConnection,
    ice: &IceCandidate,
) -> Result<(), TransportError> {
    let mut init = RtcIceCandidateInit::new(&ice.candidate);
    init.sdp_mid(Some(&ice.sdp_mid));
    init.sdp_m_line_index(Some(ice.sdp_mline_index));

    let candidate = RtcIceCandidate::new(&init)
        .map_err(|e| TransportError::Protocol(format!("Invalid ICE candidate: {:?}", e)))?;

    wasm_bindgen_futures::JsFuture::from(
        pc.add_ice_candidate_with_opt_rtc_ice_candidate(Some(&candidate)),
    )
    .await
    .map_err(|e| TransportError::Protocol(format!("addIceCandidate failed: {:?}", e)))?;

    log::debug!("[RTC Browser] Added remote ICE candidate");
    Ok(())
}

// ─── Data Channel Callback Wiring ────────────────────────────────────────────

/// Wire up onmessage, onclose, onerror callbacks on a data channel.
/// Returns the closures so the caller can keep them alive.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn wire_data_channel_callbacks(
    dc: &RtcDataChannel,
    data_tx: &mpsc::UnboundedSender<Vec<u8>>,
    closed: &Rc<RefCell<bool>>,
    ready: &Rc<RefCell<bool>>,
) -> (
    Closure<dyn FnMut(MessageEvent)>,
    Closure<dyn FnMut(JsValue)>,
    Closure<dyn FnMut(JsValue)>,
) {
    // Set binary type
    dc.set_binary_type(web_sys::RtcDataChannelType::Arraybuffer);

    // onmessage
    let tx = data_tx.clone();
    let on_message = Closure::wrap(Box::new( move |event: MessageEvent| {
        if let Ok(array_buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
            let uint8 = js_sys::Uint8Array::new(&array_buffer);
            let data = uint8.to_vec();
            tx.send(data).ok();
        } else if let Ok(txt) = event.data().dyn_into::<JsString>() {
            let data = txt.as_string().unwrap_or_default().into_bytes();
            tx.send(data).ok();
        }
    }) as Box<dyn FnMut(_)>);
    dc.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

    // onclose
    let closed_clone = closed.clone();
    let on_close = Closure::wrap(Box::new(move |_: JsValue| {
        log::info!("[RTC Browser] Data channel closed");
        *closed_clone.borrow_mut() = true;
    }) as Box<dyn FnMut(_)>);
    dc.set_onclose(Some(on_close.as_ref().unchecked_ref()));

    // onerror
    let on_error = Closure::wrap(Box::new(move |e: JsValue| {
        log::error!("[RTC Browser] Data channel error: {:?}", e);
    }) as Box<dyn FnMut(_)>);
    dc.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    // onopen — mark ready when the channel opens
    let ready_clone = ready.clone();
    let on_open = Closure::wrap(Box::new(move |_: JsValue| {
        log::info!("[RTC Browser] Data channel opened");
        *ready_clone.borrow_mut() = true;
    }) as Box<dyn FnMut(_)>);
    dc.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    on_open.forget(); // onopen is fire-once, safe to forget

    (on_message, on_close, on_error)
}

// ─── Signaling Transport ─────────────────────────────────────────────────────
//
// Thin wrapper around the browser WebSocket for signaling-phase communication.
// This is intentionally NOT the same as WebSocketBrowser from ws_browser.rs —
// that one implements Transport and is meant for long-lived data transfer.
// This one is short-lived (dropped after signaling) and speaks SignalingMessage.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
struct SignalingTransport {
    ws: web_sys::WebSocket,
    rx: mpsc::UnboundedReceiver<String>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    // _on_open: Closure<dyn FnMut(JsValue)>,
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl SignalingTransport {
    async fn connect(url: &str) -> Result<Self, TransportError> {
        let ws = web_sys::WebSocket::new(url)
            .map_err(|e| TransportError::Protocol(format!("WebSocket connect failed: {:?}", e)))?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let (tx, rx) = mpsc::unbounded_channel::<String>();
        let connected = Rc::new(RefCell::new(false));

        // onmessage — collect signaling messages as strings
        let tx_clone = tx.clone();
        let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
            if let Ok(txt) = event.data().dyn_into::<JsString>() {
                let s = txt.as_string().unwrap_or_default();
                log::info!("[SignalingTransport] on_message TEXT: {}", s);
                tx_clone.send(s).ok();
            } else if let Ok(array_buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let uint8 = js_sys::Uint8Array::new(&array_buffer);
                let bytes = uint8.to_vec();
                log::info!("[SignalingTransport] on_message BINARY: {} bytes", bytes.len());
                if let Ok(text) = String::from_utf8(bytes) {
                    tx_clone.send(text).ok();
                }
            } else {
                log::warn!("[SignalingTransport] on_message UNKNOWN type");
            }
        }) as Box<dyn FnMut(_)>);
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        // onopen
        let connected_clone = connected.clone();
        let on_open = Closure::wrap(Box::new(move |_: JsValue| {
            *connected_clone.borrow_mut() = true;
        }) as Box<dyn FnMut(_)>);
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        // Wait for connection
        let mut attempts = 0;
        while !*connected.borrow() && attempts < 500 {
            ego_platform::sleep(std::time::Duration::from_millis(10)).await;
            attempts += 1;
            if ws.ready_state() == web_sys::WebSocket::CLOSED {
                ws.set_onmessage(None);
                    ws.set_onopen(None);
                    ws.close().ok();
                return Err(TransportError::Protocol(
                    "Signaling WebSocket connection failed".to_string(),
                ));
            }
        }

        if !*connected.borrow() {
            ws.set_onmessage(None);
                ws.set_onopen(None);
                ws.close().ok();
            return Err(TransportError::Protocol(
                "Signaling WebSocket connection timeout".to_string(),
            ));
        }

        ego_platform::sleep(std::time::Duration::from_millis(10)).await;

        Ok(Self {
            ws,
            rx,
            _on_message: on_message,
            // _on_open: on_open,
        })
    }

    async fn send_msg(&self, msg: &SignalingMessage) -> Result<(), TransportError> {
        let text = msg.serialize();
        self.ws
            .send_with_str(&text)
            .map_err(|e| TransportError::Protocol(format!("Signaling send failed: {:?}", e)))?;
        Ok(())
    }

    async fn recv_msg(&mut self) -> Result<SignalingMessage, TransportError> {
        let mut attempts = 0;
        loop {
            match self.rx.try_recv() {
                Ok(text) => {
                    log::info!("[SignalingTransport] recv_msg got: {}", text);
                    if let Some(msg) = SignalingMessage::deserialize(&text) {
                        return Ok(msg);
                    }
                    log::warn!("[RTC Browser] Unparseable signaling message: {}", text);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(TransportError::Closed);
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    if self.ws.ready_state() == web_sys::WebSocket::CLOSED {
                        return Err(TransportError::Closed);
                    }
                    attempts += 1;
                    if attempts >= 3000 {
                        return Err(TransportError::Protocol(
                            "Signaling receive timeout".to_string(),
                        ));
                    }
                    ego_platform::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl Drop for SignalingTransport {
    fn drop(&mut self) {
        self.ws.set_onmessage(None);
        self.ws.set_onopen(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        self.ws.close().ok();
    }
}