//! Test actors for ego_transport integration tests.
//!
//! ## SignalingTestActor
//! Tick-based state machine that drives one side of a signaling handshake.
//! Two modes:
//! - **Direct** (via `new`): role is predetermined, used for dumb-relay tests.
//! - **HubMode** (via `new_hub_mode`): JOINs a room through a SignalingHub,
//!   gets assigned a role. Used for AutoDetectListener + SignalingHub tests.
//!
//! Both modes use `TransportSignalingChannel` from the library — newline-
//! delimited framing is now consistent across the entire signaling stack.

use ego_proc::ControlSignal;
use ego_proc::actor::ActorState;
use ego_transport::transport::rtc_signaling::*;
use ego_transport::transport::{Transport, TransportError};
use std::time::Duration;

// ─── SignalingTestActor ──────────────────────────────────────────────────────

/// Which side of the handshake this actor plays.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SignalingRole {
    Offerer,
    Answerer,
}

/// Events emitted upward through the orchestrator.
#[derive(Debug, Clone)]
pub enum SignalingTestEvent {
    Complete {
        role: SignalingRole,
        success: bool,
        detail: String,
    },
}

/// Internal state machine phases.
enum Phase {
    /// Hub mode: send JOIN, wait for READY to learn our role.
    JoiningRoom,
    /// Offerer: send offer + ICE + IceDone, then transition to Receiving.
    Sending,
    /// Wait for the peer's signals.
    Receiving { got_sdp: bool, got_ice_done: bool },
    /// Answerer only: send answer + ICE + IceDone after receiving offer.
    Responding,
    /// Terminal.
    Done { success: bool, detail: String },
}

pub struct SignalingTestActor {
    role: SignalingRole,
    room: String,
    channel: TransportSignalingChannel,
    /// Whether to JOIN a room first (hub mode) or start directly.
    hub_mode: bool,
    phase: Phase,
    output: Vec<SignalingTestEvent>,
}

impl SignalingTestActor {
    /// Create an actor in direct mode — role is known, no room join needed.
    /// Used with dumb relays where there's no SignalingHub.
    pub fn new(role: SignalingRole, room: &str, transport: Box<dyn Transport>) -> Self {
        Self {
            role,
            room: room.to_string(),
            channel: TransportSignalingChannel::new(transport),
            hub_mode: false,
            phase: match role {
                SignalingRole::Offerer => Phase::Sending,
                SignalingRole::Answerer => Phase::Receiving {
                    got_sdp: false,
                    got_ice_done: false,
                },
            },
            output: Vec::new(),
        }
    }

    /// Create an actor in hub mode — sends JOIN and waits for READY.
    /// The SignalingHub assigns the role.
    pub fn new_hub_mode(room: &str, transport: Box<dyn Transport>) -> Self {
        Self {
            role: SignalingRole::Offerer, // overwritten by READY
            room: room.to_string(),
            channel: TransportSignalingChannel::new(transport),
            hub_mode: true,
            phase: Phase::JoiningRoom,
            output: Vec::new(),
        }
    }

    async fn send_offer_bundle(&mut self) -> Result<(), TransportError> {
        let sdp = SdpBuilder::new().build_offer();
        self.channel
            .send_signal(&SignalingMessage::offer(&self.room, &sdp))
            .await?;
        let ice = IceCandidate::new(
            "candidate:1 1 udp 2130706431 10.0.0.1 5000 typ host",
            "0",
            0,
        );
        self.channel
            .send_signal(&SignalingMessage::ice(&self.room, &ice))
            .await?;
        self.channel
            .send_signal(&SignalingMessage::ice_done(&self.room))
            .await?;
        Ok(())
    }

    async fn send_answer_bundle(&mut self) -> Result<(), TransportError> {
        let sdp = SdpBuilder::new().build_answer();
        self.channel
            .send_signal(&SignalingMessage::answer(&self.room, &sdp))
            .await?;
        let ice = IceCandidate::new(
            "candidate:1 1 udp 2130706431 10.0.0.2 5001 typ host",
            "0",
            0,
        );
        self.channel
            .send_signal(&SignalingMessage::ice(&self.room, &ice))
            .await?;
        self.channel
            .send_signal(&SignalingMessage::ice_done(&self.room))
            .await?;
        Ok(())
    }

    async fn try_recv_one(&mut self) -> Result<Option<SignalingMessage>, TransportError> {
        match ego_platform::timeout(Duration::from_millis(30), self.channel.recv_signal()).await {
            Ok(Ok(msg)) => Ok(Some(msg)),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(None),
        }
    }

    /// Send JOIN and wait for READY from the SignalingHub.
    async fn join_room(&mut self) -> Result<SignalingRole, TransportError> {
        // JOIN uses the same newline-framed channel
        self.channel
            .send_signal(&SignalingMessage::join(&self.room))
            .await?;

        // Wait for READY
        match self.try_recv_one().await {
            Ok(Some(msg)) => match msg.kind {
                SignalingKind::Ready => {
                    let role = PeerRole::from_str(&msg.payload).ok_or_else(|| {
                        TransportError::Protocol(format!("Invalid role: {}", msg.payload))
                    })?;
                    Ok(match role {
                        PeerRole::Offerer => SignalingRole::Offerer,
                        PeerRole::Answerer => SignalingRole::Answerer,
                    })
                }
                SignalingKind::Error => Err(TransportError::Protocol(msg.payload)),
                other => Err(TransportError::Protocol(format!(
                    "Expected READY, got {:?}",
                    other
                ))),
            },
            Ok(None) => {
                // Timeout — retry next tick
                Err(TransportError::Protocol("join timeout".into()))
            }
            Err(e) => Err(e),
        }
    }

    fn finish(&mut self, success: bool, detail: &str) {
        self.phase = Phase::Done {
            success,
            detail: detail.to_string(),
        };
        self.output.push(SignalingTestEvent::Complete {
            role: self.role,
            success,
            detail: detail.to_string(),
        });
    }
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    async_trait::async_trait
)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
impl ActorState for SignalingTestActor {
    type D = ();
    type O = SignalingTestEvent;

    fn interval(&self) -> Duration {
        Duration::from_millis(50)
    }

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        match &self.phase {
            Phase::Done { .. } => return Ok(false),
            _ => {}
        }

        match std::mem::replace(
            &mut self.phase,
            Phase::Done {
                success: false,
                detail: "unexpected".into(),
            },
        ) {
            Phase::JoiningRoom => match self.join_room().await {
                Ok(role) => {
                    log::info!("[HubMode] Assigned role: {:?}", role);
                    self.role = role;
                    match role {
                        SignalingRole::Offerer => self.phase = Phase::Sending,
                        SignalingRole::Answerer => {
                            self.phase = Phase::Receiving {
                                got_sdp: false,
                                got_ice_done: false,
                            }
                        }
                    }
                }
                Err(TransportError::Protocol(ref s)) if s == "join timeout" => {
                    self.phase = Phase::JoiningRoom;
                }
                Err(e) => {
                    self.finish(false, &format!("join failed: {:?}", e));
                    return Ok(false);
                }
            },

            Phase::Sending => match self.send_offer_bundle().await {
                Ok(()) => {
                    log::info!("[{:?}] Sent offer bundle", self.role);
                    self.phase = Phase::Receiving {
                        got_sdp: false,
                        got_ice_done: false,
                    };
                }
                Err(e) => {
                    self.finish(false, &format!("send offer failed: {:?}", e));
                    return Ok(false);
                }
            },

            Phase::Receiving {
                mut got_sdp,
                mut got_ice_done,
            } => {
                for _ in 0..20 {
                    match self.try_recv_one().await {
                        Ok(Some(msg)) => {
                            match (&self.role, &msg.kind) {
                                (SignalingRole::Offerer, SignalingKind::Answer) => {
                                    log::info!("[Offerer] Got answer");
                                    got_sdp = true;
                                }
                                (SignalingRole::Answerer, SignalingKind::Offer) => {
                                    log::info!("[Answerer] Got offer");
                                    got_sdp = true;
                                }
                                (_, SignalingKind::Ice) => {
                                    log::info!("[{:?}] Got ICE candidate", self.role);
                                }
                                (_, SignalingKind::IceDone) => {
                                    log::info!("[{:?}] Got ICE done", self.role);
                                    got_ice_done = true;
                                }
                                _ => {}
                            }
                            if got_sdp && got_ice_done {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            self.finish(false, &format!("recv error: {:?}", e));
                            return Ok(false);
                        }
                    }
                }

                if got_sdp && got_ice_done {
                    match self.role {
                        SignalingRole::Offerer => {
                            self.finish(true, "signaling complete");
                            return Ok(false);
                        }
                        SignalingRole::Answerer => {
                            self.phase = Phase::Responding;
                        }
                    }
                } else {
                    self.phase = Phase::Receiving {
                        got_sdp,
                        got_ice_done,
                    };
                }
            }

            Phase::Responding => match self.send_answer_bundle().await {
                Ok(()) => {
                    log::info!("[Answerer] Sent answer bundle");
                    self.finish(true, "signaling complete");
                    return Ok(false);
                }
                Err(e) => {
                    self.finish(false, &format!("send answer failed: {:?}", e));
                    return Ok(false);
                }
            },

            Phase::Done { success, detail } => {
                self.phase = Phase::Done { success, detail };
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn on_signal(&mut self, _signal: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_data(&mut self, _data: ()) -> anyhow::Result<()> {
        Ok(())
    }

    fn take_output(&mut self) -> Vec<SignalingTestEvent> {
        std::mem::take(&mut self.output)
    }
}
