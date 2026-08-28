pub mod endpoint;
pub mod flow;
pub mod framing;
pub mod identity;
pub mod path;
pub mod platform;
pub mod stun;
pub mod transport;

pub use endpoint::{Availability, Endpoint, Scheme, SchemeSupport};
pub use flow::{ConnectionMetrics, InboundBuffer, MetricsSnapshot, PushOutcome};
pub use framing::FramedTransport;
pub use identity::{KeyIdentity, PeerIdentity};
pub use path::{CandidateKind, PathInfo, PathKind};
pub use stun::{MappingReport, NatMapping, ProbeConfig, StunError, StunProbe};

#[cfg(not(target_arch = "wasm32"))]
pub use platform::ssh_native as ssh;

/// The TURN relay. Native only: a browser cannot run a relay server, and its
/// ICE stack consumes one through `IceServerConfig` instead.
#[cfg(not(target_arch = "wasm32"))]
pub use platform::turn_native as turn;

#[cfg(not(target_arch = "wasm32"))]
pub use platform::ws_native::WebSocketNative;

#[cfg(not(target_arch = "wasm32"))]
pub use platform::tcp_native::TcpStreamNative;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub use platform::tcp_wasi::TcpStreamWasi;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub use platform::ws_wasi::WebSocketWasi;
