# ego-transport

[![CI](https://github.com/Aloecraft-org/ego-transport/actions/workflows/ci.yml/badge.svg)](https://github.com/Aloecraft-org/ego-transport/actions/workflows/ci.yml)

Cross-platform transport layer for the ego ecosystem: one `Transport` trait
over TCP, WebSocket, WebRTC data channels, and SSH, targeting **native**,
**WASI Preview 2**, and the **browser**. Built on
[ego-platform](https://github.com/Aloecraft-org/ego-platform).

Schemes are named (`tcp`, `wssc`, `wssd`, `webrtc`, `ssh`) and gated per
platform in one table (`endpoint::Scheme`); a scheme that a platform can't
provide is a *typed refusal* at parse/dial/bind time
(`TransportError::SchemeUnavailable`), never a stub that half-works.

## The `Transport` trait

```rust
// Native/WASI (browser variant drops the Send + Sync bounds)
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}
```

`transport::connect(addr)` picks an implementation from the address scheme
(`ws://`/`wss://` → WebSocket, otherwise TCP) and the compile target.

## What's implemented where

| | Native | WASI P2 | Browser |
|---|---|---|---|
| TCP client | `tokio::net` | `wasip2` sockets | — |
| TCP server | `tokio::net` | `wasip2` sockets (sequential) | — |
| WebSocket client | `tokio-tungstenite` | `tungstenite` over a sync adapter | `web_sys::WebSocket` |
| WebSocket server | `tokio-tungstenite` | `tungstenite` over a sync adapter | — |
| WebRTC data channel | `webrtc` crate | relay fallback over the signaling channel | `RtcPeerConnection` |
| SSH client | `russh` | — (typed refusal) | — (typed refusal) |
| SSH server | `russh` | — (typed refusal) | — (typed refusal) |

### The `ssh` scheme

`platform::ssh_native` (re-exported as `ego_transport::ssh`) provides both
directions on [russh](https://github.com/Eugeny/russh), with deliberate
constraints: **public-key auth only** and a **modern suite only** (ed25519
keys, curve25519 KEX, chacha20-poly1305) — no legacy-algorithm table to
maintain. The server surfaces two channel kinds per connection — interactive
**PTY channels** (window-size changes included) and named **subsystem
channels** for framed programmatic traffic — and reports the authenticated
client key verbatim as the connection's principal; mapping principals to
permissions is the consumer's job. The host-key fingerprint is exposed on
both ends as the node identity primitive. Clients verify host keys against
caller-supplied keys or fingerprints; trust-on-first-use is an explicit
opt-in, never the default. Wrong-key and wrong-host-key connections fail
with typed errors (`SshError::AuthRejected`, `SshError::HostKeyMismatch`).

Supporting pieces:

- **`platform::server::ServerBuilder`** — accept-loop helper with
  `.concurrent()` (native) and `.sequential()` (WASI) modes
- **`platform::server::AutoDetectListener`** — sniffs each incoming
  connection (raw TCP vs. WebSocket upgrade vs. `JOIN:` signaling) and routes
  it to the right handler; on WASI the consumed detection bytes are replayed
  via `BufferedTransport`
- **`transport::signaling_hub::SignalingHub`** — embeddable room-based
  signaling (offer/answer/ICE relay) that can share a port with an
  application server
- **`transport::rtc_signaling`** — signaling message types, SDP builder, and
  a framed `TransportSignalingChannel` so signaling can run over any
  `Transport`
- **`transport::TransportBridge`** — pumps bytes between a `Transport` and a
  `tokio::io` stream
- **`endpoint::Endpoint`** — location-independent `scheme://authority[/path]`
  references resolved at dial/bind time, plus the per-platform scheme
  support table
- **`framing::FramedTransport`** — one length-prefixed frame convention
  (4-byte big-endian header, opaque payload) over any `Transport`;
  oversized frames are typed refusals, never unbounded allocations
- **`flow::InboundBuffer` / `flow::ConnectionMetrics`** — a bounded inbound
  queue whose "full" is an observable O(1) outcome (backpressure you can
  see), with pollable per-connection counters (queue depth, bytes,
  last activity)
- **`identity::PeerIdentity`** — handshake-proven peer identity (algorithm,
  SHA-256 fingerprint, wire-encoded key) reported verbatim, with
  `platform::server::IdentifiedListener` surfacing
  (transport, identity, remote address) per accept

## Building

### Prerequisites

- Rust 1.88+ (2024 edition)
- Targets: `rustup target add wasm32-wasip2 wasm32-unknown-unknown`
- WASI test runtime: [wasmtime](https://wasmtime.dev/) (configured as the
  cargo runner in `.cargo/config.toml`)
- Browser tests: `wasm-bindgen-cli` **matching the `wasm-bindgen` version in
  `Cargo.lock`** (currently 0.2.114) plus a browser + webdriver
- Browser demos: [trunk](https://trunkrs.dev/) to serve
  `test_websocket_browser.html` / `test_rtc_browser.html`

The devcontainer in `.devcontainer/` has all of this preinstalled.

### Quick Build

```bash
make check   # cargo check on all three targets
make test    # cargo test on all three targets
make build   # build all three targets
make ci      # fmt_check + clippy (all targets) + check + test
```

## Testing

Three layers, from cheap to involved:

1. **`make test`** — `cargo test` on all three targets: unit/integration
   tests in `tests/` (signaling handshakes through the hub and over relays,
   channel framing, bridge behavior). WASI runs under wasmtime; browser
   test binaries run under `wasm-bindgen-test-runner`.
2. **`make test_standalone`** — self-contained binaries that start a real
   server and client in one process: TCP echo, `ServerBuilder`, WebSocket
   echo. Deterministic and CI-safe.
3. **Manual / known-issue tests** — `make test_SA_signaling`,
   `test_SA_p2p`, `test_SA_embedded_signaling`, `test_SA_routed_signaling`,
   `test_SA_multihop_signaling` can hang (documented in the Makefile), and
   the `fixture_*` / `client_*` targets drive real cross-platform pairs
   (native server ↔ WASI/browser client) across separate terminals.
   See `make help`.

## Continuous Integration

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs
on every push and pull request:

- `make fmt_check` and `make clippy` (all three targets, warnings denied)
- Native build + `cargo test` + `make test_standalone` on Linux
  (plus build/test on macOS and Windows)
- WASI build + tests under wasmtime
- Browser build + tests under `wasm-bindgen-test-runner` with a headless
  browser

## Module Layout

```
src/
├── lib.rs
├── endpoint.rs          # scheme table, Endpoint parsing, typed refusals
├── framing.rs           # length-prefixed frames over any Transport
├── flow.rs              # bounded inbound buffer, connection metrics
├── identity.rs          # PeerIdentity / KeyIdentity
├── platform/            # per-platform implementations
│   ├── tcp_native.rs / tcp_wasi.rs
│   ├── ws_native.rs / ws_wasi.rs / ws_browser.rs
│   ├── rtc_native.rs / rtc_wasi.rs / rtc_browser.rs
│   ├── ssh_native.rs    # ssh scheme: russh client + server (native)
│   ├── server.rs        # ServerBuilder, Listener, AutoDetectListener
│   └── wasi_sync_adapter.rs
├── transport/           # platform-independent layer
│   ├── mod.rs           # Transport trait, TransportError, connect()
│   ├── tcp.rs / websocket.rs / p2p.rs
│   ├── rtc_signaling.rs # signaling messages, SDP builder, framing
│   ├── signaling_hub.rs # embeddable signaling server
│   ├── buffered.rs      # detection-byte replay
│   └── bridge.rs        # Transport <-> tokio::io bridge
└── bin/                 # standalone test/demo binaries (see Makefile)
```

## Contributing

1. All tests pass: `make test` (and `make test_standalone` for the
   integration binaries)
2. Code is formatted: `make fmt`
3. No clippy warnings on any target: `make clippy`

`make ci` runs the same sequence as GitHub Actions.
