# ego-transport - Cross-Platform Networking Library

MIKE: This whole document is pretty messed up... have fun :D

A production-ready, cross-platform networking library for Rust that provides unified TCP and WebSocket support across Native, WASI Preview 2, and Browser environments.

## 🎯 Features

- **Unified Transport Abstraction** - Single `Transport` trait for all protocols and platforms
- **Cross-Platform Support**
  - ✅ Native (Linux, macOS, Windows)
  - ✅ WASI Preview 2 (wasmtime)
  - ✅ Browser (wasm32-unknown-unknown)
- **Multiple Protocols**
  - TCP (Native & WASI)
  - WebSocket (Native, WASI, & Browser)
- **Platform-Optimized**
  - Concurrent connections on Native
  - Sequential connections on WASI
  - Event-driven in Browser
- **Battle-Tested** - Uses `tungstenite` for WebSocket protocol compliance

## 📦 Installation

Add to your `Cargo.toml`:
```toml
[dependencies]
ego_transport = ...

# Platform-specific dependencies are handled automatically
```

## 🚀 Quick Start

### TCP Echo Server (Native)
```rust
use ego_transport::platform::tcp_native::TcpListenerNative;
use ego_transport::server::ServerBuilder;
use ego_transport::transport::Transport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListenerNative::bind("127.0.0.1:9999")?;
    
    ServerBuilder::new(listener)
        .concurrent()  // Use concurrent mode on native
        .run(|mut transport| async move {
            let mut buf = [0u8; 1024];
            while let Ok(n) = transport.recv(&mut buf).await {
                transport.send(&buf[..n]).await.ok();
            }
        })
        .await?;
    
    Ok(())
}
```

### WebSocket Client (All Platforms)
```rust
use ego_transport::transport::Transport;

// Native
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::ws_native::WebSocketNative as WebSocket;

// WASI
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use ego_transport::platform::ws_wasi::WebSocketWasi as WebSocket;

// Browser
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use ego_transport::platform::ws_browser::WebSocketBrowser as WebSocket;

async fn connect_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut ws = WebSocket::connect("ws://127.0.0.1:9999").await?;
    
    // Send message
    ws.send(b"Hello, Server!").await?;
    
    // Receive response
    let mut buf = [0u8; 1024];
    let n = ws.recv(&mut buf).await?;
    println!("Received: {}", String::from_utf8_lossy(&buf[..n]));
    
    Ok(())
}
```

## 📚 Core Concepts

### Transport Trait

All networking in `ego_transport` uses the `Transport` trait:
```rust
#[async_trait(?Send)]
pub trait Transport {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}
```

This provides a unified interface across all protocols and platforms.

### Platform-Specific Implementations

The library automatically uses the best implementation for each platform:

| Platform | TCP | WebSocket |
|----------|-----|-----------|
| Native   | `tokio::net::TcpStream` | `tokio-tungstenite` |
| WASI P2  | `wasip2` sockets | `tungstenite` (with sync adapter) |
| Browser  | N/A | `web_sys::WebSocket` |

## 🔧 API Reference

### TCP (Native & WASI)

#### Native TCP Client
```rust
use ego_transport::platform::tcp_native::TcpStreamNative;

let mut stream = TcpStreamNative::connect("127.0.0.1:9999").await?;
stream.send(b"Hello").await?;
```

#### Native TCP Server
```rust
use ego_transport::platform::tcp_native::TcpListenerNative;

let listener = TcpListenerNative::bind("127.0.0.1:9999")?;
let transport = listener.accept().await?;
```

#### WASI TCP Client
```rust
use ego_transport::platform::tcp_wasi::TcpStreamWasi;

let mut stream = TcpStreamWasi::connect("127.0.0.1:9999").await?;
stream.send(b"Hello").await?;
```

#### WASI TCP Server
```rust
use ego_transport::platform::tcp_wasi::TcpStreamWasi;

let listener = TcpStreamWasi::bind("127.0.0.1:9999").await?;
let transport = listener.accept().await?;
```

### WebSocket (All Platforms)

#### Native WebSocket Client
```rust
use ego_transport::platform::ws_native::WebSocketNative;

let mut ws = WebSocketNative::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

#### Native WebSocket Server
```rust
use ego_transport::platform::tcp_native::TcpListenerNative;

let listener = TcpListenerNative::bind("127.0.0.1:9999")?;

loop {
    let ws = listener.accept_websocket().await?;
    // Handle WebSocket connection
}
```

#### WASI WebSocket Client
```rust
use ego_transport::platform::ws_wasi::WebSocketWasi;

let mut ws = WebSocketWasi::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

#### WASI WebSocket Server
```rust
use ego_transport::platform::tcp_wasi::TcpStreamWasi;
use ego_transport::platform::ws_wasi::WebSocketWasi;

let listener = TcpStreamWasi::bind("127.0.0.1:9999").await?;

loop {
    let tcp_stream = listener.accept().await?;
    let ws = WebSocketWasi::accept(tcp_stream).await?;
    // Handle WebSocket connection
}
```

#### Browser WebSocket Client
```rust
use ego_transport::platform::ws_browser::WebSocketBrowser;

let mut ws = WebSocketBrowser::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

### Server Abstraction

The `ServerBuilder` provides platform-appropriate concurrency:
```rust
use ego_transport::server::ServerBuilder;

// Native - Concurrent by default
ServerBuilder::new(listener)
    .concurrent()  // Spawns handler per connection
    .run(handler)
    .await?;

// WASI - Sequential only
ServerBuilder::new(listener)
    .sequential()  // Handles one connection at a time
    .run(handler)
    .await?;

// You can also use sequential on native for testing
ServerBuilder::new(listener)
    .sequential()
    .run(handler)
    .await?;
```

## 🎮 Platform-Specific Notes

### Native

- **Concurrency**: Full multi-threaded support via `tokio::spawn`
- **Performance**: Optimized for high-throughput scenarios
- **Requirements**: `tokio` runtime

### WASI Preview 2

- **Concurrency**: Sequential connection handling (one at a time)
- **Runtime**: Single-threaded tokio runtime
- **Socket API**: Uses `wasip2` crate for networking
- **Limitations**: No `tokio::spawn` support, but can handle multiple connections sequentially

**Example WASI Setup:**
```rust
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async_main());
}
```

**Running WASI:**
```bash
wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/my_app.wasm
```

### Browser# AloeCraft Client - Cross-Platform Networking Library

A production-ready, cross-platform networking library for Rust that provides unified TCP and WebSocket support across Native, WASI Preview 2, and Browser environments.

## 🎯 Features

- **Unified Transport Abstraction** - Single `Transport` trait for all protocols and platforms
- **Cross-Platform Support**
  - ✅ Native (Linux, macOS, Windows)
  - ✅ WASI Preview 2 (wasmtime)
  - ✅ Browser (wasm32-unknown-unknown)
- **Multiple Protocols**
  - TCP (Native & WASI)
  - WebSocket (Native, WASI, & Browser)
- **Mixed-Protocol Servers** - `AutoDetectListener` serves TCP and WebSocket on a single port with transparent protocol detection
- **Platform-Optimized**
  - Concurrent connections on Native
  - Sequential connections on WASI
  - Event-driven in Browser
- **Battle-Tested** - Uses `tungstenite` for WebSocket protocol compliance

## 📦 Installation

Add to your `Cargo.toml`:
```toml
[dependencies]
ego_transport = ...

# Platform-specific dependencies are handled automatically
```

## 🚀 Quick Start

### TCP Echo Server (Native)
```rust
use ego_transport::platform::tcp_native::TcpListenerNative;
use ego_transport::platform::server::ServerBuilder;
use ego_transport::transport::Transport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListenerNative::bind("127.0.0.1:9999")?;

    ServerBuilder::new(listener)
        .concurrent()  // Use concurrent mode on native
        .run(|mut transport| async move {
            let mut buf = [0u8; 1024];
            while let Ok(n) = transport.recv(&mut buf).await {
                transport.send(&buf[..n]).await.ok();
            }
        })
        .await?;

    Ok(())
}
```

### Mixed-Protocol Server (AutoDetect)

Serve TCP and WebSocket clients on the same port. The listener inspects the first bytes of each connection and routes accordingly — the handler receives a unified `Transport` and doesn't need to know which protocol the client used.

```rust
use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
use ego_transport::transport::Transport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = AutoDetectListener::bind("127.0.0.1:9999").await?;

    ServerBuilder::new(listener)
        .concurrent()  // Native: concurrent | WASI: sequential (automatic)
        .run(|mut transport| async move {
            // Handler is identical regardless of whether the client
            // connected via TCP or WebSocket.
            let mut buf = [0u8; 1024];
            while let Ok(n) = transport.recv(&mut buf).await {
                transport.send(&buf[..n]).await.ok();
            }
        })
        .await?;

    Ok(())
}
```

Both of the following clients will connect and echo successfully against this server with no server-side changes:
```rust
// TCP client
let mut stream = TcpStreamNative::connect("127.0.0.1:9999").await?;
stream.send(b"hello via TCP").await?;

// WebSocket client
let mut ws = WebSocketNative::connect("ws://127.0.0.1:9999").await?;
ws.send(b"hello via WebSocket").await?;
```

### WebSocket Client (All Platforms)
```rust
use ego_transport::transport::Transport;

// Native
#[cfg(not(target_arch = "wasm32"))]
use ego_transport::platform::ws_native::WebSocketNative as WebSocket;

// WASI
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use ego_transport::platform::ws_wasi::WebSocketWasi as WebSocket;

// Browser
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use ego_transport::platform::ws_browser::WebSocketBrowser as WebSocket;

async fn connect_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut ws = WebSocket::connect("ws://127.0.0.1:9999").await?;

    // Send message
    ws.send(b"Hello, Server!").await?;

    // Receive response
    let mut buf = [0u8; 1024];
    let n = ws.recv(&mut buf).await?;
    println!("Received: {}", String::from_utf8_lossy(&buf[..n]));

    Ok(())
}
```

## 📚 Core Concepts

### Transport Trait

All networking in `ego_transport` uses the `Transport` trait:
```rust
#[async_trait(?Send)]
pub trait Transport {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}
```

This provides a unified interface across all protocols and platforms.

### Platform-Specific Implementations

The library automatically uses the best implementation for each platform:

| Platform | TCP | WebSocket |
|----------|-----|-----------|
| Native   | `tokio::net::TcpStream` | `tokio-tungstenite` |
| WASI P2  | `wasip2` sockets | `tungstenite` (with sync adapter) |
| Browser  | N/A | `web_sys::WebSocket` |

## 🔧 API Reference

### TCP (Native & WASI)

#### Native TCP Client
```rust
use ego_transport::platform::tcp_native::TcpStreamNative;

let mut stream = TcpStreamNative::connect("127.0.0.1:9999").await?;
stream.send(b"Hello").await?;
```

#### Native TCP Server
```rust
use ego_transport::platform::tcp_native::TcpListenerNative;

let listener = TcpListenerNative::bind("127.0.0.1:9999")?;
let transport = listener.accept().await?;
```

#### WASI TCP Client
```rust
use ego_transport::platform::tcp_wasi::TcpStreamWasi;

let mut stream = TcpStreamWasi::connect("127.0.0.1:9999").await?;
stream.send(b"Hello").await?;
```

#### WASI TCP Server
```rust
use ego_transport::platform::tcp_wasi::TcpListenerWasi;

let listener = TcpListenerWasi::bind("127.0.0.1:9999").await?;
let transport = listener.accept().await?;
```

### WebSocket (All Platforms)

#### Native WebSocket Client
```rust
use ego_transport::platform::ws_native::WebSocketNative;

let mut ws = WebSocketNative::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

#### Native WebSocket Server
```rust
use ego_transport::platform::tcp_native::TcpListenerNative;

let listener = TcpListenerNative::bind("127.0.0.1:9999")?;

loop {
    let ws = listener.accept_websocket().await?;
    // Handle WebSocket connection
}
```

#### WASI WebSocket Client
```rust
use ego_transport::platform::ws_wasi::WebSocketWasi;

let mut ws = WebSocketWasi::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

#### WASI WebSocket Server
```rust
use ego_transport::platform::tcp_wasi::TcpListenerWasi;
use ego_transport::platform::ws_wasi::WebSocketWasi;

let listener = TcpListenerWasi::bind("127.0.0.1:9999").await?;

loop {
    let tcp_stream = listener.accept().await?;
    let ws = WebSocketWasi::accept(tcp_stream).await?;
    // Handle WebSocket connection
}
```

#### Browser WebSocket Client
```rust
use ego_transport::platform::ws_browser::WebSocketBrowser;

let mut ws = WebSocketBrowser::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

### AutoDetectListener (Native & WASI)

`AutoDetectListener` is the recommended listener for any server that needs to accept more than one protocol. It binds a single TCP port and transparently routes each incoming connection to TCP or WebSocket based on its opening bytes. The handler always receives a `Box<dyn Transport>` — detection is invisible to application code.

#### How Detection Works

Every incoming connection's first 4 bytes are examined before the handler runs. If they match `GET ` (the opening of an HTTP/1.1 upgrade request), the connection is completed as a WebSocket handshake and delivered as a WebSocket transport. Any other prefix means plain TCP. The detection cost is paid once per connection, during `accept()` — it does not add latency to the handler or to subsequent recv/send calls.

#### Basic Usage
```rust
use ego_transport::platform::server::{AutoDetectListener, ServerBuilder};
use ego_transport::transport::Transport;

let listener = AutoDetectListener::bind("127.0.0.1:9999").await?;

ServerBuilder::new(listener)
    .concurrent()
    .run(|mut transport| async move {
        let mut buf = [0u8; 1024];
        while let Ok(n) = transport.recv(&mut buf).await {
            transport.send(&buf[..n]).await.ok();
        }
    })
    .await?;
```

#### Protocol Filtering

By default both TCP and WebSocket are accepted. Use `.tcp_only()` or `.ws_only()` to restrict to a single protocol:

```rust
// WebSocket only — TCP connections are logged and dropped
let listener = AutoDetectListener::bind("127.0.0.1:9999").await?;
let listener = listener.ws_only();

// TCP only — WebSocket upgrade requests are logged and dropped
let listener = AutoDetectListener::bind("127.0.0.1:9999").await?;
let listener = listener.tcp_only();
```

Rejected connections are logged at `WARN` level and silently closed. The server loop continues to the next connection — rejections never surface as errors to the handler or to `ServerBuilder`.

#### Platform Behavior

| Aspect | Native | WASI |
|--------|--------|------|
| Detection method | `peek()` — non-consuming | 4-byte read + prefix replay |
| TCP overhead | Zero — stream passes through untouched | Minimal — `BufferedTransport` coalesces prefix with first handler read |
| WebSocket overhead | None | Prefix replayed through sync adapter for handshake |
| Concurrency | Concurrent (spawns per connection) | Sequential (one connection at a time) |

On native, `peek()` reads without consuming, so TCP connections arrive at the handler with the full stream intact — zero copies, zero buffering, identical to `TcpListenerNative` directly. On WASI, where streams have no peek, the 4 detection bytes are read and replayed transparently: `BufferedTransport` coalesces them with the first handler `recv()` so the handler sees one contiguous chunk (matching native behavior), and the WebSocket path replays them through the sync adapter so `tungstenite` sees the complete HTTP upgrade request.

#### When to Use AutoDetectListener vs. Single-Protocol Listeners

Use `AutoDetectListener` when your server needs to accept both TCP and WebSocket clients, or when you want a single port that handles either. If you are certain your server will only ever speak one protocol, the dedicated listeners (`TcpListenerNative`, `TcpListenerWasi`, or `accept_websocket()`) have marginally simpler call sites and skip the detection step entirely. The performance difference is negligible in practice — detection is 4 bytes peeked once per connection.

### Server Abstraction

The `ServerBuilder` provides platform-appropriate concurrency and works with any listener type — `TcpListenerNative`, `TcpListenerWasi`, or `AutoDetectListener`:

```rust
use ego_transport::platform::server::ServerBuilder;

// Native — concurrent by default, spawns a handler task per connection
ServerBuilder::new(listener)
    .concurrent()
    .run(handler)
    .await?;

// WASI — sequential only, handles one connection at a time
ServerBuilder::new(listener)
    .sequential()
    .run(handler)
    .await?;

// Sequential is also available on native (useful for testing)
ServerBuilder::new(listener)
    .sequential()
    .run(handler)
    .await?;
```

Note: `.concurrent()` is only available on native. On WASI it is not compiled in, so attempting to call it is a compile-time error rather than a runtime one.

## 🎮 Platform-Specific Notes

### Native

- **Concurrency**: Full multi-threaded support via `tokio::spawn`
- **Performance**: Optimized for high-throughput scenarios
- **Requirements**: `tokio` runtime

### WASI Preview 2

- **Concurrency**: Sequential connection handling (one at a time)
- **Runtime**: Single-threaded tokio runtime
- **Socket API**: Uses `wasip2` crate for networking
- **Limitations**: No `tokio::spawn` support, but can handle multiple connections sequentially

**Example WASI Setup:**
```rust
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn main() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async_main());
}
```

**Running WASI:**
```bash
wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/my_app.wasm
```

### Browser

- **WebSocket Only**: TCP not available in browsers
- **Event-Driven**: Uses browser's native WebSocket API
- **Security**: Subject to browser CORS and mixed content policies
- **No Servers**: Browsers can only be clients

**Browser Setup with Trunk:**
```html
<!DOCTYPE html>
<html>
  <head>
    <link data-trunk rel="rust" data-bin="my_app" />
  </head>
  <body>
    <script type="module">
      import init, { start_app } from './my_app.js';
      async function run() {
        await init();
        start_app();
      }
      run();
    </script>
  </body>
</html>
```

## 🧪 Testing

The library includes comprehensive tests for all platforms:
```bash
# Run all native tests
make quick_test

# Single-protocol tests
make test_tcp_native            # Native TCP client + server
make test_ws_native             # Native WebSocket client + server
make test_ws_wasi_client        # WASI WebSocket client
make test_ws_browser_client     # Browser WebSocket client

# AutoDetect tests
make test_auto_detect_native    # Native: concurrent TCP + WS on one port, plus ws_only rejection
make test_auto_detect_wasi_server  # WASI server + native clients: TCP and WS detection across platforms

# See all test options
make help
```

### AutoDetect Test Details

**`test_auto_detect_native`** runs entirely on native and covers three scenarios:

1. A concurrent `AutoDetectListener` echo server on one port. A TCP client and a WebSocket client connect simultaneously, each send two messages, and verify their echoes arrive correctly. This confirms detection doesn't interfere with concurrent accept/spawn.
2. A `ws_only` server on a second port. A TCP client connects and verifies it is rejected (connection reset after the server logs a warning). This confirms protocol filtering works end-to-end.

**`test_auto_detect_wasi_server`** is a split-platform test: the server binary runs under `wasmtime`, the clients run natively. This is necessary because WASI and native share the same source file but compile to different targets via `cfg`. The test sequence is:

1. Start the WASI server in the background: `wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/test_auto_detect_wasi_server.wasm &`
2. Run the native clients: `./target/debug/test_auto_detect_wasi_server`
3. The native binary waits 1 second for the WASI server to bind, then runs a TCP client followed by a WebSocket client, verifying echo on both.

This exercises the WASI-specific code paths: the consuming 4-byte read during detection, `BufferedTransport`'s prefix coalescing for TCP, and `accept_with_prefix` + `WasiSyncStream` prefix replay for WebSocket.

### Testing Your Application

#### Native Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection() {
        let mut ws = WebSocketNative::connect("ws://localhost:9999").await.unwrap();
        ws.send(b"test").await.unwrap();

        let mut buf = [0u8; 1024];
        let n = ws.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"test");
    }
}
```

#### WASI Tests
```bash
# Build for WASI
cargo build --target wasm32-wasip2 --bin my_test

# Run with wasmtime
wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/my_test.wasm
```

#### Browser Tests
```bash
# Serve with trunk
trunk serve --port 9001

# Open http://localhost:9001 and check browser console
```

## 🔍 Error Handling

The library uses the `TransportError` enum for all errors:
```rust
pub enum TransportError {
    /// Operation is not supported on this platform or transport type.
    Unsupported(String),
    /// Underlying I/O error.
    Io(std::io::Error),
    /// WASI-specific socket error (cfg-gated, WASI only).
    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    Wasi(String),
    /// WebSocket protocol error (handshake failure, framing error, etc.).
    WebSocket(String),
    /// Connection was closed by the remote end.
    Closed,
    /// Application-level protocol error.
    Protocol(String),
}

// Usage
match transport.recv(&mut buf).await {
    Ok(n)                          => { /* process n bytes */ },
    Err(TransportError::Closed)    => { /* peer closed cleanly */ },
    Err(TransportError::Io(e))     => { /* OS-level I/O error */ },
    Err(TransportError::Protocol(msg)) => { /* application protocol violation */ },
    Err(e)                         => { /* handle remaining variants */ },
}
```

## 📊 Performance Considerations

### Native

- **Throughput**: Optimized for high-bandwidth scenarios
- **Latency**: Low overhead with direct tokio integration
- **Concurrency**: Handles thousands of concurrent connections

### WASI

- **Sequential Processing**: Best for moderate connection counts
- **Memory Efficient**: Single-threaded design uses less memory
- **Suitable For**: IoT, edge computing, sandboxed environments

### Browser

- **Network Bound**: Performance limited by browser's WebSocket implementation
- **Optimized**: Uses native browser APIs for best performance
- **Typical Use**: Client-side game clients, web apps

## 🛠️ Advanced Usage

### Custom Connection Handlers
```rust
async fn handle_client(mut transport: Box<dyn Transport>) {
    let mut buf = [0u8; 4096];

    loop {
        match transport.recv(&mut buf).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                // Process message
                let response = process_message(&buf[..n]);
                transport.send(&response).await.ok();
            }
            Err(TransportError::Closed) => break,
            Err(e) => {
                log::error!("Error: {:?}", e);
                break;
            }
        }
    }
}
```

### Protocol Versioning
```rust
async fn handshake(transport: &mut impl Transport) -> Result<u32, TransportError> {
    // Send version
    transport.send(&[1, 0, 0, 0]).await?;

    // Receive version
    let mut buf = [0u8; 4];
    transport.recv(&mut buf).await?;

    let version = u32::from_le_bytes(buf);
    Ok(version)
}
```

### Graceful Shutdown
```rust
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListenerNative::bind("127.0.0.1:9999")?;

    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Shutting down gracefully...");
        }
        _ = run_server(listener) => {}
    }

    Ok(())
}
```

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [tokio](https://tokio.rs/) for async runtime
- Uses [tungstenite](https://github.com/snapview/tungstenite-rs) for WebSocket protocol
- WASI support via [wasip2](https://crates.io/crates/wasip2)
- Browser support via [wasm-bindgen](https://rustwasm.github.io/wasm-bindgen/)

## 🗺️ Roadmap

- [ ] TLS/SSL support
- [ ] UDP transport
- [ ] QUIC protocol support
- [ ] Connection pooling
- [ ] Automatic reconnection
- [ ] Rate limiting
- [ ] Metrics and observability

---

**Made with ❤️ for cross-platform Rust networking**

- **WebSocket Only**: TCP not available in browsers
- **Event-Driven**: Uses browser's native WebSocket API
- **Security**: Subject to browser CORS and mixed content policies
- **No Servers**: Browsers can only be clients

**Browser Setup with Trunk:**
```html
<!DOCTYPE html>
<html>
  <head>
    <link data-trunk rel="rust" data-bin="my_app" />
  </head>
  <body>
    <script type="module">
      import init, { start_app } from './my_app.js';
      async function run() {
        await init();
        start_app();
      }
      run();
    </script>
  </body>
</html>
```

## 🧪 Testing

The library includes comprehensive tests for all platforms:
```bash
# Run all native tests
make quick_test

# Test specific components
make test_tcp_native      # Native TCP
make test_ws_native       # Native WebSocket
make test_ws_wasi_client  # WASI WebSocket client
make test_ws_browser_client  # Browser WebSocket client

# See all test options
make help
```

### Testing Your Application

#### Native Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_connection() {
        let mut ws = WebSocketNative::connect("ws://localhost:9999").await.unwrap();
        ws.send(b"test").await.unwrap();
        
        let mut buf = [0u8; 1024];
        let n = ws.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"test");
    }
}
```

#### WASI Tests
```bash
# Build for WASI
cargo build --target wasm32-wasip2 --bin my_test

# Run with wasmtime
wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/my_test.wasm
```

#### Browser Tests
```bash
# Serve with trunk
trunk serve --port 9001

# Open http://localhost:9001 and check browser console
```

## 🔍 Error Handling

The library uses the `TransportError` enum for all errors:
```rust
pub enum TransportError {
    Io(std::io::Error),
    Protocol(String),
    Closed,
    WouldBlock,
}

// Usage
match transport.recv(&mut buf).await {
    Ok(n) => { /* Process n bytes */ },
    Err(TransportError::Closed) => { /* Connection closed */ },
    Err(TransportError::WouldBlock) => { /* Retry */ },
    Err(e) => { /* Handle other errors */ },
}
```

## 📊 Performance Considerations

### Native

- **Throughput**: Optimized for high-bandwidth scenarios
- **Latency**: Low overhead with direct tokio integration
- **Concurrency**: Handles thousands of concurrent connections

### WASI

- **Sequential Processing**: Best for moderate connection counts
- **Memory Efficient**: Single-threaded design uses less memory
- **Suitable For**: IoT, edge computing, sandboxed environments

### Browser

- **Network Bound**: Performance limited by browser's WebSocket implementation
- **Optimized**: Uses native browser APIs for best performance
- **Typical Use**: Client-side game clients, web apps

## 🛠️ Advanced Usage

### Custom Connection Handlers
```rust
async fn handle_client(mut transport: Box<dyn Transport>) {
    let mut buf = [0u8; 4096];
    
    loop {
        match transport.recv(&mut buf).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                // Process message
                let response = process_message(&buf[..n]);
                transport.send(&response).await.ok();
            }
            Err(TransportError::Closed) => break,
            Err(e) => {
                log::error!("Error: {:?}", e);
                break;
            }
        }
    }
}
```

### Protocol Versioning
```rust
async fn handshake(transport: &mut impl Transport) -> Result<u32, TransportError> {
    // Send version
    transport.send(&[1, 0, 0, 0]).await?;
    
    // Receive version
    let mut buf = [0u8; 4];
    transport.recv(&mut buf).await?;
    
    let version = u32::from_le_bytes(buf);
    Ok(version)
}
```

### Graceful Shutdown
```rust
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListenerNative::bind("127.0.0.1:9999")?;
    
    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Shutting down gracefully...");
        }
        _ = run_server(listener) => {}
    }
    
    Ok(())
}
```

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [tokio](https://tokio.rs/) for async runtime
- Uses [tungstenite](https://github.com/snapview/tungstenite-rs) for WebSocket protocol
- WASI support via [wasip2](https://crates.io/crates/wasip2)
- Browser support via [wasm-bindgen](https://rustwasm.github.io/wasm-bindgen/)

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/aloeclient/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/aloeclient/discussions)
- **Documentation**: [docs.rs/aloeclient](https://docs.rs/aloeclient)

## 🗺️ Roadmap

- [ ] TLS/SSL support
- [ ] UDP transport
- [ ] QUIC protocol support
- [ ] Connection pooling
- [ ] Automatic reconnection
- [ ] Rate limiting
- [ ] Metrics and observability

---

**Made with ❤️ for cross-platform Rust networking**