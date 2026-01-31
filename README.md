# AloeCraft Client - Cross-Platform Networking Library

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
aloeclient = { path = "../aloeclient" }  # Or from crates.io when published

# Platform-specific dependencies are handled automatically
```

## 🚀 Quick Start

### TCP Echo Server (Native)
```rust
use aloeclient::platform::tcp_native::TcpListenerNative;
use aloeclient::server::ServerBuilder;
use aloeclient::transport::Transport;

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
use aloeclient::transport::Transport;

// Native
#[cfg(not(target_arch = "wasm32"))]
use aloeclient::platform::ws_native::WebSocketNative as WebSocket;

// WASI
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use aloeclient::platform::ws_wasi::WebSocketWasi as WebSocket;

// Browser
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use aloeclient::platform::ws_browser::WebSocketBrowser as WebSocket;

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

All networking in `aloeclient` uses the `Transport` trait:
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
use aloeclient::platform::tcp_native::TcpStreamNative;

let mut stream = TcpStreamNative::connect("127.0.0.1:9999").await?;
stream.send(b"Hello").await?;
```

#### Native TCP Server
```rust
use aloeclient::platform::tcp_native::TcpListenerNative;

let listener = TcpListenerNative::bind("127.0.0.1:9999")?;
let transport = listener.accept().await?;
```

#### WASI TCP Client
```rust
use aloeclient::platform::tcp_wasi::TcpStreamWasi;

let mut stream = TcpStreamWasi::connect("127.0.0.1:9999").await?;
stream.send(b"Hello").await?;
```

#### WASI TCP Server
```rust
use aloeclient::platform::tcp_wasi::TcpStreamWasi;

let listener = TcpStreamWasi::bind("127.0.0.1:9999").await?;
let transport = listener.accept().await?;
```

### WebSocket (All Platforms)

#### Native WebSocket Client
```rust
use aloeclient::platform::ws_native::WebSocketNative;

let mut ws = WebSocketNative::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

#### Native WebSocket Server
```rust
use aloeclient::platform::tcp_native::TcpListenerNative;

let listener = TcpListenerNative::bind("127.0.0.1:9999")?;

loop {
    let ws = listener.accept_websocket().await?;
    // Handle WebSocket connection
}
```

#### WASI WebSocket Client
```rust
use aloeclient::platform::ws_wasi::WebSocketWasi;

let mut ws = WebSocketWasi::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

#### WASI WebSocket Server
```rust
use aloeclient::platform::tcp_wasi::TcpStreamWasi;
use aloeclient::platform::ws_wasi::WebSocketWasi;

let listener = TcpStreamWasi::bind("127.0.0.1:9999").await?;

loop {
    let tcp_stream = listener.accept().await?;
    let ws = WebSocketWasi::accept(tcp_stream).await?;
    // Handle WebSocket connection
}
```

#### Browser WebSocket Client
```rust
use aloeclient::platform::ws_browser::WebSocketBrowser;

let mut ws = WebSocketBrowser::connect("ws://127.0.0.1:9999").await?;
ws.send(b"Hello").await?;

let mut buf = [0u8; 1024];
let n = ws.recv(&mut buf).await?;
```

### Server Abstraction

The `ServerBuilder` provides platform-appropriate concurrency:
```rust
use aloeclient::server::ServerBuilder;

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