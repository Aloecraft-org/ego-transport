use crate::transport::{Transport, TransportError};

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use wasip2::io::poll::{Pollable, poll};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use wasip2::io::streams::{InputStream, OutputStream, StreamError};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use wasip2::sockets::instance_network::instance_network;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use wasip2::sockets::network::IpAddressFamily;
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use wasip2::sockets::network::{IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress};
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
use wasip2::sockets::tcp::TcpSocket;

use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct TcpStreamWasi {
    pub input: InputStream,
    pub output: OutputStream,
    pub inner: TcpSocket,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct TcpListenerWasi {
    inner: TcpSocket,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl TcpStreamWasi {
    /// Get the remote peer address
    pub fn peer_addr(&self) -> Option<String> {
        use wasip2::sockets::tcp::TcpSocket;

        // WASI P2 provides remote_address() on the socket
        match self.inner.remote_address() {
            Ok(addr) => {
                // Convert IpSocketAddress to string
                let addr_str = match addr {
                    wasip2::sockets::network::IpSocketAddress::Ipv4(v4) => {
                        format!(
                            "{}.{}.{}.{}:{}",
                            v4.address.0, v4.address.1, v4.address.2, v4.address.3, v4.port
                        )
                    }
                    wasip2::sockets::network::IpSocketAddress::Ipv6(v6) => {
                        format!(
                            "[{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}]:{}",
                            v6.address.0,
                            v6.address.1,
                            v6.address.2,
                            v6.address.3,
                            v6.address.4,
                            v6.address.5,
                            v6.address.6,
                            v6.address.7,
                            v6.port
                        )
                    }
                };
                Some(addr_str)
            }
            Err(e) => {
                log::warn!("Failed to get remote address: {:?}", e);
                None
            }
        }
    }

    pub async fn connect(addr: &str) -> Result<Self, TransportError> {
        log::info!("[WASI TCP] Attempting to connect to {}", addr);

        // Parse address string "127.0.0.1:9999" into IP and port
        let (ip_str, port_str) = addr
            .split_once(':')
            .ok_or_else(|| TransportError::Protocol(format!("Invalid address format: {}", addr)))?;

        let port: u16 = port_str
            .parse()
            .map_err(|_| TransportError::Protocol(format!("Invalid port: {}", port_str)))?;

        // Parse IP address
        let remote_addr = parse_ip_address(ip_str, port)?;

        log::info!("[WASI TCP] Parsed address: {:?}", remote_addr);

        // Get the default network instance
        let network = instance_network();

        // Create TCP socket for IPv4
        let socket = match &remote_addr {
            IpSocketAddress::Ipv4(_) => wasip2::sockets::tcp_create_socket::create_tcp_socket(
                wasip2::sockets::network::IpAddressFamily::Ipv4,
            )
            .map_err(|e| TransportError::Protocol(format!("Failed to create socket: {:?}", e)))?,
            IpSocketAddress::Ipv6(_) => wasip2::sockets::tcp_create_socket::create_tcp_socket(
                wasip2::sockets::network::IpAddressFamily::Ipv6,
            )
            .map_err(|e| TransportError::Protocol(format!("Failed to create socket: {:?}", e)))?,
        };

        log::info!("[WASI TCP] Socket created, starting connect...");

        // Start the connection
        socket
            .start_connect(&network, remote_addr)
            .map_err(|e| TransportError::Protocol(format!("Failed to start connect: {:?}", e)))?;

        // Get pollable for the socket
        // let pollable = socket.subscribe();

        // Poll until connection is ready
        log::info!("[WASI TCP] Polling for connection...");
        let (input, output) = loop {
            match socket.finish_connect() {
                Ok((input, output)) => {
                    log::info!("[WASI TCP] Connection established!");
                    break (input, output);
                }
                Err(e) => {
                    // Check the error type
                    match e {
                        wasip2::sockets::network::ErrorCode::WouldBlock => {
                            aloeplatform::sleep(Duration::from_millis(1)).await;
                            continue;
                        }
                        _ => {
                            return Err(TransportError::Protocol(format!(
                                "Connection failed: {:?}",
                                e
                            )));
                        }
                    }
                }
            }
        };

        log::info!("[WASI TCP] Streams acquired");

        Ok(Self {
            inner: socket,
            input,
            output,
        })
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn parse_ip_address(ip_str: &str, port: u16) -> Result<IpSocketAddress, TransportError> {
    // Try to parse as IPv4
    if let Ok(octets) = parse_ipv4(ip_str) {
        return Ok(IpSocketAddress::Ipv4(Ipv4SocketAddress {
            address: octets,
            port,
        }));
    }

    // Try to parse as IPv6
    if let Ok(hextets) = parse_ipv6(ip_str) {
        return Ok(IpSocketAddress::Ipv6(Ipv6SocketAddress {
            address: hextets,
            port,
            flow_info: 0,
            scope_id: 0,
        }));
    }

    Err(TransportError::Protocol(format!(
        "Invalid IP address: {}",
        ip_str
    )))
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn parse_ipv4(ip_str: &str) -> Result<(u8, u8, u8, u8), TransportError> {
    let parts: Vec<&str> = ip_str.split('.').collect();
    if parts.len() != 4 {
        return Err(TransportError::Protocol("Invalid IPv4 format".to_string()));
    }

    let mut octets = [0u8; 4];
    for (i, part) in parts.iter().enumerate() {
        octets[i] = part
            .parse()
            .map_err(|_| TransportError::Protocol("Invalid IPv4 octet".to_string()))?;
    }

    Ok((octets[0], octets[1], octets[2], octets[3]))
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
fn parse_ipv6(ip_str: &str) -> Result<(u16, u16, u16, u16, u16, u16, u16, u16), TransportError> {
    // Simplified IPv6 parser - only handles full addresses for now
    let parts: Vec<&str> = ip_str.split(':').collect();
    if parts.len() != 8 {
        return Err(TransportError::Protocol(
            "Invalid IPv6 format (full address required)".to_string(),
        ));
    }

    let mut hextets = [0u16; 8];
    for (i, part) in parts.iter().enumerate() {
        hextets[i] = u16::from_str_radix(part, 16)
            .map_err(|_| TransportError::Protocol("Invalid IPv6 hextet".to_string()))?;
    }

    Ok((
        hextets[0], hextets[1], hextets[2], hextets[3], hextets[4], hextets[5], hextets[6],
        hextets[7],
    ))
}

use async_trait::async_trait;

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
#[async_trait]
impl Transport for TcpStreamWasi {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        let mut written = 0;

        while written < data.len() {
            let chunk = &data[written..];

            match self.output.write(chunk) {
                Ok(()) => {
                    // write() doesn't return bytes written in this version
                    // Assume all data was written
                    written = data.len();
                    log::debug!("[WASI TCP] Wrote data");
                }
                Err(e) => match e {
                    StreamError::Closed => {
                        return Err(TransportError::Closed);
                    }
                    StreamError::LastOperationFailed(err) => {
                        let error_str = err.to_debug_string();
                        if error_str.contains("would-block") {
                            aloeplatform::sleep(Duration::from_millis(1)).await;
                            continue;
                        } else {
                            return Err(TransportError::Protocol(format!(
                                "Write error: {}",
                                error_str
                            )));
                        }
                    }
                },
            }
        }

        // With this (non-blocking flush):
        // Note: Don't flush synchronously in WASI - it can block indefinitely
        // The stream will flush on its own
        log::debug!("[WASI TCP] Write complete (flush skipped)");

        Ok(())
    }
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        loop {
            match self.input.read(buf.len() as u64) {
                Ok(data) => {
                    if data.is_empty() {
                        aloeplatform::sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                    let n = data.len().min(buf.len());
                    buf[..n].copy_from_slice(&data[..n]);
                    return Ok(n);
                }
                Err(e) => {
                    // Handle errors...
                    match e {
                        StreamError::Closed => {
                            log::info!("[WASI TCP] Connection closed");
                            return Err(TransportError::Closed);
                        }
                        StreamError::LastOperationFailed(err) => {
                            let error_str = err.to_debug_string();

                            if err.to_debug_string().contains("would-block") {
                                aloeplatform::sleep(Duration::from_millis(1)).await;
                                continue;
                            } else if error_str.contains("closed") {
                                log::info!("[WASI TCP] Connection closed");
                                return Err(TransportError::Closed);
                            } else {
                                return Err(TransportError::Protocol(format!(
                                    "Read error: {}",
                                    error_str
                                )));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl Drop for TcpStreamWasi {
    fn drop(&mut self) {
        // Explicitly drop streams before socket to avoid "resource has children" error
        // WASI requires children to be dropped before parent
        log::debug!("[WASI TCP] Dropping streams and socket");

        // Rust will drop fields in declaration order, but we can be explicit:
        // The streams (input, output) will drop first, then inner (socket)
        // This is already correct due to field order, but we document it here
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl TcpListenerWasi {
    pub async fn bind(addr: &str) -> Result<Self, TransportError> {
        log::info!("[WASI TCP Listener] Attempting to bind to {}", addr);

        // Parse address
        let (ip_str, port_str) = addr
            .split_once(':')
            .ok_or_else(|| TransportError::Protocol(format!("Invalid address format: {}", addr)))?;

        let port: u16 = port_str
            .parse()
            .map_err(|_| TransportError::Protocol(format!("Invalid port: {}", port_str)))?;

        let bind_addr = parse_ip_address(ip_str, port)?;

        log::info!("[WASI TCP Listener] Parsed bind address: {:?}", bind_addr);

        // Get network instance
        let network = instance_network();

        // Create TCP socket
        let socket = match &bind_addr {
            IpSocketAddress::Ipv4(_) => wasip2::sockets::tcp_create_socket::create_tcp_socket(
                wasip2::sockets::network::IpAddressFamily::Ipv4,
            )
            .map_err(|e| TransportError::Protocol(format!("Failed to create socket: {:?}", e)))?,
            IpSocketAddress::Ipv6(_) => wasip2::sockets::tcp_create_socket::create_tcp_socket(
                wasip2::sockets::network::IpAddressFamily::Ipv6,
            )
            .map_err(|e| TransportError::Protocol(format!("Failed to create socket: {:?}", e)))?,
        };

        log::info!("[WASI TCP Listener] Socket created, starting bind...");

        // Start listening
        socket
            .start_bind(&network, bind_addr)
            .map_err(|e| TransportError::Protocol(format!("Failed to bind: {:?}", e)))?;

        // Get pollable
        // let pollable = socket.subscribe();

        // Poll until bind is complete
        log::info!("[WASI TCP Listener] Polling for bind completion...");
        loop {
            match socket.finish_bind() {
                Ok(()) => {
                    log::info!("[WASI TCP Listener] Bind complete!");
                    break;
                }
                Err(e) => match e {
                    wasip2::sockets::network::ErrorCode::WouldBlock => {
                        aloeplatform::sleep(Duration::from_millis(5)).await;
                        continue;
                    }
                    _ => {
                        return Err(TransportError::Protocol(format!("Bind failed: {:?}", e)));
                    }
                },
            }
        }

        // Start listening
        log::info!("[WASI TCP Listener] Starting listen...");
        socket
            .start_listen()
            .map_err(|e| TransportError::Protocol(format!("Failed to start listen: {:?}", e)))?;

        // Finish listen (this is the missing step!)
        log::info!("[WASI TCP Listener] Polling for listen completion...");
        loop {
            match socket.finish_listen() {
                Ok(()) => {
                    log::info!("[WASI TCP Listener] Listen complete!");
                    break;
                }
                Err(e) => match e {
                    wasip2::sockets::network::ErrorCode::WouldBlock => {
                        aloeplatform::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    _ => {
                        return Err(TransportError::Protocol(format!("Listen failed: {:?}", e)));
                    }
                },
            }
        }

        log::info!("[WASI TCP Listener] Ready to accept connections!");

        Ok(Self { inner: socket })
    }

    pub async fn accept(&self) -> Result<TcpStreamWasi, TransportError> {
        // let pollable = self.inner.subscribe();
        loop {
            match self.inner.accept() {
                Ok((client_socket, input, output)) => {
                    //  Yield to the async executor to give the connection a moment to stabilize
                    tokio::task::yield_now().await;

                    let tcp_stream = TcpStreamWasi {
                        input,
                        output,
                        inner: client_socket,
                    };
                    log::info!(
                        "[WASI TCP Listener] Connection accepted from {:?}, streams acquired",
                        tcp_stream.peer_addr()
                    );

                    return Ok(tcp_stream);
                }
                Err(e) => match e {
                    wasip2::sockets::network::ErrorCode::WouldBlock => {
                        use crate::platform;
                        aloeplatform::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    _ => {
                        log::info!("TransportError");
                        return Err(TransportError::Protocol(format!("Accept failed: {:?}", e)));
                    }
                },
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl TcpStreamWasi {
    pub async fn bind(addr: &str) -> Result<TcpListenerWasi, TransportError> {
        TcpListenerWasi::bind(addr).await
    }
}

// Add this implementation after TcpListenerWasi
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
#[async_trait::async_trait]
impl crate::platform::server::Listener for TcpListenerWasi {
    async fn accept(&self) -> Result<Box<dyn Transport>, TransportError> {
        let transport = self.accept().await?;
        Ok(Box::new(transport))
    }
}
