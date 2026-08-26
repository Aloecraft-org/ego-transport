//! Visible backpressure and connection health.
//!
//! Consumers of this crate often promise their own callers that accepting a
//! message is O(1) and never waits. The handoff out of a transport therefore
//! has to be *bounded*: a queue whose "full" is an observable outcome the
//! caller can act on — never an unbounded buffer, never a hidden park.
//!
//! [`InboundBuffer`] is that handoff. [`ConnectionMetrics`] is the matching
//! health surface: per-connection counters (queue depth, bytes, last
//! activity) that a consumer can poll and feed into whatever saturation or
//! health model it runs. This crate exposes the data; policy stays with the
//! consumer.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Milliseconds since an arbitrary process-local epoch. Only differences are
/// meaningful; the value exists so "last activity" can be compared to "now".
pub fn now_millis() -> u64 {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        use std::sync::OnceLock;
        use std::time::Instant;
        static EPOCH: OnceLock<Instant> = OnceLock::new();
        EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        js_sys::Date::now() as u64
    }
}

/// Counters for one connection (or one listener), updated by the transport
/// side and pollable by the consumer at any time. All operations are O(1) and
/// lock-free.
#[derive(Debug, Default)]
pub struct ConnectionMetrics {
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    queue_depth: AtomicUsize,
    queue_capacity: AtomicUsize,
    rejected: AtomicU64,
    last_activity_ms: AtomicU64,
}

impl ConnectionMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_in(&self, bytes: usize) {
        self.bytes_in.fetch_add(bytes as u64, Ordering::Relaxed);
        self.touch();
    }

    pub fn record_out(&self, bytes: usize) {
        self.bytes_out.fetch_add(bytes as u64, Ordering::Relaxed);
        self.touch();
    }

    pub fn touch(&self) {
        self.last_activity_ms.store(now_millis(), Ordering::Relaxed);
    }

    /// A point-in-time copy of every counter.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_capacity: self.queue_capacity.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            last_activity_ms: self.last_activity_ms.load(Ordering::Relaxed),
        }
    }
}

/// The pollable view of [`ConnectionMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// Messages currently held in the inbound buffer.
    pub queue_depth: usize,
    /// Message capacity of the inbound buffer (0 when no buffer is attached).
    pub queue_capacity: usize,
    /// Messages refused because the buffer was full.
    pub rejected: u64,
    /// [`now_millis`] timestamp of the most recent send/recv/queue activity.
    pub last_activity_ms: u64,
}

impl MetricsSnapshot {
    /// Queue fill fraction in `[0, 1]`, the natural saturation input.
    pub fn saturation(&self) -> f64 {
        if self.queue_capacity == 0 {
            0.0
        } else {
            self.queue_depth as f64 / self.queue_capacity as f64
        }
    }
}

/// Outcome of offering a message to an [`InboundBuffer`]. Full is a value,
/// not a wait.
#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The message was queued.
    Accepted,
    /// The buffer is at capacity; the message is handed back to the caller,
    /// who decides whether to hold it (and stop reading — backpressure) or
    /// drop it.
    Full(Vec<u8>),
}

/// A bounded inbound message buffer with observable fullness.
///
/// Every operation is O(1) and non-blocking: `try_push` either accepts or
/// reports [`PushOutcome::Full`], and `try_pop` either yields a message or
/// `None`. Capacity is enforced on both message count and total bytes so
/// neither many small messages nor a few huge ones can grow the buffer
/// without bound.
///
/// The buffer is single-threaded by design (wrap it in whatever your
/// concurrency model needs); its [`ConnectionMetrics`] handle is the shared,
/// thread-safe view of its state.
pub struct InboundBuffer {
    queue: VecDeque<Vec<u8>>,
    max_messages: usize,
    max_bytes: usize,
    bytes: usize,
    metrics: Arc<ConnectionMetrics>,
}

impl InboundBuffer {
    /// A buffer holding at most `max_messages` messages and `max_bytes` total
    /// payload bytes, whichever fills first.
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        let metrics = ConnectionMetrics::new();
        metrics
            .queue_capacity
            .store(max_messages, Ordering::Relaxed);
        Self {
            queue: VecDeque::with_capacity(max_messages.min(1024)),
            max_messages,
            max_bytes,
            bytes: 0,
            metrics,
        }
    }

    /// Offer a message. Never waits; a full buffer returns the message.
    pub fn try_push(&mut self, msg: Vec<u8>) -> PushOutcome {
        if self.queue.len() >= self.max_messages || self.bytes + msg.len() > self.max_bytes {
            self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
            return PushOutcome::Full(msg);
        }
        self.bytes += msg.len();
        self.queue.push_back(msg);
        self.metrics
            .queue_depth
            .store(self.queue.len(), Ordering::Relaxed);
        self.metrics.touch();
        PushOutcome::Accepted
    }

    /// Take the oldest queued message, if any. Never waits.
    pub fn try_pop(&mut self) -> Option<Vec<u8>> {
        let msg = self.queue.pop_front()?;
        self.bytes -= msg.len();
        self.metrics
            .queue_depth
            .store(self.queue.len(), Ordering::Relaxed);
        self.metrics.touch();
        Some(msg)
    }

    /// Whether the next `try_push` of a `len`-byte message would be refused.
    pub fn would_refuse(&self, len: usize) -> bool {
        self.queue.len() >= self.max_messages || self.bytes + len > self.max_bytes
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The shared metrics handle for this buffer. Clone it out to wherever
    /// health is polled from.
    pub fn metrics(&self) -> Arc<ConnectionMetrics> {
        self.metrics.clone()
    }
}
