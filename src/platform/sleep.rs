use std::time::Duration;

/// Sleep for a duration.
pub async fn sleep(duration: Duration) {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        gloo_timers::future::sleep(duration).await;
    }

    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    {
        // wasi::clocks::monotonic_clock::subscribe_duration(duration.as_nanos() as u64);
        std::thread::sleep(duration);
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        // Native: Use Tokio Async Sleep
        tokio::time::sleep(duration).await;
    }
}

pub async fn yield_now() {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    tokio::task::yield_now().await
}