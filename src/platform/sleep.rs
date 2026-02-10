use std::time::Duration;

/// Sleep for a duration.
pub async fn sleep(duration: Duration) {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        gloo_timers::future::sleep(duration).await;
    }

    #[cfg(all(target_arch = "wasm32", target_env = "p2"))]
    {
        // Use async sleep to yield to the runtime, matching aloeplatform's behavior
        tokio::time::sleep(duration).await;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        // Native: Use Tokio Async Sleep
        tokio::time::sleep(duration).await;
    }
}

pub async fn yield_now() {
    #[cfg(not(target_arch = "wasm32"))]
    tokio::task::yield_now();

    #[cfg(target_arch = "wasm32")]
    tokio::time::sleep(std::time::Duration::from_millis(1));
}