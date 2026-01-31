use std::future::Future;

// --- SPAWN: BROWSER (No Send required) ---
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static, // <--- Removed "+ Send"
{
    wasm_bindgen_futures::spawn_local(future);
}

// --- SPAWN: NATIVE / WASI (Send required by Tokio) ---
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static + Send, // <--- Keep "+ Send"
{
    tokio::spawn(future);
}

// For WASI: We need to manually manage task execution
#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
pub struct TaskQueue {
    tasks: std::cell::RefCell<Vec<std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>>>,
}

#[cfg(all(target_arch = "wasm32", target_env = "p2"))]
impl TaskQueue {
    pub fn new() -> Self {
        Self {
            tasks: std::cell::RefCell::new(Vec::new()),
        }
    }
    
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.tasks.borrow_mut().push(Box::pin(future));
    }
    
    pub async fn poll_tasks(&self) {
        use std::task::{Context, Poll, Waker};
        use std::future::Future;
        use std::pin::Pin;
        
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        
        let mut tasks = self.tasks.borrow_mut();
        tasks.retain_mut(|task| {
            match task.as_mut().poll(&mut context) {
                Poll::Ready(()) => false, // Remove completed tasks
                Poll::Pending => true,    // Keep pending tasks
            }
        });
    }
}