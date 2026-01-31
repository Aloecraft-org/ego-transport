#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use log::{Level, Metadata, Record, LevelFilter};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct SimpleLogger;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Trace
    }

    fn log(&self, record: &Record) {
        // println!("{}",record.args());
        if self.enabled(record.metadata()) {
            // Constraint: Format as [LEVEL] {msg} to prevent JS shim collisions
            println!("[{}] {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
static LOGGER: SimpleLogger = SimpleLogger;

pub fn init() {
    #[cfg(not(target_arch = "wasm32"))]
    {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .expect("Logger initialization failed");
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        console_log::init_with_level(log::Level::Info).expect("error initializing logger");
        std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    }
    #[cfg(all(target_arch = "wasm32", target_os = "wasi"))]
    {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .expect("Logger initialization failed");
    }
}