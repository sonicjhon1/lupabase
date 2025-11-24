use std::sync::OnceLock;
use tracing_subscriber::{EnvFilter, fmt};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

pub fn init_tracing_for_tests() {
    TRACING_INIT.get_or_init(|| {
        let env_filter = std::env::var("RUST_LOG")
            .ok()
            .map(|s| EnvFilter::try_new(s).unwrap())
            .unwrap_or_else(|| EnvFilter::new("debug"));

        let subscriber = fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .compact();

        let _ = tracing::subscriber::set_global_default(subscriber.finish());
    });
}

#[macro_export]
macro_rules! span_and_info {
    ($name:literal) => {
        let span = tracing::info_span!($name);
        let _guard = span.enter();
        tracing::info!($name);
    };
    ($name:literal, $($arg:tt)+) => {
        let span = tracing::info_span!($name);
        let _guard = span.enter();
        tracing::info!($($arg)+);
    };
}
