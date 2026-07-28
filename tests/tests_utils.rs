use std::{
    fmt::Display,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tempfile::TempDir;
use tracing::info;
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

pub fn create_temp_working_dir(tester: impl Display) -> (PathBuf, TempDir) {
    let temp_dir =
        TempDir::with_prefix(format!("{tester}-")).expect("Temporary directory creation failed");
    let pathbuf = temp_dir.path().to_path_buf();

    info!("Created temporary TempDir: [{}]", pathbuf.display());

    (pathbuf, temp_dir)
}

pub fn temp_file_path(path: impl AsRef<Path>) -> (PathBuf, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Temporary directory creation failed");
    let path = temp_dir.path().join(path);

    (path, temp_dir)
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
