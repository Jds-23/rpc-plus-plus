use std::io::IsTerminal;

use tracing_subscriber::fmt::time::ChronoUtc;

pub fn init() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_timer(ChronoUtc::rfc_3339())
        // Logs are the only observability surface, so they get piped and grepped
        // far more often than they get read on a terminal. Colour only when one
        // is actually attached.
        .with_ansi(std::io::stdout().is_terminal())
        .init();
}
