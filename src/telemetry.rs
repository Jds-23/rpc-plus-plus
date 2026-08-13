use std::io::IsTerminal;

use tracing_subscriber::fmt::time::ChronoUtc;

pub fn init() {
    let builder = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_timer(ChronoUtc::rfc_3339());

    if std::io::stdout().is_terminal() {
        builder.with_ansi(true).init();
    } else {
        builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .init();
    }
}
