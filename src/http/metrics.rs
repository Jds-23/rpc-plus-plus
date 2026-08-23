use std::sync::Arc;

use axum::{
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use prometheus::{Encoder, Registry, TextEncoder};
use tracing::error;

pub async fn get_metrics(State(registry): State<Arc<Registry>>) -> Response {
    let encoder = TextEncoder::new();

    match encoder.encode_to_string(&registry.gather()) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, encoder.format_type())],
            body,
        )
            .into_response(),
        Err(err) => {
            error!(event = "metrics_render_failed", error = %err);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::extract::State;
    use prometheus::{Encoder, Registry, TextEncoder};

    use crate::http::metrics::get_metrics;

    #[tokio::test]
    async fn get_metrics_answers_ok_with_the_exposition_content_type() {
        let registry = Arc::new(Registry::new());

        let response = get_metrics(State(registry)).await;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            TextEncoder::new().format_type()
        );
    }
}
