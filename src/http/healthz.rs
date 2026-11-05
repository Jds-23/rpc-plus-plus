use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::proxy::Pipeline;

pub async fn get_healthz(State(pipeline): State<Arc<Pipeline>>) -> Response {
    if pipeline.is_ready() {
        (StatusCode::OK, "OK").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT READY").into_response()
    }
}

pub async fn get_livez() -> impl IntoResponse {
    "OK"
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{extract::State, http::StatusCode};

    use crate::{
        decider::Decider, http::healthz::get_healthz, observer::MetricsObserver, proxy::Pipeline,
        upstream::Upstream,
    };

    struct Fixed(bool);

    impl Decider for Fixed {
        fn decide(&self, _max: usize) -> Vec<Arc<Upstream>> {
            Vec::new()
        }

        fn upstream_len(&self) -> usize {
            0
        }

        fn ready(&self) -> bool {
            self.0
        }
    }

    fn pipeline(ready: bool) -> Arc<Pipeline> {
        Arc::new(
            Pipeline::builder()
                .decider(Arc::new(Fixed(ready)))
                .observer(Arc::new(MetricsObserver::new([])))
                .build()
                .expect("pipeline build failed"),
        )
    }

    #[tokio::test]
    async fn get_healthz_answers_ok_when_the_decider_is_ready() {
        let response = get_healthz(State(pipeline(true))).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_healthz_answers_service_unavailable_when_the_decider_is_not_ready() {
        let response = get_healthz(State(pipeline(false))).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
