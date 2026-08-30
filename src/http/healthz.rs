use axum::response::IntoResponse;

pub async fn get_healthz() -> impl IntoResponse {
    "OK"
}
