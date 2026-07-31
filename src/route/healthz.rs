use axum::response::IntoResponse;

pub async fn get_health() -> impl IntoResponse {
    "OK"
}
