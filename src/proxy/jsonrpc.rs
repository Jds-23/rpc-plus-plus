use axum::{
    Json,
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;

pub(super) const JSONRPC_INTERNAL_ERROR: i64 = -32603;

pub(super) fn is_batch(body: &Bytes) -> bool {
    body.iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte == b'[')
}

pub(super) fn rpc_error(code: i64, msg: &str) -> Response {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": msg },
        "id": null,
    });
    (StatusCode::OK, Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn rpc_error_escapes_the_message() {
        let msg = "upstream said \"nope\"\nand hung up";
        let response = rpc_error(JSONRPC_INTERNAL_ERROR, msg);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed["error"]["message"], msg);
        assert_eq!(parsed["error"]["code"], JSONRPC_INTERNAL_ERROR);
        assert!(parsed["id"].is_null());
    }
}
