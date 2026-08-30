use std::{borrow::Cow, sync::LazyLock};

use axum::{
    Json,
    body::Bytes,
    response::{IntoResponse, Response},
};
use memchr::{memchr2, memmem};
use reqwest::StatusCode;

pub(crate) const JSONRPC_INTERNAL_ERROR: i64 = -32603;

pub enum Shape {
    Batch,
    Single,
    Malformed,
}

pub(crate) fn shape(body: &Bytes) -> Shape {
    match memchr2(b'{', b'[', body) {
        Some(i) if body[..i].iter().all(u8::is_ascii_whitespace) => {
            if body[i] == b'[' {
                Shape::Batch
            } else {
                Shape::Single
            }
        }
        _ => Shape::Malformed,
    }
}

static ERROR_KEY: LazyLock<memmem::Finder<'static>> =
    LazyLock::new(|| memmem::Finder::new(br#""error""#));

static REVERTED: LazyLock<memmem::Finder<'static>> =
    LazyLock::new(|| memmem::Finder::new(b"execution reverted"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RpcFault {
    pub code: i64,
    pub retryable: bool,
}

#[derive(serde::Deserialize)]
struct ErrorEnvelope<'a> {
    #[serde(borrow, default)]
    error: Option<ErrorObject<'a>>,
}

#[derive(serde::Deserialize)]
struct ErrorObject<'a> {
    code: i64,
    #[serde(borrow, default)]
    message: Cow<'a, str>,
}

pub(crate) fn rpc_fault_in(body: &Bytes) -> Option<RpcFault> {
    ERROR_KEY.find(body)?;

    let envelope: ErrorEnvelope = serde_json::from_slice(body).ok()?;
    let error = envelope.error?;
    Some(RpcFault {
        code: error.code,
        retryable: is_retryable(error.code, error.message.as_bytes()),
    })
}

fn is_retryable(code: i64, message: &[u8]) -> bool {
    match code {
        // rate limited, resource unavailable, upstream's own internal error
        -32005 | -32002 | -32603 => true,
        // geth's catch-all. A revert is deterministic — every upstream reverts it.
        -32000 => REVERTED.find(message).is_none(),
        // -32700/-32600/-32601/-32602/-32003: the request is wrong. Retrying re-sends
        // the same wrong request to a fresh upstream and burns an attempt.
        _ => false,
    }
}

pub(crate) fn rpc_error(code: i64, msg: &str) -> Response {
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

    #[test]
    fn result_string_containing_the_word_error_is_not_a_fault() {
        let body = Bytes::from(r#"{"jsonrpc":"2.0","id":1,"result":"error: none"}"#);
        assert_eq!(rpc_fault_in(&body), None);
    }

    #[test]
    fn rate_limit_is_a_retryable_fault() {
        let body = Bytes::from(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limit exceeded"}}"#,
        );
        assert_eq!(
            rpc_fault_in(&body),
            Some(RpcFault {
                code: -32005,
                retryable: true
            })
        );
    }

    #[test]
    fn revert_is_final() {
        let body = Bytes::from(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}"#,
        );
        assert_eq!(
            rpc_fault_in(&body),
            Some(RpcFault {
                code: -32000,
                retryable: false
            })
        );
    }

    #[test]
    fn clean_result_never_reaches_the_parser() {
        let body = Bytes::from(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
        assert_eq!(rpc_fault_in(&body), None);
    }
}
