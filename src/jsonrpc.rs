use std::{borrow::Cow, io, sync::LazyLock};

use axum::{
    Json,
    body::Bytes,
    response::{IntoResponse, Response},
};
use memchr::{memchr2, memmem};
use reqwest::StatusCode;
use serde_json::Value;
use xxhash_rust::xxh3::Xxh3;

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

#[derive(serde::Deserialize)]
struct MethodEnvelope<'a> {
    #[serde(borrow)]
    method: Cow<'a, str>,
}

// No byte prefilter: a skip can't prove `method` exists, and misses `\u` escapes.
pub(crate) fn is_write(body: &Bytes) -> bool {
    match serde_json::from_slice::<MethodEnvelope>(body) {
        Ok(envelope) => !is_idempotent_read(&envelope.method),
        // Batches, missing or non-string methods. Never treat what we can't classify as a read.
        Err(_) => true,
    }
}

// An allowlist, so an unknown write fails closed: a missing read is only treated as a write.
// Filters are out on purpose — each node keeps its own, and polling one consumes it.
fn is_idempotent_read(method: &str) -> bool {
    matches!(
        method,
        "eth_blockNumber"
            | "eth_call"
            | "eth_chainId"
            | "eth_estimateGas"
            | "eth_createAccessList"
            | "eth_feeHistory"
            | "eth_gasPrice"
            | "eth_maxPriorityFeePerGas"
            | "eth_blobBaseFee"
            | "eth_getBalance"
            | "eth_getCode"
            | "eth_getStorageAt"
            | "eth_getProof"
            | "eth_getTransactionCount"
            | "eth_getLogs"
            | "eth_getBlockByHash"
            | "eth_getBlockByNumber"
            | "eth_getBlockReceipts"
            | "eth_getBlockTransactionCountByHash"
            | "eth_getBlockTransactionCountByNumber"
            | "eth_getTransactionByHash"
            | "eth_getTransactionByBlockHashAndIndex"
            | "eth_getTransactionByBlockNumberAndIndex"
            | "eth_getTransactionReceipt"
            | "eth_getUncleCountByBlockHash"
            | "eth_getUncleCountByBlockNumber"
            | "eth_getUncleByBlockHashAndIndex"
            | "eth_getUncleByBlockNumberAndIndex"
            | "eth_syncing"
            | "eth_protocolVersion"
            | "net_version"
            | "net_listening"
            | "net_peerCount"
            | "web3_clientVersion"
            | "web3_sha3"
    )
}

/// What a flight is keyed by: the question, not the caller's envelope (`id`, `jsonrpc`).
///
/// 128 bits, not 64: a collision merges two questions and serves one caller the other's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DedupKey(u128);

/// `None` = send it on its own: writes, unknown methods, batches, malformed bodies.
///
/// Hashed from the parsed value, so whitespace never splits a key. Array order is kept —
/// params are positional. Object keys are sorted: `Map` is a `BTreeMap` without
/// `preserve_order`. A missing `params` asks the same question as `[]`. The quoted method
/// string delimits itself, so method and params need no separator.
#[cfg_attr(not(test), expect(dead_code, reason = "coalesce is the first caller"))]
pub(crate) fn dedup_key(body: &Bytes) -> Option<DedupKey> {
    let Value::Object(mut request) = serde_json::from_slice(body).ok()? else {
        return None;
    };
    let params = request
        .remove("params")
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let Some(Value::String(method)) = request.get("method") else {
        return None;
    };
    if !is_idempotent_read(method) {
        return None;
    }

    let mut hasher = Xxh3Writer(Xxh3::new());
    serde_json::to_writer(&mut hasher, method).ok()?;
    serde_json::to_writer(&mut hasher, &params).ok()?;
    Some(DedupKey(hasher.0.digest128()))
}

// Serializes straight into the hasher: no key string is ever allocated.
struct Xxh3Writer(Xxh3);

impl io::Write for Xxh3Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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

    fn request(method: &str) -> Bytes {
        Bytes::from(format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":[]}}"#
        ))
    }

    #[test]
    fn every_send_method_is_a_write() {
        for method in [
            "eth_sendRawTransaction",
            "eth_sendTransaction",
            "eth_sendRawTransactionConditional",
            "eth_sendBundle",
            "eth_sendPrivateTransaction",
        ] {
            assert!(is_write(&request(method)), "{method} should be a write");
        }
    }

    #[test]
    fn a_known_read_is_not_a_write() {
        assert!(!is_write(&request("eth_call")));
        assert!(!is_write(&request("eth_blockNumber")));
    }

    #[test]
    fn an_unknown_method_counts_as_a_write() {
        assert!(is_write(&request("eth_sendUserOperation")));
    }

    #[test]
    fn an_unreadable_method_counts_as_a_write() {
        for body in [
            r#"{"jsonrpc":"2.0","id":1,"params":[]}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":7}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_call""#,
        ] {
            assert!(is_write(&Bytes::from(body)), "{body} should be a write");
        }
    }

    #[test]
    fn a_send_method_inside_params_does_not_fool_the_guard() {
        let body = Bytes::from(
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_call","params":["eth_sendRawTransaction"]}"#,
        );
        assert!(!is_write(&body));
    }

    #[test]
    fn method_after_params_is_still_found() {
        let body = Bytes::from(
            "{ \"params\" : [\"0x1\"] ,\n  \"id\" : 1 ,\n  \"method\" : \"eth_sendRawTransaction\" }",
        );
        assert!(is_write(&body));
    }

    #[test]
    fn escaped_method_is_decoded_before_the_check() {
        let write = Bytes::from(r#"{"id":1,"method":"eth_sendRawTransaction"}"#);
        let read = Bytes::from(r#"{"id":1,"method":"eth_call"}"#);
        assert!(is_write(&write));
        assert!(!is_write(&read));
    }

    #[test]
    fn a_batch_counts_as_a_write() {
        let body = Bytes::from(r#"[{"jsonrpc":"2.0","id":1,"method":"eth_call"}]"#);
        assert!(is_write(&body));
    }

    fn key(body: &str) -> Option<DedupKey> {
        dedup_key(&Bytes::from(body.to_owned()))
    }

    #[test]
    fn the_id_is_not_part_of_the_question() {
        let a = key(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}"#);
        let b = key(r#"{"jsonrpc":"2.0","id":"abc","method":"eth_blockNumber","params":[]}"#);
        assert!(a.is_some(), "eth_blockNumber is a read, it must get a key");
        assert_eq!(
            a, b,
            "every caller picks its own id — keep it in the key and no two requests ever coalesce"
        );
    }

    #[test]
    fn object_key_order_does_not_split_the_key() {
        let a = key(r#"{"id":1,"method":"eth_call","params":[{"to":"0x2","data":"0x"},"0x10"]}"#);
        let b = key(r#"{"id":2,"method":"eth_call","params":[{"data":"0x","to":"0x2"},"0x10"]}"#);
        assert_eq!(
            a, b,
            "JSON objects are unordered — is serde_json/preserve_order enabled via unification?"
        );
    }

    #[test]
    fn array_order_is_part_of_the_question() {
        let a = key(r#"{"id":1,"method":"eth_getStorageAt","params":["0xabc","0x0","0x10"]}"#);
        let b = key(r#"{"id":1,"method":"eth_getStorageAt","params":["0xabc","0x10","0x0"]}"#);
        assert_ne!(
            a, b,
            "params are positional: slot 0 at block 16 is not slot 16 at block 0"
        );
    }

    #[test]
    fn whitespace_does_not_split_the_key() {
        let a = key(r#"{"id":1,"method":"eth_getBalance","params":["0xabc","0x10"]}"#);
        let b = key(
            "{ \"id\" : 1 ,\n \"method\" : \"eth_getBalance\" , \"params\" : [ \"0xabc\" , \"0x10\" ] }",
        );
        assert_eq!(
            a, b,
            "the key is built from the parsed value, not the raw bytes"
        );
    }

    #[test]
    fn missing_params_is_the_empty_list() {
        let a = key(r#"{"id":1,"method":"eth_chainId"}"#);
        let b = key(r#"{"id":1,"method":"eth_chainId","params":[]}"#);
        assert!(a.is_some(), "params is optional in JSON-RPC");
        assert_eq!(
            a, b,
            "omitting params asks the same question as an empty list"
        );
    }

    #[test]
    fn different_questions_get_different_keys() {
        let at_16 = key(r#"{"id":1,"method":"eth_getBalance","params":["0xabc","0x10"]}"#);
        let at_17 = key(r#"{"id":1,"method":"eth_getBalance","params":["0xabc","0x11"]}"#);
        let code = key(r#"{"id":1,"method":"eth_getCode","params":["0xabc","0x10"]}"#);
        assert_ne!(
            at_16, at_17,
            "different params: merging them serves the wrong balance"
        );
        assert_ne!(
            at_16, code,
            "different method, same params: still a different question"
        );
    }

    #[test]
    fn writes_are_never_coalesced() {
        let body = r#"{"id":1,"method":"eth_sendRawTransaction","params":["0xf8"]}"#;
        assert_eq!(
            key(body),
            None,
            "two identical sends are two intents — coalescing one away is a lost write"
        );
    }

    #[test]
    fn unknown_methods_fail_closed() {
        let body = r#"{"id":1,"method":"eth_newFilter","params":[{}]}"#;
        assert_eq!(
            key(body),
            None,
            "an allowlist: unknown methods are sent on their own"
        );
    }

    #[test]
    fn unreadable_bodies_get_no_key() {
        for body in [
            r#"[{"id":1,"method":"eth_blockNumber"}]"#,
            r#"{"id":1,"method":"eth_blockNumber""#,
            r#"{"id":1,"method":7}"#,
            r#"{"id":1,"params":[]}"#,
            r#""eth_blockNumber""#,
        ] {
            assert_eq!(key(body), None, "{body} must be sent on its own, not keyed");
        }
    }
}
