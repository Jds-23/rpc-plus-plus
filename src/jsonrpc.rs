use std::{borrow::Cow, collections::BTreeMap, sync::LazyLock};

use axum::body::Bytes;
use memchr::{memchr2, memmem};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

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
        Ok(envelope) => !is_hedge_safe(&envelope.method),
        // Batches, missing or non-string methods. Never hedge what we can't classify.
        Err(_) => true,
    }
}

// An allowlist, so an unknown write fails closed: a missing read only loses a hedge.
// Filters are out on purpose — each node keeps its own, and polling one consumes it.
pub(crate) fn is_hedge_safe(method: &str) -> bool {
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

pub(crate) fn rpc_error_body(code: i64, msg: &str) -> Bytes {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": msg },
        "id": null,
    });
    Bytes::from(body.to_string())
}

#[derive(serde::Deserialize)]
struct IdEnvelope<'a> {
    #[serde(borrow, default, deserialize_with = "present")]
    id: Option<&'a RawValue>,
}

fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(d).map(Some)
}

pub fn request_id(body: &Bytes) -> Option<Box<RawValue>> {
    if !matches!(shape(body), Shape::Single) {
        return None;
    }
    let envelope: IdEnvelope = serde_json::from_slice(body).ok()?;
    envelope.id.map(RawValue::to_owned)
}

pub fn readdress(body: &Bytes, id: &RawValue) -> Option<Bytes> {
    let mut envelope: BTreeMap<&str, &RawValue> = serde_json::from_slice(body).ok()?;
    envelope.insert("id", id);
    serde_json::to_vec(&envelope).ok().map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_error_escapes_the_message() {
        let msg = "upstream said \"nope\"\nand hung up";
        let bytes = rpc_error_body(JSONRPC_INTERNAL_ERROR, msg);

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

    fn id_of(body: &str) -> Option<String> {
        request_id(&Bytes::from(body.to_owned())).map(|id| id.get().to_owned())
    }

    #[test]
    fn the_id_is_kept_as_written() {
        assert_eq!(
            id_of(r#"{"jsonrpc":"2.0","id":1,"method":"eth_call"}"#).as_deref(),
            Some("1")
        );
        assert_eq!(
            id_of(r#"{"jsonrpc":"2.0","id":"0x01","method":"eth_call"}"#).as_deref(),
            Some(r#""0x01""#),
            "a string id stays a string — the caller matches on it verbatim"
        );
    }

    #[test]
    fn a_null_id_is_an_id() {
        assert_eq!(
            id_of(r#"{"jsonrpc":"2.0","id":null,"method":"eth_call"}"#).as_deref(),
            Some("null"),
            "only a missing id makes a notification"
        );
    }

    #[test]
    fn a_notification_has_no_id() {
        assert_eq!(id_of(r#"{"jsonrpc":"2.0","method":"eth_call"}"#), None);
    }

    #[test]
    fn a_body_that_is_not_an_object_has_no_id() {
        for body in [r#"[{"id":1}]"#, "not json", "7"] {
            assert_eq!(id_of(body), None, "{body}");
        }
    }

    fn reply(body: &str, id: &str) -> String {
        let id = RawValue::from_string(id.to_owned()).unwrap();
        let out = readdress(&Bytes::from(body.to_owned()), &id)
            .expect("a JSON-RPC body must be re-addressable");
        String::from_utf8(out.to_vec()).unwrap()
    }

    #[test]
    fn each_follower_gets_its_own_id() {
        let leader = r#"{"jsonrpc":"2.0","id":1,"result":"0x10"}"#;

        for id in ["7", r#""abc""#, "null"] {
            let out: serde_json::Value = serde_json::from_str(&reply(leader, id)).unwrap();
            let want: serde_json::Value = serde_json::from_str(id).unwrap();
            assert_eq!(
                out["id"], want,
                "a client matches responses to requests by id — the leader's id is a wrong answer"
            );
            assert_eq!(out["result"], "0x10", "the answer itself is untouched");
        }
    }

    #[test]
    fn the_result_comes_through_byte_for_byte() {
        let leader = r#"{"jsonrpc":"2.0","id":1,"result":{"z":1,"a":2.50,"n":1e3}}"#;

        let out = reply(leader, "9");

        assert!(
            out.contains(r#""result":{"z":1,"a":2.50,"n":1e3}"#),
            "via `Value` the keys come back sorted and 2.50 / 1e3 as 2.5 / 1000.0 — got {out}"
        );
    }

    #[test]
    fn a_null_result_stays_null() {
        let leader = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;

        let out = reply(leader, "2");

        assert!(
            out.contains(r#""result":null"#),
            "null is an answer (\"no such block yet\"), not a missing field — got {out}"
        );
    }

    #[test]
    fn errors_are_readdressed_too() {
        let leader =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}"#;

        let out = reply(leader, "3");
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();

        assert_eq!(
            parsed["id"], 3,
            "a shared failure still goes to the right caller"
        );
        assert!(
            out.contains(r#""error":{"code":-32000,"message":"execution reverted"}"#),
            "the error object is untouched — got {out}"
        );
    }

    #[test]
    fn a_body_that_is_not_an_object_gets_no_reply() {
        let id = RawValue::from_string("1".to_owned()).unwrap();
        for body in ["not json", "[1,2]", r#""str""#] {
            assert_eq!(
                readdress(&Bytes::from(body), &id),
                None,
                "{body}: no envelope to re-address — send this one on its own"
            );
        }
    }
}
