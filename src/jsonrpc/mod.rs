use std::{borrow::Cow, collections::BTreeMap, io, sync::LazyLock};

use axum::body::Bytes;
use memchr::{memchr2, memmem};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, value::RawValue};
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

pub(crate) fn is_write_method(method: &str) -> bool {
    !is_idempotent_read(method)
}

// No byte prefilter: a skip can't prove `method` exists, and misses `\u` escapes.
pub(crate) fn method_label(body: &Bytes) -> &'static str {
    let Ok(envelope) = serde_json::from_slice::<MethodEnvelope>(body) else {
        return OTHER_METHOD;
    };
    method_labels()
        .find(|label| *label == envelope.method)
        .unwrap_or(OTHER_METHOD)
}

pub(crate) fn method_labels() -> impl Iterator<Item = &'static str> {
    IDEMPOTENT_READS
        .iter()
        .chain(SENDS)
        .copied()
        .chain([OTHER_METHOD])
}

pub(crate) const OTHER_METHOD: &str = "other";

const SENDS: &[&str] = &[
    "eth_sendRawTransaction",
    "eth_sendTransaction",
    "eth_sendRawTransactionConditional",
    "eth_sendBundle",
    "eth_sendPrivateTransaction",
];

// An allowlist, so an unknown write fails closed: a missing read is only treated as a write.
// Filters are out on purpose — each node keeps its own, and polling one consumes it.
const IDEMPOTENT_READS: &[&str] = &[
    "eth_blockNumber",
    "eth_call",
    "eth_chainId",
    "eth_estimateGas",
    "eth_createAccessList",
    "eth_feeHistory",
    "eth_gasPrice",
    "eth_maxPriorityFeePerGas",
    "eth_blobBaseFee",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getProof",
    "eth_getTransactionCount",
    "eth_getLogs",
    "eth_getBlockByHash",
    "eth_getBlockByNumber",
    "eth_getBlockReceipts",
    "eth_getBlockTransactionCountByHash",
    "eth_getBlockTransactionCountByNumber",
    "eth_getTransactionByHash",
    "eth_getTransactionByBlockHashAndIndex",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionReceipt",
    "eth_getUncleCountByBlockHash",
    "eth_getUncleCountByBlockNumber",
    "eth_getUncleByBlockHashAndIndex",
    "eth_getUncleByBlockNumberAndIndex",
    "eth_syncing",
    "eth_protocolVersion",
    "net_version",
    "net_listening",
    "net_peerCount",
    "web3_clientVersion",
    "web3_sha3",
];

fn is_idempotent_read(method: &str) -> bool {
    IDEMPOTENT_READS.contains(&method)
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

// Without it a `null` id reads as `None`; only a missing id makes a notification.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(d).map(Some)
}

pub(crate) fn request_id(body: &Bytes) -> Option<Box<RawValue>> {
    if !matches!(shape(body), Shape::Single) {
        return None;
    }
    let envelope: IdEnvelope = serde_json::from_slice(body).ok()?;
    envelope.id.map(RawValue::to_owned)
}

/// Swaps in `id` and leaves every other field byte for byte: via `Value`, keys
/// come back sorted and numbers reformatted.
pub(crate) fn readdress(body: &Bytes, id: &RawValue) -> Option<Bytes> {
    let mut envelope: BTreeMap<&str, &RawValue> = serde_json::from_slice(body).ok()?;
    envelope.insert("id", id);
    serde_json::to_vec(&envelope).ok().map(Bytes::from)
}

#[cfg(test)]
mod test;
