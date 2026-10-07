use std::io;

use axum::body::Bytes;
use serde_json::Value;
use xxhash_rust::xxh3::Xxh3;

use crate::jsonrpc::is_hedge_safe;

/// What a flight is keyed by: the question, not the caller's envelope (`id`, `jsonrpc`).
///
/// 128 bits, not 64: a collision merges two questions and serves one caller the other's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DedupKey(u128);

/// `None` = send it on its own: writes, unknown methods, batches, malformed bodies.
///
/// Hashed from the parsed value, so whitespace never splits a key. Array order is kept —
/// params are positional. Object keys are sorted: `Map` is a `BTreeMap` without
/// `preserve_order`. A missing `params` asks the same question as `[]`. The quoted method
/// string delimits itself, so method and params need no separator.
pub fn dedup_key(body: &Bytes) -> Option<DedupKey> {
    let Value::Object(mut request) = serde_json::from_slice(body).ok()? else {
        return None;
    };
    let params = request
        .remove("params")
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let Some(Value::String(method)) = request.get("method") else {
        return None;
    };
    if !is_hedge_safe(method) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
