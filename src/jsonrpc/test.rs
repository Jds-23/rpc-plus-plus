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
fn a_clean_result_is_not_a_fault() {
    let body = Bytes::from(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
    assert_eq!(rpc_fault_in(&body), None);
}

fn is_write(body: &Bytes) -> bool {
    is_write_method(method_label(body))
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

#[test]
fn a_known_method_is_its_own_label() {
    assert_eq!(method_label(&request("eth_call")), "eth_call");
    assert_eq!(
        method_label(&request("eth_sendRawTransaction")),
        "eth_sendRawTransaction"
    );
}

#[test]
fn anything_else_is_labelled_other() {
    for body in [
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_sendUserOperation"}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":7}"#,
        r#"{"jsonrpc":"2.0","id":1,"params":[]}"#,
        r#"[{"jsonrpc":"2.0","id":1,"method":"eth_call"}]"#,
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_call""#,
    ] {
        assert_eq!(method_label(&Bytes::from(body)), OTHER_METHOD, "{body}");
    }
}

#[test]
fn every_label_is_unique() {
    let labels: Vec<_> = method_labels().collect();
    let unique: std::collections::HashSet<_> = labels.iter().collect();
    assert_eq!(labels.len(), unique.len());
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
