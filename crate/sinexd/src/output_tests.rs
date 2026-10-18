use super::*;
use serde_json::json;
#[test]
fn malformed_request_rows_reach_validation_unchanged() {
    use sinex_protocol::rpc::{Method, RpcRequest};
    for params in [
        json!({"rows":[1]}),
        json!({"rows":[[]]}),
        json!({"rows":[null]}),
        json!({"rows":[true]}),
        json!({"rows":["plain"]}),
        json!({"rows":1}),
        json!({"rows":[]}),
        json!([1]),
        json!(null),
    ] {
        let original = params.clone();
        assert!(request_safe(&RpcId::Number(7), &params));
        assert_eq!(params, original);
        let error = RpcRequest::new(RpcId::Number(7), Method::Status, params)
            .validate()
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadQuery);
    }
}
#[test]
fn malformed_response_rows_are_refused() {
    for rows in [
        json!([1]),
        json!([[]]),
        json!([null]),
        json!([true]),
        json!(["plain"]),
        json!(1),
        json!(null),
        json!({}),
    ] {
        let bytes = encode(RpcResponse::success(RpcId::Number(9), json!({"rows":rows})));
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["id"], 9);
        assert_eq!(wire["error"]["kind"], "refused");
        assert!(wire.get("result").is_none());
    }
    let bytes = encode(RpcResponse::success(RpcId::Number(9), json!({"rows":[]})));
    let wire: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(wire["result"]["rows"], json!([]));
}
#[test]
fn refusal_sends_no_part_of_the_response() {
    let token = format!("ghp_{}", "A".repeat(36));
    for payload in [
        json!({token.clone():"one", "other":"two"}),
        json!({"ordinary":"before", "nested":[{"text":format!("before https://u:{}", "A".repeat(600))}]}),
    ] {
        let bytes = encode(RpcResponse::success(
            RpcId::Number(7),
            json!({"row":{"payload":payload}}),
        ));
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["id"], 7);
        assert_eq!(wire["error"]["kind"], "refused");
        assert!(wire.get("result").is_none());
        assert!(!String::from_utf8(bytes).unwrap().contains(&token));
    }
    let mut read = json!({"rows":[{"payload":{"nested":["--secret=x",{"text":"plain"}]}}]});
    mask(&mut read).unwrap();
    assert_eq!(read["rows"][0]["secret"], true);
    assert_eq!(read["rows"][0]["payload"]["nested"][1]["text"], "plain");
}
#[test]
fn values_are_masked_but_credentials_in_keys_or_ids_are_refused() {
    let token = format!("ghp_{}", "A".repeat(36));
    let mut row = json!({"payload":{"command":format!("before {token} after"),"nested":["--secret=x"]},"hostname":token});
    assert!(mask(&mut row).unwrap());
    assert_eq!(
        row["payload"]["command"],
        "before [credential: GitHub token, masked] after"
    );
    for value in [
        json!({token.clone():"x"}),
        json!({"native_id":token.clone()}),
        json!({"page":{"cursor":token.clone()}}),
    ] {
        assert!(mask(&mut value.clone()).is_err());
    }
    let bytes = encode(RpcResponse::success(
        RpcId::String(token.clone()),
        json!({"ok":true}),
    ));
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["id"], Value::Null);
    assert_eq!(value["error"]["kind"], "refused");
    assert!(!String::from_utf8(bytes).unwrap().contains(&token));
}
