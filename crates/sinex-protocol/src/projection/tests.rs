use super::*;
#[devkit::test]
fn record_hash_uses_common_canonical_bytes() {
    let value:Value=serde_json::from_str(r#"{"𝄞":{"z":null,"a":true},"negative_zero":-0.0,"unicode":"é𝄞","large":18446744073709551615,"float":1.25,"escapes":"\n\\\"","array":[3,{"z":2,"a":1}],"":false}"#).unwrap();
    let golden = r#"{"array":[3,{"a":1,"z":2}],"escapes":"\n\\\"","float":1.25,"large":18446744073709551615,"negative_zero":-0.0,"unicode":"é𝄞","":false,"𝄞":{"a":true,"z":null}}"#;
    assert_eq!(
        String::from_utf8(canonical_json::canonical_json(&value).unwrap()).unwrap(),
        golden
    );
    let identity = ProjectionIdentity::for_payload(&value);
    assert_eq!(
        identity.payload_hash,
        blake3::hash(golden.as_bytes()).to_hex().to_string()
    );
    assert!(identity.verifies(&value));
    assert!(!identity.verifies(&serde_json::json!({"different":"view"})));
}
