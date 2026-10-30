use super::*;
#[devkit::test]
fn sjson1_hashes_sorted_nested_objects_and_numeric_unicode_values_without_source_hash_claims() {
    let value:Value=serde_json::from_str(r#"{"𝄞":{"z":null,"a":true},"negative_zero":-0.0,"unicode":"é𝄞","large":18446744073709551615,"float":1.25,"escapes":"\n\\\"","array":[3,{"z":2,"a":1}],"":false}"#).unwrap();
    let golden = r#"{"array":[3,{"a":1,"z":2}],"escapes":"\n\\\"","float":1.25,"large":18446744073709551615,"negative_zero":-0.0,"unicode":"é𝄞","":false,"𝄞":{"a":true,"z":null}}"#;
    assert_eq!(
        String::from_utf8(sinex_model::json_identity::bytes(&value)).unwrap(),
        golden
    );
    let identity = ProjectionIdentity::for_payload(42, &value);
    assert_eq!(
        identity.payload_hash,
        blake3::hash(golden.as_bytes()).to_hex().to_string()
    );
    assert_eq!(identity.policy_epoch, 42);
    assert!(identity.verifies(&value));
    assert!(!identity.verifies(&serde_json::json!({"different":"view"})));
}
