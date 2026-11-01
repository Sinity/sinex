use super::*;

#[devkit::test]
fn sjson1_keeps_the_versioned_numeric_unicode_and_nested_object_bytes() {
    let value:Value=serde_json::from_str(r#"{"𝄞":{"z":null,"a":true},"negative_zero":-0.0,"unicode":"é𝄞","large":18446744073709551615,"float":1.25,"escapes":"\n\\\"","array":[3,{"z":2,"a":1}],"":false}"#).unwrap();
    let golden = r#"{"array":[3,{"a":1,"z":2}],"escapes":"\n\\\"","float":1.25,"large":18446744073709551615,"negative_zero":-0.0,"unicode":"é𝄞","":false,"𝄞":{"a":true,"z":null}}"#;
    assert_eq!(bytes(&value), golden.as_bytes());
    assert_eq!(
        hash(&value),
        blake3::hash(golden.as_bytes()).to_hex().to_string()
    );
    let mut changed = value.clone();
    changed["array"] = serde_json::json!([{"a":1,"z":2},3]);
    assert_ne!(hash(&value), hash(&changed));
}
