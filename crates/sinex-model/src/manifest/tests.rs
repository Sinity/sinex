use super::*;
use serde_json::json;

#[devkit::test]
fn known_fields_are_typed_and_the_rest_kept() {
    let value = json!({
        "role": "records",
        "path": "a.csv",
        "previous_through": 12,
        "members": [{"path": "x", "stream_offset": 0, "length": 3, "removed_at_ns": "1"}],
        "host": "h"
    });
    let manifest: Manifest = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(manifest.role, MaterialRole::Records);
    assert_eq!(manifest.previous_through, Some(12));
    assert_eq!(manifest.members[0].details["removed_at_ns"], json!("1"));
    assert_eq!(manifest.detail("host"), &json!("h"));
    assert_eq!(serde_json::to_value(&manifest).unwrap(), value);
    assert_eq!(
        serde_json::to_value(Manifest::default()).unwrap(),
        json!({})
    );
}

#[devkit::test]
fn the_member_at_an_offset() {
    let manifest: Manifest = serde_json::from_value(json!({"members": [
        {"path": "a", "stream_offset": 0, "length": 4},
        {"path": "b", "stream_offset": 4, "length": 0},
        {"path": "c", "stream_offset": 4, "length": 2}
    ]}))
    .unwrap();
    assert_eq!(manifest.member_at(3).unwrap().path, "a");
    assert_eq!(manifest.member_at(5).unwrap().path, "c");
    assert!(manifest.member_at(9).is_none());
}
