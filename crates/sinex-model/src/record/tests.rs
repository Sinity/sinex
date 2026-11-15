use super::*;
use serde_json::json;
#[test]
fn observation_keys_keep_the_existing_captured_identity_and_tuple_encoding() {
    let location = Some(("material".into(), 7, 2));
    assert_eq!(
        stable_from_parts(
            "t",
            "s",
            &json!({"native_id":"n"}),
            location.clone(),
            Some("k"),
            "id"
        ),
        "[\"s\",\"t\",\"k\"]"
    );
    assert_eq!(
        stable_from_parts(
            "t",
            "s",
            &json!({"native_id":"n"}),
            location.clone(),
            None,
            "id"
        ),
        "[\"s\",\"t\",\"t:n\"]"
    );
    assert_eq!(
        stable_from_parts("t", "s", &json!({}), location, None, "id"),
        "[\"s\",\"t\",\"material:7:2\"]"
    );
    assert_eq!(
        stable_from_parts("t", "s", &json!({}), None, None, "id"),
        "[\"s\",\"t\",\"id\"]"
    );
    assert_eq!(
        citation("t", "s", &json!({"native_id":"n"}), None, Some("k"), "id"),
        "t:n"
    );
}
