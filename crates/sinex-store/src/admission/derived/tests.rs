use super::DerivedScope;
use serde_json::json;

#[devkit::test]
fn computed_outputs_require_the_read_mark() {
    for mut scope in [
        json!({"keys": {"keys": [], "read": {"changes": 0, "excised": 0}}}),
        json!({"position": {"inputs": [], "position": 0, "read": {"changes": 0, "excised": 0}}}),
    ] {
        let parsed: DerivedScope = serde_json::from_value(scope.clone()).unwrap();
        assert_eq!(parsed.to_value(), scope);

        scope
            .as_object_mut()
            .unwrap()
            .values_mut()
            .next()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("read");
        assert!(serde_json::from_value::<DerivedScope>(scope).is_err());
    }

    assert!(serde_json::from_value::<DerivedScope>(json!({"recompute": {"keys": []}})).is_ok());
}
