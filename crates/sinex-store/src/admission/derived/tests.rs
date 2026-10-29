use super::DerivedScope;
use serde_json::json;

#[devkit::test]
fn computed_outputs_require_the_read_mark() {
    for mut scope in [
        json!({"keys": {"keys": [], "read": {"changes": 0, "excised": 0, "policy_epoch": null}}}),
        json!({"position": {"inputs": [], "position": 0, "read": {"changes": 0, "excised": 0, "policy_epoch": null}}}),
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
    let legacy:DerivedScope=serde_json::from_value(json!({"keys":{"keys":[],"read":{"changes":0,"excised":0}}})).unwrap();
    assert!(matches!(legacy,DerivedScope::Keys{read,..} if read.policy_epoch.is_none()),"historical marks never acquire an invented policy epoch");
}
