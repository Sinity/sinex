use super::*;
use serde_json::json;

#[devkit::test]
fn key_depends_on_type_fields_and_order() {
    let p = json!({"a": 1, "b": "x", "c": true});
    let k = EquivalenceKey::of("t", &["a", "b"], &p);
    assert_eq!(
        k,
        EquivalenceKey::of("t", &["a", "b"], &json!({"b": "x", "a": 1, "c": false}))
    );
    assert_ne!(k, EquivalenceKey::of("u", &["a", "b"], &p));
    assert_ne!(k, EquivalenceKey::of("t", &["b", "a"], &p));
    assert_eq!(k.0.len(), 64);
}

#[devkit::test]
fn missing_key_field_is_null() {
    assert_eq!(
        EquivalenceKey::of("t", &["a"], &json!({})),
        EquivalenceKey::of("t", &["a"], &json!({"a": null}))
    );
}

#[devkit::test]
fn check_lists_variants() {
    assert_eq!(
        ReplaceRule::check("rule"),
        "CHECK (rule IN ('replace_when_newer', 'suppress_repeat'))"
    );
    assert_eq!(ReplaceRule::SuppressRepeat.as_db(), "suppress_repeat");
}
