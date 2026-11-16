use super::JsonExt;
use serde_json::{Value, json};

#[test]
fn json_fields_keep_types_and_unknown_values() {
    let value = json!({"name": "track", "negative": -2, "count": 4, "fraction": 1.5,
        "flag": true, "rows": [1, 2], "nested": {"key": "value"}, "unknown": null});
    let object = value.as_object().unwrap();
    for fields in [&value as &dyn JsonExt, object as &dyn JsonExt] {
        assert_eq!(fields.str("name"), Some("track"));
        assert_eq!(fields.i64("negative"), Some(-2));
        assert_eq!(fields.u64("negative"), None);
        assert_eq!(fields.u64("count"), Some(4));
        assert_eq!(fields.f64("fraction"), Some(1.5));
        assert_eq!(fields.bool("flag"), Some(true));
        assert_eq!(fields.array("rows"), Some(&vec![json!(1), json!(2)]));
        assert_eq!(fields.object("nested"), json!({"key": "value"}).as_object());
        assert_eq!(fields.str("count"), None);
        assert_eq!(fields.u64("unknown"), None);
        assert_eq!(fields.str("missing"), None);
    }
    assert_eq!(Value::Null.str("name"), None);
    assert_eq!(json!(["name"]).str("name"), None);
}
