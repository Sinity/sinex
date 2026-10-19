use super::*;
use serde_json::json;

fn request() -> Request {
    Request {
        recipe: "day_panel".into(),
        recipe_version: 1,
        params: json!({"metrics": ["shell.commands"], "b": 1, "a": 2.5}),
        methods: vec![],
        plans: vec![Plan {
            relation: "command.fused".into(),
            from: "2026-09-14T02:00:00Z".into(),
            to: "2026-09-16T02:00:00Z".into(),
        }],
        as_of: 42,
        day_setting: "04:00 Europe/Warsaw".into(),
        family: None,
        intent: Intent::Exploratory,
    }
}

#[test]
fn canonical_bytes_sort_keys_and_stringify_numbers() {
    assert_eq!(
        canonical(&json!({"b": 1, "a": [2.5, null, "x"]})),
        r#"{"a":["2.5",null,"x"],"b":"1"}"#
    );
}

#[test]
fn identity_depends_on_content_not_on_key_order() {
    let a = request();
    let mut b = request();
    b.params = json!({"a": 2.5, "b": 1, "metrics": ["shell.commands"]});
    assert_eq!(a.id(), b.id());
    let mut c = request();
    c.as_of = 43;
    assert_ne!(a.id(), c.id());
    // An integer and a float are different values.
    let mut d = request();
    d.params = json!({"metrics": ["shell.commands"], "b": 1.0, "a": 2.5});
    assert_ne!(a.id(), d.id());
}

#[test]
fn readset_digest_ignores_row_order_and_counts_empty_reads() {
    let plan = &request().plans[0];
    let r1 = Readset::new(plan, 7, vec!["b".into(), "a".into()], vec![]);
    let r2 = Readset::new(plan, 7, vec!["a".into(), "b".into()], vec![]);
    assert_eq!(r1, r2);
    let empty = Readset::new(plan, 7, vec![], vec![]);
    assert_eq!(empty.rows, 0);
    assert_ne!(empty.digest, r1.digest);
}
