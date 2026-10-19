use super::*;
use serde_json::{Map, Value, json};

fn sample(kind: ArgKind) -> Value {
    match kind {
        ArgKind::Text | ArgKind::Rest => json!("x"),
        ArgKind::Reference => json!("sinex:ev/00000000-0000-7000-8000-000000000000"),
        ArgKind::Integer => json!(5),
        ArgKind::Bool => json!(true),
        ArgKind::List => json!(["x"]),
    }
}

#[test]
fn the_table_and_the_requests_agree() {
    for c in COMMANDS {
        let all: Map<String, Value> = c
            .args
            .iter()
            .map(|a| (a.name.to_string(), sample(a.kind)))
            .collect();
        let r = Request::from_args(c.name, all).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(r.method(), c.name);
        assert_eq!(r.access(), c.access);
        let required: Map<String, Value> = c
            .args
            .iter()
            .filter(|a| a.required)
            .map(|a| (a.name.to_string(), sample(a.kind)))
            .collect();
        Request::from_args(c.name, required).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn requests_have_stable_wire_names() {
    let r = Request::Excise {
        reference: Some("sinex:ev/x".into()),
        material: None,
        dry_run: None,
    };
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"method": "excise", "reference": "sinex:ev/x"})
    );
    assert_eq!(r.access(), Access::Operator);
    assert_eq!(Request::Status.access(), Access::Read);
}

#[test]
fn missing_and_unknown_arguments_are_refused() {
    assert!(
        Request::from_args("show", Map::new())
            .unwrap_err()
            .contains("reference")
    );
    let mut extra = Map::new();
    extra.insert("query".into(), json!("tide"));
    extra.insert("bogus".into(), json!(1));
    assert!(Request::from_args("query", extra).is_err());
    assert!(
        Request::from_args("nope", Map::new())
            .unwrap_err()
            .contains("commands:")
    );
}
