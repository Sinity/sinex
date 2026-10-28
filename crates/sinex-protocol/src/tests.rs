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

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
fn unadvertised_requests_keep_wire_decoding_and_explicit_write_access() {
    for value in [
        json!({"method":"stage", "path":"/synthetic/input"}),
        json!({"method":"same", "a":"sinex:ev/a", "b":"sinex:ev/b"}),
        json!({"method":"distinct", "a":"sinex:ev/a", "b":"sinex:ev/b"}),
    ] {
        let request: Request = serde_json::from_value(value).unwrap();
        assert!(command(request.method()).is_none());
        assert_eq!(request.access(), Access::Agent);
        assert!(Request::from_args(request.method(), Map::new()).is_err());
    }
}

#[cfg(feature = "cli")]
#[devkit::test]
fn query_authoring_options_are_not_consumed_by_the_text_argument() {
    for (name, args, field, expected) in [
        (
            "mark",
            vec!["mark", "synthetic note", "--at", "2026-09-14T08:00:00Z"],
            "at",
            json!("2026-09-14T08:00:00Z"),
        ),
        (
            "query-save",
            vec![
                "query-save",
                "by-source",
                "events where source = $name",
                "--parameter",
                "name:text",
            ],
            "parameter",
            json!(["name:text"]),
        ),
        (
            "timeline",
            vec!["timeline", "where source = synthetic", "--page", "2"],
            "page",
            json!(2),
        ),
    ] {
        let spec = command(name).unwrap();
        let matches = crate::cli::subcommand(spec)
            .try_get_matches_from(args)
            .unwrap();
        let arguments = crate::cli::arguments(spec, &matches);
        assert_eq!(arguments[field], expected);
        Request::from_args(name, arguments).unwrap();
    }
}
