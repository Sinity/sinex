use super::*;
use command_surface::{Failure, FailureKind};
use serde_json::{Map, Value, json};

fn sample(kind: ArgKind) -> Value {
    match kind {
        ArgKind::Text | ArgKind::Rest => json!("x"),
        ArgKind::Reference => json!("sinex:ev/00000000-0000-7000-8000-000000000000"),
        ArgKind::Integer => json!(5),
        ArgKind::Bool => json!(true),
        ArgKind::List => json!(["x"]),
        ArgKind::Objects => json!([{"act": "note", "target": {"item": "x"}, "text": "x"}]),
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
    let r = Request::Undo {
        operation: "x".into(),
        dry_run: None,
    };
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"method": "undo", "operation": "x", "dry-run": null})
    );
    assert_eq!(r.access(), Access::Agent);
    let tombstone = Request::from_args(
        "tombstone",
        serde_json::from_value(json!({"material": ["m"]})).unwrap(),
    )
    .unwrap();
    assert_eq!(tombstone.access(), Access::Operator);
    assert_eq!(Request::Status.access(), Access::Read);
}

#[devkit::test]
fn missing_and_unknown_arguments_are_refused() {
    let missing = Request::from_args("show", Map::new()).unwrap_err();
    assert_eq!(missing.kind, FailureKind::BadRequest);
    assert!(missing.message.contains("reference"), "{missing:?}");
    let mut extra = Map::new();
    extra.insert("query".into(), json!("tide"));
    extra.insert("bogus".into(), json!(1));
    let unknown = Request::from_args("query", extra).unwrap_err();
    assert_eq!(unknown.kind, FailureKind::BadRequest);
    let nope = Request::from_args("nope", Map::new()).unwrap_err();
    assert_eq!(nope.kind, FailureKind::BadRequest);
    assert!(nope.candidates().iter().any(|name| name == "query"));
}

#[devkit::test]
fn a_failure_reply_carries_the_shared_failure_on_the_wire() {
    let reply = Reply::failed(
        Failure::new(FailureKind::UnknownField, "no field `exit_cod`").with_code("unknown_field"),
    );
    let wire = serde_json::to_value(&reply).unwrap();
    assert_eq!(wire["outcome"], "failed");
    assert_eq!(wire["failure"]["kind"], "unknown_field");
    assert_eq!(serde_json::from_value::<Reply>(wire).unwrap(), reply);
    let stopped = Reply::StoppedEarly {
        failure: stopped_early(Failure::new(FailureKind::Conflict, "a material is held")),
        result: json!({"moved": 1}),
    };
    let wire = serde_json::to_value(&stopped).unwrap();
    assert_eq!(wire["failure"]["kind"], "stopped_early");
    assert_eq!(wire["failure"]["code"], "conflict");
    assert_eq!(serde_json::from_value::<Reply>(wire).unwrap(), stopped);
}

#[devkit::test]
fn duplicate_decisions_are_agent_commands_naming_two_records() {
    for name in ["same", "distinct", "withdraw-decision"] {
        let spec = command(name).unwrap();
        assert_eq!(spec.access, Access::Agent);
        assert!(spec.mcp);
        let mut args = Map::new();
        args.insert("a".into(), json!("sinex:ev/a"));
        assert!(Request::from_args(name, args.clone()).is_err());
        args.insert("b".into(), json!("sinex:ev/b"));
        let request = Request::from_args(name, args).unwrap();
        assert_eq!(request.method(), name);
        assert_eq!(request.access(), Access::Agent);
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
            "save",
            vec![
                "save",
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
            vec!["timeline", "where source = synthetic", "--page-size", "2"],
            "page-size",
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

#[devkit::test]
fn curation_writes_arrive_as_typed_acts_and_a_malformed_write_is_refused() {
    let request = Request::from_args(
        "library-curate",
        serde_json::from_value(json!({"writes": [
            {"act": "tag", "target": {"item": "i"}, "tag": "-"},
            {"act": "set", "target": {"item": "i"}, "property": "medium", "value": null},
        ]}))
        .unwrap(),
    )
    .unwrap();
    let Request::LibraryCurate { writes, request_id } = request else {
        panic!("{request:?}")
    };
    assert_eq!(request_id, None);
    assert!(
        matches!(&writes[0], sinex_model::event_types::library::Act::Tag { tag, .. } if tag == "-")
    );
    assert!(matches!(
        &writes[1],
        sinex_model::event_types::library::Act::Set {
            value: Value::Null,
            ..
        }
    ));
    for bad in [
        json!({"writes": [{"act": "tag", "target": {"item": "i"}}]}),
        json!({"writes": [{"act": "paint", "target": {"item": "i"}}]}),
        json!({"writes": ["not an object"]}),
        json!({"writes": [{"act": "set", "target": {"item": "i"}, "property": "p"}]}),
    ] {
        assert!(
            Request::from_args(
                "library-curate",
                serde_json::from_value(bad.clone()).unwrap()
            )
            .is_err(),
            "{bad}"
        );
    }
}

#[devkit::test]
fn deadlines_keep_absence_none_and_large_milliseconds_distinct() {
    use command_surface::Deadline;
    for (wire, expected) in [
        (json!({"method":"status"}), Deadline::Default),
        (json!({"method":"status","deadline":null}), Deadline::None),
        (
            json!({"method":"status","deadline":0}),
            Deadline::Milliseconds(0),
        ),
        (
            json!({"method":"status","deadline":u64::MAX}),
            Deadline::Milliseconds(u64::MAX),
        ),
    ] {
        let call: Call = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(call.deadline, expected);
        assert_eq!(serde_json::to_value(call).unwrap(), wire);
    }
    for invalid in [json!(-1), json!(1.5), json!("none"), json!(true)] {
        assert!(
            serde_json::from_value::<Call>(json!({"method":"status","deadline":invalid})).is_err()
        );
    }
    assert!(
        serde_json::from_value::<Call>(json!({"method":"status","deadline":null,"unknown":true}))
            .is_err()
    );
    let call = Call::from_args(
        "query",
        serde_json::from_value(json!({"query":"events","deadline":null})).unwrap(),
    )
    .unwrap();
    assert_eq!(call.deadline, Deadline::None);
    assert_eq!(call.request.method(), "query");
    assert_eq!(deadline("none").unwrap(), Deadline::None);
    assert_eq!(
        deadline(&u64::MAX.to_string()).unwrap(),
        Deadline::Milliseconds(u64::MAX)
    );
}

#[devkit::test]
fn every_option_name_has_one_kind() {
    use std::collections::BTreeMap;
    let value_kind = |kind| match kind {
        // Repetition changes cardinality, while each value is still text.
        ArgKind::List => ArgKind::Text,
        other => other,
    };
    let mut kinds = BTreeMap::new();
    for command in COMMANDS {
        for argument in command.args.iter().filter(|argument| !argument.positional) {
            let kind = value_kind(argument.kind);
            if let Some(previous) = kinds.insert(argument.name, kind) {
                assert_eq!(previous, kind, "{} --{}", command.name, argument.name);
            }
        }
    }
    for (name, option, kind) in [
        ("query", "page-size", ArgKind::Integer),
        ("sql", "page-size", ArgKind::Integer),
        ("backup", "page-size", ArgKind::Integer),
        ("comparisons", "page-size", ArgKind::Integer),
        ("comparisons", "cursor", ArgKind::Text),
        ("replay", "kind", ArgKind::Text),
        ("privacy-rule", "kind", ArgKind::Text),
        ("replay", "since", ArgKind::Text),
        ("recompute", "since", ArgKind::Text),
        ("archive", "since", ArgKind::Text),
        ("restore", "until", ArgKind::Text),
        ("tombstone", "dry-run", ArgKind::Bool),
        ("undo", "dry-run", ArgKind::Bool),
        ("replay", "dry-run", ArgKind::Bool),
        ("backup", "to", ArgKind::Text),
        ("library-curate", "request-id", ArgKind::Text),
        ("brief", "bytes", ArgKind::Integer),
        ("library-rank", "next-size", ArgKind::Integer),
        ("replay", "every-change", ArgKind::Bool),
    ] {
        let argument = command(name)
            .unwrap()
            .args
            .iter()
            .find(|argument| argument.name == option)
            .unwrap_or_else(|| panic!("{name} needs --{option}"));
        assert_eq!(value_kind(argument.kind), kind, "{name} --{option}");
    }
}

#[devkit::test]
fn every_listed_command_has_an_example() {
    for command in COMMANDS {
        assert!(
            (1..=3).contains(&command.examples.len()),
            "{}",
            command.name
        );
        for example in command.examples {
            assert!(
                example.starts_with(&format!("sinexctl {}", command.name)),
                "{example}"
            );
        }
    }
}

#[devkit::test]
fn tag_name_matches_the_serde_tag() {
    for command in COMMANDS {
        let arguments = command
            .args
            .iter()
            .map(|argument| (argument.name.to_owned(), sample(argument.kind)))
            .collect();
        let request = Request::from_args(command.name, arguments).unwrap();
        assert_eq!(
            serde_json::to_value(&request).unwrap()["method"],
            request.method()
        );
    }

    #[derive(serde::Serialize, sinex_model::TagName)]
    #[serde(tag = "outcome", rename_all = "snake_case")]
    enum Example {
        AlreadyDone,
        #[serde(rename = "waiting-for-input")]
        Waiting {
            count: usize,
        },
    }
    for (example, expected) in [
        (Example::AlreadyDone, "already_done"),
        (Example::Waiting { count: 3 }, "waiting-for-input"),
    ] {
        assert_eq!(example.outcome(), expected);
        assert_eq!(serde_json::to_value(&example).unwrap()["outcome"], expected);
    }
}
