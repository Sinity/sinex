use super::*;
use serde_json::json;
use sinex_model::event_types::CommandImported;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};

fn applier(rules: Value) -> TagApplier {
    TagApplier::new(serde_json::from_value(rules).unwrap()).unwrap()
}
fn event() -> Event {
    Event::build(
        &CommandImported {
            command: "echo hello".into(),
            hostname: "test".into(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "terminal.zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap()
}
fn read(applier: &TagApplier, event: &Event) -> Vec<Event> {
    applier
        .derive(Input { event, bytes: None }, &Env::at(Timestamp::now()))
        .unwrap()
}
fn rules() -> Value {
    json!([
        {"tag": "terminal", "event_type": "command.imported"},
        {"tag": "echo", "event_type": "command.imported", "when": [{"field": "/command", "starts_with": "ECHO "}]},
        {"tag": "echo", "event_type": "command.imported", "when": [{"field": "/command", "contains": "hello"}]}
    ])
}

#[devkit::test]
fn all_matches_are_attributed_and_unknown_time_and_class_are_preserved() {
    let applier = applier(rules());
    let event = event();
    let tags = read(&applier, &event);
    let names: Vec<_> = tags
        .iter()
        .map(|tag| tag.payload["tag"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["echo", "terminal"]);
    assert_eq!(tags[0].payload["rules"].as_array().unwrap().len(), 2);
    for tag in tags {
        assert_eq!(tag.class, event.class);
        assert_eq!(tag.ts_orig, event.ts_orig);
        assert_eq!(
            tag.origin,
            Origin::Derived {
                parents: vec![event.id]
            }
        );
    }
}

#[devkit::test]
fn rule_order_repeats_and_replay_keep_the_tag_identity() {
    let input = event();
    let first = read(&applier(rules()), &input);
    let mut reversed = rules().as_array().unwrap().clone();
    reversed.reverse();
    let reordered = applier(json!(reversed));
    assert_eq!(reordered.version(), applier(rules()).version());
    let same = read(&reordered, &input);
    assert_eq!(
        first
            .iter()
            .map(|e| (&e.payload, &e.record_hash))
            .collect::<Vec<_>>(),
        same.iter()
            .map(|e| (&e.payload, &e.record_hash))
            .collect::<Vec<_>>()
    );
    let mut replay = input;
    replay.id = sinex_model::EventId::new();
    let again = read(&reordered, &replay);
    for (before, after) in first.iter().zip(&again) {
        assert_eq!(before.payload, after.payload);
        assert_eq!(before.equivalence_key, after.equivalence_key);
        assert_ne!(before.record_hash, after.record_hash);
    }
}

#[devkit::test]
fn conditions_compare_literals_and_missing_values() {
    let tests: Vec<Condition> = serde_json::from_value(json!([
        {"field": "/app", "is": "kitty"},
        {"field": "/title", "contains": "PROJECT"},
        {"field": "/title", "starts_with": "editor:"},
        {"field": "/title", "ends_with": ".RS"},
        {"field": "/missing", "exists": false},
        {"field": "/null", "exists": false},
        {"field": "/app", "exists": true}
    ]))
    .unwrap();
    let payload = json!({"app": "kitty", "title": "Editor: project/main.rs", "null": null});
    assert!(tests.iter().all(|condition| condition.holds(&payload)));
    assert!(!tests[0].holds(&json!({"app": "Kitty"})));
    assert!(!tests[1].holds(&json!({"title": 2})));
    assert!(!tests[2].holds(&json!({})));
}

#[devkit::test]
fn invalid_rules_are_refused_and_empty_rules_are_valid() {
    assert!(read(&applier(json!([])), &event()).is_empty());
    for rule in [
        json!({"tag": " ", "event_type": "command.imported"}),
        json!({"tag": "x", "event_type": "unknown"}),
        json!({"tag": "x", "event_type": "tag.applied"}),
        json!({"tag": "x", "event_type": "sinex.telemetry"}),
        json!({"tag": "x", "event_type": "command.imported", "when": [{"field": "command", "exists": true}]}),
        json!({"tag": "x", "event_type": "command.imported", "when": [{"field": "/a~2b", "exists": true}]}),
    ] {
        let rules = serde_json::from_value(json!([rule])).unwrap();
        assert!(TagApplier::new(rules).is_err());
    }
    assert!(valid_pointer(""));
    assert!(valid_pointer("/a~0b/~1"));
}

#[devkit::test]
fn declared_file_extension_and_mime_rules_keep_the_ported_cases() {
    let rules = applier(json!([
        {"tag": "inferred.file-type.rust", "event_type": "command.imported", "when": [{"field": "/path", "ends_with": ".rs"}]},
        {"tag": "sys.mime.text-markdown", "event_type": "command.imported", "when": [{"field": "/media_type", "is": "text/markdown"}]}
    ]));
    let mut input = event();
    input.payload["path"] = json!("/p/main.rs");
    input.payload["media_type"] = json!("text/markdown");
    assert_eq!(read(&rules, &input).len(), 2);
    input.payload["path"] = json!("/p/file.xyz");
    input.payload["media_type"] = json!("image/png");
    assert!(read(&rules, &input).is_empty());
}
