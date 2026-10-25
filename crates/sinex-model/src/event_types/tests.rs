use super::*;
use crate::Timestamp;
use crate::{KeyKind, RecordState, ReplaceRule, TimeQuality};
use serde_json::Map;

fn atuin(deleted: bool) -> CommandExecuted {
    CommandExecuted {
        native_id: "01h".into(),
        command: "ls".into(),
        cwd: "/".into(),
        session: "s".into(),
        hostname: "h".into(),
        started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
        duration_ns: None,
        exit_code: Some(-9),
        deleted_at: deleted.then(|| Timestamp::from_unix_secs(1_700_000_100).unwrap()),
        author: None,
        intent: None,
        shell: None,
        extra: Map::new(),
    }
}

#[devkit::test]
fn derive_declares_identity_and_time() {
    assert_eq!(CommandExecuted::EVENT_TYPE, "command.executed");
    assert_eq!(CommandExecuted::KEY, KeyKind::Fields(&["native_id"]));
    assert_eq!(CommandExecuted::REPLACE, ReplaceRule::ReplaceWhenNewer);
    assert_eq!(
        CommandImported::KEY,
        KeyKind::Fields(&["hostname", "started_at", "command", "n"])
    );
    assert_eq!(machine::FileChanged::KEY, KeyKind::Location);
    assert_eq!(CommandImported::REPLACE, ReplaceRule::SuppressRepeat);
    let a = atuin(false);
    assert_eq!(a.key_kind(), CommandExecuted::KEY);
    assert_eq!(a.ts_orig().quality, TimeQuality::Intrinsic);
    assert_eq!(a.ts_orig().at, Some(a.started_at));
    assert_eq!(a.record_state(), RecordState::Present);
    assert_eq!(atuin(true).record_state(), RecordState::Removed);
}

#[devkit::test]
fn json_schema_requires_declared_fields() {
    let schema = CommandImported::payload_schema();
    let required = schema["required"].as_array().unwrap();
    assert!(required.iter().any(|f| f == "command"));
    assert!(!required.iter().any(|f| f == "started_at"));
}

#[devkit::test]
fn json_payloads_build_through_the_rust_type() {
    let kind = find("command.executed").unwrap();
    let origin = || crate::Origin::Record {
        location: crate::RecordLocation {
            material: crate::MaterialId::new(),
            anchor: 0,
        },
        ordinal: 0,
    };
    let payload = serde_json::to_value(atuin(true)).unwrap();
    let event = (kind.build)(
        &payload,
        "producer:p",
        origin(),
        crate::ReadingOrder(1),
        [0; 32],
        crate::Class::Open,
    )
    .unwrap();
    assert_eq!(event.event_type, "command.executed");
    assert_eq!(event.record_state, RecordState::Removed);
    assert!(event.equivalence_key.is_some());
    let bad = serde_json::json!({"command": "ls"});
    assert!(matches!(
        (kind.build)(
            &bad,
            "producer:p",
            origin(),
            crate::ReadingOrder(1),
            [0; 32],
            crate::Class::Open
        ),
        Err(crate::BuildError::Payload("command.executed", _))
    ));
    assert!(find("no.such.type").is_none());
}

#[devkit::test]
fn catalog_lists_each_type_once() {
    let mut names: Vec<_> = ALL.iter().map(|k| k.event_type).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), ALL.len());
}
