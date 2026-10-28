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
        output_index: 0,
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
        Err(crate::BuildError::Payload(ref name, _)) if name == "command.executed"
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

#[devkit::test]
fn decoder_errors_name_fields_without_echoing_payload_values() {
    let kind = find("command.executed").unwrap();
    let check = |payload: Value| {
        (kind.build)(
            &payload,
            "test",
            crate::Origin::Record {
                location: crate::RecordLocation {
                    material: crate::MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            crate::ReadingOrder(0),
            [0; 32],
            crate::Class::Open,
        )
        .unwrap_err()
        .to_string()
    };
    let mut value = serde_json::to_value(atuin(false)).unwrap();
    value["exit_code"] = serde_json::json!("synthetic-private-value");
    let error = check(value);
    assert!(error.contains("decode at /exit_code"));
    assert!(!error.contains("synthetic-private-value"));
    let mut missing = serde_json::to_value(atuin(false)).unwrap();
    missing.as_object_mut().unwrap().remove("hostname");
    assert!(check(missing).contains("required at /hostname"));
}

#[devkit::test]
fn search_policy_keeps_full_text_for_high_volume_kinds() {
    let excluded: Vec<_> = ALL
        .iter()
        .filter(|kind| !kind.trigram_indexed)
        .map(|kind| kind.event_type)
        .collect();
    assert_eq!(excluded, ["journal.entry", "lake.observed"]);
    assert!(find("command.imported").unwrap().trigram_indexed);
}

#[devkit::test]
fn current_declarations_reserve_version_one_for_historical_readings() {
    for kind in ALL {
        let declaration = (kind.declaration)();
        assert!(declaration.kind.version >= 2, "{}", declaration.kind.name);
    }
}
