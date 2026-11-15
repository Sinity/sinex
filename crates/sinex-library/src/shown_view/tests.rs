use super::*;
use sinex_model::Timestamp;
use sinex_model::event_types::library::{CurationKind, Target, ValueOrigin};

fn record(rev: &str, kind: CurationKind, key: &str, value: Option<Value>) -> Curation {
    Curation {
        root: rev.into(),
        rev: rev.into(),
        revises: vec![],
        withdraws: vec![],
        kind,
        target: Some(Target {
            item: "i".into(),
            ..Target::default()
        }),
        key: Some(key.into()),
        value,
        origin: ValueOrigin::Authored,
        cites: None,
        author: "operator".into(),
        via: "test".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        request: rev.into(),
        request_digest: String::new(),
        slots: vec![],
    }
}

#[devkit::test]
fn one_row_per_tag_and_property_with_its_state() {
    let values = shown::values(&[
        record("1", CurationKind::Tag, "cat", None),
        record("2", CurationKind::Property, "medium", Some(json!("ink"))),
        record("3", CurationKind::Property, "empty", Some(Value::Null)),
        record(
            "4",
            CurationKind::Note,
            "",
            Some(json!("not a shown value")),
        ),
    ]);
    let rows = rows(&values);
    let found: Vec<(&str, &str, &str, Option<&Value>)> = rows
        .iter()
        .map(|r| {
            (
                r.kind.as_str(),
                r.key.as_str(),
                r.state.as_str(),
                r.value.as_ref(),
            )
        })
        .collect();
    assert_eq!(
        found,
        [
            ("tag", "cat", "applies", None),
            ("property", "empty", "empty", None),
            ("property", "medium", "value", Some(&json!("ink"))),
        ]
    );
    assert!(rows.iter().all(|r| r.item == "i" && r.level == "operator"));
}
