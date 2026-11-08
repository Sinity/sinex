use super::*;
use serde_json::json;
use sinex_model::Timestamp;

fn record(
    rev: &str,
    kind: CurationKind,
    key: &str,
    value: Option<Value>,
    author: &str,
    origin: ValueOrigin,
) -> Curation {
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
        origin,
        cites: (origin != ValueOrigin::Authored).then(|| "sidecar".into()),
        author: author.into(),
        via: "test".into(),
        at: Timestamp::from_unix_secs(10).unwrap(),
        request: rev.into(),
        request_digest: String::new(),
        slots: vec![],
    }
}

fn prop(rev: &str, value: Value, author: &str, origin: ValueOrigin) -> Curation {
    record(
        rev,
        CurationKind::Property,
        "f",
        Some(value),
        author,
        origin,
    )
}

#[devkit::test]
fn operator_beats_agent_beats_import_beats_proposal() {
    let proposed = prop("1", json!("s"), "model", ValueOrigin::Proposed);
    let imported = prop("2", json!("q"), "operator", ValueOrigin::Imported);
    let agent = prop("3", json!("a"), "agent", ValueOrigin::Authored);
    let operator = prop("4", json!("o"), "operator", ValueOrigin::Authored);
    let shown = |records: &[&Curation]| match property(records) {
        Shown::Value { value, level, .. } => (value, level),
        other => panic!("{other:?}"),
    };
    assert_eq!(shown(&[&proposed]), (json!("s"), Level::Proposed));
    assert_eq!(
        shown(&[&proposed, &imported]),
        (json!("q"), Level::Imported)
    );
    assert_eq!(
        shown(&[&proposed, &imported, &agent]),
        (json!("a"), Level::Agent)
    );
    assert_eq!(
        shown(&[&proposed, &imported, &agent, &operator]),
        (json!("o"), Level::Operator)
    );
    assert_eq!(property(&[]), Shown::Unknown);
}

#[devkit::test]
fn an_explicit_empty_stops_fallback_and_differs_from_an_empty_string() {
    let imported = prop("1", json!("q"), "operator", ValueOrigin::Imported);
    let empty = prop("2", Value::Null, "operator", ValueOrigin::Authored);
    assert!(matches!(
        property(&[&imported, &empty]),
        Shown::Empty {
            level: Level::Operator,
            ..
        }
    ));
    let blank = prop("3", json!(""), "operator", ValueOrigin::Authored);
    assert!(matches!(
        property(&[&imported, &blank]),
        Shown::Value { ref value, .. } if value == &json!("")
    ));
}

#[devkit::test]
fn two_values_at_one_level_conflict_with_every_supporter() {
    let shown = property(&[
        &prop("1", json!("x"), "operator", ValueOrigin::Imported),
        &prop("2", json!("y"), "operator", ValueOrigin::Imported),
        &prop("3", json!("x"), "operator", ValueOrigin::Imported),
    ]);
    let Shown::Conflict { level, values } = shown else {
        panic!("{shown:?}")
    };
    assert_eq!(level, Level::Imported);
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].1.len(), 2, "x has two supporters");
    // The same value twice is not a conflict.
    assert!(matches!(
        property(&[
            &prop("1", json!("x"), "operator", ValueOrigin::Imported),
            &prop("2", json!("x"), "operator", ValueOrigin::Imported),
        ]),
        Shown::Value { .. }
    ));
}

fn tag(rev: &str, name: &str, rejected: bool, author: &str, origin: ValueOrigin) -> Curation {
    record(
        rev,
        CurationKind::Tag,
        name,
        rejected.then_some(Value::Bool(false)),
        author,
        origin,
    )
}

#[devkit::test]
fn a_tag_applies_while_its_deciding_level_applies_it() {
    let imported = tag("1", "cat", false, "operator", ValueOrigin::Imported);
    let operator = tag("2", "cat", false, "operator", ValueOrigin::Authored);
    let shown = tags(&[&imported, &operator]);
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0].state, TagState::Applies);
    assert_eq!(shown[0].level, Level::Operator);
    assert_eq!(shown[0].applied_by.len(), 2, "every supporter is listed");
    // Without the operator's record the import still supports it.
    assert_eq!(tags(&[&imported])[0].state, TagState::Applies);
    // The operator's rejection outranks the import.
    let rejection = tag("3", "cat", true, "operator", ValueOrigin::Authored);
    let shown = tags(&[&imported, &rejection]);
    assert_eq!(shown[0].state, TagState::Rejected);
    assert_eq!(shown[0].rejected_by.len(), 1);
    // Two agents disagreeing at one level is a conflict.
    let shown = tags(&[
        &tag("4", "dog", false, "agent", ValueOrigin::Authored),
        &tag("5", "dog", true, "agent:other", ValueOrigin::Authored),
    ]);
    assert_eq!(shown[0].state, TagState::Conflict);
}

#[devkit::test]
fn labels_of_punctuation_are_ordinary_tags() {
    for label in ["-", "!x", "-cat", "?"] {
        let shown = tags(&[&tag("1", label, false, "operator", ValueOrigin::Authored)]);
        assert_eq!(shown[0].tag, label);
        assert_eq!(shown[0].state, TagState::Applies);
    }
}
