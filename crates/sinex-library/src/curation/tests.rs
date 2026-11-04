use super::*;
use serde_json::{Value, json};
use sinex_model::Timestamp;

fn target(item: &str) -> Target {
    Target {
        item: item.into(),
        ..Target::default()
    }
}

fn statement<'a>(
    kind: CurationKind,
    target: Option<&'a Target>,
    key: Option<&'a str>,
    value: Option<&'a Value>,
    author: &'a str,
    origin: ValueOrigin,
    cites: Option<&'a str>,
) -> Vec<String> {
    Statement {
        kind,
        target,
        key,
        value,
        author,
        origin,
        cites,
    }
    .slots()
    .iter()
    .map(slot_digest)
    .collect()
}

#[devkit::test]
fn a_tag_and_its_rejection_hold_different_slots_and_each_author_its_own() {
    let t = target("i");
    let rejected = Value::Bool(false);
    let apply = statement(
        CurationKind::Tag,
        Some(&t),
        Some("cat"),
        None,
        OPERATOR,
        ValueOrigin::Authored,
        None,
    );
    let reject = statement(
        CurationKind::Tag,
        Some(&t),
        Some("cat"),
        Some(&rejected),
        OPERATOR,
        ValueOrigin::Authored,
        None,
    );
    let agent = statement(
        CurationKind::Tag,
        Some(&t),
        Some("cat"),
        None,
        "agent",
        ValueOrigin::Authored,
        None,
    );
    assert_eq!(apply.len(), 1);
    assert_ne!(apply, reject);
    assert_ne!(apply, agent);
    assert_eq!(
        uniqueness(CurationKind::Tag).unwrap().when_held,
        WhenHeld::Already
    );
}

#[devkit::test]
fn each_imported_source_keeps_its_own_support() {
    let t = target("i");
    let value = json!("x");
    let from = |cites| {
        statement(
            CurationKind::Property,
            Some(&t),
            Some("medium"),
            Some(&value),
            OPERATOR,
            ValueOrigin::Imported,
            Some(cites),
        )
    };
    assert_ne!(from("sidecar-a"), from("sidecar-b"));
    assert_eq!(from("sidecar-a"), from("sidecar-a"));
    // A property's slot ignores its value, so a second value revises.
    let other = json!("y");
    assert_eq!(
        statement(
            CurationKind::Property,
            Some(&t),
            Some("medium"),
            Some(&other),
            OPERATOR,
            ValueOrigin::Imported,
            Some("sidecar-a"),
        ),
        from("sidecar-a")
    );
}

#[devkit::test]
fn names_are_one_slot_whoever_writes_them_and_entries_are_many() {
    let by = |author| {
        statement(
            CurationKind::Collection,
            None,
            Some("reading"),
            None,
            author,
            ValueOrigin::Authored,
            None,
        )
    };
    assert_eq!(by(OPERATOR), by("agent"));
    for many in [
        CurationKind::Note,
        CurationKind::Comparison,
        CurationKind::Entry,
        CurationKind::Withdrawal,
    ] {
        assert!(uniqueness(many).is_none(), "{many:?}");
    }
}

#[devkit::test]
fn a_stored_record_gives_back_its_header() {
    let record = Curation {
        root: "a".into(),
        rev: "b.2".into(),
        revises: vec!["a@a".into()],
        withdraws: vec!["c".into(), "d@d.1".into()],
        kind: CurationKind::Property,
        target: Some(target("i")),
        key: Some("medium".into()),
        value: Some(json!("x")),
        origin: ValueOrigin::Authored,
        cites: None,
        author: OPERATOR.into(),
        via: "api".into(),
        at: Timestamp::from_unix_secs(2).unwrap(),
        request: "b".into(),
        request_digest: String::new(),
        slots: vec![],
    };
    let header = header(&record).unwrap();
    assert_eq!(header.revision(), Revision::new("a", "b.2"));
    assert_eq!(header.at, 2_000);
    assert!(header.revises.contains(&Revision::new("a", "a")));
    assert!(header.withdraws.contains(&Withdrawn::Root("c".into())));
    assert!(
        header
            .withdraws
            .contains(&Withdrawn::Revision(Revision::new("d", "d.1")))
    );
    let mut broken = record;
    broken.revises = vec!["no-revision".into()];
    assert!(matches!(header_of(&broken), Err(Error::Identity(..))));
}

fn header_of(record: &Curation) -> Result<Header> {
    header(record)
}
