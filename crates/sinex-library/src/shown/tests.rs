use super::*;
use sinex_model::Timestamp;
use sinex_model::event_types::library::Target;

fn a(id: &str, value: Option<&str>, level: Level, withdrawn: bool) -> Assignment {
    Assignment {
        assignment: id.into(),
        target: Target {
            item: "i".into(),
            ..Target::default()
        },
        field: "f".into(),
        value: value.map(str::to_owned),
        level,
        source_ref: None,
        by: "o".into(),
        at: Timestamp::from_unix_secs(10).unwrap(),
        withdrawn_at: withdrawn.then(|| Timestamp::from_unix_secs(20).unwrap()),
    }
}

#[test]
fn override_beats_import_beats_proposal() {
    let imported = a("1", Some("q"), Level::Import, false);
    let proposed = a("2", Some("s"), Level::Proposal, false);
    assert_eq!(
        property(std::slice::from_ref(&proposed)),
        Shown::Value {
            value: "s".into(),
            by: "2".into(),
            level: Level::Proposal
        }
    );
    assert!(matches!(
        property(&[proposed.clone(), imported.clone()]),
        Shown::Value { ref value, .. } if value == "q"
    ));
    let empty = a("3", None, Level::Override, false);
    assert_eq!(
        property(&[proposed.clone(), imported.clone(), empty]),
        Shown::Empty { by: "3".into() }
    );
    // Withdrawing the override falls back again.
    let withdrawn = a("3", None, Level::Override, true);
    assert!(matches!(
        property(&[proposed, imported, withdrawn]),
        Shown::Value {
            level: Level::Import,
            ..
        }
    ));
    assert_eq!(property(&[]), Shown::Unknown);
}

#[test]
fn two_values_at_one_level_conflict() {
    let s = property(&[
        a("1", Some("x"), Level::Import, false),
        a("2", Some("y"), Level::Import, false),
    ]);
    assert!(matches!(s, Shown::Conflict { level: Level::Import, ref values } if values.len() == 2));
    // The same value twice is not a conflict.
    assert!(matches!(
        property(&[
            a("1", Some("x"), Level::Import, false),
            a("2", Some("x"), Level::Import, false)
        ]),
        Shown::Value { .. }
    ));
}

#[test]
fn a_tag_applies_while_any_live_assignment_supports_it() {
    let both = [
        a("1", Some("cat"), Level::Import, false),
        a("2", Some("cat"), Level::Override, false),
        a("3", Some("dog"), Level::Proposal, true),
    ];
    let t = tags(&both);
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].level, Level::Override);
    assert_eq!(t[0].supported_by.len(), 2);
    let cleared = [
        a("1", Some("cat"), Level::Import, false),
        a("4", None, Level::Override, false),
    ];
    assert!(tags(&cleared).is_empty());
    let denied = [
        a("1", Some("cat"), Level::Import, false),
        a("5", Some("dog"), Level::Import, false),
        a("6", Some("-cat"), Level::Override, false),
    ];
    let t = tags(&denied);
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].tag, "dog");
}
