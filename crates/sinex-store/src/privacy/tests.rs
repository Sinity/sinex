use super::*;
use serde_json::json;
use sinex_model::{
    Event, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp,
    event_types::CommandImported,
};

fn event(command: &str) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            n: 0,
        },
        "terminal.zsh-history",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}

fn rule(field: Option<&str>, matcher: Matcher, class: Class, action: Action) -> Rule {
    Rule {
        id: Uuid::nil(),
        name: "r".into(),
        source: "terminal.zsh-history".into(),
        event_type: Some("command.imported".into()),
        field: field.map(str::to_owned),
        matcher,
        class,
        action,
    }
}

fn set(rules: Vec<Rule>) -> RuleSet {
    RuleSet::new(rules, HashMap::new(), [7; 32])
}

#[test]
fn masks_only_the_matched_span_and_raises_class() {
    let mut e = event("export TOKEN=hunter2 && ls");
    let r = rule(
        Some("/command"),
        Matcher::parse("pattern", Some("TOKEN=\\S+")).unwrap(),
        Class::Guarded,
        Action::Mask,
    );
    let applied = set(vec![r]).apply(&mut e).unwrap();
    assert_eq!(
        applied,
        Applied {
            matched: 1,
            changed: true
        }
    );
    assert_eq!(e.payload["command"], json!("export [masked] && ls"));
    assert_eq!(e.class, Class::Guarded);
}

#[test]
fn hashes_join_without_revealing() {
    let mut a = event("ssh alice@host");
    let mut b = event("ssh alice@host");
    let r = rule(Some("/command"), Matcher::All, Class::Open, Action::Hash);
    let rs = set(vec![r]);
    rs.apply(&mut a).unwrap();
    rs.apply(&mut b).unwrap();
    let h = a.payload["command"].as_str().unwrap();
    assert!(h.starts_with("hash:") && !h.contains("alice"));
    assert_eq!(a.payload, b.payload);
}

#[test]
fn drop_field_removes_it_and_rekeys() {
    let mut e = event("pass show bank");
    let before = e.equivalence_key.clone();
    let r = rule(
        Some("/command"),
        Matcher::parse("terms", Some("pass")).unwrap(),
        Class::Open,
        Action::DropField,
    );
    set(vec![r]).apply(&mut e).unwrap();
    assert!(e.payload.get("command").is_none());
    if let (Some(b), Some(a)) = (before, &e.equivalence_key) {
        assert_ne!(&b, a);
        assert!(a.0.starts_with("hash:"));
    }
}

#[test]
fn terms_are_whole_words_without_case() {
    let m = Matcher::parse("terms", Some("Secret, pass")).unwrap();
    assert_eq!(m.spans("my SECRET plan").len(), 1);
    assert!(m.spans("secretary passport").is_empty());
}

#[test]
fn structural_detectors() {
    let luhn = Matcher::Luhn;
    assert_eq!(luhn.spans("card 4111 1111 1111 1111 ok").len(), 1);
    assert!(luhn.spans("card 4111 1111 1111 1112").is_empty());
    let iban = Matcher::Iban;
    assert_eq!(iban.spans("to GB82 WEST 1234 5698 7654 32 now").len(), 1);
    assert!(iban.spans("GB82 WEST 1234 5698 7654 33").is_empty());
}

#[test]
fn floors_raise_and_other_sources_untouched() {
    let mut e = event("ls");
    let mut floors = HashMap::new();
    floors.insert("terminal.zsh-history".to_string(), Class::Guarded);
    RuleSet::new(vec![], floors, [0; 32]).apply(&mut e).unwrap();
    assert_eq!(e.class, Class::Guarded);
    let mut other = rule(None, Matcher::All, Class::Guarded, Action::Classify);
    other.source = "x".into();
    let mut e = event("ls");
    set(vec![other]).apply(&mut e).unwrap();
    assert_eq!(e.class, Class::Open);
}

#[test]
fn an_unsealed_event_of_a_sealed_class_fails_closed() {
    let mut e = event("ls");
    let r = rule(None, Matcher::All, Class::Sealed, Action::Classify);
    assert!(matches!(
        set(vec![r]).apply(&mut e),
        Err(Error::Unsealed(_))
    ));
    let mut sealed = event("ls");
    let key = crate::sealed::new_key().unwrap();
    crate::sealed::seal_event(&mut sealed, &key).unwrap();
    let mask = rule(None, Matcher::All, Class::Open, Action::Mask);
    let before = sealed.payload.clone();
    set(vec![mask]).apply(&mut sealed).unwrap();
    assert_eq!(
        sealed.payload, before,
        "rules never rewrite a sealed payload"
    );
}
