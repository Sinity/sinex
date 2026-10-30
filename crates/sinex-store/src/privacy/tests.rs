use super::*;
use serde_json::json;
use sinex_model::{
    Event, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp,
    event_types::CommandImported,
};

#[devkit::test]
async fn effective_rule_reads_use_read_only_snapshots_without_initializing_hash_material(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.settings")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    add_rule(
        &mut conn,
        &NewRule {
            name: "read-only-rule".into(),
            source: "synthetic".into(),
            event_type: None,
            field: Some("/command".into()),
            matcher: "all".into(),
            matcher_arg: None,
            class: Class::Guarded,
            action: Action::Mask,
            author: "synthetic-test".into(),
        },
    )
    .await
    .unwrap();
    let mut read = conn.begin().await.unwrap();
    crate::query::read_one_snapshot(&mut read).await.unwrap();
    assert_eq!(
        active_rules(&mut read, &["synthetic".into()])
            .await
            .unwrap()
            .len(),
        1
    );
    read.rollback().await.unwrap();
    end_rule(&mut conn, "read-only-rule", "synthetic-test")
        .await
        .unwrap();
    let mut read = conn.begin().await.unwrap();
    crate::query::read_one_snapshot(&mut read).await.unwrap();
    assert!(
        active_rules(&mut read, &["synthetic".into()])
            .await
            .unwrap()
            .is_empty()
    );
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.settings")
        .fetch_one(&mut *read)
        .await
        .unwrap();
    assert_eq!(after, before);
    read.rollback().await.unwrap();
}

fn event(command: &str) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "terminal.zsh-history",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
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

#[devkit::test]
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
            changed: true,
            trace: vec![AffectedPath {
                path: "/command".into(),
                action: Action::Mask,
                rule: Uuid::nil()
            }]
        }
    );
    assert_eq!(e.payload["command"], json!("export [masked] && ls"));
    assert_eq!(e.class, Class::Guarded);
}

#[devkit::test]
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

#[devkit::test]
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

#[devkit::test]
fn terms_are_whole_words_without_case() {
    let m = Matcher::parse("terms", Some("Secret, pass")).unwrap();
    assert_eq!(m.spans("my SECRET plan").len(), 1);
    assert!(m.spans("secretary passport").is_empty());
}

#[devkit::test]
fn structural_detectors() {
    let luhn = Matcher::Luhn;
    assert_eq!(luhn.spans("card 4111 1111 1111 1111 ok").len(), 1);
    assert!(luhn.spans("card 4111 1111 1111 1112").is_empty());
    let iban = Matcher::Iban;
    assert_eq!(iban.spans("to GB82 WEST 1234 5698 7654 32 now").len(), 1);
    assert!(iban.spans("GB82 WEST 1234 5698 7654 33").is_empty());
}

#[devkit::test]
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

#[devkit::test]
fn an_unsealed_event_of_a_sealed_class_fails_closed() {
    let mut e = event("ls");
    let r = rule(None, Matcher::All, Class::Sealed, Action::Classify);
    assert!(matches!(
        set(vec![r]).apply(&mut e),
        Err(Error::Unsealed(_))
    ));
    let mut sealed = event("ls");
    let key = sinex_sealing::new_key().unwrap();
    sinex_sealing::seal_event(&mut sealed, &key).unwrap();
    let mask = rule(None, Matcher::All, Class::Open, Action::Mask);
    let before = sealed.payload.clone();
    set(vec![mask]).apply(&mut sealed).unwrap();
    assert_eq!(
        sealed.payload, before,
        "rules never rewrite a sealed payload"
    );
}
