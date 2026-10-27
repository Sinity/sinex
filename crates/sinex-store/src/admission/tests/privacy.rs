//! Privacy rules against a real database: rules apply once, at admission, to
//! source and derived events alike; a derived event is never below its parents'
//! class; values of a class a caller is not granted never leave through the
//! read views; a source never emits below its floor.
use super::*;
use crate::privacy::{self, Action, NewRule};
use crate::read_models::tests::catch_up_search_text;
use sinex_model::event_types::{CommandExecuted, CommandImported};
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, RunId};
use sinex_model::{RecordOutcome, Timestamp};
use sqlx::Connection;

const SECRET: &str = "hunter2-correct-horse";

fn imported(source: &str, command: &str, origin: Origin) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1_700_000_000).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        source,
        origin,
        ReadingOrder(0),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn record() -> Origin {
    Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor: 0,
        },
        output_index: 0,
    }
}

fn batch(run: RunId, events: Vec<Event>) -> Batch {
    Batch {
        run,
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

async fn admit_one(conn: &mut crate::AdmissionContext, b: &Batch) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let out = tx.admit(b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    out
}

fn rule(name: &str, source: &str, class: Class, action: Action, pattern: Option<&str>) -> NewRule {
    NewRule {
        name: name.into(),
        source: source.into(),
        event_type: None,
        field: None,
        matcher: if pattern.is_some() { "pattern" } else { "all" }.into(),
        matcher_arg: pattern.map(str::to_owned),
        class,
        action,
        author: "test".into(),
    }
}

async fn payload_count(conn: &mut PgConnection, needle: &str) -> i64 {
    sqlx::query_scalar!(
        r#"SELECT count(*) AS "n!" FROM core.events WHERE payload::text LIKE '%' || $1 || '%'"#,
        needle
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn one_privacy_point_covers_source_and_derived_events(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    privacy::add_rule(
        &mut conn,
        &rule(
            "pw",
            "terminal.zsh-history",
            Class::Guarded,
            Action::Mask,
            Some(SECRET),
        ),
    )
    .await
    .unwrap();
    let run = RunId::new();
    sqlx::query!(
        "INSERT INTO core.runs (id, name, host) VALUES ($1, 'test', 'h')",
        run.0
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let parent = imported(
        "terminal.zsh-history",
        &format!("export PW={SECRET}"),
        record(),
    );
    let parent_id = parent.id;
    admit_one(&mut conn, &batch(run, vec![parent])).await;

    // A derivation reads the committed parent and copies its text.
    let stored: String = sqlx::query_scalar!(
        r#"SELECT payload->>'command' AS "c!" FROM core.events WHERE id = $1"#,
        parent_id.0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(stored, "export PW=[masked]");
    let derived = Event::build(
        &CommandExecuted {
            native_id: "d1".into(),
            command: stored.clone(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            duration_ns: None,
            exit_code: None,
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "derived.test",
        Origin::Derived {
            parents: vec![parent_id],
        },
        ReadingOrder(0),
        *blake3::hash(stored.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap();
    admit_one(&mut conn, &batch(RunId::new(), vec![derived])).await;

    assert_eq!(
        payload_count(&mut conn, SECRET).await,
        0,
        "no stored payload holds the secret"
    );
    let classes: Vec<String> =
        sqlx::query_scalar!("SELECT class FROM core.events ORDER BY commit_seq")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        classes,
        ["guarded", "guarded"],
        "the derived event inherits its parent's class"
    );
    let seq: Option<i64> = sqlx::query_scalar!(
        "SELECT privacy_rules_seq FROM core.runs WHERE id = $1",
        run.0
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(
        seq.is_some_and(|s| s > 0),
        "the run records the rule set it was admitted under"
    );
}

#[devkit::test]
async fn guarded_values_leave_only_with_the_grant(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    privacy::set_source_class(&mut conn, "terminal.zsh-history", Class::Guarded, "test")
        .await
        .unwrap();
    let e = imported("terminal.zsh-history", "ssh vault.example", record());
    let id = e.id;
    admit_one(&mut conn, &batch(RunId::new(), vec![e])).await;
    let open = imported("terminal.atuin", "ls -la", record());
    admit_one(&mut conn, &batch(RunId::new(), vec![open])).await;
    catch_up_search_text(&mut conn).await.unwrap();

    let view = |grants: &'static str| {
        let id = id.0;
        async move |conn: &mut PgConnection| {
            sqlx::query_scalar!("SELECT set_config('sinex.grants', $1, false)", grants)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            let row = sqlx::query!(
                r#"SELECT payload AS "p!", equivalence_key, class AS "c!" FROM core.visible_events WHERE id = $1"#,
                id
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            let found: i64 = sqlx::query_scalar!(
                r#"SELECT count(*) AS "n!" FROM core.visible_search_text WHERE text LIKE '%vault%'"#
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            (row.p, row.equivalence_key, row.c, found)
        }
    };
    let (p, key, class, found) = view("")(&mut conn).await;
    assert_eq!(p, serde_json::json!({"withheld": "guarded"}));
    assert_eq!(key, None);
    assert_eq!(
        class, "guarded",
        "the event, its type and class stay visible"
    );
    assert_eq!(
        found, 0,
        "search does not match text the caller may not read"
    );
    let (p, _, _, found) = view("guarded")(&mut conn).await;
    assert_eq!(p["command"], "ssh vault.example");
    assert_eq!(found, 1);

    // Read-only SQL runs as sinex_reader, which reaches events only through
    // the views.
    let mut tx = conn.begin().await.unwrap();
    sqlx::query!("SET LOCAL ROLE sinex_reader")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        sqlx::query!("SELECT count(*) FROM core.events")
            .fetch_one(&mut *tx)
            .await
            .is_err(),
        "sinex_reader cannot read core.events directly"
    );
}

#[devkit::test]
async fn class_verification_finds_events_below_a_raised_floor(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let e = imported("terminal.zsh-history", "ls", record());
    admit_one(&mut conn, &batch(RunId::new(), vec![e])).await;
    let before = privacy::verify_classes(&mut conn).await.unwrap();
    assert_eq!(before.below_floor, 0);
    privacy::set_source_class(&mut conn, "terminal.zsh-history", Class::Guarded, "test")
        .await
        .unwrap();
    let after = privacy::verify_classes(&mut conn).await.unwrap();
    assert_eq!(after.below_floor, 1);
    assert_eq!(
        after.by_source,
        vec![("terminal.zsh-history".to_string(), 1)]
    );
    // New readings never emit below the floor.
    let e = imported("terminal.zsh-history", "pwd", record());
    admit_one(&mut conn, &batch(RunId::new(), vec![e])).await;
    assert_eq!(
        privacy::verify_classes(&mut conn)
            .await
            .unwrap()
            .below_floor,
        1
    );
}

#[devkit::test]
async fn rules_are_insert_only_versions(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    privacy::add_rule(
        &mut conn,
        &rule("a", "s", Class::Open, Action::Mask, Some("x")),
    )
    .await
    .unwrap();
    privacy::add_rule(
        &mut conn,
        &rule("a", "s", Class::Guarded, Action::Hash, Some("y")),
    )
    .await
    .unwrap();
    let current = privacy::list_rules(&mut conn, false).await.unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].action, "hash");
    privacy::end_rule(&mut conn, "a", "op").await.unwrap();
    assert!(
        privacy::list_rules(&mut conn, false)
            .await
            .unwrap()
            .is_empty()
    );
    let history = privacy::list_rules(&mut conn, true).await.unwrap();
    assert_eq!(history.len(), 3, "every version and the end stay readable");
    assert!(privacy::end_rule(&mut conn, "a", "op").await.is_err());
    let set = privacy::load(&mut conn, std::iter::once("s"))
        .await
        .unwrap();
    assert!(set.rules.is_empty() && set.seq == 3);
    assert!(
        privacy::add_rule(
            &mut conn,
            &rule("bad", "s", Class::Open, Action::Mask, Some("("))
        )
        .await
        .is_err(),
        "a rule that cannot be applied is never stored"
    );
}
