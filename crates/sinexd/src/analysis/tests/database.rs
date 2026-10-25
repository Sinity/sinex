//! The analysis layer against a real database: admission dirties metric days
//! and marks dependent results stale; maintenance recomputes the cells;
//! products are stored, reused for identical requests and superseded by
//! newer answers to the same standing question.
use super::super::cells::maintain;
use super::super::*;
use crate::dirty_keys::Registered as Keys;
use crate::writer::{self, Priority};
use serde_json::json;
use sinex_analysis::Eligibility;
use sinex_model::event_types::code::GitCommit;
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId,
};
use sinex_store::analysis as store;
use sqlx::Connection;
use sqlx::PgConnection;
use std::sync::Arc;

/// 2026-09-14 10:00 CEST.
const T: i64 = 1_789_372_800;

fn at(hours: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + hours * 3600).unwrap()
}

fn commit(id: &str, hours: i64) -> Event {
    Event::build(
        &GitCommit {
            repository: "/r".into(),
            commit: id.into(),
            parents: vec![],
            tree: "t".into(),
            author_name: "a".into(),
            author_email: "a@x".into(),
            authored_at: at(hours),
            committer_name: "a".into(),
            committer_email: "a@x".into(),
            committed_at: at(hours),
            message: "m".into(),
            refs: vec![],
        },
        "code.git",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        *blake3::hash(id.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

struct World {
    _db: devkit::pg::TestDatabase,
    url: String,
    handle: writer::WriterHandle,
    _writer: tokio::task::JoinHandle<()>,
}

impl World {
    async fn new(db: devkit::pg::TestDatabase) -> Self {
        let url = db.url(None);
        let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .unwrap();
        let (handle, queues) = writer::channels();
        let task = tokio::spawn(writer::run(
            url.clone(),
            connection,
            queues,
            Arc::new(Keys::new(sinex_derivations::keyed())),
            None,
        ));
        Self {
            _db: db,
            url,
            handle,
            _writer: task,
        }
    }

    async fn admit(&self, events: Vec<Event>) {
        let batch = Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "test".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        };
        self.handle.submit(Priority::Live, batch).await.unwrap();
    }

    async fn conn(&self) -> PgConnection {
        PgConnection::connect(&self.url).await.unwrap()
    }
}

fn result_on(
    day_from: i64,
    day_to: i64,
    as_of: i64,
    standing: Option<&str>,
    tag: &str,
) -> store::NewResult {
    store::NewResult {
        request_id: *blake3::hash(tag.as_bytes()).as_bytes(),
        request_body: json!({"tag": tag}),
        recipe: "test".into(),
        recipe_version: 1,
        intent: "exploratory",
        family: None,
        as_of,
        standing: standing.map(str::to_owned),
        requested_by: json!({"kind": "test"}),
        producer: "test".into(),
        started_at: OffsetDateTime::now_utc(),
        status: "estimated",
        reason: None,
        inputs_read: json!([]),
        tables: json!({}),
        diagnostics: json!({}),
        output_hash: [0; 32],
        dependencies: vec![(
            "git.commit".into(),
            at(day_from * 24 - 10).0,
            at(day_to * 24 + 14).0,
        )],
        findings: vec![],
    }
}

async fn state(conn: &mut PgConnection, id: uuid::Uuid) -> String {
    sqlx::query_scalar("SELECT state FROM analysis.result_status WHERE result = $1")
        .bind(id)
        .fetch_one(conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn admission_dirties_metric_days_and_maintenance_recomputes_them(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    w.admit(vec![commit("a", 0), commit("b", 2), commit("c", 24)])
        .await;
    let mut conn = w.conn().await;
    let dirty: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT key FROM derivation.dirty WHERE derivation = 'metric:git.commits' ORDER BY key->>'day'",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        dirty,
        [json!({"day": "2026-09-14"}), json!({"day": "2026-09-15"})]
    );

    let settings = Settings::operator_default();
    assert_eq!(maintain(&mut conn, &settings, 0).await.unwrap(), 2);
    let cells = store::metric_values(
        &mut conn,
        "git.commits",
        1,
        &settings.label,
        at(-48).0,
        at(48).0,
    )
    .await
    .unwrap();
    let totals: Vec<_> = cells
        .iter()
        .filter(|c| c.dimension.is_empty())
        .map(|c| (c.unit_label.as_str(), c.state.as_str(), c.value))
        .collect();
    // The store has git records from 10:00 on the 14th to 10:00 on the 15th,
    // so both days are partly observed; the counts are what was seen.
    assert_eq!(
        totals,
        [
            ("2026-09-14", "partial", Some(2.0)),
            ("2026-09-15", "partial", Some(1.0))
        ]
    );
    assert!(
        cells
            .iter()
            .any(|c| c.dimension == "repository" && c.dimension_value == "/r")
    );
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM derivation.dirty WHERE derivation LIKE 'metric:%'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(left, 0);
}

#[devkit::test]
async fn a_result_goes_stale_when_its_inputs_change_and_not_otherwise(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    w.admit(vec![commit("a", 0)]).await;
    let mut conn = w.conn().await;
    let head = store::head(&mut conn).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    let day1 = store::store_result(&mut tx, &result_on(0, 0, head, None, "day1"))
        .await
        .unwrap();
    let day9 = store::store_result(&mut tx, &result_on(9, 9, head, None, "day9"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        (day1.state.as_str(), day9.state.as_str()),
        ("current", "current")
    );

    w.admit(vec![commit("b", 3)]).await;
    assert_eq!(state(&mut conn, day1.id).await, "stale");
    assert_eq!(state(&mut conn, day9.id).await, "current");
}

#[devkit::test]
async fn a_result_computed_before_a_racing_commit_is_stored_stale(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    let mut conn = w.conn().await;
    let before = store::head(&mut conn).await.unwrap();
    w.admit(vec![commit("a", 0)]).await;
    let mut tx = conn.begin().await.unwrap();
    let r = store::store_result(&mut tx, &result_on(0, 0, before, None, "raced"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(r.state, "stale");
}

#[devkit::test]
async fn a_newer_answer_supersedes_the_standing_questions_previous_one(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let mut conn = w.conn().await;
    let mut tx = conn.begin().await.unwrap();
    let first = store::store_result(&mut tx, &result_on(0, 0, 0, Some("q"), "first"))
        .await
        .unwrap();
    let second = store::store_result(&mut tx, &result_on(0, 0, 0, Some("q"), "second"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(state(&mut conn, first.id).await, "superseded");
    assert_eq!(state(&mut conn, second.id).await, "current");
}

#[devkit::test]
async fn a_recipe_is_stored_and_an_identical_request_returns_it(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![commit("a", 0)]).await;
    let mut conn = w.conn().await;
    let settings = Settings::operator_default();
    let (from, to) = (
        parse_day("2026-09-01").unwrap(),
        parse_day("2026-09-14").unwrap(),
    );
    let first = run(
        &mut conn,
        &settings,
        Recipe::Trends,
        from,
        to,
        Eligibility::default(),
        true,
    )
    .await
    .unwrap();
    assert!(!first.reused);
    // No tracked metric has an observed series here: every method refused,
    // and the refusals are the answer.
    let refusal = &first.tables["metrics"][0]["trend"];
    assert_eq!(refusal["status"], "not_estimable");
    let again = run(
        &mut conn,
        &settings,
        Recipe::Trends,
        from,
        to,
        Eligibility::default(),
        true,
    )
    .await
    .unwrap();
    assert!(again.reused);
    assert_eq!(again.result.id, first.result.id);
}

#[devkit::test]
async fn the_brief_reports_coverage_first_and_never_shows_missing_as_zero(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    w.admit(vec![commit("a", 0), commit("b", 2)]).await;
    let mut conn = w.conn().await;
    let p = brief(
        &mut conn,
        &Settings::operator_default(),
        parse_day("2026-09-14").unwrap(),
        6000,
    )
    .await
    .unwrap();
    let titles: Vec<_> = p.sections.iter().map(|s| s.title.as_str()).collect();
    assert_eq!(titles[0], "Data quality");
    assert!(
        p.markdown
            .contains("Commits made (git.commits) on 2026-09-14: 2"),
        "{}",
        p.markdown
    );
    // Nothing from the shells was stored, so the day's command count is
    // unknown, not zero.
    assert!(
        p.markdown
            .contains("shell.commands on 2026-09-14 is not known: no coverage"),
        "{}",
        p.markdown
    );
    assert!(p.markdown.contains("Absent: no stored correlation result"));
}
