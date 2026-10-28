use super::*;
use crate::test_schema::without_positions;
use sqlx::Executor;

type Time = time::OffsetDateTime;
type Counts = Vec<(String, i64, Option<Time>, Option<Time>, Time)>;

async fn kept(conn: &mut PgConnection) -> Counts {
    sqlx::query_as(
        "SELECT source, events, first_event, last_event, last_seen
         FROM core.source_counts ORDER BY source",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

/// The same counts computed from every stored event.
async fn counted(conn: &mut PgConnection) -> Counts {
    sqlx::query_as(
        "SELECT e.source,
           count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)),
           min(e.ts_orig), max(e.ts_orig), max(e.ts_coided)
         FROM core.events e GROUP BY e.source ORDER BY e.source",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

async fn run_events(conn: &mut PgConnection, run: RunId) -> i64 {
    sqlx::query_scalar("SELECT events FROM core.runs WHERE id = $1")
        .bind(run.0)
        .fetch_one(conn)
        .await
        .unwrap()
}

/// A batch under a run that has its row.
async fn under_run(conn: &mut crate::AdmissionContext, events: Vec<Event>) -> RunId {
    let run = RunId::new();
    sqlx::query("INSERT INTO core.runs (id, name, host) VALUES ($1, 'test', 'h')")
        .bind(run.0)
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut b = batch(events);
    b.run = run;
    admit_one(conn, &b).await;
    run
}

fn change(target: sinex_model::EventId, hide: bool) -> Batch {
    let operation = OperationId::new();
    let mut b = batch(vec![]);
    b.archive.push(if hide {
        ArchiveRequest::Hide { target, operation }
    } else {
        ArchiveRequest::Unhide { target, operation }
    });
    b
}

#[devkit::test]
async fn counts_follow_admission_replacement_hiding_and_unhiding(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let first: Vec<Event> = (0..4).map(|key| keyed("a", key, 1, 1)).collect();
    admit_one(&mut conn, &batch(first.clone())).await;
    admit_one(&mut conn, &batch(vec![keyed("b", 9, 1, 1)])).await;
    // A newer reading of key 0 archives the first one.
    admit_one(&mut conn, &batch(vec![keyed("a", 0, 2, 5)])).await;
    admit_one(&mut conn, &change(first[1].id, true)).await;
    admit_one(&mut conn, &change(first[2].id, true)).await;
    admit_one(&mut conn, &change(first[2].id, false)).await;

    let kept = kept(&mut conn).await;
    assert_eq!(kept, counted(&mut conn).await);
    let live: Vec<(&str, i64)> = kept.iter().map(|c| (c.0.as_str(), c.1)).collect();
    assert_eq!(live, [("a", 3), ("b", 1)]);
}

/// A database at version 0 had no counts and no excised materials. Upgrading
/// it fills the counts from the stored events, keeps every row, and gives
/// the schema a fresh database has.
#[devkit::test]
async fn upgrading_from_version_0_counts_the_stored_events(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let first: Vec<Event> = (0..3).map(|key| keyed("a", key, 1, 1)).collect();
    let run = under_run(&mut conn, first.clone()).await;
    assert_eq!(run_events(&mut conn, run).await, 3);
    admit_one(&mut conn, &batch(vec![keyed("b", 9, 1, 1)])).await;
    admit_one(&mut conn, &change(first[0].id, true)).await;
    let fresh = without_positions(sinex_schema::catalog(&mut conn).await.unwrap());
    let expected = kept(&mut conn).await;
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();

    crate::test_schema::downgrade(&mut conn).await;
    // Version 0 is this schema without the counts.
    super::equivalence::version_two_keys(&mut conn).await;
    // Version 0 is this schema without the counts.
    conn.execute("DROP TABLE core.source_counts").await.unwrap();
    conn.execute("ALTER TABLE core.runs DROP COLUMN events")
        .await
        .unwrap();
    conn.execute("DROP TABLE core.excised_materials")
        .await
        .unwrap();
    conn.execute("DELETE FROM registry.event_types WHERE name IN ('operator.mark','query.saved')")
        .await
        .unwrap();
    conn.execute("DROP TRIGGER blob_changes ON core.blobs; DROP FUNCTION core.note_blob_change(); DELETE FROM core.counters WHERE name='blob_changes'")
        .await.unwrap();
    conn.execute("UPDATE sinex_schema.applied SET version = 0")
        .await
        .unwrap();
    assert!(sinex_schema::require_current(&mut conn).await.is_err());

    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 0,
            to: sinex_schema::VERSION
        }
    );
    sinex_schema::require_current(&mut conn).await.unwrap();
    assert_eq!(kept(&mut conn).await, expected);
    assert_eq!(
        run_events(&mut conn, run).await,
        3,
        "every stored event of the run"
    );
    assert_eq!(
        without_positions(sinex_schema::catalog(&mut conn).await.unwrap()),
        fresh
    );
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(after, events);
    // Admission keeps them current from there.
    admit_one(&mut conn, &batch(vec![keyed("b", 10, 1, 1)])).await;
    assert_eq!(kept(&mut conn).await, counted(&mut conn).await);
}

#[devkit::test]
async fn an_upgrade_waits_for_no_writer(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    crate::test_schema::downgrade(&mut conn).await;
    let mut conn = PgConnection::connect(&url).await.unwrap();
    super::equivalence::version_two_keys(&mut conn).await;
    conn.execute("DROP TABLE core.source_counts").await.unwrap();
    conn.execute("ALTER TABLE core.runs DROP COLUMN events")
        .await
        .unwrap();
    conn.execute("DROP TABLE core.excised_materials")
        .await
        .unwrap();
    conn.execute("DELETE FROM registry.event_types WHERE name IN ('operator.mark','query.saved')")
        .await
        .unwrap();
    conn.execute("DROP TRIGGER blob_changes ON core.blobs; DROP FUNCTION core.note_blob_change(); DELETE FROM core.counters WHERE name='blob_changes'")
        .await.unwrap();
    conn.execute("UPDATE sinex_schema.applied SET version = 0")
        .await
        .unwrap();
    let mut writer = PgConnection::connect(&url).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    assert!(matches!(
        sinex_schema::apply(&mut conn).await,
        Err(sinex_schema::Error::WriterRunning)
    ));
    let version: Option<i32> = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, Some(0));
}

#[devkit::test]
async fn upgrading_from_version_3_registers_query_kinds_and_keeps_events(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    under_run(&mut conn, vec![keyed("synthetic", 1, 1, 1)]).await;
    let fresh = sinex_schema::catalog(&mut conn).await.unwrap();
    let before: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM core.events ORDER BY id")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    conn.execute("DELETE FROM registry.event_types WHERE name IN ('operator.mark','query.saved')")
        .await
        .unwrap();
    conn.execute("DROP TRIGGER blob_changes ON core.blobs; DROP FUNCTION core.note_blob_change(); DELETE FROM core.counters WHERE name='blob_changes'")
    super::equivalence::version_two_keys(&mut conn).await;
    conn.execute("CREATE INDEX events_coverage ON core.events (source, run_id, ts_orig) INCLUDE (commit_seq)")
        .await.unwrap();
    conn.execute("UPDATE sinex_schema.applied SET version=3")
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 3,
            to: sinex_schema::VERSION
        }
    );
    let after: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM core.events ORDER BY id")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        without_positions(sinex_schema::catalog(&mut conn).await.unwrap()),
        without_positions(fresh)
    );
}
