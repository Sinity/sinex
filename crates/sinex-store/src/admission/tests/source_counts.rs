use super::*;
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

/// The schema description without column positions: the dropped column that
/// makes this test's version 0 keeps its position, so the column added back
/// comes after it. Positions are compared by the fresh-schema test.
fn without_positions(catalog: Vec<String>) -> Vec<String> {
    catalog
        .into_iter()
        .map(|row| match row.split_once(' ') {
            Some((name, rest)) if name.matches('.').count() == 2 => {
                let rest = rest.split_once(' ').map_or(rest, |(_, after)| after);
                format!("{name} {rest}")
            }
            _ => row,
        })
        .collect()
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
    let fresh = without_positions(crate::test_schema::catalog(&mut conn).await);
    let expected = kept(&mut conn).await;
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();

    super::equivalence::version_two_keys(&mut conn).await;
    // Version 0 is this schema without the counts.
    conn.execute("DROP TABLE core.source_counts").await.unwrap();
    conn.execute("ALTER TABLE core.runs DROP COLUMN events")
        .await
        .unwrap();
    conn.execute("DROP TABLE core.excised_materials")
        .await
        .unwrap();
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
        without_positions(crate::test_schema::catalog(&mut conn).await),
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
    let mut conn = PgConnection::connect(&url).await.unwrap();
    super::equivalence::version_two_keys(&mut conn).await;
    conn.execute("DROP TABLE core.source_counts").await.unwrap();
    conn.execute("ALTER TABLE core.runs DROP COLUMN events")
        .await
        .unwrap();
    conn.execute("DROP TABLE core.excised_materials")
        .await
        .unwrap();
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
    let fresh = crate::test_schema::catalog(&mut conn).await;
    let before: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM core.events ORDER BY id")
        .fetch_all(&mut conn)
        .await
        .unwrap();
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
    assert_eq!(crate::test_schema::catalog(&mut conn).await, fresh);
}

async fn upgrade_keeps_activity_and_telemetry(db: devkit::pg::TestDatabase, from: i32) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let parent = keyed("source", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![parent.clone()])).await;
    let mut child = keyed("derived", 2, 2, 1);
    child.origin = Origin::Derived {
        parents: vec![parent.id],
    };
    admit_one(&mut conn, &batch(vec![child.clone()])).await;
    let telemetry = Event::build(
        &sinex_model::event_types::telemetry::SinexTelemetry {
            component: "writer".into(),
            name: String::new(),
            sampled_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            measures: [("events".into(), 1.0)].into(),
        },
        "sinex.telemetry",
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
    .unwrap();
    let mut sample = batch(vec![telemetry]);
    sample.kind = BatchKind::Replay {operation: OperationId::new(),source:None,host:None};
    admit_one(&mut conn, &sample).await;
    let before: Vec<Value> =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY e.id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    crate::test_schema::remove_foundations(&mut conn).await;
    if from < 4 {
        crate::test_schema::remove_version_4(&mut conn).await;
        if from == 3 {
            conn.execute("CREATE INDEX events_coverage ON core.events (source, run_id, ts_orig) INCLUDE (commit_seq)").await.unwrap();
        }
    }
    if from >= 5 {
        restore_blob_tracking(&mut conn).await;
    }
    sqlx::query("UPDATE sinex_schema.applied SET version = $1")
        .bind(from)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from,
            to: sinex_schema::VERSION
        }
    );
    let after: Vec<Value> =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY e.id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(before, after);
    let edges: Vec<(uuid::Uuid, uuid::Uuid)> =
        sqlx::query_as("SELECT parent, child FROM core.event_children")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(edges, [(parent.id.0, child.id.0)]);
    let kinds: i64 = sqlx::query_scalar("SELECT count(*) FROM registry.event_types WHERE name IN ('sinex.telemetry', 'tag.applied', 'activity.window', 'activity.session', 'activity.span', 'conversation.day', 'rest.fused', 'document.text')")
        .fetch_one(&mut conn).await.unwrap();
    assert_eq!(kinds, 8);
    assert!(matches!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Current { .. }
    ));
}

#[devkit::test]
async fn upgrading_from_version_2_keeps_activity_and_telemetry(db: devkit::pg::TestDatabase) {
    upgrade_keeps_activity_and_telemetry(db, 2).await;
}

#[devkit::test]
async fn upgrading_from_version_3_keeps_activity_and_telemetry(db: devkit::pg::TestDatabase) {
    upgrade_keeps_activity_and_telemetry(db, 3).await;
}

#[devkit::test]
async fn upgrading_refuses_to_drop_a_populated_telemetry_table(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    admit_one(&mut conn, &batch(vec![keyed("synthetic", 1, 1, 1)])).await;
    crate::test_schema::remove_foundations(&mut conn).await;
    restore_blob_tracking(&mut conn).await;
    conn.execute("INSERT INTO telemetry.events (id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,equivalence_key,reading_order,record_hash,record_state,class) SELECT id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,ts_persisted,commit_seq,payload,equivalence_key,reading_order,record_hash,record_state,class FROM core.events").await.unwrap();
    conn.execute("UPDATE sinex_schema.applied SET version = 5")
        .await
        .unwrap();
    let error = sinex_schema::apply(&mut conn).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the telemetry table holds records"),
        "{error}"
    );
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM telemetry.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(stored, 1);
    let ordinary: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(ordinary, 1);
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, 5);
}

#[devkit::test]
async fn upgrading_from_version_5_keeps_activity_and_telemetry(db: devkit::pg::TestDatabase) {
    upgrade_keeps_activity_and_telemetry(db, 5).await;
}

async fn restore_blob_tracking(conn: &mut PgConnection) {
    sqlx::raw_sql("INSERT INTO core.counters(name,value) VALUES('blob_changes',0); CREATE FUNCTION core.note_blob_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE core.counters SET value=value+1 WHERE name='blob_changes'; RETURN NULL; END $$; CREATE TRIGGER blob_changes AFTER INSERT OR DELETE OR TRUNCATE ON core.blobs FOR EACH STATEMENT EXECUTE FUNCTION core.note_blob_change();").execute(conn).await.unwrap();
}
