use super::*;
use sqlx::{Connection, Executor};

#[devkit::test]
async fn evidence_keeps_visibility_positions_unknown_times_and_archived_run_records(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let run = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO core.runs (id, name, host) VALUES ($1, 'test', 'synthetic')")
        .bind(run)
        .execute(&mut conn)
        .await
        .unwrap();
    for (sequence, seconds) in [(1i64, Some(10i64)), (2, Some(20)), (3, None)] {
        sqlx::query("INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, ts_orig, ts_quality, commit_seq, payload, reading_order, record_hash) VALUES (uuidv7(), 'command.executed', 'test.source', $1, uuidv7(), 0, to_timestamp($2::double precision), $3, $4, '{}', 0, decode(repeat('00',32),'hex'))")
            .bind(run).bind(seconds.map(|s| s as f64)).bind(if seconds.is_some() { "intrinsic" } else { "unknown" })
            .bind(sequence).execute(&mut conn).await.unwrap();
    }
    let before = sinex_schema::catalog(&mut conn).await.unwrap();
    conn.execute("DROP INDEX core.events_coverage")
        .await
        .unwrap();
    crate::test_schema::downgrade(&mut conn).await;
    conn.execute("DROP VIEW IF EXISTS event_types.operator_mark, event_types.query_saved")
        .await
        .unwrap();
    conn.execute("DELETE FROM registry.event_types WHERE name IN ('operator.mark','query.saved')")
        .await
        .unwrap();
    conn.execute("UPDATE sinex_schema.applied SET version = 2")
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    assert_eq!(
        crate::test_schema::without_positions(sinex_schema::catalog(&mut conn).await.unwrap()),
        crate::test_schema::without_positions(before)
    );
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(events, 3);
    let first = source_evidence(&mut conn, "test.source", 1).await.unwrap();
    assert_eq!(first.records.unwrap().0.unix_timestamp(), 10);
    assert_eq!(first.runs[0].last_record.unwrap().unix_timestamp(), 10);
    conn.execute("INSERT INTO audit.archive_changes (change_seq, event_id, commit_seq, archived, reason) SELECT 1, id, 4, true, 'hide' FROM core.events WHERE commit_seq = 1").await.unwrap();
    let latest = source_evidence(&mut conn, "test.source", 4).await.unwrap();
    assert_eq!(latest.records.unwrap().0.unix_timestamp(), 20);
    assert_eq!(latest.runs[0].last_record.unwrap().unix_timestamp(), 20);
    assert_eq!(
        source_evidence(&mut conn, "test.source", 1)
            .await
            .unwrap()
            .records,
        first.records
    );
    let missing = source_evidence(&mut conn, "missing", 4).await.unwrap();
    assert!(missing.records.is_none() && missing.runs.is_empty());
    conn.execute("INSERT INTO audit.archive_changes (change_seq, event_id, commit_seq, archived, reason) SELECT 2, id, 5, true, 'hide' FROM core.events WHERE commit_seq = 2").await.unwrap();
    let hidden = source_evidence(&mut conn, "test.source", 5).await.unwrap();
    assert!(hidden.records.is_none());
    assert_eq!(hidden.runs[0].last_record.unwrap().unix_timestamp(), 20);
    let unknown = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO core.runs (id, name, host) VALUES ($1, 'unknown', 'synthetic')")
        .bind(unknown)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, ts_orig, ts_quality, commit_seq, payload, reading_order, record_hash) VALUES (uuidv7(), 'command.executed', 'test.source', $1, uuidv7(), 0, NULL, 'unknown', 6, '{}', 0, decode(repeat('00',32),'hex'))")
        .bind(unknown).execute(&mut conn).await.unwrap();
    assert_eq!(
        source_evidence(&mut conn, "test.source", 5)
            .await
            .unwrap()
            .runs
            .len(),
        1
    );
    let unknown = source_evidence(&mut conn, "test.source", 6).await.unwrap();
    assert_eq!(unknown.runs.len(), 2);
    assert!(unknown.runs.iter().any(|run| run.last_record.is_none()));
}

#[devkit::test]
async fn coverage_run_index_contains_the_position_filter(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let definition: String = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE schemaname = 'core' AND indexname = 'events_coverage'")
        .fetch_one(&mut conn).await.unwrap();
    assert!(
        definition.contains("(source, run_id, ts_orig) INCLUDE (commit_seq)"),
        "{definition}"
    );
}

#[devkit::test]
#[ignore = "100,000-event coverage measurement"]
async fn measure_coverage_scan(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("INSERT INTO core.runs (id, name, host) SELECT uuidv7(), n::text, 'synthetic' FROM generate_series(1,100) n").await.unwrap();
    conn.execute("INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, ts_orig, ts_quality, commit_seq, payload, reading_order, record_hash) SELECT uuidv7(), 'command.executed', 'test.source', r.id, uuidv7(), 0, to_timestamp((r.name::int-1)*1000+n), 'intrinsic', (r.name::int-1)*1000+n, '{}', 0, decode(repeat('00',32),'hex') FROM core.runs r CROSS JOIN generate_series(1,1000) n").await.unwrap();
    conn.execute("VACUUM ANALYZE core.events").await.unwrap();
    conn.execute("ANALYZE core.runs").await.unwrap();
    let old = "SELECT r.started_at, r.ended_at, max(e.ts_orig) FROM core.events e JOIN core.runs r ON r.id = e.run_id WHERE e.source = 'test.source' AND e.commit_seq <= 100000 GROUP BY r.id, r.started_at, r.ended_at ORDER BY r.started_at";
    let new = "SELECT r.started_at, r.ended_at, (SELECT max(e.ts_orig) FROM core.events e WHERE e.source = 'test.source' AND e.run_id = r.id AND e.commit_seq <= 100000) AS last_record FROM core.runs r JOIN LATERAL (SELECT 1 FROM core.events e WHERE e.source = 'test.source' AND e.run_id = r.id AND e.commit_seq <= 100000 ORDER BY e.ts_orig DESC LIMIT 1) recorded ON true ORDER BY r.started_at";
    for (name, query) in [("old runs", old), ("new runs", new)] {
        let plan: serde_json::Value =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"))
                .fetch_one(&mut conn)
                .await
                .unwrap();
        eprintln!("{name}: {plan}");
        if name == "new runs" {
            assert_bounded(&plan[0]["Plan"]);
            assert!(plan.to_string().contains("events_coverage"), "{plan}");
        }
    }
    let evidence = source_evidence(&mut conn, "test.source", 100000)
        .await
        .unwrap();
    assert_eq!(evidence.runs.len(), 100);
    assert_eq!(
        evidence
            .runs
            .iter()
            .filter_map(|run| run.last_record)
            .map(|time| time.unix_timestamp())
            .max(),
        Some(100000)
    );
}

fn assert_bounded(plan: &serde_json::Value) {
    if plan["Relation Name"]
        .as_str()
        .is_some_and(|relation| relation != "runs")
    {
        assert!(plan["Actual Rows"].as_f64().unwrap_or(0.0) <= 1.0, "{plan}");
        assert_eq!(
            plan["Rows Removed by Filter"].as_f64().unwrap_or(0.0),
            0.0,
            "{plan}"
        );
    }
    if let Some(children) = plan["Plans"].as_array() {
        for child in children {
            assert_bounded(child);
        }
    }
}
