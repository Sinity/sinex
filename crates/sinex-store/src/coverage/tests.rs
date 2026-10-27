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
    // Reconstruct the preceding schema's indexes over populated events.
    for schema in ["core", "telemetry"] {
        conn.execute(format!("DROP INDEX {schema}.events_source_ts_orig_idx").as_str())
            .await
            .unwrap();
        conn.execute(
            format!("CREATE INDEX events_source_ts_orig_idx ON {schema}.events (source, ts_orig)")
                .as_str(),
        )
        .await
        .unwrap();
    }
    conn.execute("UPDATE sinex_schema.applied SET version = 2")
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    assert_eq!(sinex_schema::catalog(&mut conn).await.unwrap(), before);
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
}

#[devkit::test]
async fn coverage_index_contains_every_event_column_the_queries_read(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let definition: String = sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE schemaname = 'core' AND indexname = 'events_source_ts_orig_idx'")
        .fetch_one(&mut conn).await.unwrap();
    assert!(
        definition.contains("INCLUDE (id, run_id, commit_seq)"),
        "{definition}"
    );
}

#[devkit::test]
#[ignore = "100,000-event coverage measurement"]
async fn measure_coverage_scan(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("INSERT INTO core.runs (id, name, host) VALUES (uuidv7(), 'test', 'synthetic')")
        .await
        .unwrap();
    conn.execute("INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, ts_orig, ts_quality, commit_seq, payload, reading_order, record_hash) SELECT uuidv7(), 'command.executed', 'test.source', (SELECT id FROM core.runs LIMIT 1), uuidv7(), 0, to_timestamp(n), 'intrinsic', n, '{}', 0, decode(repeat('00',32),'hex') FROM generate_series(1,100000) n").await.unwrap();
    conn.execute("VACUUM ANALYZE core.events").await.unwrap();
    let old = "SELECT min(e.ts_orig), max(e.ts_orig) FROM core.events e WHERE source = 'test.source' AND e.ts_orig IS NOT NULL AND e.commit_seq <= 100000 AND NOT audit.is_archived_at(e.id,100000)";
    let bounds = "SELECT (SELECT e.ts_orig FROM core.events e WHERE source = 'test.source' AND e.ts_orig IS NOT NULL AND e.commit_seq <= 100000 AND NOT audit.is_archived_at(e.id,100000) ORDER BY ts_orig LIMIT 1), (SELECT e.ts_orig FROM core.events e WHERE source = 'test.source' AND e.ts_orig IS NOT NULL AND e.commit_seq <= 100000 AND NOT audit.is_archived_at(e.id,100000) ORDER BY ts_orig DESC LIMIT 1)";
    let runs = "SELECT r.started_at, r.ended_at, max(e.ts_orig) FROM core.events e JOIN core.runs r ON r.id = e.run_id WHERE e.source = 'test.source' AND e.commit_seq <= 100000 GROUP BY r.id, r.started_at, r.ended_at ORDER BY r.started_at";
    for (name, query) in [
        ("old bounds", old),
        ("new bounds", bounds),
        ("run aggregate", runs),
    ] {
        let plan: serde_json::Value =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"))
                .fetch_one(&mut conn)
                .await
                .unwrap();
        eprintln!("{name}: {plan}");
    }
}
