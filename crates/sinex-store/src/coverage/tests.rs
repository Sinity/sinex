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
    crate::test_schema::downgrade(&mut conn).await;
    sqlx::raw_sql(include_str!(
        "../../../sinex-schema/src/tests/version_two.sql"
    ))
    .execute(&mut conn)
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
async fn compressed_coverage_keeps_run_results(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("INSERT INTO core.runs (id, name, host, started_at) SELECT uuidv7(), n::text, 'synthetic', to_timestamp(n) FROM generate_series(1,1000) n").await.unwrap();
    conn.execute("INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, ts_orig, ts_quality, commit_seq, payload, reading_order, record_hash) SELECT uuidv7(), 'command.executed', CASE WHEN r.name::int <= 100 THEN 'test.source' ELSE 'another.source' END, r.id, uuidv7(), 0, CASE WHEN r.name::int = 100 THEN NULL ELSE to_timestamp((r.name::int-1)*1000+n) END, CASE WHEN r.name::int = 100 THEN 'unknown' ELSE 'intrinsic' END, (r.name::int-1)*1000+n, '{}', 0, decode(repeat('00',32),'hex') FROM core.runs r CROSS JOIN generate_series(1,1000) n WHERE r.name::int <= 100 OR n = 1").await.unwrap();
    let chunks: Vec<String> = sqlx::query_scalar("SELECT compress_chunk(c, if_not_compressed => true)::text FROM show_chunks('core.events') c")
        .fetch_all(&mut conn).await.unwrap();
    assert!(!chunks.is_empty());
    let compressed: i64 = sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed")
        .fetch_one(&mut conn).await.unwrap();
    assert_eq!(compressed as usize, chunks.len());
    let evidence = source_evidence(&mut conn, "test.source", 100000)
        .await
        .unwrap();
    assert_eq!(evidence.runs.len(), 100);
    for (index, run) in evidence.runs.iter().enumerate() {
        assert_eq!(run.started_at.unix_timestamp(), index as i64 + 1);
        assert!(run.ended_at.is_none());
        assert_eq!(
            run.last_record.map(|time| time.unix_timestamp()),
            (index < 99).then_some((index as i64 + 1) * 1000)
        );
    }
    let earlier = source_evidence(&mut conn, "test.source", 1000)
        .await
        .unwrap();
    assert_eq!(earlier.runs.len(), 1);
    assert_eq!(earlier.runs[0].last_record.unwrap().unix_timestamp(), 1000);
}

#[devkit::test]
async fn version_two_with_compressed_chunks_upgrades_without_changing_rows(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = sinex_schema::catalog(&mut conn).await.unwrap();
    conn.execute("DROP SCHEMA sinex_schema, core, audit, raw, registry, derivation, privacy, telemetry, event_types CASCADE")
        .await.unwrap();
    crate::test_schema::downgrade(&mut conn).await;
    sqlx::raw_sql(include_str!("version_two.sql"))
        .execute(&mut conn)
        .await
        .unwrap();
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, 2);
    let registered: i64 = sqlx::query_scalar("SELECT count(*) FROM registry.event_types")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(registered > 0);
    let index: Option<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes WHERE schemaname='core' AND indexname='events_coverage'",
    )
    .fetch_optional(&mut conn)
    .await
    .unwrap();
    assert!(index.is_none());
    conn.execute("INSERT INTO core.runs(id,name,host) VALUES (uuidv7(),'known','synthetic'), (uuidv7(),'unknown','synthetic')")
        .await.unwrap();
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_orig,ts_quality,commit_seq,payload,reading_order,record_hash) SELECT uuidv7(),'command.executed','test.source',r.id,uuidv7(),0,CASE WHEN r.name='known' THEN to_timestamp(n) END,CASE WHEN r.name='known' THEN 'intrinsic' ELSE 'unknown' END,row_number() OVER (ORDER BY r.name,n),jsonb_build_object('case',r.name,'number',n),0,decode(repeat('00',32),'hex') FROM core.runs r CROSS JOIN generate_series(1,2048) n")
        .await.unwrap();
    conn.execute("INSERT INTO core.source_counts(source,events,first_event,last_event,last_seen) SELECT source,count(*),min(ts_orig),max(ts_orig),max(ts_coided) FROM core.events GROUP BY source").await.unwrap();
    conn.execute(
        "UPDATE core.runs r SET events=(SELECT count(*) FROM core.events e WHERE e.run_id=r.id)",
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO core.commit_log(commit_seq,event_id,event_type) SELECT commit_seq,id,event_type FROM core.events").await.unwrap();
    conn.execute("UPDATE core.counters SET value=4096 WHERE name='commit_seq'")
        .await
        .unwrap();
    conn.execute(
        "SELECT compress_chunk(c, if_not_compressed=>true) FROM show_chunks('core.events') c",
    )
    .await
    .unwrap();
    let compressed_before: i64 = sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed")
        .fetch_one(&mut conn).await.unwrap();
    assert!(compressed_before > 0);
    let rows_before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY e.id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(rows_before.len(), 4096);
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 2,
            to: sinex_schema::VERSION
        }
    );
    assert_eq!(sinex_schema::catalog(&mut conn).await.unwrap(), fresh);
    let rows_after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY e.id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(rows_after, rows_before);
    let compressed_after: i64 = sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed")
        .fetch_one(&mut conn).await.unwrap();
    assert_eq!(compressed_after, compressed_before);
    assert_eq!(
        source_evidence(&mut conn, "test.source", 2048)
            .await
            .unwrap()
            .runs
            .len(),
        1
    );
    let evidence = source_evidence(&mut conn, "test.source", 4096)
        .await
        .unwrap();
    assert_eq!(evidence.runs.len(), 2);
    assert!(evidence.runs.iter().any(|run| run.last_record.is_none()));
    assert_eq!(evidence.records.unwrap().1.unix_timestamp(), 2048);
}
