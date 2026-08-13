use super::*;
use std::time::Instant;
use xtask::sandbox::sinex_test;

/// sinex-h8no regression coverage: `core.events` used to carry inbound FKs
/// from `core.event_embeddings`/`core.event_cluster_members`, which blocks
/// TimescaleDB columnstore chunk conversion ("found a FK into a chunk while
/// truncating") while the compression policy job still reported vacuous
/// Success. PR #2497 dropped both FKs and installed a real 7-day
/// compression policy. This proves a chunk can actually be
/// columnstore-compressed end-to-end against the schema this repo converges
/// to, not just that the policy row exists or that the FK is absent in
/// isolation.
#[sinex_test]
async fn events_chunk_can_be_columnstore_compressed(ctx: TestContext) -> TestResult<()> {
    // Structural half of the fix: no inbound FK from event_embeddings or
    // event_cluster_members into core.events(id).
    let inbound_fks: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT tc.table_name, tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
         AND tc.constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_schema = 'core'
          AND ccu.table_name = 'events'
          AND tc.table_schema = 'core'
          AND tc.table_name IN ('event_embeddings', 'event_cluster_members')
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;
    assert!(
        inbound_fks.is_empty(),
        "core.event_embeddings/event_cluster_members must not hold an inbound FK into \
         core.events(id) -- it blocks columnstore chunk conversion (sinex-h8no): {inbound_fks:?}"
    );

    // Behavioral half of the fix: seed a real event so a chunk exists, then
    // actually compress it -- the exact operation that failed pre-fix with
    // "found a FK into a chunk while truncating".
    let material_id = ctx
        .create_source_material(Some("h8no-compression-material"))
        .await?;
    let event = DynamicPayload::new(
        "h8no-test-source",
        "h8no.compression.probe",
        serde_json::json!({"probe": "h8no"}),
    )
    .from_material(material_id)
    .build()?;
    ctx.pool().events().insert_batch(vec![event]).await?;

    sqlx::query(
        "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('core.events') AS c",
    )
    .execute(&ctx.pool)
    .await?;

    let compressed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM timescaledb_information.chunks \
         WHERE hypertable_name = 'events' AND hypertable_schema = 'core' AND is_compressed",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(
        compressed >= 1,
        "expected at least one compressed core.events chunk after compress_chunk"
    );

    Ok(())
}

/// Bounded pre-wipe evidence for the archive half of replay. The fixture is
/// synthetic and isolated by `sinex_test`; it never dispatches a source scan.
#[sinex_test]
async fn compressed_chunk_archive_cost_is_measurable(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("y0o3-10-compressed-archive-probe"))
        .await?;
    let fixture_events = 256_u64;
    let events = (0..fixture_events)
        .map(|index| {
            DynamicPayload::new(
                "y0o3.measurement",
                "y0o3.compressed_archive_probe",
                serde_json::json!({ "index": index }),
            )
            .from_material(material_id)
            .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let inserted = ctx.pool().events().insert_batch(events).await?;
    let event_ids = inserted
        .iter()
        .map(|event| event.id.expect("inserted event has an id"))
        .map(|id| *id.as_uuid())
        .collect::<Vec<_>>();

    sqlx::query(
        "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('core.events') AS c",
    )
    .execute(&ctx.pool)
    .await?;
    let compressed_before: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))), 0)::bigint \
         FROM timescaledb_information.chunks \
         WHERE hypertable_schema = 'core' AND hypertable_name = 'events' AND is_compressed",
    )
    .fetch_one(&ctx.pool)
    .await?;
    let uncompressed_before: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))), 0)::bigint \
         FROM timescaledb_information.chunks \
         WHERE hypertable_schema = 'core' AND hypertable_name = 'events' AND NOT is_compressed",
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert!(
        compressed_before > 0,
        "fixture chunk must be compressed before archive"
    );

    let start_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&ctx.pool)
        .await?;
    let started = Instant::now();
    let operation_id = sinex_primitives::Uuid::now_v7().to_string();
    let archived = ctx
        .pool()
        .events()
        .execute_cascade_archive(
            &event_ids,
            "y0o3.10 bounded compressed archive probe",
            &operation_id,
            "test",
        )
        .await?;
    let archive_wall_ms = started.elapsed().as_millis() as u64;
    let end_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&ctx.pool)
        .await?;
    let wal_bytes: f64 =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::double precision")
            .bind(&end_lsn)
            .bind(&start_lsn)
            .fetch_one(&ctx.pool)
            .await?;
    let archived_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit.archived_events WHERE id = ANY($1::uuid[])")
            .bind(&event_ids)
            .fetch_one(&ctx.pool)
            .await?;
    let compressed_after: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))), 0)::bigint \
         FROM timescaledb_information.chunks \
         WHERE hypertable_schema = 'core' AND hypertable_name = 'events' AND is_compressed",
    )
    .fetch_one(&ctx.pool)
    .await?;
    let uncompressed_after: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))), 0)::bigint \
         FROM timescaledb_information.chunks \
         WHERE hypertable_schema = 'core' AND hypertable_name = 'events' AND NOT is_compressed",
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(archived, fixture_events);
    assert_eq!(archived_rows, fixture_events as i64);
    println!(
        "compressed_chunk_archive_cost {}",
        serde_json::json!({
            "fixture_events": fixture_events,
            "archived_events": archived,
            "archive_wall_ms": archive_wall_ms,
            "wal_bytes": wal_bytes,
            "compressed_bytes_before": compressed_before,
            "uncompressed_bytes_before": uncompressed_before,
            "compressed_bytes_after": compressed_after,
            "uncompressed_bytes_after": uncompressed_after,
            "archived_rows": archived_rows,
        })
    );
    Ok(())
}

/// sinex-h8no regression coverage: a real 7-day compression policy job must
/// target `core.events`, not just `reflection.events` (the original vacuous
/// success was a policy on the wrong hypertable reporting Success on an
/// almost-empty lane).
#[sinex_test]
async fn events_hypertable_has_compression_policy(ctx: TestContext) -> TestResult<()> {
    let policy_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM timescaledb_information.jobs j
        WHERE j.proc_name = 'policy_compression'
          AND j.hypertable_schema = 'core'
          AND j.hypertable_name = 'events'
        "#,
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        policy_count, 1,
        "expected exactly one compression policy job targeting core.events"
    );

    Ok(())
}
