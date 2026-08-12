use super::*;
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
