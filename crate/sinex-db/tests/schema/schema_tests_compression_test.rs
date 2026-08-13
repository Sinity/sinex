use super::*;
use serde::Serialize;
use sinex_db::replay::state_machine::ReplayScope;
use sinex_db::repositories::EventRepositoryTx;
use std::time::{Duration, Instant};
use xtask::sandbox::sinex_test;

const Y0O3_MEASUREMENT_SIZES: [u64; 3] = [256, 2_048, 8_192];
const Y0O3_MEASUREMENT_CHUNK_INTERVAL: &str = "1 millisecond";
const Y0O3_MEASUREMENT_INSERT_BURSTS: u64 = 8;

#[derive(Debug, Serialize)]
struct ChunkState {
    schema: String,
    name: String,
    is_compressed: bool,
    total_bytes: i64,
}

#[derive(Debug, Serialize)]
struct WalSnapshot {
    current_lsn: Option<String>,
    pg_stat_wal_bytes: Option<f64>,
}

#[derive(Debug, Serialize)]
struct WalDelta {
    before: WalSnapshot,
    after: WalSnapshot,
    lsn_bytes: Option<f64>,
    pg_stat_wal_bytes: Option<f64>,
}

#[derive(Debug, Serialize)]
struct PhaseMeasurement {
    wall_ms: u64,
    wal: WalDelta,
}

#[derive(Debug, Serialize)]
struct BoundedReplayMeasurement {
    fixture_events: u64,
    chunk_interval: &'static str,
    live_rows_before_replay: i64,
    direct_replay_roots: i64,
    cascade_rows: usize,
    archived_rows: u64,
    live_rows_after_replay: i64,
    archived_rows_after_replay: i64,
    chunks_before_compression: Vec<ChunkState>,
    chunks_after_compression: Vec<ChunkState>,
    chunks_after_replay: Vec<ChunkState>,
    chunks_after_recompression: Vec<ChunkState>,
    fixture_insert: PhaseMeasurement,
    compression: PhaseMeasurement,
    scoped_replay_archive: PhaseMeasurement,
    recompression: PhaseMeasurement,
}

struct PhaseStart {
    started: Instant,
    wal_before: WalSnapshot,
}

async fn chunk_states(pool: &sqlx::PgPool) -> TestResult<Vec<ChunkState>> {
    let rows: Vec<(String, String, bool, i64)> = sqlx::query_as(
        "SELECT chunk_schema, chunk_name, is_compressed, \
                pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name))::bigint \
         FROM timescaledb_information.chunks \
         WHERE hypertable_schema = 'core' AND hypertable_name = 'events' \
         ORDER BY chunk_schema, chunk_name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(schema, name, is_compressed, total_bytes)| ChunkState {
            schema,
            name,
            is_compressed,
            total_bytes,
        })
        .collect())
}

async fn wal_snapshot(pool: &sqlx::PgPool) -> WalSnapshot {
    let current_lsn = sqlx::query_scalar::<_, String>("SELECT pg_current_wal_lsn()::text")
        .fetch_one(pool)
        .await
        .ok();
    let pg_stat_wal_bytes =
        sqlx::query_scalar::<_, f64>("SELECT wal_bytes::double precision FROM pg_stat_wal")
            .fetch_one(pool)
            .await
            .ok();
    WalSnapshot {
        current_lsn,
        pg_stat_wal_bytes,
    }
}

async fn begin_phase(pool: &sqlx::PgPool) -> PhaseStart {
    PhaseStart {
        started: Instant::now(),
        wal_before: wal_snapshot(pool).await,
    }
}

async fn finish_phase(pool: &sqlx::PgPool, start: PhaseStart) -> PhaseMeasurement {
    let after = wal_snapshot(pool).await;
    let lsn_bytes = match (&start.wal_before.current_lsn, &after.current_lsn) {
        (Some(before), Some(after)) => sqlx::query_scalar::<_, f64>(
            "SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::double precision",
        )
        .bind(after)
        .bind(before)
        .fetch_one(pool)
        .await
        .ok(),
        _ => None,
    };
    let pg_stat_wal_bytes = match (start.wal_before.pg_stat_wal_bytes, after.pg_stat_wal_bytes) {
        (Some(before), Some(after)) => Some(after - before),
        _ => None,
    };
    PhaseMeasurement {
        wall_ms: start.started.elapsed().as_millis() as u64,
        wal: WalDelta {
            before: start.wal_before,
            after,
            lsn_bytes,
            pg_stat_wal_bytes,
        },
    }
}

async fn compress_fixture_chunks(pool: &sqlx::PgPool, recompress: bool) -> TestResult<()> {
    sqlx::query(
        "SELECT compress_chunk(c, if_not_compressed => true, recompress => $1) \
         FROM show_chunks('core.events') AS c",
    )
    .bind(recompress)
    .execute(pool)
    .await?;
    Ok(())
}

async fn count_material_rows(
    pool: &sqlx::PgPool,
    table: &str,
    material_id: uuid::Uuid,
) -> TestResult<i64> {
    let query = match table {
        "core.events" => {
            "SELECT count(*)::bigint FROM core.events WHERE source_material_id = $1::uuid"
        }
        "audit.archived_events" => {
            "SELECT count(*)::bigint FROM audit.archived_events WHERE source_material_id = $1::uuid"
        }
        _ => unreachable!("measurement only permits the live and archive event tables"),
    };
    Ok(sqlx::query_scalar(query)
        .bind(material_id)
        .fetch_one(pool)
        .await?)
}

async fn archive_scoped_replay_roots(
    pool: &sqlx::PgPool,
    scope: &ReplayScope,
    execution_window: (sinex_primitives::Timestamp, sinex_primitives::Timestamp),
) -> TestResult<(i64, usize, u64)> {
    let operation_id = sinex_primitives::Uuid::now_v7().to_string();
    let session_id = format!(
        "y0o3_measurement_{}",
        sinex_primitives::Uuid::now_v7().simple()
    );
    let result = pool
        .with_transaction(async |tx| {
            sqlx::query("LOCK TABLE core.events IN SHARE MODE")
                .execute(&mut **tx)
                .await?;
            let mut repo_tx = EventRepositoryTx::new(tx);
            let table_name = repo_tx.prepare_cascade_session(&session_id, false).await?;
            let direct_roots = repo_tx
                .populate_cascade_roots_for_replay_scope(&table_name, scope, execution_window)
                .await?;
            repo_tx.expand_cascade(&table_name, 64).await?;
            let cascade_ids = repo_tx.get_cascade_ids(&table_name).await?;
            let archived = repo_tx
                .execute_cascade_archive(
                    &cascade_ids,
                    "y0o3.10.1 bounded compressed scoped replay measurement",
                    &operation_id,
                    "test",
                )
                .await?;
            repo_tx.cleanup_cascade_session(&table_name).await?;
            Ok((direct_roots, cascade_ids.len(), archived))
        })
        .await?;
    Ok(result)
}

async fn run_bounded_replay_measurement(
    ctx: &TestContext,
    fixture_events: u64,
) -> TestResult<BoundedReplayMeasurement> {
    let material_id = ctx
        .create_source_material(Some("y0o3-10-1-compressed-replay-measurement"))
        .await?;
    let material_uuid = *material_id.as_uuid();
    let source = "y0o3.measurement";
    let event_type = "y0o3.compressed_scoped_replay_probe";
    let execution_window = (
        sinex_primitives::Timestamp::now() - time::Duration::hours(1),
        sinex_primitives::Timestamp::now() + time::Duration::hours(1),
    );

    sqlx::query("SELECT set_chunk_time_interval('core.events', INTERVAL '1 millisecond')")
        .execute(&ctx.pool)
        .await?;

    let insert_phase_start = begin_phase(&ctx.pool).await;
    let burst_size = fixture_events.div_ceil(Y0O3_MEASUREMENT_INSERT_BURSTS);
    for burst in 0..Y0O3_MEASUREMENT_INSERT_BURSTS {
        let start = burst * burst_size;
        let end = (start + burst_size).min(fixture_events);
        if start == end {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let events = (start..end)
            .map(|index| {
                DynamicPayload::new(
                    source,
                    event_type,
                    serde_json::json!({ "fixture_events": fixture_events, "index": index }),
                )
                .from_material(material_id)
                .build()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let inserted = ctx.pool.events().insert_batch(events).await?;
        assert_eq!(
            inserted.len() as u64,
            end - start,
            "fixture insert must persist every burst row"
        );
    }
    let fixture_insert = finish_phase(&ctx.pool, insert_phase_start).await;
    let chunks_before_compression = chunk_states(&ctx.pool).await?;
    assert!(
        chunks_before_compression.len() >= 3,
        "the 1 ms interval plus eight natural UUIDv7 insertion bursts must create a multi-chunk fixture: {chunks_before_compression:?}"
    );

    let compression_phase_start = begin_phase(&ctx.pool).await;
    compress_fixture_chunks(&ctx.pool, false).await?;
    let compression = finish_phase(&ctx.pool, compression_phase_start).await;
    let chunks_after_compression = chunk_states(&ctx.pool).await?;
    assert!(
        chunks_after_compression
            .iter()
            .all(|chunk| chunk.is_compressed),
        "every fixture chunk must be compressed before scoped replay: {chunks_after_compression:?}"
    );

    let scope = ReplayScope {
        source_name: source.to_string(),
        material_filter: Some(vec![material_uuid]),
        filters: std::collections::HashMap::from([(
            "event_types".to_string(),
            serde_json::json!([event_type]),
        )]),
        ..Default::default()
    };
    let live_rows_before_replay =
        count_material_rows(&ctx.pool, "core.events", material_uuid).await?;
    let replay_phase_start = begin_phase(&ctx.pool).await;
    let (direct_replay_roots, cascade_rows, archived_rows) =
        archive_scoped_replay_roots(&ctx.pool, &scope, execution_window).await?;
    let scoped_replay_archive = finish_phase(&ctx.pool, replay_phase_start).await;
    let chunks_after_replay = chunk_states(&ctx.pool).await?;
    let live_rows_after_replay =
        count_material_rows(&ctx.pool, "core.events", material_uuid).await?;
    let archived_rows_after_replay =
        count_material_rows(&ctx.pool, "audit.archived_events", material_uuid).await?;

    let recompression_phase_start = begin_phase(&ctx.pool).await;
    compress_fixture_chunks(&ctx.pool, true).await?;
    let recompression = finish_phase(&ctx.pool, recompression_phase_start).await;
    let chunks_after_recompression = chunk_states(&ctx.pool).await?;

    assert_eq!(live_rows_before_replay, fixture_events as i64);
    assert_eq!(direct_replay_roots, fixture_events as i64);
    assert_eq!(cascade_rows, fixture_events as usize);
    assert_eq!(archived_rows, fixture_events);
    assert_eq!(live_rows_after_replay, 0);
    assert_eq!(archived_rows_after_replay, fixture_events as i64);
    assert!(
        chunks_after_recompression
            .iter()
            .all(|chunk| chunk.is_compressed),
        "recompression must leave every fixture chunk compressed: {chunks_after_recompression:?}"
    );

    Ok(BoundedReplayMeasurement {
        fixture_events,
        chunk_interval: Y0O3_MEASUREMENT_CHUNK_INTERVAL,
        live_rows_before_replay,
        direct_replay_roots,
        cascade_rows,
        archived_rows,
        live_rows_after_replay,
        archived_rows_after_replay,
        chunks_before_compression,
        chunks_after_compression,
        chunks_after_replay,
        chunks_after_recompression,
        fixture_insert,
        compression,
        scoped_replay_archive,
        recompression,
    })
}

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

/// Bounded pre-wipe evidence for the database half of the production replay
/// archive path. Each sample is synthetic and checkout-local: `sinex_test`
/// gives it a fresh dev database, never the production database. The harness
/// exercises the production replay scope query, cascade session, archive
/// trigger, and chunk recompression. It deliberately stops before source-host
/// re-scanning and confirmed publishing, which require a running daemon.
#[sinex_test]
async fn compressed_chunk_scoped_replay_cost_is_measurable(ctx: TestContext) -> TestResult<()> {
    let mut measurements = Vec::with_capacity(Y0O3_MEASUREMENT_SIZES.len());
    for fixture_events in Y0O3_MEASUREMENT_SIZES {
        measurements.push(run_bounded_replay_measurement(&ctx, fixture_events).await?);
    }
    println!(
        "y0o3_10_1_compressed_scoped_replay_measurement {}",
        serde_json::json!({
            "measurement": "sinex-y0o3.10.1",
            "database": ctx.database_name(),
            "sample_sizes": Y0O3_MEASUREMENT_SIZES,
            "results": measurements,
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
