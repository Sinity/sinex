//! Adversarial tests for node crash recovery during material acquisition.
//!
//! These scenarios exercise Stage-as-You-Go’s `JetStream` material pipeline when
//! nodes crash mid-acquisition. Since nodes no longer write directly
//! to Postgres, we stand up a test `ingestd` to consume begin/slice/end messages
//! and persist registry state.

use serde_json::json;
use sinex_db::repositories::material_status as status;
use sinex_node_sdk::{
    AcquisitionManager, Checkpoint, CheckpointManager, CheckpointState, RotationPolicy,
};
use sinex_primitives::Uuid;
use sqlx::Row;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use xtask::sandbox::{
    TestIngestdConfig, TestIngestdHandle, prelude::*, start_test_ingestd_with_config,
    timing::Timeouts,
};

/// Return type for `setup_ingestd` — holds ownership of the work directory
/// so it isn't cleaned up while ingestd is still running.
struct IngestdSetup {
    ctx: TestContext,
    ingest_handle: TestIngestdHandle,
    nats_client: async_nats::Client,
    namespace: String,
    _work_dir: tempfile::TempDir,
}

async fn setup_ingestd(ctx: TestContext) -> Result<IngestdSetup> {
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();

    AcquisitionManager::bootstrap_streams_with_namespace(&nats_client, Some(&namespace)).await?;

    let work_dir = tempfile::tempdir()?;
    let ingest_config = TestIngestdConfig {
        nats: ctx.nats_handle()?.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir.path().to_path_buf()),
        namespace: Some(namespace.clone()),
        ..Default::default()
    };

    let ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Wait for ingestd's MaterialAssembler to attach a consumer on the BEGIN stream.
    // start_test_ingestd_with_config already waits for the RAW_EVENTS consumer, but
    // the MaterialAssembler starts slightly after. Without this, begin_material()
    // messages may arrive before the assembler is consuming, causing wait_for_material_row
    // to time out.
    let nats = ctx.nats_handle()?;
    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let begin_stream =
        env.nats_stream_name_with_namespace(Some(&namespace), "SOURCE_MATERIAL_BEGIN");
    nats.wait_for_consumer_on_stream(&js, &begin_stream, Duration::from_secs(Timeouts::STANDARD))
        .await?;

    Ok(IngestdSetup {
        ctx,
        ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    })
}

async fn wait_for_material_row(
    ctx: &TestContext,
    material_id: Uuid,
) -> Result<sqlx::postgres::PgRow> {
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                Ok::<bool, sqlx::Error>(
                    sqlx::query(
                        r"
                        SELECT 1
                        FROM raw.source_material_registry
                        WHERE id = $1::uuid
                        ",
                    )
                    .bind(material_id)
                    .fetch_optional(&pool)
                    .await?
                    .is_some(),
                )
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    sqlx::query(
        r"
        SELECT status, optional_blob_id
        FROM raw.source_material_registry
        WHERE id = $1::uuid
        ",
    )
    .bind(material_id)
    .fetch_one(&ctx.pool)
    .await
    .map_err(Into::into)
}

async fn ledger_count(
    ctx: &TestContext,
    material_id: Uuid,
    source_type: Option<&str>,
) -> Result<i64> {
    let count = match source_type {
        Some(source_type) => {
            sqlx::query_scalar!(
                r#"
                SELECT COUNT(*) FROM raw.temporal_ledger
                WHERE source_material_id = $1::uuid
                  AND source_type = $2
                "#,
                Uuid::from(material_id),
                source_type
            )
            .fetch_one(&ctx.pool)
            .await?
        }
        None => {
            sqlx::query_scalar!(
                r#"
                SELECT COUNT(*) FROM raw.temporal_ledger
                WHERE source_material_id = $1::uuid
                "#,
                Uuid::from(material_id)
            )
            .fetch_one(&ctx.pool)
            .await?
        }
    };

    Ok(count.unwrap_or(0))
}

async fn wait_for_material_ledger_counts(
    ctx: &TestContext,
    material_id: Uuid,
    expected_staged_at: i64,
    expected_realtime_capture: i64,
) -> Result<()> {
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let staged_at = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) FROM raw.temporal_ledger
                    WHERE source_material_id = $1::uuid
                      AND source_type = 'staged_at'
                    "#,
                    Uuid::from(material_id)
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0);
                let realtime_capture = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) FROM raw.temporal_ledger
                    WHERE source_material_id = $1::uuid
                      AND source_type = 'realtime_capture'
                    "#,
                    Uuid::from(material_id)
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0);

                Ok::<bool, sqlx::Error>(
                    staged_at == expected_staged_at
                        && realtime_capture == expected_realtime_capture,
                )
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    Ok(())
}

/// Node crashes immediately after registering a material (begin published, no slices/end).
#[sinex_test]
async fn test_crash_during_early_material_acquisition(ctx: TestContext) -> Result<()> {
    let IngestdSetup {
        ctx,
        mut ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    } = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new_with_namespace(
        nats_client.clone(),
        RotationPolicy::default(),
        "crash_early_stream".to_string(),
        Some(namespace.clone()),
    )
    .with_work_dir(work_dir.path().join("crash_early"));

    let handle = acquisition_mgr.begin_material("crash-early-source").await?;
    let material_id = handle.material_id;

    // SIMULATE CRASH: Drop handle without finalizing
    drop(handle);

    let row = wait_for_material_row(&ctx, material_id).await?;
    let status_val: String = row.try_get("status")?;
    let optional_blob_id: Option<Uuid> = row.try_get("optional_blob_id")?;

    assert_eq!(status_val, status::SENSING);
    assert!(optional_blob_id.is_none());

    wait_for_material_ledger_counts(&ctx, material_id, 1, 0).await?;
    assert_eq!(ledger_count(&ctx, material_id, None).await?, 1);
    assert_eq!(ledger_count(&ctx, material_id, Some("staged_at")).await?, 1);
    assert_eq!(
        ledger_count(&ctx, material_id, Some("realtime_capture")).await?,
        0
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Node crashes mid-acquisition after several slices (no end).
#[sinex_test]
async fn test_crash_during_mid_material_acquisition(ctx: TestContext) -> Result<()> {
    let IngestdSetup {
        ctx,
        mut ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    } = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new_with_namespace(
        nats_client.clone(),
        RotationPolicy::default(),
        "crash_mid_stream".to_string(),
        Some(namespace.clone()),
    )
    .with_work_dir(work_dir.path().join("crash_mid"));

    let mut handle = acquisition_mgr.begin_material("crash-mid-source").await?;
    let material_id = handle.material_id;

    let chunk1 = b"First chunk of data\n";
    let chunk2 = b"Second chunk of data\n";
    let chunk3 = b"Third chunk of data\n";
    acquisition_mgr.append_slice(&mut handle, chunk1).await?;
    acquisition_mgr.append_slice(&mut handle, chunk2).await?;
    acquisition_mgr.append_slice(&mut handle, chunk3).await?;

    drop(handle);

    let row = wait_for_material_row(&ctx, material_id).await?;
    let status_val: String = row.try_get("status")?;
    let optional_blob_id: Option<Uuid> = row.try_get("optional_blob_id")?;

    assert_eq!(status_val, status::SENSING);
    assert!(optional_blob_id.is_none());

    wait_for_material_ledger_counts(&ctx, material_id, 1, 0).await?;
    assert_eq!(ledger_count(&ctx, material_id, None).await?, 1);
    assert_eq!(ledger_count(&ctx, material_id, Some("staged_at")).await?, 1);
    assert_eq!(
        ledger_count(&ctx, material_id, Some("realtime_capture")).await?,
        0
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Detect orphaned materials left sensing after multiple nodes crash.
#[sinex_test]
async fn test_orphaned_material_detection_and_recovery(ctx: TestContext) -> Result<()> {
    let IngestdSetup {
        ctx,
        mut ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    } = setup_ingestd(ctx).await?;

    let acq_mgr1 = AcquisitionManager::new_with_namespace(
        nats_client.clone(),
        RotationPolicy::default(),
        "orphan_stream_1".to_string(),
        Some(namespace.clone()),
    )
    .with_work_dir(work_dir.path().join("orphan1"));
    let acq_mgr2 = AcquisitionManager::new_with_namespace(
        nats_client.clone(),
        RotationPolicy::default(),
        "orphan_stream_2".to_string(),
        Some(namespace.clone()),
    )
    .with_work_dir(work_dir.path().join("orphan2"));

    let mut handle1 = acq_mgr1.begin_material("orphan-source-1").await?;
    let material_id1 = handle1.material_id;
    let mut handle2 = acq_mgr2.begin_material("orphan-source-2").await?;
    let material_id2 = handle2.material_id;

    acq_mgr1
        .append_slice(&mut handle1, b"Material 1 data\n")
        .await?;
    acq_mgr2
        .append_slice(&mut handle2, b"Material 2 data\n")
        .await?;

    drop(handle1);
    drop(handle2);

    // Wait for both registry rows to land
    let _ = wait_for_material_row(&ctx, material_id1).await?;
    let _ = wait_for_material_row(&ctx, material_id2).await?;

    let orphaned_materials = sqlx::query!(
        r#"
        SELECT id::uuid as id, source_identifier, status
        FROM raw.source_material_registry
        WHERE status = $1
        AND id IN ($2::uuid, $3::uuid)
        ORDER BY staged_at
        "#,
        status::SENSING,
        Uuid::from(material_id1),
        Uuid::from(material_id2)
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(orphaned_materials.len(), 2);

    ingest_handle.stop().await?;
    Ok(())
}

/// Checkpoint manager round-trips a material+offset external checkpoint.
#[sinex_test]
async fn test_checkpoint_recovery_with_material_reference(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let kv = ctx.checkpoint_kv().await?;
    let checkpoint_mgr = CheckpointManager::new(
        kv,
        "crash-node".to_string(),
        "default".to_string(),
        "test-instance".to_string(),
    );

    let material_id = Uuid::now_v7();
    let offset: i64 = 2048;

    let mut state = CheckpointState::default();
    state.checkpoint = Checkpoint::External {
        position: json!({
            "current_material_id": material_id.to_string(),
            "offset": offset,
            "bytes_written": 1024
        }),
        description: "Mid-acquisition before crash".to_string(),
    };
    state.processed_count = 42;

    checkpoint_mgr.save_checkpoint(&state).await?;
    let recovered = checkpoint_mgr.load_checkpoint().await?;

    match recovered.checkpoint {
        Checkpoint::External {
            position,
            description,
        } => {
            assert!(description.contains("Mid-acquisition"));
            let recovered_material_id = position["current_material_id"]
                .as_str()
                .expect("material id");
            assert_eq!(recovered_material_id, material_id.to_string());
            let recovered_offset = position["offset"].as_i64().expect("offset");
            assert_eq!(recovered_offset, offset);
        }
        _ => panic!("expected External checkpoint"),
    }

    assert_eq!(recovered.processed_count, 42);
    Ok(())
}

/// Concurrent acquisitions where half crash and half finalize cleanly.
#[sinex_test]
async fn test_concurrent_material_acquisition_with_random_crashes(ctx: TestContext) -> Result<()> {
    let IngestdSetup {
        ctx,
        mut ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    } = setup_ingestd(ctx).await?;

    let successful_materials = Arc::new(AtomicU64::new(0));
    let crashed_materials = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::new();

    for worker_id in 0..20_u64 {
        let nats_clone = nats_client.clone();
        let success_count = successful_materials.clone();
        let crash_count = crashed_materials.clone();
        let ns = namespace.clone();
        let worker_work_dir = work_dir.path().join(format!("concurrent_{worker_id}"));

        joins.push(tokio::spawn(async move {
            let acquisition_mgr = AcquisitionManager::new_with_namespace(
                nats_clone,
                RotationPolicy::default(),
                format!("concurrent_stream_{worker_id}"),
                Some(ns),
            )
            .with_work_dir(worker_work_dir);

            let mut handle = acquisition_mgr
                .begin_material(&format!("concurrent-source-{worker_id}"))
                .await?;
            acquisition_mgr
                .append_slice(
                    &mut handle,
                    format!("Data from worker {worker_id}\n").as_bytes(),
                )
                .await?;

            if worker_id % 2 == 0 {
                crash_count.fetch_add(1, Ordering::SeqCst);
                drop(handle);
            } else {
                acquisition_mgr.finalize(handle, "test-success").await?;
                success_count.fetch_add(1, Ordering::SeqCst);
            }

            Result::<()>::Ok(())
        }));
    }

    for join in joins {
        join.await??;
    }

    let successful = successful_materials.load(Ordering::SeqCst);
    let crashed = crashed_materials.load(Ordering::SeqCst);
    assert_eq!(successful + crashed, 20);

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let completed = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) FROM raw.source_material_registry
                    WHERE status = $1 AND source_identifier LIKE 'concurrent-source-%'
                    "#,
                    status::COMPLETED
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0) as u64;
                let sensing = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) FROM raw.source_material_registry
                    WHERE status = $1 AND source_identifier LIKE 'concurrent-source-%'
                    "#,
                    status::SENSING
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0) as u64;

                Ok::<bool, sqlx::Error>(completed == successful && sensing == crashed)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let completed_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.source_material_registry
        WHERE status = $1 AND source_identifier LIKE 'concurrent-source-%'
        "#,
        status::COMPLETED
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0) as u64;
    let sensing_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.source_material_registry
        WHERE status = $1 AND source_identifier LIKE 'concurrent-source-%'
        "#,
        status::SENSING
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0) as u64;

    assert_eq!(completed_count, successful);
    assert_eq!(sensing_count, crashed);

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let staged_at = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) FROM raw.temporal_ledger tl
                    JOIN raw.source_material_registry smr ON tl.source_material_id = smr.id
                    WHERE smr.source_identifier LIKE 'concurrent-source-%'
                      AND tl.source_type = 'staged_at'
                    "#
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0) as u64;
                let realtime_capture = sqlx::query_scalar!(
                    r#"
                    SELECT COUNT(*) FROM raw.temporal_ledger tl
                    JOIN raw.source_material_registry smr ON tl.source_material_id = smr.id
                    WHERE smr.source_identifier LIKE 'concurrent-source-%'
                      AND tl.source_type = 'realtime_capture'
                    "#
                )
                .fetch_one(&pool)
                .await?
                .unwrap_or(0) as u64;

                Ok::<bool, sqlx::Error>(
                    staged_at == successful + crashed && realtime_capture == successful,
                )
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let total_ledger_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.temporal_ledger tl
        JOIN raw.source_material_registry smr ON tl.source_material_id = smr.id
        WHERE smr.source_identifier LIKE 'concurrent-source-%'
        "#
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0) as u64;
    assert_eq!(total_ledger_count, successful + crashed + successful);

    ingest_handle.stop().await?;
    Ok(())
}

/// Crash after data written but before finalize sends end message.
#[sinex_test]
async fn test_crash_during_finalization(ctx: TestContext) -> Result<()> {
    let IngestdSetup {
        ctx,
        mut ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    } = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new_with_namespace(
        nats_client,
        RotationPolicy::default(),
        "crash_finalize_stream".to_string(),
        Some(namespace.clone()),
    )
    .with_work_dir(work_dir.path().join("crash_finalize"));

    let mut handle = acquisition_mgr
        .begin_material("crash-finalize-source")
        .await?;
    let material_id = handle.material_id;

    acquisition_mgr
        .append_slice(&mut handle, b"Complete dataset\n")
        .await?;

    drop(handle);

    let row = wait_for_material_row(&ctx, material_id).await?;
    let status_val: String = row.try_get("status")?;
    let optional_blob_id: Option<Uuid> = row.try_get("optional_blob_id")?;

    assert_eq!(status_val, status::SENSING);
    assert!(optional_blob_id.is_none());

    wait_for_material_ledger_counts(&ctx, material_id, 1, 0).await?;
    assert_eq!(ledger_count(&ctx, material_id, None).await?, 1);
    assert_eq!(ledger_count(&ctx, material_id, Some("staged_at")).await?, 1);
    assert_eq!(
        ledger_count(&ctx, material_id, Some("realtime_capture")).await?,
        0
    );

    ingest_handle.stop().await?;
    Ok(())
}

/// Mark a crashed material as recovered_partial with recovery metadata.
#[sinex_test]
async fn test_marking_crashed_materials_as_recovered_partial(ctx: TestContext) -> Result<()> {
    let IngestdSetup {
        ctx,
        mut ingest_handle,
        nats_client,
        namespace,
        _work_dir: work_dir,
    } = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new_with_namespace(
        nats_client,
        RotationPolicy::default(),
        "recovery_stream".to_string(),
        Some(namespace.clone()),
    )
    .with_work_dir(work_dir.path().join("recovery"));

    let mut handle = acquisition_mgr.begin_material("recovery-source").await?;
    let material_id = handle.material_id;
    acquisition_mgr
        .append_slice(&mut handle, b"Partial data before crash\n")
        .await?;
    drop(handle);

    let _ = wait_for_material_row(&ctx, material_id).await?;

    sqlx::query!(
        r#"
        UPDATE raw.source_material_registry
        SET status = $1,
            metadata = core.jsonb_merge_deep(metadata, jsonb_build_object(
                'recovery_info', jsonb_build_object(
                    'recovered_at', NOW(),
                    'recovery_reason', 'node_crash',
                    'original_status', 'sensing'
                )
            ))
        WHERE id = $2::uuid
        "#,
        status::RECOVERED_PARTIAL,
        Uuid::from(material_id)
    )
    .execute(&ctx.pool)
    .await?;

    let recovered_material = sqlx::query!(
        r#"
        SELECT status, metadata
        FROM raw.source_material_registry
        WHERE id = $1::uuid
        "#,
        Uuid::from(material_id)
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(recovered_material.status, status::RECOVERED_PARTIAL);
    assert!(
        recovered_material.metadata["recovery_info"].is_object(),
        "expected recovery metadata"
    );

    ingest_handle.stop().await?;
    Ok(())
}
