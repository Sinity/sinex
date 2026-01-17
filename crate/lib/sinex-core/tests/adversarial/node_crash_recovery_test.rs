//! Adversarial tests for node crash recovery during material acquisition.
//!
//! These scenarios exercise Stage-as-You-Go’s JetStream material pipeline when
//! nodes crash mid-acquisition. Since nodes no longer write directly
//! to Postgres, we stand up a test `ingestd` to consume begin/slice/end messages
//! and persist registry state.

use serde_json::json;
use sinex_core::db::repositories::source_materials::status;
use sinex_node_sdk::{
    AcquisitionManager, Checkpoint, CheckpointManager, CheckpointState, RotationPolicy,
};
use sinex_schema::ulid::Ulid;
use sinex_test_utils::{
    prelude::*, start_test_ingestd_with_config, TestIngestdConfig, TestIngestdHandle,
};
use sqlx::Row;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

async fn setup_ingestd(
    ctx: TestContext,
) -> Result<(TestContext, TestIngestdHandle, async_nats::Client)> {
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let ingest_config = TestIngestdConfig {
        nats: ctx.nats_handle()?.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    sleep(Duration::from_millis(200)).await;
    Ok((ctx, ingest_handle, nats_client))
}

async fn wait_for_material_row(
    ctx: &TestContext,
    material_id: Ulid,
) -> Result<sqlx::postgres::PgRow> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query(
            r#"
            SELECT status, optional_blob_id
            FROM raw.source_material_registry
            WHERE id = $1::uuid::ulid
            "#,
        )
        .bind(Uuid::from(material_id))
        .fetch_optional(&ctx.pool)
        .await?;

        if let Some(row) = row {
            return Ok(row);
        }
        if Instant::now() > deadline {
            panic!("material {} was never registered by ingestd", material_id);
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Node crashes immediately after registering a material (begin published, no slices/end).
#[sinex_test]
async fn test_crash_during_early_material_acquisition(ctx: TestContext) -> Result<()> {
    let (ctx, mut ingest_handle, nats_client) = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new(
        nats_client.clone(),
        RotationPolicy::default(),
        "crash_early_stream".to_string(),
        "/test/crash_early.log".to_string(),
    );

    let handle = acquisition_mgr.begin_material("crash-early-source").await?;
    let material_id = handle.material_id;

    // SIMULATE CRASH: Drop handle without finalizing
    drop(handle);

    let row = wait_for_material_row(&ctx, material_id).await?;
    let status_val: String = row.try_get("status")?;
    let optional_blob_id: Option<Ulid> = row.try_get("optional_blob_id")?;

    assert_eq!(status_val, status::SENSING);
    assert!(optional_blob_id.is_none());

    let ledger_count: Option<i64> = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.temporal_ledger
        WHERE source_material_id = $1::uuid::ulid
        "#,
        Uuid::from(material_id)
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(ledger_count.unwrap_or(0), 0, "no ledger until finalized");

    ingest_handle.stop().await?;
    Ok(())
}

/// Node crashes mid-acquisition after several slices (no end).
#[sinex_test]
async fn test_crash_during_mid_material_acquisition(ctx: TestContext) -> Result<()> {
    let (ctx, mut ingest_handle, nats_client) = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new(
        nats_client.clone(),
        RotationPolicy::default(),
        "crash_mid_stream".to_string(),
        "/test/crash_mid.log".to_string(),
    );

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
    let optional_blob_id: Option<Ulid> = row.try_get("optional_blob_id")?;

    assert_eq!(status_val, status::SENSING);
    assert!(optional_blob_id.is_none());

    let ledger_count: Option<i64> = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.temporal_ledger
        WHERE source_material_id = $1::uuid::ulid
        "#,
        Uuid::from(material_id)
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(ledger_count.unwrap_or(0), 0, "no ledger until finalized");

    ingest_handle.stop().await?;
    Ok(())
}

/// Detect orphaned materials left sensing after multiple nodes crash.
#[sinex_test]
async fn test_orphaned_material_detection_and_recovery(ctx: TestContext) -> Result<()> {
    let (ctx, mut ingest_handle, nats_client) = setup_ingestd(ctx).await?;

    let acq_mgr1 = AcquisitionManager::new(
        nats_client.clone(),
        RotationPolicy::default(),
        "orphan_stream_1".to_string(),
        "/test/orphan1.log".to_string(),
    );
    let acq_mgr2 = AcquisitionManager::new(
        nats_client.clone(),
        RotationPolicy::default(),
        "orphan_stream_2".to_string(),
        "/test/orphan2.log".to_string(),
    );

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
        AND id IN ($2::uuid::ulid, $3::uuid::ulid)
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

    let material_id = Ulid::new();
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
    let (ctx, mut ingest_handle, nats_client) = setup_ingestd(ctx).await?;

    let successful_materials = Arc::new(AtomicU64::new(0));
    let crashed_materials = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::new();

    for worker_id in 0..20_u64 {
        let nats_clone = nats_client.clone();
        let success_count = successful_materials.clone();
        let crash_count = crashed_materials.clone();

        joins.push(tokio::spawn(async move {
            let acquisition_mgr = AcquisitionManager::new(
                nats_clone,
                RotationPolicy::default(),
                format!("concurrent_stream_{worker_id}"),
                format!("/test/concurrent_{worker_id}.log"),
            );

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

    // Poll until ingestd persists all statuses.
    let deadline = Instant::now() + Duration::from_secs(10);
    let (completed_count, sensing_count) = loop {
        let completed: Option<i64> = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) FROM raw.source_material_registry
            WHERE status = $1 AND source_identifier LIKE 'concurrent-source-%'
            "#,
            status::COMPLETED
        )
        .fetch_one(&ctx.pool)
        .await?;
        let sensing: Option<i64> = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) FROM raw.source_material_registry
            WHERE status = $1 AND source_identifier LIKE 'concurrent-source-%'
            "#,
            status::SENSING
        )
        .fetch_one(&ctx.pool)
        .await?;

        let completed = completed.unwrap_or(0) as u64;
        let sensing = sensing.unwrap_or(0) as u64;
        if completed == successful && sensing == crashed {
            break (completed, sensing);
        }
        if Instant::now() > deadline {
            panic!("ingestd did not settle counts (completed={completed} sensing={sensing})");
        }
        sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(completed_count, successful);
    assert_eq!(sensing_count, crashed);

    let ledger_count: Option<i64> = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.temporal_ledger tl
        JOIN raw.source_material_registry smr ON tl.source_material_id = smr.id
        WHERE smr.source_identifier LIKE 'concurrent-source-%'
        "#
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(ledger_count.unwrap_or(0) as u64, successful);

    ingest_handle.stop().await?;
    Ok(())
}

/// Crash after data written but before finalize sends end message.
#[sinex_test]
async fn test_crash_during_finalization(ctx: TestContext) -> Result<()> {
    let (ctx, mut ingest_handle, nats_client) = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new(
        nats_client,
        RotationPolicy::default(),
        "crash_finalize_stream".to_string(),
        "/test/crash_finalize.log".to_string(),
    );

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
    let optional_blob_id: Option<Ulid> = row.try_get("optional_blob_id")?;

    assert_eq!(status_val, status::SENSING);
    assert!(optional_blob_id.is_none());

    let ledger_count: Option<i64> = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM raw.temporal_ledger
        WHERE source_material_id = $1::uuid::ulid
        "#,
        Uuid::from(material_id)
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(ledger_count.unwrap_or(0), 0);

    ingest_handle.stop().await?;
    Ok(())
}

/// Mark a crashed material as recovered_partial with recovery metadata.
#[sinex_test]
async fn test_marking_crashed_materials_as_recovered_partial(ctx: TestContext) -> Result<()> {
    let (ctx, mut ingest_handle, nats_client) = setup_ingestd(ctx).await?;

    let acquisition_mgr = AcquisitionManager::new(
        nats_client,
        RotationPolicy::default(),
        "recovery_stream".to_string(),
        "/test/recovery.log".to_string(),
    );

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
            metadata = metadata || jsonb_build_object(
                'recovery_info', jsonb_build_object(
                    'recovered_at', NOW(),
                    'recovery_reason', 'node_crash',
                    'original_status', 'sensing'
                )
            )
        WHERE id = $2::uuid::ulid
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
        WHERE id = $1::uuid::ulid
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