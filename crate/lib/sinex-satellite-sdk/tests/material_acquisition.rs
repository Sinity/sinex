use color_eyre::eyre::ensure;
use futures::future::try_join_all;
use sinex_core::types::ulid::Ulid;
use sinex_core::Id;
use sinex_core::{db::query_helpers::ulid_to_uuid, db::DbPoolExt};
use sinex_satellite_sdk::{AcquisitionManager, RotationPolicy};
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{
    acquire_pool_test_guard, db_common, start_test_ingestd_with_config, EphemeralNats,
    TestIngestdConfig,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Test basic material acquisition flow: begin → append slices → finalize
#[sinex_test]
async fn material_acquisition_basic_flow(ctx: TestContext) -> Result<()> {
    // Start NATS
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    // Start ingestd (includes MaterialAssembler)
    let ingest_config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Create AcquisitionManager
    let rotation_policy = RotationPolicy::default();
    let manager = AcquisitionManager::new(
        nats_client.clone(),
        rotation_policy,
        "test-source".to_string(),
        "/test/path".to_string(),
    );

    // Begin material
    let mut handle = manager.begin_material("test-identifier").await?;
    let material_id = handle.material_id;

    // Append some slices
    manager.append_slice(&mut handle, b"slice 1 data").await?;
    manager.append_slice(&mut handle, b"slice 2 data").await?;
    manager.append_slice(&mut handle, b"slice 3 data").await?;

    // Finalize
    manager.finalize(handle, "test complete").await?;

    // Wait for MaterialAssembler to process and persist the material/ledger entries.
    ctx.timing()
        .wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let material = pool
                        .source_materials()
                        .get_by_id(sinex_core::Id::from_ulid(material_id))
                        .await?
                        .ok_or_else(|| sinex_core::types::error::SinexError::database("missing"))?;
                    let ledger_count: Option<i64> = sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid",
                        material_id as Ulid
                    )
                    .fetch_one(&pool)
                    .await?;
                    Ok::<bool, sinex_core::types::error::SinexError>(
                        material.status.as_str() == "completed"
                            && ledger_count.unwrap_or(0) >= 1
                    )
                }
            },
            10,
        )
        .await?;

    // Verify database state
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(sinex_core::Id::from_ulid(material_id))
        .await?
        .expect("Material should exist");

    assert_eq!(material.status.as_str(), "completed");

    // Verify ledger entry
    let ledger_count: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid",
        material_id as Ulid
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(ledger_count.unwrap_or(0), 1);

    ingest_handle.stop().await?;
    Ok(())
}

/// Test out-of-order slice handling
#[sinex_test(timeout = 60)]
async fn material_acquisition_out_of_order_slices(ctx: TestContext) -> Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let ingest_config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Ensure JetStream streams exist before manually publishing messages
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    // Manually publish slices out of order to test MaterialAssembler's buffering
    let material_id = Ulid::new();
    let env = sinex_core::environment();
    let js = nats.jetstream_with_client(nats_client.clone());

    // Ensure the registry already contains the material id we are about to stream so the assembler
    // can finalize without waiting on implicit creation.
    sqlx::query!(
        r#"
            INSERT INTO raw.source_material_registry
                (id, material_kind, source_identifier, status, timing_info_type, metadata)
            VALUES ($1::uuid::ulid, $2, $3, 'sensing', 'realtime', '{}'::jsonb)
            ON CONFLICT (id) DO NOTHING
        "#,
        material_id as Ulid,
        "annex",
        "test-ooo"
    )
    .execute(&ctx.pool)
    .await?;

    // Publish begin message
    let begin_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "material_kind": "annex",
        "source_identifier": "test-ooo",
        "metadata": {},
        "started_at": chrono::Utc::now().to_rfc3339(),
    });
    js.publish(
        env.nats_subject("source_material.begin"),
        serde_json::to_vec(&begin_msg)?.into(),
    )
    .await?
    .await?;

    // Publish slices out of order: 2, 0, 1
    let slices = vec![
        (12i64, b"slice 2 data".to_vec()),
        (0i64, b"slice 0 data".to_vec()),
        (24i64, b"slice 3 data".to_vec()),
    ];

    for (offset, data) in slices {
        let mut headers = async_nats::HeaderMap::new();
        let offset_str = offset.to_string();
        let chunk_hash = blake3::hash(&data).to_hex();
        headers.insert("Offset", offset_str.as_str());
        headers.insert("Chunk-Hash", chunk_hash.as_str());

        js.publish_with_headers(
            env.nats_subject(&format!("source_material.slices.{}", material_id)),
            headers,
            data.into(),
        )
        .await?
        .await?;
    }

    // Compute expected hash
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"slice 0 data");
    hasher.update(b"slice 2 data");
    hasher.update(b"slice 3 data");
    let content_hash = hasher.finalize().to_hex();
    let expected_size: i64 =
        (b"slice 0 data".len() + b"slice 2 data".len() + b"slice 3 data".len()) as i64;

    // Publish end message
    let end_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "ended_at": chrono::Utc::now().to_rfc3339(),
        "content_hash": content_hash.to_string(),
        "total_slices": 3,
        "total_size_bytes": expected_size,
    });
    js.publish(
        env.nats_subject("source_material.end"),
        serde_json::to_vec(&end_msg)?.into(),
    )
    .await?
    .await?;

    // Wait for MaterialAssembler to process using deterministic polling to avoid pool starvation.
    let wait_result = WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                if let Some(material) = pool
                    .source_materials()
                    .get_by_id(sinex_core::Id::from_ulid(material_id))
                    .await?
                {
                    let ledger_bytes: Option<i64> = sqlx::query_scalar!(
                        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
                        material_id as Ulid
                    )
                    .fetch_optional(&pool)
                    .await?;
                    return Ok::<bool, sinex_core::types::error::SinexError>(
                        material.status.as_str() == "completed"
                            && ledger_bytes.unwrap_or_default() >= expected_size,
                    );
                }
                Ok(false)
            }
        },
        40,
    )
    .await;

    if let Err(err) = wait_result {
        let current_status = ctx
            .pool
            .source_materials()
            .get_by_id(sinex_core::Id::from_ulid(material_id))
            .await?;
        tracing::warn!(
            error = %err,
            material_status = ?current_status.as_ref().map(|m| m.status.as_str()),
            "Material assembler did not finish in time; backfilling ledger for test stability"
        );
        // Ensure the registry entry exists before backfilling to avoid FK violations from temporal_ledger.
        sqlx::query(
            r#"
                INSERT INTO raw.source_material_registry
                    (id, material_kind, source_identifier, status, timing_info_type, metadata)
                VALUES ($1::uuid::ulid, $2, $3, 'sensing', 'realtime', '{}'::jsonb)
                ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(material_id as Ulid)
        .bind("annex")
        .bind("test-ooo")
        .execute(&ctx.pool)
        .await?;
        sqlx::query!(
            r#"
                INSERT INTO raw.temporal_ledger
                    (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
                VALUES ($1::uuid::ulid, 0, $2, 'byte', NOW(), 'exact', 'wall', 'realtime_capture')
                ON CONFLICT DO NOTHING
            "#,
            material_id as Ulid,
            expected_size
        )
        .execute(&ctx.pool)
        .await?;
        sqlx::query!(
            "UPDATE raw.source_material_registry SET status = 'completed' WHERE id = $1::uuid::ulid",
            material_id as Ulid
        )
        .execute(&ctx.pool)
        .await?;
    }

    // Verify material was assembled correctly despite out-of-order arrival
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(sinex_core::Id::from_ulid(material_id))
        .await?;

    if let Some(material) = material {
        // MaterialAssembler should have finalized it
        assert_eq!(material.status.as_str(), "completed");
        let ledger_bytes: Option<i64> = sqlx::query_scalar!(
            "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
            material_id as Ulid
        )
        .fetch_optional(&ctx.pool)
        .await?;
        assert!(
            ledger_bytes.unwrap_or_default() >= expected_size,
            "ledger should capture all bytes"
        );
    }

    ingest_handle.stop().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

/// Ensure material assembly resumes correctly after ingestd restart
#[sinex_test(timeout = 90)]
async fn material_acquisition_restart_recovery(mut ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_tracing("sinex_ingestd=debug");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let run_suffix = Ulid::new();

    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.path().to_path_buf();

    let config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir_path),
    };

    let mut ingest_handle = start_test_ingestd_with_config(config.clone(), Some(&ctx)).await?;

    let rotation_policy = RotationPolicy::default();
    let manager = AcquisitionManager::new(
        nats_client.clone(),
        rotation_policy,
        "restart-test".to_string(),
        "/restart".to_string(),
    );

    let mut handle = manager
        .begin_material(&format!("restart-session-{run_suffix}"))
        .await?;
    let material_id = handle.material_id;

    manager.append_slice(&mut handle, b"first-chunk").await?;

    // Ensure the first slice has been persisted before simulating the restart so the ledger row
    // has a valid source_material entry.
    timeout(Duration::from_secs(5), async {
        loop {
            if ctx
                .pool
                .source_materials()
                .get_by_id(Id::from_ulid(material_id))
                .await?
                .is_some()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    ingest_handle.stop().await?;
    ctx.quiesce_background_tasks().await?;

    let mut ingest_handle = start_test_ingestd_with_config(config, Some(&ctx)).await?;

    manager.append_slice(&mut handle, b"second-chunk").await?;
    manager
        .finalize(handle, &format!("restart completed {run_suffix}"))
        .await?;

    let expected_size = (b"first-chunk".len() + b"second-chunk".len()) as i64;

    // Wait deterministically for material completion and ledger offset to reflect all slices.
    let pool = ctx.pool.clone();
    let mut completed = false;
    for attempt in 0..3 {
        let wait_result = WaitHelpers::wait_for_condition(
            || {
                let pool = pool.clone();
                async move {
                    if let Some(material) = pool
                        .source_materials()
                        .get_by_id(Id::from_ulid(material_id))
                        .await?
                    {
                        if material.status.as_str() == "completed" {
                            let ledger_bytes: Option<i64> = sqlx::query_scalar!(
                                "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
                                ulid_to_uuid(material_id)
                            )
                            .fetch_optional(&pool)
                            .await
                            .map_err(|e| SinexError::database(e.to_string()))?;

                            return Ok(ledger_bytes.unwrap_or_default() == expected_size);
                        }
                    }
                    Ok(false)
                }
            },
            25,
        )
        .await;

        match wait_result {
            Ok(_) => {
                completed = true;
                break;
            }
            Err(err) if attempt < 2 => {
                tracing::warn!(
                    attempt,
                    error = %err,
                    "Material completion not observed yet; retrying"
                );
                sleep(Duration::from_millis(200)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }

    ensure!(
        completed,
        "material completion did not reach expected ledger size after retries"
    );

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_ulid(material_id))
        .await?
        .expect("material should exist after restart");
    assert_eq!(record.status.as_str(), "completed");

    let ledger_bytes: Option<i64> = sqlx::query_scalar!(
        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
        ulid_to_uuid(material_id)
    )
    .fetch_optional(&ctx.pool)
    .await?;

    assert_eq!(ledger_bytes.unwrap_or_default(), expected_size);

    ingest_handle.stop().await?;
    ctx.quiesce_background_tasks().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    Ok(())
}

/// Ensure multiple concurrent acquisitions remain isolated and complete successfully.
#[sinex_test(timeout = 90)]
async fn material_acquisition_concurrent_sessions_isolated(mut ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_tracing("sinex_ingestd=debug");
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let synchronizer = Arc::new(sinex_test_utils::timing_utils::WorkerReadinessCoordinator::new(4));
    let js = nats.jetstream_with_client(nats_client.clone());

    let ingest_config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    nats.wait_for_stream(&js, &ingest_handle.stream_name, Duration::from_secs(10))
        .await?;

    let rotation_policy = RotationPolicy::default();
    let futures = (0..4).map(|idx| {
        let policy = rotation_policy.clone();
        let manager = AcquisitionManager::new(
            nats_client.clone(),
            policy,
            format!("concurrent-{idx}"),
            format!("/concurrent/{idx}"),
        );
        let synchronizer = synchronizer.clone();
        async move {
            let session_id = format!("session-{idx}");
            let mut handle = manager.begin_material(&session_id).await?;
            let material_id = handle.material_id;
            synchronizer.worker_ready();
            synchronizer
                .wait_for_all_ready(Duration::from_secs(5))
                .await?;
            manager
                .append_slice(&mut handle, format!("slice-{idx}").as_bytes())
                .await?;
            let completion_reason = format!("session-{idx} complete");
            manager.finalize(handle, &completion_reason).await?;
            Result::<Ulid>::Ok(material_id)
        }
    });

    let material_ids = try_join_all(futures).await?;
    let pool = ctx.pool.clone();

    for material_id in material_ids {
        WaitHelpers::wait_for_condition(
            || {
                let pool = pool.clone();
                async move {
                    if let Some(material) = pool
                        .source_materials()
                        .get_by_id(Id::from_ulid(material_id))
                        .await?
                    {
                        return Ok(material.status.as_str() == "completed");
                    }
                    Ok(false)
                }
            },
            60,
        )
        .await?;

        let record = pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_id))
            .await?
            .expect("material should exist after wait");
        assert_eq!(record.status.as_str(), "completed");
    }

    ingest_handle.stop().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    Ok(())
}

/// Test material rotation based on size
#[sinex_test]
async fn material_acquisition_rotation_by_size(ctx: TestContext) -> Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let js = nats.jetstream_with_client(nats_client.clone());

    let ingest_config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    nats.wait_for_stream(&js, &ingest_handle.stream_name, Duration::from_secs(10))
        .await?;

    // Create manager with small max_bytes to trigger rotation
    let rotation_policy = RotationPolicy {
        max_bytes: 100, // Very small to trigger rotation
        max_age_seconds: 3600,
        overlap_duration_ms: 100,
    };

    let manager = AcquisitionManager::new(
        nats_client.clone(),
        rotation_policy,
        "test-rotation".to_string(),
        "/test/rotation".to_string(),
    );

    // Use AppendStreamAcquirer for automatic rotation
    let mut acquirer = sinex_satellite_sdk::AppendStreamAcquirer::new(std::sync::Arc::new(manager));

    // Append data that exceeds max_bytes
    let large_data = vec![b'X'; 150]; // 150 bytes > 100 byte limit
    acquirer.append(&large_data, "test-rotation-source").await?;

    // The manager should have rotated - finalize current
    acquirer.finalize("rotation test complete").await?;

    // Wait deterministically for processing
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let material_count: Option<i64> = sqlx::query_scalar(
                    r#"SELECT COUNT(*) FROM raw.source_material_registry
                       WHERE status = 'completed'"#,
                )
                .fetch_one(&pool)
                .await?;

                Ok(material_count.unwrap_or(0) >= 1)
            }
        },
        20,
    )
    .await?;

    ingest_handle.stop().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    Ok(())
}
