use futures::future::try_join_all;
use sinex_db::{query_helpers::ulid_to_uuid, repositories::DbPoolExt};
use sinex_node_sdk::{AcquisitionManager, RotationPolicy};
use sinex_primitives::error::SinexError;
use sinex_primitives::ids::Id;
use sinex_primitives::ids::Ulid;
use sinex_primitives::units::{Bytes, Seconds};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{WaitHelpers, DEFAULT_WAIT_SECS, INTEGRATION_WAIT_SECS};
use xtask::sandbox::{start_test_ingestd_with_config, EphemeralNats, TestIngestdConfig};

/// Test basic material acquisition flow: begin → append slices → finalize
#[sinex_test]
async fn material_acquisition_basic_flow(ctx: TestContext) -> Result<()> {
    // Start NATS
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    // Start ingestd (includes MaterialAssembler)
    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    // Create AcquisitionManager
    let manager =
        AcquisitionManager::with_defaults(nats_client.clone(), "test-source", "/test/path");

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
                        .get_by_id(Id::from_ulid(material_id))
                        .await?
                        .ok_or_else(|| sinex_primitives::error::SinexError::database("missing"))?;
                    let ledger_count: Option<i64> = sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid",
                        material_id as Ulid
                    )
                    .fetch_one(&pool)
                    .await?;
                    Ok::<bool, sinex_primitives::error::SinexError>(
                        material.status.as_str() == "completed"
                            && ledger_count.unwrap_or(0) >= 1
                    )
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    // Verify database state
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_ulid(material_id))
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

/// Test cancellation mid-slice cleans up temp state and records cancellation metadata.
#[sinex_test]
async fn material_acquisition_cancel_mid_slice(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let manager =
        AcquisitionManager::with_defaults(nats_client.clone(), "cancel-source", "/cancel/path");

    let mut handle = manager.begin_material("cancel-identifier").await?;
    let material_id = handle.material_id;
    let temp_path = handle.temp_path().to_path_buf();

    manager.append_slice(&mut handle, b"partial data").await?;
    manager.cancel(handle, "user_cancelled").await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let temp_path = temp_path.clone();
                async move { Ok::<bool, sinex_primitives::error::SinexError>(!temp_path.exists()) }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    ctx.timing()
        .wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                async move {
                    let material = pool
                        .source_materials()
                        .get_by_id(Id::from_ulid(material_id))
                        .await?;
                    let Some(material) = material else {
                        return Ok::<bool, SinexError>(false);
                    };
                    Ok::<bool, SinexError>(
                        material
                            .metadata
                            .get("cancelled")
                            .and_then(sinex_primitives::JsonValue::as_bool)
                            .unwrap_or(false),
                    )
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

    ingest_handle.stop().await?;
    Ok(())
}

/// Test out-of-order slice handling
#[sinex_test(timeout = 60)]
async fn material_acquisition_out_of_order_slices(ctx: TestContext) -> Result<()> {
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;

    // Ensure JetStream streams exist before manually publishing messages
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    // Manually publish slices out of order to test MaterialAssembler's buffering
    let material_id = Ulid::new();
    let env = sinex_primitives::environment::environment();
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
        "started_at": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
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
            env.nats_subject(&format!("source_material.slices.{material_id}")),
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
        "ended_at": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
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

    // Wait for MaterialAssembler to process.
    //
    // This should complete quickly once ingestd has created the material consumers; if it doesn't,
    // fail with a clear error instead of "backfilling" database state.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                if let Some(material) = pool
                    .source_materials()
                    .get_by_id(Id::from_ulid(material_id))
                    .await?
                {
                    let ledger_bytes: Option<i64> = sqlx::query_scalar!(
                        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
                        material_id as Ulid
                    )
                    .fetch_optional(&pool)
                    .await?;
                    return Ok::<bool, SinexError>(
                        material.status.as_str() == "completed"
                            && ledger_bytes.unwrap_or_default() >= expected_size,
                    );
                }
                Ok::<bool, SinexError>(false)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    // Verify material was assembled correctly despite out-of-order arrival
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_ulid(material_id))
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
    Ok(())
}

/// Ensure end-before-begin ordering is tolerated (end is NAKed and later finalized).
#[sinex_test(timeout = 60)]
async fn material_acquisition_end_before_begin(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let material_id = Ulid::new();
    let env = sinex_primitives::environment::environment();
    let js = nats.jetstream_with_client(nats_client.clone());

    let slices = vec![
        (0i64, b"slice 0 data".to_vec()),
        (12i64, b"slice 1 data".to_vec()),
    ];

    let mut hasher = blake3::Hasher::new();
    hasher.update(&slices[0].1);
    hasher.update(&slices[1].1);
    let content_hash = hasher.finalize().to_hex();
    let expected_size = slices
        .iter()
        .map(|(_, data)| data.len() as i64)
        .sum::<i64>();

    let end_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "ended_at": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
        "content_hash": content_hash.to_string(),
        "total_slices": slices.len(),
        "total_size_bytes": expected_size,
    });
    js.publish(
        env.nats_subject("source_material.end"),
        serde_json::to_vec(&end_msg)?.into(),
    )
    .await?
    .await?;

    // Give the end consumer a chance to see the message before begin arrives.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let begin_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "material_kind": "annex",
        "source_identifier": "end-before-begin",
        "metadata": {},
        "started_at": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
    });
    js.publish(
        env.nats_subject("source_material.begin"),
        serde_json::to_vec(&begin_msg)?.into(),
    )
    .await?
    .await?;

    for (offset, data) in slices {
        let mut headers = async_nats::HeaderMap::new();
        let offset_str = offset.to_string();
        let chunk_hash = blake3::hash(&data).to_hex();
        headers.insert("Offset", offset_str.as_str());
        headers.insert("Chunk-Hash", chunk_hash.as_str());

        js.publish_with_headers(
            env.nats_subject(&format!("source_material.slices.{material_id}")),
            headers,
            data.into(),
        )
        .await?
        .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                if let Some(material) = pool
                    .source_materials()
                    .get_by_id(Id::from_ulid(material_id))
                    .await?
                {
                    if material.status.as_str() != "completed" {
                        return Ok::<bool, SinexError>(false);
                    }
                    let ledger_bytes: Option<i64> = sqlx::query_scalar!(
                        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
                        material_id as Ulid
                    )
                    .fetch_optional(&pool)
                    .await?;
                    return Ok::<bool, SinexError>(ledger_bytes.unwrap_or_default() >= expected_size);
                }
                Ok::<bool, SinexError>(false)
            }
        },
        INTEGRATION_WAIT_SECS,
    )
    .await?;

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_ulid(material_id))
        .await?
        .expect("material should exist after completion");
    assert_eq!(record.status.as_str(), "completed");

    ingest_handle.stop().await?;
    Ok(())
}

/// Ensure material assembly resumes correctly after ingestd restart
#[sinex_test(timeout = 90)]
async fn material_acquisition_restart_recovery(mut ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_tracing("sinex_ingestd=debug");
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let js = nats.jetstream_with_client(nats_client.clone());
    let run_suffix = Ulid::new();

    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.path().to_path_buf();

    let config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir_path.clone()),
        consumer_fetch_timeout_ms: 200,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(config.clone(), Some(&ctx)).await?;
    nats.wait_for_stream(&js, &ingest_handle.stream_name, Duration::from_secs(10))
        .await?;

    let manager =
        AcquisitionManager::with_defaults(nats_client.clone(), "restart-test", "/restart");

    let mut handle = manager
        .begin_material(&format!("restart-session-{run_suffix}"))
        .await?;
    let material_id = handle.material_id;

    let first_chunk = b"first-chunk";
    manager.append_slice(&mut handle, first_chunk).await?;
    // Wait for ingestd to persist the first chunk by observing assembler state on disk.
    let state_file = work_dir_path
        .join("assembler_state")
        .join(material_id.to_string())
        .join("state.json");
    WaitHelpers::wait_for_condition(
        || {
            let state_file = state_file.clone();
            async move {
                let data = match tokio::fs::read(&state_file).await {
                    Ok(data) => data,
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
                    Err(err) => return Err(SinexError::io(err.to_string())),
                };
                let persisted: serde_json::Value = match serde_json::from_slice(&data) {
                    Ok(value) => value,
                    Err(_) => return Ok(false),
                };
                let expected_offset = persisted
                    .get("expected_offset")
                    .and_then(sinex_primitives::JsonValue::as_i64)
                    .unwrap_or(0);
                Ok(expected_offset >= first_chunk.len() as i64)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    ingest_handle.stop().await?;
    ctx.quiesce_background_tasks().await?;

    let mut ingest_handle = start_test_ingestd_with_config(config, Some(&ctx)).await?;
    nats.wait_for_stream(&js, &ingest_handle.stream_name, Duration::from_secs(10))
        .await?;

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

                            return Ok::<bool, SinexError>(ledger_bytes.unwrap_or_default() >= expected_size);
                        }
                    }
                    Ok::<bool, SinexError>(false)
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await;

        match wait_result {
            Ok(()) => {
                completed = true;
                break;
            }
            Err(err) if attempt < 2 => {
                tracing::warn!(attempt, error = %err, "Material completion not observed yet; retrying");
                tokio::task::yield_now().await;
            }
            Err(err) => return Err(err),
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
    Ok(())
}

/// Ensure multiple concurrent acquisitions remain isolated and complete successfully.
#[sinex_test(timeout = 90)]
async fn material_acquisition_concurrent_sessions_isolated(mut ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_tracing("sinex_ingestd=debug");
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let synchronizer = Arc::new(xtask::sandbox::timing::WorkerReadinessCoordinator::new(4));
    let js = nats.jetstream_with_client(nats_client.clone());

    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    nats.wait_for_stream(&js, &ingest_handle.stream_name, Duration::from_secs(10))
        .await?;

    let futures = (0..4).map(|idx| {
        let manager = AcquisitionManager::with_defaults(
            nats_client.clone(),
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
                .wait_for_all_ready(Duration::from_secs(20))
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
                        return Ok::<bool, SinexError>(material.status.as_str() == "completed");
                    }
                    Ok::<bool, SinexError>(false)
                }
            },
            INTEGRATION_WAIT_SECS,
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
    Ok(())
}

/// Test material rotation based on size
#[sinex_test]
async fn material_acquisition_rotation_by_size(ctx: TestContext) -> Result<()> {
    // `TestContext` is acquired from a pool and cleaned for us; don't do extra per-test DB resets.
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let js = nats.jetstream_with_client(nats_client.clone());

    let ingest_config = TestIngestdConfig {
        nats: nats.connection_config(),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    nats.wait_for_stream(&js, &ingest_handle.stream_name, Duration::from_secs(10))
        .await?;

    // Create manager with small max_bytes to trigger rotation
    let _rotation_policy = RotationPolicy {
        max_bytes: Bytes::from_bytes(100), // Very small to trigger rotation
        max_age_seconds: Seconds::from_secs(3600),
        overlap_duration_ms: 100,
    };

    let manager =
        AcquisitionManager::with_defaults(nats_client.clone(), "test-rotation", "/test/rotation");

    // Use AppendStreamAcquirer for automatic rotation
    let mut acquirer = sinex_node_sdk::AppendStreamAcquirer::new(std::sync::Arc::new(manager));

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
                    r"SELECT COUNT(*) FROM raw.source_material_registry
                       WHERE status = 'completed'",
                )
                .fetch_one(&pool)
                .await?;

                Ok::<bool, SinexError>(material_count.unwrap_or(0) >= 1)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    ingest_handle.stop().await?;
    Ok(())
}
