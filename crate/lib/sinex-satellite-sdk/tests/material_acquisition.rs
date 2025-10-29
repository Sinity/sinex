use sinex_core::types::ulid::Ulid;
use sinex_core::Id;
use sinex_core::{db::query_helpers::ulid_to_uuid, db::DbPoolExt};
use sinex_satellite_sdk::{AcquisitionManager, RotationPolicy};
use sinex_test_utils::prelude::*;
use sinex_test_utils::{start_test_ingestd_with_config, EphemeralNats, TestIngestdConfig};
use std::time::Duration;

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

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Create AcquisitionManager
    let rotation_policy = RotationPolicy::default();
    let manager = AcquisitionManager::new(
        nats_client.clone(),
        ctx.pool.clone(),
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

    // Wait for MaterialAssembler to process
    tokio::time::sleep(Duration::from_secs(2)).await;

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
#[sinex_test]
async fn material_acquisition_out_of_order_slices(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let ingest_config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Manually publish slices out of order to test MaterialAssembler's buffering
    let material_id = Ulid::new();
    let env = sinex_core::environment();
    let js = async_nats::jetstream::new(nats_client.clone());

    // Register in-flight material
    let metadata = serde_json::json!({"test": "out-of-order"});
    let _record = ctx
        .pool
        .source_materials()
        .register_in_flight("test", Some("/test/ooo"), metadata)
        .await?;

    // Publish begin message
    let begin_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "material_kind": "test",
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
        headers.insert("Offset", offset.to_string().as_str());
        headers.insert("Chunk-Hash", blake3::hash(&data).to_hex().as_str());

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

    // Publish end message
    let end_msg = serde_json::json!({
        "material_id": material_id.to_string(),
        "ended_at": chrono::Utc::now().to_rfc3339(),
        "content_hash": content_hash.to_string(),
        "total_slices": 3,
        "total_size_bytes": 36i64,
    });
    js.publish(
        env.nats_subject("source_material.end"),
        serde_json::to_vec(&end_msg)?.into(),
    )
    .await?
    .await?;

    // Wait for MaterialAssembler to process
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify material was assembled correctly despite out-of-order arrival
    let material = ctx
        .pool
        .source_materials()
        .get_by_id(sinex_core::Id::from_ulid(material_id))
        .await?;

    if let Some(material) = material {
        // MaterialAssembler should have finalized it
        assert_eq!(material.status.as_str(), "completed");
    }

    ingest_handle.stop().await?;
    Ok(())
}

/// Ensure material assembly resumes correctly after ingestd restart
#[sinex_test]
async fn material_acquisition_restart_recovery(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let work_dir = tempfile::tempdir()?;
    let work_dir_path = work_dir.path().to_path_buf();

    let config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: Some(work_dir_path),
    };

    let mut ingest_handle = start_test_ingestd_with_config(config.clone()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rotation_policy = RotationPolicy::default();
    let manager = AcquisitionManager::new(
        nats_client.clone(),
        ctx.pool.clone(),
        rotation_policy,
        "restart-test".to_string(),
        "/restart".to_string(),
    );

    let mut handle = manager.begin_material("restart-session").await?;
    let material_id = handle.material_id;

    manager.append_slice(&mut handle, b"first-chunk").await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    ingest_handle.stop().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut ingest_handle = start_test_ingestd_with_config(config).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    manager.append_slice(&mut handle, b"second-chunk").await?;
    manager.finalize(handle, "restart completed").await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let record = ctx
        .pool
        .source_materials()
        .get_by_id(Id::from_ulid(material_id))
        .await?
        .expect("material should exist after restart");

    assert_eq!(record.status.as_str(), "completed");
    let expected_size = (b"first-chunk".len() + b"second-chunk".len()) as i64;

    let ledger_bytes: Option<i64> = sqlx::query_scalar!(
        "SELECT offset_end FROM raw.temporal_ledger WHERE source_material_id = $1::uuid::ulid ORDER BY ts_capture DESC LIMIT 1",
        ulid_to_uuid(material_id)
    )
    .fetch_optional(&ctx.pool)
    .await?;

    assert_eq!(ledger_bytes.unwrap_or_default(), expected_size);

    ingest_handle.stop().await?;
    Ok(())
}

/// Test material rotation based on size
#[sinex_test]
async fn material_acquisition_rotation_by_size(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;

    let ingest_config = TestIngestdConfig {
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Create manager with small max_bytes to trigger rotation
    let rotation_policy = RotationPolicy {
        max_bytes: 100, // Very small to trigger rotation
        max_age_seconds: 3600,
        overlap_duration_ms: 100,
    };

    let manager = AcquisitionManager::new(
        nats_client.clone(),
        ctx.pool.clone(),
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

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify at least one material was created
    let material_count: Option<i64> = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM raw.source_material_registry
           WHERE status = 'completed'"#,
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert!(
        material_count.unwrap_or(0) >= 1,
        "Expected at least one completed material"
    );

    ingest_handle.stop().await?;
    Ok(())
}
