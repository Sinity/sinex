use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use serde_json::json;
use sinex_core::types::buffers::DEFAULT_EVENT_CHANNEL_SIZE;
use sinex_core::{db::models::Event, JsonValue};
use sinex_node_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use sinex_node_sdk::event_processor::{
    spawn_event_processor, EventProcessorConfig, EventTransport,
};
use sinex_node_sdk::nats_publisher::NatsPublisher;
use sinex_node_sdk::stage_as_you_go::{
    LogFileStageProcessor, StageAsYouGoContext, StageAsYouGoProcessor,
};
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use tokio::sync::{mpsc, oneshot};
use tracing::info;
use uuid::Uuid;

#[sinex_test]
async fn stage_as_you_go_pipeline_end_to_end(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    let jetstream: jetstream::Context = ctx.jetstream().await?;
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let ingest_config = TestIngestdConfig {
        nats_url: format!(
            "nats://{}",
            ctx.nats_url().expect("with_nats should provide nats_url")
        ),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
        ..Default::default()
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    WaitHelpers::wait_for_condition(
        || {
            let js = jetstream.clone();
            let stream_name = ingest_handle.stream_name.clone();
            async move {
                Ok::<bool, sinex_test_utils::SinexError>(js.get_stream(&stream_name).await.is_ok())
            }
        },
        5,
    )
    .await?;

    let (event_tx, event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));
    let processor_config = EventProcessorConfig {
        batch_size: 1,
        batch_timeout: Duration::from_millis(100),
        ..EventProcessorConfig::default()
    };
    let processor_handle = spawn_event_processor(
        EventTransport::Nats(publisher),
        processor_config,
        event_rx,
        shutdown_rx,
    );

    let context = StageAsYouGoContext::from_sender(
        Arc::new(AcquisitionManager::new(
            nats_client.clone(),
            RotationPolicy::default(),
            "integration-log".to_string(),
            "/tmp/integration.log".to_string(),
        )),
        event_tx,
        false,
    );
    let mut processor = LogFileStageProcessor::new(context, "integration-log");

    let content = b"alpha\nbeta\ngamma\n";
    let metadata = serde_json::json!({ "integration": true });

    let result = processor
        .process_with_staging(content, Some("file:///tmp/integration.log"), metadata)
        .await?;

    assert_eq!(result.bytes_processed, content.len());
    assert_eq!(result.event_ids.len(), 3, "expected one event per line");

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let material_id = Uuid::from(result.source_material_id);
            async move {
                let row = sqlx::query!(
                    r#"
                        SELECT status
                        FROM raw.source_material_registry
                        WHERE id::uuid = $1
                    "#,
                    material_id
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, sinex_test_utils::SinexError>(row.status == "completed")
            }
        },
        5,
    )
    .await?;

    let material_row = sqlx::query!(
        r#"
            SELECT
                status,
                (metadata->>'total_bytes')::bigint AS "total_bytes?",
                metadata->>'encoding' AS encoding
            FROM raw.source_material_registry
            WHERE id::uuid = $1
        "#,
        Uuid::from(result.source_material_id)
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(material_row.status.as_str(), "completed");
    assert_eq!(material_row.total_bytes, Some(content.len() as i64));
    assert_eq!(material_row.encoding.as_deref(), Some("utf-8"));

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let material_id = Uuid::from(result.source_material_id);
            let expected = result.event_ids.len() as i64;
            async move {
                let count: Option<i64> = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1::uuid::ulid",
                    material_id
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, sinex_test_utils::SinexError>(count.unwrap_or(0) == expected)
            }
        },
        5,
    )
    .await?;

    let observed_events: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1::uuid::ulid",
        Uuid::from(result.source_material_id)
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0);
    assert_eq!(observed_events, result.event_ids.len() as i64);

    WaitHelpers::wait_for_condition(
        || {
            let js = jetstream.clone();
            let stream_name = ingest_handle.stream_name.clone();
            let expected = result.event_ids.len() as u64;
            async move {
                let mut stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok(info.state.messages >= expected)
            }
        },
        5,
    )
    .await?;

    let _ = shutdown_tx.send(());
    processor_handle.await??;
    ingest_handle.stop().await?;
    Ok(())
}

#[sinex_test]
async fn stage_as_you_go_reconciliation_cancels_stale_materials(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let acquisition = Arc::new(AcquisitionManager::new(
        nats_client.clone(),
        RotationPolicy::default(),
        "reconciliation-log".to_string(),
        "/tmp/reconciliation.log".to_string(),
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let context = StageAsYouGoContext::from_sender(acquisition, event_tx, false);
    let material_id = context
        .register_in_flight("reconciliation", None, json!({}))
        .await?;

    let summary = context
        .reconcile_inflight_older_than(Duration::from_millis(0))
        .await?;

    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.errors, 0);
    assert_eq!(summary.skipped, 0);

    // No events should have been emitted for the cancelled material.
    assert!(event_rx.try_recv().is_err());

    // Subsequent reconciliation runs should do nothing.
    let second_summary = context
        .reconcile_inflight_older_than(Duration::from_millis(0))
        .await?;
    assert_eq!(second_summary.cancelled, 0);

    info!(material_id = %material_id, "Reconciliation cancelled stale material");
    Ok(())
}
