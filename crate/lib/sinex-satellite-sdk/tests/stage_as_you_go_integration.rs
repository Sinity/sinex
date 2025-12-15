use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use sinex_satellite_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use sinex_satellite_sdk::event_processor::{
    spawn_event_processor, EventProcessorConfig, EventTransport,
};
use sinex_satellite_sdk::nats_publisher::NatsPublisher;
use sinex_satellite_sdk::stage_as_you_go::{
    LogFileStageProcessor, StageAsYouGoContext, StageAsYouGoProcessor,
};
use sinex_test_utils::prelude::*;
use sinex_test_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use tokio::sync::{mpsc, oneshot};
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
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (event_tx, event_rx) = mpsc::unbounded_channel();
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

    // Allow ingestd to persist events
    tokio::time::sleep(Duration::from_millis(250)).await;

    let completion_deadline = Instant::now() + Duration::from_secs(5);
    let material_row = loop {
        let row = sqlx::query!(
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

        if row.status == "completed" {
            break row;
        }

        if Instant::now() > completion_deadline {
            panic!(
                "source material {} never reached completed status (last seen: {})",
                result.source_material_id, row.status
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(material_row.status.as_str(), "completed");
    assert_eq!(material_row.total_bytes, Some(content.len() as i64));
    assert_eq!(material_row.encoding.as_deref(), Some("utf-8"));

    let event_deadline = Instant::now() + Duration::from_secs(5);
    let observed_events = loop {
        let count: Option<i64> = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1::uuid::ulid",
            Uuid::from(result.source_material_id)
        )
        .fetch_one(&ctx.pool)
        .await?;

        let observed = count.unwrap_or(0);
        if observed == result.event_ids.len() as i64 {
            break observed;
        }
        if Instant::now() > event_deadline {
            panic!(
                "expected {} events but only saw {} in core.events",
                result.event_ids.len(),
                observed
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(observed_events, result.event_ids.len() as i64);

    // Verify events landed on JetStream
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut found = false;
    while Instant::now() < deadline {
        if let Ok(mut stream) = jetstream.get_stream(&ingest_handle.stream_name).await {
            if let Ok(info) = stream.info().await {
                if info.state.messages >= result.event_ids.len() as u64 {
                    found = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "expected JetStream to receive staged events");

    let _ = shutdown_tx.send(());
    processor_handle.await??;
    ingest_handle.stop().await?;
    Ok(())
}
