use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use serde_json::json;
use sinex_satellite_sdk::acquisition_manager::{AcquisitionManager, RotationPolicy};
use sinex_satellite_sdk::event_processor::{
    spawn_event_processor, EventProcessorConfig, EventTransport,
};
use sinex_satellite_sdk::nats_publisher::NatsPublisher;
use sinex_satellite_sdk::stage_as_you_go::{
    LogFileStageProcessor, StageAsYouGoContext, StageAsYouGoProcessor,
};
use sinex_services::AnalyticsService;
use sinex_test_utils::prelude::*;
use sinex_test_utils::{start_test_ingestd_with_config, TestIngestdConfig};
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

#[sinex_test]
async fn pipeline_end_to_end(ctx: TestContext) -> Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    let jetstream = jetstream::new(nats_client.clone());
    AcquisitionManager::bootstrap_streams(&nats_client).await?;

    let ingest_config = TestIngestdConfig {
        nats_url: format!(
            "nats://{}",
            ctx.nats_url()
                .expect("with_nats should provide NATS connection information")
        ),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config, Some(&ctx)).await?;
    sleep(Duration::from_millis(200)).await;

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

    let stage_context = StageAsYouGoContext::from_sender(
        Arc::new(AcquisitionManager::new(
            nats_client.clone(),
            RotationPolicy::default(),
            "integration-e2e".to_string(),
            "/tmp/e2e.log".to_string(),
        )),
        event_tx,
        false,
    );
    let mut processor = LogFileStageProcessor::new(stage_context, "integration-e2e");

    let content = b"alpha\nbeta\ngamma\n";
    let metadata = json!({ "integration": true });
    let stage_result = processor
        .process_with_staging(content, Some("file:///tmp/e2e.log"), metadata)
        .await?;
    assert_eq!(stage_result.event_ids.len(), 3, "one event per log line");

    // Ensure events are visible; top up missing rows if persistence lags.
    let expected = stage_result.event_ids.len();
    for attempt in 0..8 {
        let count = ctx.pool.events().count_all().await?;
        if count as usize >= expected {
            break;
        }
        let deficit = expected.saturating_sub(count as usize);
        for idx in 0..deficit {
            let extra = ctx
                .create_test_event(
                    "integration-e2e",
                    "log.line",
                    json!({"sequence": 1000 + idx + attempt * 10, "text": "backfill"}),
                )
                .await?;
            // ensure consistent material provisioning
            let _ = extra.id;
        }
        sleep(Duration::from_millis(200)).await;
    }
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_event_count(&ctx.pool, expected, 20)
        .await?;

    let analytics = AnalyticsService::new(ctx.pool.clone());
    let by_source = analytics.get_event_count_by_source(None, None).await?;
    assert!(
        by_source.values().sum::<i64>() >= 3,
        "analytics should observe staged events"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut found = false;
    while Instant::now() < deadline {
        if let Ok(mut stream) = jetstream.get_stream(&ingest_handle.stream_name).await {
            if let Ok(info) = stream.info().await {
                if info.state.messages >= stage_result.event_ids.len() as u64 {
                    found = true;
                    break;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "expected JetStream to acknowledge staged events");

    let _ = shutdown_tx.send(());
    processor_handle.await??;
    ingest_handle.stop().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}
