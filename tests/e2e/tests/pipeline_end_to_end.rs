use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
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
    let ctx = ctx.with_nats().await?;
    let nats_client = ctx.nats_client();
    let jetstream = jetstream::new(nats_client.clone());

    let ingest_config = TestIngestdConfig {
        nats_url: format!(
            "nats://{}",
            ctx.nats_url()
                .expect("with_nats should provide NATS connection information")
        ),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config).await?;
    sleep(Duration::from_millis(200)).await;

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));
    let processor_handle = spawn_event_processor(
        EventTransport::Nats(publisher),
        EventProcessorConfig::default(),
        event_rx,
        shutdown_rx,
    );

    let stage_context = StageAsYouGoContext::new(ctx.pool.clone(), event_tx);
    let mut processor = LogFileStageProcessor::new(stage_context, "integration-e2e");

    let content = b"alpha\nbeta\ngamma\n";
    let metadata = json!({ "integration": true });
    let stage_result = processor
        .process_with_staging(content, Some("file:///tmp/e2e.log"), metadata)
        .await?;
    assert_eq!(stage_result.event_ids.len(), 3, "one event per log line");

    sleep(Duration::from_millis(250)).await;

    let recent = ctx.pool.events().get_recent(10).await?;
    assert!(
        recent.len() >= 3,
        "stage-as-you-go should emit at least three events"
    );

    let analytics = AnalyticsService::new(ctx.pool.clone());
    let by_source = analytics.get_event_count_by_source(None, None).await?;
    assert!(
        by_source.values().sum::<i64>() >= 3,
        "analytics should observe staged events"
    );

    let deadline = Instant::now() + Duration::from_secs(2);
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
    Ok(())
}
