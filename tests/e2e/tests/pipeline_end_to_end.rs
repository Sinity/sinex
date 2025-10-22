use std::time::Duration;

use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::Ulid;
use sinex_gateway::ServiceContainer;
use sinex_satellite_sdk::grpc_client::IngestClient;
use sinex_satellite_sdk::stage_as_you_go::{
    LogFileStageProcessor, StageAsYouGoContext, StageAsYouGoProcessor,
};
use sinex_services::AnalyticsService;
use sinex_test_utils::prelude::*;
use sinex_test_utils::{start_test_ingestd_with_config, EphemeralNats, TestIngestdConfig};
use tokio::time::sleep;

#[sinex_test]
async fn pipeline_end_to_end(ctx: TestContext) -> Result<()> {
    // Boot a transient NATS instance for ingestd.
    let nats = EphemeralNats::start().await?;
    let nats_url = format!("nats://{}", nats.client_url());

    // Prepare a Unix socket for ingestd's gRPC server.
    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir
        .path()
        .join(format!("ingestd-{}.sock", Ulid::new()));
    let socket_path_string = socket_path.to_string_lossy().into_owned();

    // Start ingestd wired into the shared test database.
    let mut ingest_handle = start_test_ingestd_with_config(TestIngestdConfig {
        socket_path: socket_path_string.clone(),
        nats_url,
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    })
    .await?;

    // Allow the gRPC listener to bind before connecting.
    sleep(Duration::from_millis(250)).await;

    let ingest_client = IngestClient::new(&socket_path_string).await?;
    let stage_context = StageAsYouGoContext::new(ctx.pool.clone(), ingest_client);
    let mut processor = LogFileStageProcessor::new(stage_context, "integration-e2e");

    let content = b"alpha\nbeta\ngamma\n";
    let metadata = json!({ "integration": true });
    let stage_result = processor
        .process_with_staging(content, Some("file:///tmp/e2e.log"), metadata)
        .await?;
    assert_eq!(stage_result.event_ids.len(), 3, "one event per log line");

    // Give ingestd a brief moment to flush to Postgres.
    sleep(Duration::from_millis(200)).await;

    // Verify the events landed in the database.
    let recent = ctx.pool.events().get_recent(10).await?;
    assert!(
        recent.len() >= 3,
        "stage-as-you-go should emit three events"
    );

    // Analytics service should surface the staged source.
    let analytics = AnalyticsService::new(ctx.pool.clone());
    let by_source = analytics.get_event_count_by_source(None, None).await?;
    assert!(by_source.values().sum::<i64>() >= 3);

    // Wire up the gateway service container against the same environment.
    let annex_dir = tempfile::tempdir()?;
    std::env::set_var("SINEX_ANNEX_PATH", annex_dir.path());
    std::env::set_var("SINEX_INGEST_SOCKET", &socket_path_string);

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;

    let aggregated = container
        .analytics
        .get_event_count_by_source(None, None)
        .await?;
    assert!(aggregated.values().sum::<i64>() >= 3);

    ingest_handle.stop().await?;
    Ok(())
}
