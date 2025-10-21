use sinex_core::types::ulid::Ulid;
use sinex_test_utils::SinexError;
use sinex_satellite_sdk::grpc_client::IngestClient;
use sinex_satellite_sdk::stage_as_you_go::{LogFileStageProcessor, StageAsYouGoContext};
use sinex_test_utils::nats::EphemeralNats;
use sinex_test_utils::prelude::*;
use sinex_test_utils::satellite_management_utils::{
    start_test_ingestd_with_config, TestIngestdConfig,
};
use std::time::{Duration, Instant};

#[sinex_test]
async fn stage_as_you_go_pipeline_end_to_end(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let jetstream = async_nats::jetstream::new(client.clone());

    let socket_dir = tempfile::tempdir().map_err(|e| {
        SinexError::service(format!("failed to create temp socket dir: {e}"))
    })?;
    let socket_path = socket_dir.path().join(format!("stage-ingest-{}.sock", Ulid::new()));

    let ingest_config = TestIngestdConfig {
        socket_path: socket_path.to_string_lossy().into(),
        nats_url: format!("nats://{}", nats.client_url()),
        database_url: ctx.database_url().to_string(),
        work_dir: None,
    };

    let mut ingest_handle = start_test_ingestd_with_config(ingest_config).await?;

    // Allow server to stabilise
    tokio::time::sleep(Duration::from_millis(200)).await;

    let ingest_client = IngestClient::new(socket_path.to_string_lossy().as_ref())
        .await
        .map_err(|e| SinexError::service(format!("failed to create ingest client: {e}")))?;

    let context = StageAsYouGoContext::new(ctx.pool.clone(), ingest_client);
    let mut processor = LogFileStageProcessor::new(context, "integration-log");

    let content = b"alpha\nbeta\ngamma\n";
    let metadata = serde_json::json!({ "integration": true });

    let result = processor
        .process_with_staging(content, Some("file:///tmp/integration.log"), metadata)
        .await?;

    assert_eq!(result.bytes_processed, content.len());
    assert_eq!(result.event_ids.len(), 3, "expected one event per line");

    tokio::time::sleep(Duration::from_millis(250)).await;

    let material_row = sqlx::query!(
        "SELECT status, total_size_bytes, encoding FROM raw.source_material_registry WHERE id = $1::uuid::ulid",
        result.source_material_id
    )
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(material_row.status.as_deref(), Some("completed"));
    assert_eq!(material_row.total_size_bytes, Some(content.len() as i64));
    assert_eq!(material_row.encoding.as_deref(), Some("utf-8"));

    let event_count: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source_material_id = $1::uuid::ulid",
        result.source_material_id
    )
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(event_count.unwrap_or(0), result.event_ids.len() as i64);

    // Verify events landed on JetStream
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut found = false;
    while Instant::now() < deadline {
        if let Ok(info) = jetstream.stream_info(ingest_handle.stream_name.clone()).await {
            if info.state.messages >= result.event_ids.len() as u64 {
                found = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "expected JetStream to receive staged events");

    ingest_handle.stop().await?;
    Ok(())
}
