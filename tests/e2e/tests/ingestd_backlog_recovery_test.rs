use camino::Utf8PathBuf;
use serde_json::json;
use sinex_ingestd::{config::IngestdConfig, service::IngestService, JetStreamTopology};
use sinex_primitives::nats::NatsConnectionConfig;
use tempfile::TempDir;
use tokio::time::{timeout, Duration};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

#[sinex_test(timeout = 60)]
async fn ingestd_processes_backlog_after_downtime(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let js = ctx.jetstream().await?;
    let env = ctx.env();

    let base_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    let consumer_name = "ingestd-backlog".to_string();
    let topology = JetStreamTopology::new(env, base_stream.clone(), consumer_name.clone(), None);

    let work_dir = TempDir::new()?;
    let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir.path().to_path_buf())
        .unwrap_or_else(|_| Utf8PathBuf::from("/tmp"));
    let annex_path = work_dir_utf8.join("annex");
    let assembler_state_dir = work_dir_utf8.join("assembler_state");
    tokio::fs::create_dir_all(annex_path.as_std_path()).await?;
    tokio::fs::create_dir_all(assembler_state_dir.as_std_path()).await?;

    let mut config = IngestdConfig::builder()
        .database_url(ctx.database_url().to_string())
        .nats(
            NatsConnectionConfig::builder()
                .url(nats.client_url().to_string())
                .build(),
        )
        .nats_stream_name(base_stream)
        .nats_consumer_name(consumer_name)
        .batch_size(16)
        .consumer_fetch_max_messages(32)
        .consumer_fetch_timeout_ms(200.into())
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir_utf8.clone())
        .annex_repo_path(annex_path)
        .assembler_state_dir(assembler_state_dir)
        .build();

    config.database_pool_size = 4;

    let mut service = IngestService::new(config.clone()).await?;
    let mut runner = service.clone();
    let handle = tokio::spawn(async move { runner.run().await });

    nats.wait_for_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::SHORT),
    )
    .await?;

    service.shutdown().await?;
    let join_result = timeout(Duration::from_secs(Timeouts::QUICK), handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestd runner shutdown timed out"))?;
    join_result??;

    // Publish events directly to JetStream while service is offline
    let js = ctx.jetstream().await?;
    let subject = format!("{}.backlog.event", topology.events_stream);
    for idx in 0..3 {
        let payload = serde_json::to_vec(&json!({
            "source": "backlog-source",
            "type": "backlog.event",
            "seq": idx
        }))?;
        js.publish(subject.clone(), payload.into()).await?.await?;
    }

    let mut restart_service = IngestService::new(config).await?;
    let mut restart_runner = restart_service.clone();
    let restart_handle = tokio::spawn(async move { restart_runner.run().await });

    let wait_secs = Timeouts::LONG;
    WaitHelpers::wait_for_event_count(&ctx.pool, 3, wait_secs).await?;

    restart_service.shutdown().await?;
    let restart_join = timeout(Duration::from_secs(Timeouts::QUICK), restart_handle)
        .await
        .map_err(|_| color_eyre::eyre::eyre!("ingestd runner shutdown timed out"))?;
    restart_join??;

    Ok(())
}
