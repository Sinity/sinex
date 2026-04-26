use sinex_node_sdk::{SelfObserver, SelfObserverConfig};
use tempfile::tempdir;
use xtask::sandbox::prelude::*;

#[sinex_test(timeout = 60)]
async fn self_observation_metrics_persist_via_ingestd(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempdir()?;
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let mut ingestd = start_test_ingestd_with_config(
        TestIngestdConfig {
            nats: ctx.nats_handle()?.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            namespace: Some(namespace.clone()),
            ..Default::default()
        },
        Some(&ctx),
    )
    .await?;

    let observer = SelfObserver::new(
        ctx.nats_client(),
        SelfObserverConfig {
            component: "telemetry-persistence".to_string(),
            namespace: Some(namespace),
            enabled: true,
            min_emission_interval: Duration::ZERO,
        },
    );

    observer.emit_counter("requests.total", 7, None).await?;

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let count = sqlx::query_scalar::<_, i64>(
                    r"
                    SELECT COUNT(*)
                    FROM core.events
                    WHERE source = $1
                      AND event_type = $2
                      AND payload->>'component' = $3
                      AND payload->>'name' = $4
                      AND payload->>'value' = $5
                    ",
                )
                .bind("sinex")
                .bind("metric.counter")
                .bind("telemetry-persistence")
                .bind("requests.total")
                .bind("7")
                .fetch_one(&pool)
                .await?;

                Ok::<bool, xtask::sandbox::SinexError>(count > 0)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    ingestd.stop().await?;
    Ok(())
}
