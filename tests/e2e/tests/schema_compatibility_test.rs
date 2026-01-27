use sinex_ingestd::schema_sync::synchronize_schemas;
use sinex_services::AnalyticsService;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn schema_and_services_remain_compatible(ctx: TestContext) -> Result<()> {
    let sync = synchronize_schemas(&ctx.pool).await?;
    assert!(sync.discovered > 0, "schema registry should not be empty");

    // Ensure core services can execute their baseline queries against the fresh schema.
    let analytics = AnalyticsService::new(ctx.pool.clone());
    let counts = analytics.get_event_count_by_source(None, None).await?;
    assert!(counts.is_empty(), "fresh database should have no events");
    Ok(())
}
