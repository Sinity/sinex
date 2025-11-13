use sinex_gateway::ServiceContainer;
use sinex_test_utils::{sinex_test, TestContext};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[sinex_test]
async fn blob_routes_do_not_persist_events(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let temp_dir = TempDir::new()?;
    std::env::set_var("SINEX_ANNEX_PATH", temp_dir.path().to_str().unwrap());

    let initial_count: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE event_type = 'blob.ingested'"#
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0);

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;

    container
        .content
        .store_content(
            b"blob payload",
            "fixture.bin",
            "application/octet-stream",
            "test",
        )
        .await?;

    sleep(Duration::from_millis(50)).await;

    let after_count: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE event_type = 'blob.ingested'"#
    )
    .fetch_one(&ctx.pool)
    .await?
    .unwrap_or(0);

    assert_eq!(
        after_count, initial_count,
        "Gateway helpers must never write blob.ingested events; ingestd is the single writer"
    );

    Ok(())
}
