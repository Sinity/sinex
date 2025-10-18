use color_eyre::eyre::Result;
use sinex_sensd::integration_test::{run_integration_test, run_integration_test_with_pool};
use sinex_test_utils::TestContext;

#[tokio::test]
async fn sensd_integration_happy_path() -> Result<()> {
    let ctx = TestContext::with_name("sensd_integration").await?;
    run_integration_test_with_pool(ctx.pool.clone()).await?;

    if let Ok(database_url) = std::env::var("SENSD_INTEGRATION_DATABASE_URL") {
        run_integration_test(&database_url).await?;
    }

    Ok(())
}
