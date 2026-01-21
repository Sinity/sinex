use color_eyre::eyre;
use sinex_node_sdk::stream_processor::SchemaBroadcastEntry;
use sinex_test_utils::{sinex_test, TestContext, TestResult};
use futures::StreamExt;
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn ingestd_broadcasts_schema_snapshot(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let subject = ctx.nats_subject("system.schemas.active");
    let mut subscription = ctx.nats_client().subscribe(subject).await?;

    let _pipeline = ctx.pipeline_scope().await?;

    let message = timeout(Duration::from_secs(10), subscription.next())
        .await
        .map_err(|_| eyre::eyre!("Timed out waiting for schema broadcast"))?
        .ok_or_else(|| eyre::eyre!("Schema broadcast subscription closed"))?;

    let entries: Vec<SchemaBroadcastEntry> = serde_json::from_slice(&message.payload)?;
    if let Some(entry) = entries.first() {
        assert!(!entry.name.is_empty());
        assert!(!entry.version.is_empty());
        assert!(!entry.schema_id.is_empty());
    }

    Ok(())
}