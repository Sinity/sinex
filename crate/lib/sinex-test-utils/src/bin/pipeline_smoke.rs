use serde_json::json;
use sinex_core::DynamicPayload;
use sinex_test_utils::{TestContext, TestResult};

#[tokio::main]
async fn main() -> TestResult<()> {
    color_eyre::install()?;

    let ctx = TestContext::with_name("pipeline_smoke")
        .await?
        .with_nats()
        .shared()
        .await?;
    let scope = ctx.pipeline().await?;

    let event_id = scope
        .publish(DynamicPayload::new(
            "pipeline-smoke",
            "smoke.event",
            json!({"ok": true}),
        ))
        .await?;
    scope.wait_for_event_id(event_id).await?;

    scope.shutdown().await?;
    println!("pipeline smoke: ok");
    Ok(())
}
