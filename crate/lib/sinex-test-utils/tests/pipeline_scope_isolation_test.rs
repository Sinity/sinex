use serde_json::json;
use sinex_test_utils::prelude::*;

#[sinex_test(timeout = 60)]
async fn pipeline_scope_streams_are_isolated(ctx: TestContext) -> TestResult<()> {
    let ctx1 = ctx.with_shared_nats().await?;
    let ctx2 = TestContext::new().await?.with_shared_nats().await?;

    let scope1 = ctx1.pipeline_scope().await?;
    let scope2 = ctx2.pipeline_scope().await?;

    let stream1 = scope1.stream("SINEX_RAW_EVENTS");
    let stream2 = scope2.stream("SINEX_RAW_EVENTS");
    ensure!(stream1 != stream2, "pipeline streams must be namespaced");

    let event1 = scope1
        .publish("isolation.source", "isolation.event", json!({"scope": 1}))
        .await?;
    let event2 = scope2
        .publish("isolation.source", "isolation.event", json!({"scope": 2}))
        .await?;

    scope1.wait_for_event_id(event1.clone()).await?;
    scope2.wait_for_event_id(event2.clone()).await?;

    ensure!(
        ctx1.pool
            .events()
            .get_by_id(event2.clone())
            .await?
            .is_none(),
        "scope1 should not see scope2 events"
    );
    ensure!(
        ctx2.pool
            .events()
            .get_by_id(event1.clone())
            .await?
            .is_none(),
        "scope2 should not see scope1 events"
    );

    let js = ctx1.jetstream().await?;
    let info1 = js.get_stream(&stream1).await?.info().await?.state;
    let info2 = js.get_stream(&stream2).await?.info().await?.state;
    ensure!(info1.messages >= 1, "stream1 should have events");
    ensure!(info2.messages >= 1, "stream2 should have events");

    let (res1, res2) = tokio::join!(scope1.shutdown(), scope2.shutdown());
    res1?;
    res2?;
    Ok(())
}
