use serde_json::json;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::PipelineNamespace;

#[sinex_test]
async fn pipeline_namespace_subjects_are_isolated(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;

    let source = "namespace-isolation";
    let event_type = "isolation.event";
    let ns_a = PipelineNamespace::new("namespace-isolation-a");
    let ns_b = PipelineNamespace::new("namespace-isolation-b");

    let nats = ctx.nats_handle()?;
    nats.create_stream(
        &ns_a.stream("SINEX_RAW_EVENTS"),
        &[&ns_a.subject("events.raw.>")],
    )
    .await?;
    nats.create_stream(
        &ns_b.stream("SINEX_RAW_EVENTS"),
        &[&ns_b.subject("events.raw.>")],
    )
    .await?;

    let publisher = TestNodePublisher::with_namespace(
        ctx.nats_client(),
        source,
        Some(ns_a.prefix().to_string()),
    );
    publisher
        .publish(event_type, json!({"namespace": "a"}))
        .await?;

    let js = ctx.jetstream().await?;
    let stream_a = ns_a.stream("SINEX_RAW_EVENTS");
    let stream_b = ns_b.stream("SINEX_RAW_EVENTS");

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let stream_a = stream_a.clone();
            async move {
                let mut info = js
                    .get_stream(&stream_a)
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                let state = info
                    .info()
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                    .state;
                Ok(state.messages >= 1)
            }
        },
        10,
    )
    .await?;

    let state_b = js.get_stream(&stream_b).await?.info().await?.state;
    ensure!(
        state_b.messages == 0,
        "namespace B should not receive namespace A messages"
    );

    Ok(())
}
