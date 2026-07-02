use super::*;

#[sinex_test]
async fn jetstream_test_helper_creates_topology(ctx: Sandbox) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let helper = JetStreamTestHelper::new(&ctx, "helper-test").await?;

    // Verify topology was created
    assert!(!helper.topology().events_stream.is_empty());
    assert!(!helper.topology().confirmations_stream.is_empty());
    assert!(!helper.topology().dlq_stream.is_empty());

    // Verify DLQ is empty initially
    helper.assert_dlq_empty().await?;

    Ok(())
}
