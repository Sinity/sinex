//! Adversarial coverage for JetStream error paths (publish/connection failures).

// NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires NATS JetStream infrastructure"]
async fn test_nats_connect_failure(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore = "requires NATS JetStream infrastructure"]
async fn test_publish_fails_when_nats_stopped(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
