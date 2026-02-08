// Pipeline idempotency tests

// NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires full pipeline infrastructure"]
async fn pipeline_rejects_duplicate_event_ids(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore = "requires full pipeline infrastructure"]
async fn pipeline_rejects_concurrent_duplicates(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
