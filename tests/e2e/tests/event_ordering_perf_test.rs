//! Performance-oriented event ordering tests.

// NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires event ordering infrastructure"]
async fn perf_ulid_sequence_ordering_validation(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore = "requires event ordering infrastructure"]
async fn perf_concurrent_ulid_generation_ordering(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore = "requires event ordering infrastructure"]
async fn perf_database_ordering_consistency(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
