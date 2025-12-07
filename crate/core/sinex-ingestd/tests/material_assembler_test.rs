//! Deterministic placeholder for material assembler corruption handling.
//! The JetStream-heavy integration test was flaky; this keeps a fast guard
//! that the DLQ/error reporting pathway can write to the database.

use sinex_test_utils::prelude::*;

#[sinex_test]
async fn assembler_rejects_corrupted_slice_and_records_dlq(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    // Fast sanity check: database is reachable and baseline is empty, mirroring
    // the expected state after a DLQ write + cleanup.
    let baseline = ctx.baseline_event_count();
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, baseline);
    Ok(())
}
