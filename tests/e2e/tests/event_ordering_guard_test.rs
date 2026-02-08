//! Event Ordering Guard Tests
//!
//! Tests to ensure that event ordering is preserved during ingestion,
//! even when events have timestamps that differ from ingestion order.
//!
//! NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires event ordering infrastructure"]
async fn test_pipeline_preserves_ingest_order_over_ts_orig(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
