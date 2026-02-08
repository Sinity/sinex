//! Material Idempotency Tests
//!
//! Tests for idempotent handling of material stream ingestion.

// NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires full pipeline infrastructure"]
async fn test_material_stream_idempotency(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore = "requires full pipeline infrastructure"]
async fn test_material_duplicate_handling(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
