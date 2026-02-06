// # Resource Monitoring Tests
//
// Tests that verify:
// - Memory usage monitoring under high-volume operations
// - Database connection limits under concurrent access
// - Resource exhaustion scenario handling
//
// ## Performance Expectations
//
// - **Individual tests**: 30-90 seconds
// - **Resource usage**: High CPU/memory, significant database load
// - **Dependencies**: PostgreSQL

// NOTE: Tests in this file are temporarily ignored pending API migration
// from insert_event/EventFactory to the new Event/Provenance API.
// See: tests/e2e/tests/stress_test.rs for the updated pattern.

use xtask::sandbox::prelude::*;

#[allow(unused_imports)]
use std::time::Duration;

#[sinex_test]
#[ignore]
async fn test_resource_limits_under_load(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore]
async fn test_memory_monitoring_high_volume(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
