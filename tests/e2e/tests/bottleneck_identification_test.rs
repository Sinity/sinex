//! JetStream bottleneck identification suites.
//!
//! These benches exercise JetStream under stress to ensure we can detect and
//! surface bottlenecks such as ack backlog and redelivery pressure.

// NOTE: Tests are ignored — blocked on infrastructure that does not yet exist.
// Verified 2026-03: no JetStreamMetrics, consumer_stats API, or ack_backlog
// detection exists anywhere in the codebase. Blockers remain genuine.

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires dedicated performance environment"]
async fn jetstream_ack_backlog_detection(_ctx: TestContext) -> TestResult<()> {
    // Blocked: requires JetStream metrics exposure in sinex-node-sdk
    // (consumer stats API, backlog threshold config). Not implemented as of 2026-03.
    Ok(())
}

#[sinex_test]
#[ignore = "requires dedicated performance environment"]
async fn jetstream_detect_publish_pressure(_ctx: TestContext) -> TestResult<()> {
    // Blocked: requires JetStream metrics exposure in sinex-node-sdk
    // (consumer stats API, backlog threshold config). Not implemented as of 2026-03.
    Ok(())
}
