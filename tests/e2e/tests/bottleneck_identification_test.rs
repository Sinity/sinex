//! JetStream bottleneck identification suites.
//!
//! These benches exercise JetStream under stress to ensure we can detect and
//! surface bottlenecks such as ack backlog and redelivery pressure.

// NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore = "requires dedicated performance environment"]
async fn jetstream_ack_backlog_detection(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Requires JetStream metrics collection to detect ack backlog.
    // The test needs: (1) JetStream consumer stats API integration,
    // (2) backlog threshold configuration, (3) real NATS consumer under load.
    // Blocked on: JetStream metrics exposure in sinex-node-sdk.
    Ok(())
}

#[sinex_test]
#[ignore = "requires dedicated performance environment"]
async fn jetstream_detect_publish_pressure(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Requires JetStream metrics collection to detect ack backlog.
    // The test needs: (1) JetStream consumer stats API integration,
    // (2) backlog threshold configuration, (3) real NATS consumer under load.
    // Blocked on: JetStream metrics exposure in sinex-node-sdk.
    Ok(())
}
