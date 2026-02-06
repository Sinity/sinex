//! JetStream performance smoke tests.
//!
//! These benches exercise the JetStream publish/consume path that replaced the
//! legacy Redis Streams infrastructure. The goal is to keep a lightweight set of
//! throughput/latency measurements that run against an ephemeral NATS server so
//! we can spot obvious regressions while the more complete benchmarking suite is
//! rebuilt.

// NOTE: Tests temporarily ignored pending API migration

use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore]
async fn jetstream_publish_throughput(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore]
async fn jetstream_concurrent_consumer_distribution(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}

#[sinex_test]
#[ignore]
async fn jetstream_redelivery_on_expired_ack(_ctx: TestContext) -> TestResult<()> {
    // FIXME: Test body removed pending API migration
    Ok(())
}
