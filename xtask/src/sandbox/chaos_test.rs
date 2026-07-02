use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_chaos_config_clamps_failure_rate() -> ::xtask::sandbox::TestResult<()> {
    let config = ChaosConfig::new(Duration::ZERO, 1.5);
    assert_eq!(config.failure_rate, 1.0);

    let config = ChaosConfig::new(Duration::ZERO, -0.5);
    assert_eq!(config.failure_rate, 0.0);
    Ok(())
}

#[sinex_test]
async fn test_chaos_test_builder_defaults() -> ::xtask::sandbox::TestResult<()> {
    let ctx = ChaosTestBuilder::new().build();
    assert!(!ctx.should_drop());
    assert!(!ctx.should_fail());
    Ok(())
}

#[sinex_test]
async fn test_chaos_metrics_snapshot() -> ::xtask::sandbox::TestResult<()> {
    let metrics = ChaosMetrics::new();
    metrics.record_processed();
    metrics.record_processed();
    metrics.record_failed();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.processed, 2);
    assert_eq!(snapshot.failed, 1);
    Ok(())
}

#[sinex_test]
async fn test_chaos_scenarios_partition_flag() -> ::xtask::sandbox::TestResult<()> {
    let scenarios = ChaosScenarios::new();
    assert!(!scenarios.is_partition_active());

    scenarios.partition_active.store(true, Ordering::SeqCst);
    assert!(scenarios.is_partition_active());
    Ok(())
}
