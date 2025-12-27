use sinex_health_aggregator::HealthAggregatorConfig;
use sinex_test_utils::{sinex_test, TestResult};

#[sinex_test]
fn health_aggregator_config_defaults_are_sane() -> TestResult<()> {
    let config = HealthAggregatorConfig::default();
    assert!(!config.component_check_intervals.is_empty());
    assert!(config.aggregation_window_seconds > 0);
    assert!(config.unhealthy_threshold_minutes > 0);
    assert!(config.enable_system_health_status);
    assert!(config.enable_component_health_reports);
    Ok(())
}
