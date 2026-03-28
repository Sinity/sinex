use sinex_health_automaton::HealthAggregatorConfig;
use xtask::sandbox::sinex_test;

struct EnvGuard {
    key: &'static str,
    original: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let original = std::env::var(key).ok();
        unsafe { std::env::set_var(key, value) };
        Self { key, original }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

#[sinex_test]
async fn health_aggregator_config_defaults_are_sane() -> xtask::sandbox::TestResult<()> {
    let config = HealthAggregatorConfig::default();
    assert!(!config.component_check_intervals.is_empty());
    assert!(config.aggregation_window_seconds > 0);
    assert!(config.unhealthy_threshold_minutes > 0);
    assert!(config.enable_system_health_status);
    assert!(config.enable_component_health_reports);
    Ok(())
}

#[sinex_test]
async fn invalid_env_overrides_fall_back_to_defaults() -> xtask::sandbox::TestResult<()> {
    let _window = EnvGuard::set(
        "SINEX_HEALTH_AGGREGATOR_AGGREGATION_WINDOW_SECONDS",
        "not-a-number",
    );
    let _unhealthy = EnvGuard::set(
        "SINEX_HEALTH_AGGREGATOR_UNHEALTHY_THRESHOLD_MINUTES",
        "still-bad",
    );
    let _system = EnvGuard::set(
        "SINEX_HEALTH_AGGREGATOR_ENABLE_SYSTEM_HEALTH_STATUS",
        "not-a-bool",
    );
    let _components = EnvGuard::set(
        "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
        "{\"default\":0}",
    );

    let config = HealthAggregatorConfig::from_env();
    let defaults = HealthAggregatorConfig::default();

    assert_eq!(
        config.aggregation_window_seconds,
        defaults.aggregation_window_seconds
    );
    assert_eq!(
        config.unhealthy_threshold_minutes,
        defaults.unhealthy_threshold_minutes
    );
    assert_eq!(
        config.enable_system_health_status,
        defaults.enable_system_health_status
    );
    assert_eq!(
        config.component_check_intervals,
        defaults.component_check_intervals
    );
    Ok(())
}

#[sinex_test]
async fn valid_env_overrides_are_applied() -> xtask::sandbox::TestResult<()> {
    let _window = EnvGuard::set("SINEX_HEALTH_AGGREGATOR_AGGREGATION_WINDOW_SECONDS", "42");
    let _unhealthy = EnvGuard::set("SINEX_HEALTH_AGGREGATOR_UNHEALTHY_THRESHOLD_MINUTES", "7");
    let _system = EnvGuard::set("SINEX_HEALTH_AGGREGATOR_ENABLE_SYSTEM_HEALTH_STATUS", "false");
    let _component_reports = EnvGuard::set(
        "SINEX_HEALTH_AGGREGATOR_ENABLE_COMPONENT_HEALTH_REPORTS",
        "false",
    );
    let _components = EnvGuard::set(
        "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
        "{\"default\":15,\"fast\":2}",
    );

    let config = HealthAggregatorConfig::from_env();

    assert_eq!(config.aggregation_window_seconds, 42);
    assert_eq!(config.unhealthy_threshold_minutes, 7);
    assert!(!config.enable_system_health_status);
    assert!(!config.enable_component_health_reports);
    assert_eq!(config.component_check_intervals.get("default"), Some(&15));
    assert_eq!(config.component_check_intervals.get("fast"), Some(&2));
    Ok(())
}
