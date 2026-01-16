use sinex_analytics_automaton::AnalyticsAutomatonConfig;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn analytics_config_defaults_are_sane() -> sinex_test_utils::TestResult<()> {
    let config = AnalyticsAutomatonConfig::default();
    assert_eq!(config.analysis_window_seconds.as_secs(), 3600);
    assert!(config.min_events_for_pattern > 0);
    assert!(config.enable_frequency_analysis);
    assert!(config.enable_pattern_detection);
    Ok(())
}
