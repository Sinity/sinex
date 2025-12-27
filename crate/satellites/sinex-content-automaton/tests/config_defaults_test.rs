use sinex_content_automaton::ContentAutomatonConfig;
use sinex_test_utils::{sinex_test, TestResult};

#[sinex_test]
fn content_config_defaults_are_sane() -> TestResult<()> {
    let config = ContentAutomatonConfig::default();
    assert!(!config.target_event_types.is_empty());
    assert!(config.enable_text_analysis);
    assert!(config.enable_content_classification);
    assert!(config.processing_window_seconds > 0);
    assert!(config.max_content_size_bytes >= 1024);
    Ok(())
}
