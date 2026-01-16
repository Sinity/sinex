use sinex_pkm_automaton::PKMAutomatonConfig;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn pkm_config_defaults_are_sane() -> sinex_test_utils::TestResult<()> {
    let config = PKMAutomatonConfig::default();
    assert!(!config.knowledge_event_types.is_empty());
    assert!(config.enable_knowledge_extraction);
    assert!(config.enable_knowledge_graph);
    assert!(config.enable_learning_tracking);
    assert!(config.analysis_window_seconds.as_secs() > 0);
    assert!(config.min_knowledge_items_for_patterns > 0);
    Ok(())
}
