use sinex_search_automaton::SearchAutomatonConfig;
use sinex_test_utils::{sinex_test, TestResult};

#[sinex_test]
fn search_config_defaults_are_sane() -> TestResult<()> {
    let config = SearchAutomatonConfig::default();
    assert!(!config.searchable_event_types.is_empty());
    assert!(config.enable_fulltext_indexing);
    assert!(config.enable_search_analytics);
    assert!(config.indexing_window_seconds > 0);
    assert!(config.min_content_length > 0);
    assert!(config.max_index_size > 0);
    Ok(())
}
