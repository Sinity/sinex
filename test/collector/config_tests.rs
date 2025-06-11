use sinex_collector::config::CollectorConfig;
use std::collections::HashMap;

#[test]
fn test_default_config() {
    let config = CollectorConfig::default();
    assert!(!config.enabled_events.is_empty());
    assert!(config.enabled_events.contains(&"file.created".to_string()));
    assert!(config.enabled_events.contains(&"file.modified".to_string()));
    assert!(config.enabled_events.contains(&"file.deleted".to_string()));
}

#[test]
fn test_config_event_lookup() {
    let config = CollectorConfig::default();
    
    // Test that get_event_config returns a valid toml::Value
    let event_config = config.get_event_config("file.created");
    assert!(event_config.is_table());
    
    // Test hierarchical config lookup
    let command_config = config.get_event_config("command.executed");
    assert!(command_config.is_table());
}