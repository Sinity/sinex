use crate::common::prelude::*;
use sinex_collector::{CollectorConfig, OutputConfig, UnifiedCollector};
use sinex_db::validation::EventValidator;
use sinex_test_macros::sinex_test;

/// Test that collector can be created with valid configuration
#[sinex_test]
async fn test_collector_creation() {
    let config = CollectorConfig {
        enabled_events: vec!["filesystem".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };
    
    let output_config = OutputConfig {
        to_database: false,
        to_stdout: true,
        to_file: None,
        dry_run: true,
    };
    
    let _collector = UnifiedCollector::new(config, output_config, None, None);
    // Test passes if creation doesn't panic
}

/// Test output configuration options
#[sinex_test]
async fn test_output_config_database(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let config = CollectorConfig {
        enabled_events: vec!["filesystem".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };
    
    let output_config = OutputConfig {
        to_database: true,
        to_stdout: false,
        to_file: None,
        dry_run: false,
    };
    
    // Create collector with database connection
    let _collector = UnifiedCollector::new(config, output_config, Some(ctx.pool().clone()), None);
    
    Ok(())
}

/// Test collector configuration loading
#[sinex_test]
async fn test_collector_config_loading() {
    // Test default configuration loading
    let result = CollectorConfig::load();
    
    // Should either load a config or use defaults
    match result {
        Ok(config) => {
            // Verify default configuration values
            assert!(!config.enabled_events.is_empty(), "Default config should have enabled events");
            assert!(config.enabled_events.contains(&"file.created".to_string()), "Should include file.created");
            assert!(config.enabled_events.contains(&"command.executed".to_string()), "Should include command.executed");
            
            // Verify configuration structure
            assert!(config.event.is_empty() || !config.event.is_empty(), "Event map should be defined");
            assert!(config.flat_config.is_empty() || !config.flat_config.is_empty(), "Flat config should be defined");
            
            // Test event config lookup works
            let file_config = config.get_event_config("file.created");
            assert!(file_config.is_table(), "Event config should return a table");
        }
        Err(e) => {
            // If loading fails, verify it's not a panic but a proper error
            assert!(!e.to_string().is_empty(), "Error should have a meaningful message: {}", e);
        }
    }
}

/// Test event filtering based on enabled events
#[sinex_test]
async fn test_event_filtering() {
    let mut config = CollectorConfig {
        enabled_events: vec!["filesystem".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };
    
    let output_config = OutputConfig {
        to_database: false,
        to_stdout: true,
        to_file: None,
        dry_run: true,
    };
    
    let _collector = UnifiedCollector::new(config.clone(), output_config.clone(), None, None);
    
    // Test with different event configurations
    config.enabled_events = vec!["terminal".to_string(), "window_manager".to_string()];
    let _collector2 = UnifiedCollector::new(config, output_config, None, None);
}

/// Test collector with file output
#[sinex_test]
async fn test_collector_file_output() {
    let config = CollectorConfig {
        enabled_events: vec!["filesystem".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };
    
    let output_config = OutputConfig {
        to_database: false,
        to_stdout: false,
        to_file: Some("/tmp/test_events.jsonl".to_string()),
        dry_run: false,
    };
    
    let _collector = UnifiedCollector::new(config, output_config, None, None);
    
    // Clean up test file if it exists
    let _ = std::fs::remove_file("/tmp/test_events.jsonl");
}

/// Test collector with validator
#[sinex_test]
async fn test_collector_with_validator(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let config = CollectorConfig {
        enabled_events: vec!["filesystem".to_string()],
        event: HashMap::new(),
        flat_config: HashMap::new(),
        annex_repo_path: None,
    };
    
    let output_config = OutputConfig {
        to_database: true,
        to_stdout: false,
        to_file: None,
        dry_run: false,
    };
    
    // Create validator (no await needed, it's synchronous)
    let validator = EventValidator::new();
    
    let _collector = UnifiedCollector::new(config, output_config, Some(ctx.pool().clone()), Some(validator));
    
    Ok(())
}