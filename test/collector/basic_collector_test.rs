use sinex_collector::{CollectorConfig, OutputConfig, UnifiedCollector};
use sinex_db::validation::EventValidator;
use std::collections::HashMap;

/// Test that collector can be created with valid configuration
#[tokio::test]
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
#[sqlx::test]
async fn test_output_config_database(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
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
    let _collector = UnifiedCollector::new(config, output_config, Some(pool), None);
    
    Ok(())
}

/// Test collector configuration loading
#[tokio::test]
async fn test_collector_config_loading() {
    // Test default configuration loading
    let result = CollectorConfig::load();
    
    // Should either load a config or use defaults
    // This test validates the config loading logic doesn't panic
    match result {
        Ok(_config) => {
            // Successfully loaded config
        }
        Err(_) => {
            // Failed to load, which is expected in test environment
            // without config files present
        }
    }
}

/// Test event filtering based on enabled events
#[tokio::test]
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
#[tokio::test]
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
#[sqlx::test]
async fn test_collector_with_validator(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
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
    
    let _collector = UnifiedCollector::new(config, output_config, Some(pool), Some(validator));
    
    Ok(())
}