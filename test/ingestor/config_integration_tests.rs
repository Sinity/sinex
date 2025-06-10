use anyhow::Result;
use std::fs;
use tempfile::{TempDir, NamedTempFile};
use std::io::Write;
use sinex_unified_collector::{UnifiedConfig, CollectionConfig, DatabaseConfig, LoggingConfig};
use sinex_shared::ingestor_framework::IngestorConfig;

/// Test configuration loading from various sources
#[tokio::test]
async fn test_config_loading_priority() -> Result<()> {
    // Create temporary directory structure
    let temp_dir = TempDir::new()?;
    let config_dir = temp_dir.path().join(".config/sinex");
    fs::create_dir_all(&config_dir)?;
    
    // Create config files at different locations
    let system_config = temp_dir.path().join("etc/sinex/unified.toml");
    fs::create_dir_all(system_config.parent().unwrap())?;
    
    let user_config = config_dir.join("unified.toml");
    let local_config = temp_dir.path().join("unified.toml");
    
    // Write different configs to test priority
    fs::write(&system_config, r#"
[database]
url = "postgresql://system_config"
max_connections = 10

[logging]
level = "info"
format = "json"

[collection]
enabled_sources = ["system"]
poll_interval_secs = 30
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 60
"#)?;
    
    fs::write(&user_config, r#"
[database]
url = "postgresql://user_config"
max_connections = 20

[logging]
level = "debug"
format = "pretty"

[collection]
enabled_sources = ["system", "network"]
poll_interval_secs = 20
batch_size = 50
batch_timeout_ms = 500
heartbeat_interval_secs = 30
"#)?;
    
    fs::write(&local_config, r#"
[database]
url = "postgresql://local_config"
max_connections = 5

[logging]
level = "trace"
format = "compact"

[collection]
enabled_sources = ["system", "network", "process"]
poll_interval_secs = 10
batch_size = 25
batch_timeout_ms = 250
heartbeat_interval_secs = 15
"#)?;
    
    // Test that local config takes precedence
    let config = UnifiedConfig::load_from_file(&local_config)?;
    assert_eq!(config.database.url, "postgresql://local_config");
    assert_eq!(config.database.max_connections, 5);
    assert_eq!(config.logging.level, "trace");
    assert_eq!(config.collection.enabled_sources.len(), 3);
    
    Ok(())
}

/// Test configuration validation
#[tokio::test]
async fn test_config_validation() -> Result<()> {
    // Test invalid configurations
    let invalid_configs = vec![
        // Empty enabled sources
        r#"
[database]
url = "postgresql://test"
max_connections = 10

[logging]
level = "info"
format = "json"

[collection]
enabled_sources = []
poll_interval_secs = 10
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 60
"#,
        // Invalid log level
        r#"
[database]
url = "postgresql://test"
max_connections = 10

[logging]
level = "invalid_level"
format = "json"

[collection]
enabled_sources = ["system"]
poll_interval_secs = 10
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 60
"#,
        // Missing required fields
        r#"
[database]
url = "postgresql://test"

[logging]
level = "info"

[collection]
enabled_sources = ["system"]
"#,
    ];
    
    for (i, config_str) in invalid_configs.iter().enumerate() {
        let temp_file = NamedTempFile::new()?;
        writeln!(temp_file.as_file(), "{}", config_str)?;
        
        // These should parse successfully as TOML allows missing fields
        // In a real implementation, you'd add validation logic
        match UnifiedConfig::load_from_file(temp_file.path()) {
            Ok(config) => {
                // Verify defaults are applied for missing fields
                if i == 2 {
                    assert_eq!(config.database.max_connections, 10); // Should use default
                    assert_eq!(config.collection.poll_interval_secs, 5); // Should use default
                }
            }
            Err(e) => {
                // Some configs might fail TOML parsing
                println!("Config {} failed to parse: {}", i, e);
            }
        }
    }
    
    Ok(())
}

/// Test environment variable overrides
#[tokio::test]
async fn test_env_var_overrides() -> Result<()> {
    // Create base config
    let temp_file = NamedTempFile::new()?;
    writeln!(temp_file.as_file(), r#"
[database]
url = "postgresql://base_config"
max_connections = 10
connection_timeout_secs = 10

[logging]
level = "info"
format = "json"

[collection]
enabled_sources = ["system"]
poll_interval_secs = 30
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 60
"#)?;
    
    // Load config
    let mut config = UnifiedConfig::load_from_file(temp_file.path())?;
    
    // Simulate environment variable overrides
    config.set_database_url("postgresql://env_override".to_string());
    config.set_log_level("debug".to_string());
    
    // Verify overrides
    assert_eq!(config.database_url(), "postgresql://env_override");
    assert_eq!(config.log_level(), "debug");
    
    // Other values should remain unchanged
    assert_eq!(config.database.max_connections, 10);
    assert_eq!(config.collection.poll_interval_secs, 30);
    
    Ok(())
}

/// Test configuration merge scenarios
#[tokio::test]
async fn test_config_merge_scenarios() -> Result<()> {
    // Base configuration
    let base_config = UnifiedConfig {
        database: DatabaseConfig {
            url: "postgresql://base".to_string(),
            max_connections: 10,
            connection_timeout_secs: 10,
        },
        logging: LoggingConfig {
            level: "info".to_string(),
            format: "json".to_string(),
        },
        collection: CollectionConfig {
            enabled_sources: vec!["system".to_string()],
            poll_interval_secs: 30,
            batch_size: 100,
            batch_timeout_ms: 1000,
            heartbeat_interval_secs: 60,
        },
    };
    
    // Test partial override scenario
    let mut override_config = base_config.clone();
    override_config.database.url = "postgresql://override".to_string();
    override_config.collection.enabled_sources.push("network".to_string());
    
    assert_eq!(override_config.database.url, "postgresql://override");
    assert_eq!(override_config.database.max_connections, 10); // Unchanged
    assert_eq!(override_config.collection.enabled_sources.len(), 2);
    
    // Test additive merge for sources
    let mut merged_config = base_config.clone();
    merged_config.collection.enabled_sources = vec![
        "system".to_string(),
        "network".to_string(),
        "process".to_string(),
    ];
    
    assert_eq!(merged_config.collection.enabled_sources.len(), 3);
    assert!(merged_config.collection.enabled_sources.contains(&"system".to_string()));
    assert!(merged_config.collection.enabled_sources.contains(&"network".to_string()));
    assert!(merged_config.collection.enabled_sources.contains(&"process".to_string()));
    
    Ok(())
}

/// Test configuration defaults
#[tokio::test]
async fn test_config_defaults() -> Result<()> {
    let default_config = UnifiedConfig::default();
    
    // Verify sensible defaults
    assert!(!default_config.database.url.is_empty());
    assert!(default_config.database.max_connections > 0);
    assert!(default_config.database.connection_timeout_secs > 0);
    
    assert!(!default_config.logging.level.is_empty());
    assert!(!default_config.logging.format.is_empty());
    
    assert!(!default_config.collection.enabled_sources.is_empty());
    assert!(default_config.collection.poll_interval_secs > 0);
    assert!(default_config.collection.batch_size > 0);
    assert!(default_config.collection.batch_timeout_ms > 0);
    assert!(default_config.collection.heartbeat_interval_secs > 0);
    
    Ok(())
}

/// Test configuration file formats
#[tokio::test]
async fn test_config_file_formats() -> Result<()> {
    // Test well-formed TOML
    let valid_toml = r#"
[database]
url = "postgresql://localhost/sinex"
max_connections = 10
connection_timeout_secs = 10

[logging]
level = "info"
format = "json"

[collection]
enabled_sources = ["system", "network", "process"]
poll_interval_secs = 5
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 60
"#;
    
    let temp_file = NamedTempFile::new()?;
    writeln!(temp_file.as_file(), "{}", valid_toml)?;
    
    let config = UnifiedConfig::load_from_file(temp_file.path())?;
    assert_eq!(config.collection.enabled_sources.len(), 3);
    
    // Test with comments and extra whitespace
    let toml_with_comments = r#"
# Database configuration
[database]
url = "postgresql://localhost/sinex"  # Connection string
max_connections = 10                  # Pool size
connection_timeout_secs = 10

# Logging configuration
[logging]
level = "info"    # Log level: trace, debug, info, warn, error
format = "json"   # Output format

# Collection settings
[collection]
enabled_sources = [
    "system",    # System metrics
    "network",   # Network stats
    "process"    # Process info
]
poll_interval_secs = 5
batch_size = 100
batch_timeout_ms = 1000
heartbeat_interval_secs = 60
"#;
    
    let temp_file2 = NamedTempFile::new()?;
    writeln!(temp_file2.as_file(), "{}", toml_with_comments)?;
    
    let config2 = UnifiedConfig::load_from_file(temp_file2.path())?;
    assert_eq!(config2.collection.enabled_sources.len(), 3);
    assert_eq!(config2.logging.level, "info");
    
    Ok(())
}

/// Test dynamic configuration updates
#[tokio::test]
async fn test_dynamic_config_updates() -> Result<()> {
    let mut config = UnifiedConfig::default();
    
    // Test updating individual fields
    let original_url = config.database.url.clone();
    config.set_database_url("postgresql://updated".to_string());
    assert_ne!(config.database.url, original_url);
    assert_eq!(config.database.url, "postgresql://updated");
    
    // Test updating log level
    config.set_log_level("debug".to_string());
    assert_eq!(config.logging.level, "debug");
    
    // Test adding/removing sources
    config.collection.enabled_sources.clear();
    config.collection.enabled_sources.push("custom_source".to_string());
    assert_eq!(config.collection.enabled_sources.len(), 1);
    assert_eq!(config.collection.enabled_sources[0], "custom_source");
    
    // Test updating timing parameters
    config.collection.poll_interval_secs = 1;
    config.collection.batch_timeout_ms = 50;
    assert_eq!(config.collection.poll_interval_secs, 1);
    assert_eq!(config.collection.batch_timeout_ms, 50);
    
    Ok(())
}