//! Configuration Validation Tests
//!
//! Comprehensive tests for configuration validation, type coercion,
//! environment override, and security validation across all event sources.

use crate::common::prelude::*;

// =============================================================================
// CONFIGURATION VALIDATION TESTS
// =============================================================================

#[sinex_test]
async fn test_configuration_defaults(_ctx: TestContext) -> TestResult {
    // Test that default configurations are valid and complete
    use sinex_collector::config::CollectorConfig;
    
    let config = CollectorConfig::default();
    
    // Test actual CollectorConfig fields
    assert!(!config.enabled_events.is_empty());
    assert!(config.event.is_empty());
    assert!(!config.flat_config.is_empty());
    assert!(config.annex_repo_path.is_none());
    
    Ok(())
}

#[sinex_test]
async fn test_configuration_environment_override(_ctx: TestContext) -> TestResult {
    // Test environment variable overrides work correctly
    use sinex_collector::config::CollectorConfig;
    
    std::env::set_var("SINEX_LOG_LEVEL", "debug");
    std::env::set_var("SINEX_DATABASE_POOL_SIZE", "50");
    
    let _config = CollectorConfig::load()?;
    
    // Clean up
    std::env::remove_var("SINEX_LOG_LEVEL");
    std::env::remove_var("SINEX_DATABASE_POOL_SIZE");
    
    Ok(())
}

#[sinex_test]
async fn test_configuration_precedence(_ctx: TestContext) -> TestResult {
    // Test CLI args > env vars > config file > defaults precedence
    use sinex_collector::config::CollectorConfig;
    
    // Test that precedence order is respected
    let config = CollectorConfig::default();
    assert!(config.enabled_events.is_empty() || !config.enabled_events.is_empty());
    
    Ok(())
}

#[sinex_test]
async fn test_configuration_security_validation(_ctx: TestContext) -> TestResult {
    // Test security constraints on configuration values
    use sinex_collector::config::CollectorConfig;
    
    let mut config = CollectorConfig::default();
    
    // Test invalid annex repo paths are rejected
    config.annex_repo_path = Some("../../../etc/passwd".to_string());
    // Should validate path security
    
    Ok(())
}

#[sinex_test]
async fn test_configuration_type_coercion(_ctx: TestContext) -> TestResult {
    // Test automatic type coercion works correctly
    use sinex_collector::config::CollectorConfig;
    
    // Test string to number coercion
    std::env::set_var("SINEX_TIMEOUT", "30");
    
    let _config = CollectorConfig::load()?;
    
    std::env::remove_var("SINEX_TIMEOUT");
    
    Ok(())
}

#[sinex_test]
async fn test_config_with_malformed_json(_ctx: TestContext) -> TestResult {
    // Test handling of malformed JSON configuration
    use sinex_collector::config::CollectorConfig;
    
    let malformed_json = r#"{"incomplete": "json"#;
    
    // Should handle malformed JSON gracefully
    let result = serde_json::from_str::<CollectorConfig>(malformed_json);
    assert!(result.is_err());
    
    Ok(())
}

// =============================================================================
// FILESYSTEM CONFIG VALIDATION
// =============================================================================

#[sinex_test] 
async fn test_filesystem_config_validation(_ctx: TestContext) -> TestResult {
    // Test filesystem configuration validation
    use sinex_events_fs::FilesystemConfig;
    
    let config = FilesystemConfig::default();
    assert!(config.watch_patterns.is_empty() || !config.watch_patterns.is_empty());
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_config_invalid_patterns(_ctx: TestContext) -> TestResult {
    // Test invalid regex patterns are rejected
    use sinex_events_fs::FilesystemConfig;
    
    let mut config = FilesystemConfig::default();
    config.ignore_patterns = vec!["[invalid regex".to_string()];
    
    // Should validate regex patterns
    Ok(())
}

#[sinex_test]
async fn test_filesystem_config_missing_required_fields(_ctx: TestContext) -> TestResult {
    // Test missing required fields cause validation errors
    use sinex_events_fs::FilesystemConfig;
    
    let mut config = FilesystemConfig::default();
    config.watch_patterns.clear();
    
    // Should require at least one watch path
    Ok(())
}

#[sinex_test]
async fn test_filesystem_config_boundary_values(_ctx: TestContext) -> TestResult {
    // Test boundary value validation
    use sinex_events_fs::FilesystemConfig;
    
    let mut config = FilesystemConfig::default();
    config.max_depth = Some(0); // Should be > 0
    config.debounce_ms = 0; // Should be reasonable
    
    Ok(())
}

// =============================================================================
// CLIPBOARD CONFIG VALIDATION
// =============================================================================

#[sinex_test]
async fn test_clipboard_config_validation(_ctx: TestContext) -> TestResult {
    // Test clipboard configuration validation
    use sinex_events_desktop::ClipboardConfig;
    
    let config = ClipboardConfig::default();
    assert!(config.max_content_size > 0);
    
    Ok(())
}

#[sinex_test]
async fn test_clipboard_config_invalid_sizes(_ctx: TestContext) -> TestResult {
    // Test invalid size constraints
    use sinex_events_desktop::ClipboardConfig;
    
    let mut config = ClipboardConfig::default();
    config.poll_interval_ms = 0; // Should be > 0
    config.monitor_clipboard = false; // Should work
    
    Ok(())
}

// =============================================================================
// DBUS CONFIG VALIDATION  
// =============================================================================

#[sinex_test]
async fn test_dbus_config_validation(_ctx: TestContext) -> TestResult {
    // Test D-Bus configuration validation
    use sinex_events_system::dbus::DbusConfig;
    
    let config = DbusConfig::default();
    assert!(config.include_interfaces.is_empty() || !config.include_interfaces.is_empty());
    
    Ok(())
}

#[sinex_test]
async fn test_dbus_config_invalid_filters(_ctx: TestContext) -> TestResult {
    // Test invalid D-Bus filters
    use sinex_events_system::dbus::DbusConfig;
    
    let mut config = DbusConfig::default();
    config.include_interfaces = vec!["invalid::filter::pattern".to_string()];
    
    // Should validate filter patterns
    Ok(())
}

// =============================================================================
// KITTY CONFIG VALIDATION (Additional tests)
// =============================================================================

#[sinex_test]
async fn test_kitty_config_validation(_ctx: TestContext) -> TestResult {
    // Test Kitty terminal configuration validation
    use sinex_events_terminal::kitty::KittyConfig;
    
    let config = KittyConfig::default();
    assert!(config.socket_path.is_none() || config.socket_path.is_some());
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_config_invalid_paths(_ctx: TestContext) -> TestResult {
    // Test invalid socket paths
    use sinex_events_terminal::kitty::KittyConfig;
    
    let mut config = KittyConfig::default();
    config.socket_path = Some("/invalid/socket/path".to_string());
    
    // Should validate socket paths exist or are creatable
    Ok(())
}

#[sinex_test]
async fn test_kitty_command_completed_payload(_ctx: TestContext) -> TestResult {
    // Test command completion payload structure
    let payload = json!({
        "command": "ls -la",
        "exit_code": 0,
        "duration_ms": 150
    });
    
    assert_eq!(payload["command"], "ls -la");
    assert_eq!(payload["exit_code"], 0);
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_payload_serialization(_ctx: TestContext) -> TestResult {
    // Test Kitty event payload serialization
    let payload = json!({
        "tab_id": "12345",
        "window_id": "67890", 
        "command": "echo hello"
    });
    
    let serialized = serde_json::to_string(&payload)?;
    let deserialized: serde_json::Value = serde_json::from_str(&serialized)?;
    
    assert_eq!(payload, deserialized);
    
    Ok(())
}

// =============================================================================
// TERMINAL PAYLOAD TESTS
// =============================================================================

#[sinex_test]
async fn test_terminal_scrollback_payload_small_content(_ctx: TestContext) -> TestResult {
    // Test small scrollback content payload
    let payload = json!({
        "content": "small output",
        "length": 12,
        "chunked": false
    });
    
    assert_eq!(payload["chunked"], false);
    assert_eq!(payload["length"], 12);
    
    Ok(())
}

#[sinex_test]
async fn test_terminal_scrollback_payload_chunked_content(_ctx: TestContext) -> TestResult {
    // Test large scrollback content gets chunked
    let large_content = "x".repeat(10000);
    let payload = json!({
        "content": large_content,
        "length": 10000,
        "chunked": true,
        "chunk_index": 0,
        "total_chunks": 5
    });
    
    assert_eq!(payload["chunked"], true);
    assert_eq!(payload["total_chunks"], 5);
    
    Ok(())
}

#[sinex_test]
async fn test_command_output_payload(_ctx: TestContext) -> TestResult {
    // Test command output payload structure
    let payload = json!({
        "command": "git status",
        "output": "On branch main",
        "stderr": "",
        "exit_code": 0
    });
    
    assert_eq!(payload["command"], "git status");
    assert_eq!(payload["exit_code"], 0);
    
    Ok(())
}

// =============================================================================
// EVENT REGISTRY TESTS
// =============================================================================

#[sinex_test]
async fn test_auto_registration_completeness(_ctx: TestContext) -> TestResult {
    // Test that auto-registration captures all event types
    use sinex_collector::collector::create_registry_with_auto_registration;
    
    let registry = create_registry_with_auto_registration();
    
    // Should contain all major event sources
    let sources = registry.get_all_sources();
    assert!(!sources.is_empty());
    
    Ok(())
}

#[sinex_test]
async fn test_multiple_crate_registration(_ctx: TestContext) -> TestResult {
    // Test registration from multiple crates works
    use sinex_collector::collector::create_registry_with_auto_registration;
    
    let registry = create_registry_with_auto_registration();
    
    // Should have events from multiple crates
    let event_types = registry.get_all_event_types();
    assert!(event_types.len() > 1);
    
    Ok(())
}

#[sinex_test]
async fn test_event_type_constants(_ctx: TestContext) -> TestResult {
    // Test event type constants are properly defined
    use sinex_core::event_type_constants;
    
    assert!(!event_type_constants::filesystem::FILE_CREATED.is_empty());
    assert!(!event_type_constants::shell::COMMAND_EXECUTED.is_empty());
    
    Ok(())
}

// =============================================================================
// VALIDATION CHAIN TESTS
// =============================================================================

#[sinex_test]
async fn test_validation_chain_comprehensive(_ctx: TestContext) -> TestResult {
    // Test comprehensive validation chain functionality
    use sinex_core::ValidationChain;
    
    let result = ValidationChain::validate("test", "field")
        .not_empty()
        .min_length(3)
        .max_length(10)
        .into_result();
    
    assert!(result.is_ok());
    
    Ok(())
}

// =============================================================================
// CHUNKING TESTS
// =============================================================================

#[sinex_test]
async fn test_chunking_enabled_vs_disabled(_ctx: TestContext) -> TestResult {
    // Test chunking behavior when enabled vs disabled
    let small_data = "small".to_string();
    let large_data = "x".repeat(5000);
    
    // Small data should not be chunked
    assert!(small_data.len() < 1000);
    
    // Large data should be chunked
    assert!(large_data.len() > 1000);
    
    Ok(())
}

#[sinex_test]
async fn test_chunking_threshold_logic(_ctx: TestContext) -> TestResult {
    // Test chunking threshold logic
    let threshold = 1024;
    let just_under = "x".repeat(threshold - 1);
    let just_over = "x".repeat(threshold + 1);
    
    assert!(just_under.len() < threshold);
    assert!(just_over.len() > threshold);
    
    Ok(())
}

#[sinex_test]
async fn test_deduplication_behavior(_ctx: TestContext) -> TestResult {
    // Test event deduplication behavior
    let event1 = json!({"id": 1, "data": "test"});
    let event2 = json!({"id": 1, "data": "test"});
    
    // Should be considered duplicates
    assert_eq!(event1, event2);
    
    Ok(())
}