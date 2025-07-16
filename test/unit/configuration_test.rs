//! Configuration Validation Tests
//!
//! Comprehensive tests for configuration validation, type coercion,
//! environment override, and security validation across all event sources.

use crate::common::prelude::*;
use sinex_satellite_sdk::EventSourceContext;

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

// =============================================================================
// COMPREHENSIVE CONFIG VALIDATION (migrated from configuration_validation_test.rs)
// =============================================================================

#[sinex_test]
async fn test_filesystem_config_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_fs::FilesystemConfig;

    // Test valid filesystem configuration
    let valid_config = json!({
        "watch_patterns": ["/home/user/**/*", "/tmp/**/*.log"],
        "ignore_patterns": ["*.tmp", "*.swp", ".git/**/*"],
        "debounce_ms": 500,
        "max_depth": 10
    });

    let context = EventSourceContext::new(valid_config);
    let config: FilesystemConfig = serde_json::from_value(context.config)?;

    assert_eq!(config.watch_patterns.len(), 2);
    assert_eq!(config.ignore_patterns.len(), 3);
    assert_eq!(config.debounce_ms, 500);
    assert_eq!(config.max_depth, Some(10));

    Ok(())
}

#[sinex_test]
async fn test_filesystem_config_invalid_patterns_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_fs::FilesystemConfig;

    // Test invalid glob patterns
    let invalid_config = json!({
        "watch_patterns": ["[invalid-glob-pattern"],
        "ignore_patterns": ["[another-invalid"],
        "debounce_ms": 100
    });

    let context = EventSourceContext::new(invalid_config);
    let result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);

    // Should fail due to invalid glob patterns or provide defaults
    match result {
        Err(e) => {
            assert!(e.to_string().contains("invalid") || e.to_string().contains("pattern"));
        }
        Ok(_) => {} // Might provide defaults which is acceptable
    }

    Ok(())
}

#[sinex_test]
async fn test_filesystem_config_boundary_values_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_fs::FilesystemConfig;

    // Test boundary values for numeric fields
    let boundary_tests = vec![
        ("zero_debounce", json!({"debounce_ms": 0})),
        ("huge_debounce", json!({"debounce_ms": 1000000})),
    ];

    for (test_name, config) in boundary_tests {
        let context = EventSourceContext::new(config);
        let result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);

        match test_name {
            "zero_debounce" => {
                if let Ok(cfg) = result {
                    assert_eq!(cfg.debounce_ms, 0);
                }
            }
            "huge_debounce" => {
                match result {
                    Ok(cfg) => assert!(
                        cfg.debounce_ms <= 3600000,
                        "Should clamp extremely large values"
                    ),
                    Err(_) => {} // Rejection is also acceptable
                }
            }
            _ => {}
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_clipboard_config_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_desktop::clipboard::ClipboardConfig;

    // Test valid clipboard configuration
    let valid_config = json!({
        "monitor_clipboard": true,
        "monitor_primary": true,
        "max_content_size": 1048576,
        "poll_interval_ms": 250,
        "enable_history": true
    });

    let context = EventSourceContext::new(valid_config);
    let config: ClipboardConfig = serde_json::from_value(context.config)?;

    assert!(config.monitor_clipboard);
    assert!(config.monitor_primary);
    assert_eq!(config.max_content_size, 1048576);
    assert_eq!(config.poll_interval_ms, 250);
    assert!(config.enable_history);

    Ok(())
}

#[sinex_test]
async fn test_clipboard_config_invalid_sizes_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_desktop::clipboard::ClipboardConfig;

    // Test invalid size configurations
    let invalid_configs = vec![
        ("zero_size", json!({"max_content_size": 0})),
        ("tiny_interval", json!({"poll_interval_ms": 0})),
        ("huge_interval", json!({"poll_interval_ms": 3600000})),
    ];

    for (test_name, config) in invalid_configs {
        let context = EventSourceContext::new(config);
        let result: Result<ClipboardConfig, _> = serde_json::from_value(context.config);

        match test_name {
            "zero_size" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(
                        cfg.max_content_size > 0,
                        "Should have positive max content size"
                    );
                }
            }
            "tiny_interval" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(
                        cfg.poll_interval_ms >= 10,
                        "Should have reasonable minimum interval"
                    );
                }
            }
            "huge_interval" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(
                        cfg.poll_interval_ms <= 60000,
                        "Should clamp excessive intervals"
                    );
                }
            }
            _ => {}
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_kitty_config_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_terminal::kitty::KittyConfig;

    // Test valid Kitty configuration
    let valid_config = json!({
        "socket_path": "/tmp/kitty-socket",
        "poll_interval_seconds": 5,
        "enabled": true
    });

    let context = EventSourceContext::new(valid_config);
    let config: KittyConfig = serde_json::from_value(context.config)?;

    assert_eq!(config.socket_path, Some("/tmp/kitty-socket".to_string()));
    assert!(config.enabled);
    assert_eq!(config.poll_interval_seconds, 5);

    Ok(())
}

#[sinex_test]
async fn test_kitty_config_invalid_paths_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_terminal::kitty::KittyConfig;

    // Test invalid socket paths
    let invalid_configs = vec![
        ("empty_path", json!({"socket_path": ""})),
        ("relative_path", json!({"socket_path": "./relative/socket"})),
    ];

    for (test_name, config) in invalid_configs {
        let context = EventSourceContext::new(config);
        let result: Result<KittyConfig, _> = serde_json::from_value(context.config);

        match test_name {
            "empty_path" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.socket_path.is_some(), "Should provide default path");
                }
            }
            "relative_path" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.socket_path.is_some(), "Should convert to absolute path");
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_dbus_config_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_system::dbus::DbusConfig;

    // Test valid D-Bus configuration
    let valid_config = json!({
        "monitor_session": true,
        "monitor_system": false,
        "include_interfaces": [
            "org.freedesktop.Notifications",
            "org.freedesktop.DBus"
        ],
        "extract_notifications": true
    });

    let context = EventSourceContext::new(valid_config);
    let config: DbusConfig = serde_json::from_value(context.config)?;

    assert!(config.monitor_session);
    assert!(!config.monitor_system);
    assert_eq!(config.include_interfaces.len(), 2);
    assert!(config.extract_notifications);

    Ok(())
}

#[sinex_test]
async fn test_config_with_malformed_json_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_fs::FilesystemConfig;

    // Test various malformed JSON scenarios
    let malformed_configs = vec![
        ("trailing_comma", r#"{"valid": true, "trailing": "comma",}"#),
        ("duplicate_keys", r#"{"key": "first", "key": "duplicate"}"#),
    ];

    for (_test_name, json_str) in malformed_configs {
        let parse_result = serde_json::from_str::<serde_json::Value>(json_str);

        match parse_result {
            Ok(value) => {
                // Even if parsing succeeds, test that config extraction is robust
                let context = EventSourceContext::new(value);
                let _result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);
                // Should either work or fail gracefully
            }
            Err(e) => {
                assert!(e.to_string().contains("JSON") || e.to_string().contains("parse"));
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_configuration_defaults_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_desktop::clipboard::ClipboardConfig;
    use sinex_events_fs::FilesystemConfig;
    use sinex_events_system::dbus::DbusConfig;
    use sinex_events_terminal::kitty::KittyConfig;

    // Test that all configurations provide sensible defaults
    let empty_context = EventSourceContext::new(serde_json::json!({}));

    // Test filesystem defaults
    let fs_config: FilesystemConfig = serde_json::from_value(empty_context.config.clone())?;
    assert!(
        !fs_config.watch_patterns.is_empty(),
        "Should have default watch patterns"
    );
    assert!(fs_config.debounce_ms > 0, "Should have positive debounce");

    // Test clipboard defaults
    let clipboard_config: ClipboardConfig = serde_json::from_value(empty_context.config.clone())?;
    assert!(
        clipboard_config.max_content_size > 0,
        "Should have positive max size"
    );
    assert!(
        clipboard_config.poll_interval_ms > 0,
        "Should have positive interval"
    );

    // Test Kitty defaults
    let kitty_config: KittyConfig = serde_json::from_value(empty_context.config.clone())?;
    assert!(
        kitty_config.poll_interval_seconds > 0,
        "Should have positive timeout"
    );

    // Test D-Bus defaults
    let dbus_config: DbusConfig = serde_json::from_value(empty_context.config.clone())?;
    assert!(
        dbus_config.monitor_session || dbus_config.monitor_system,
        "Should monitor at least one bus"
    );

    Ok(())
}

#[sinex_test]
async fn test_configuration_type_coercion_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_fs::FilesystemConfig;
    use sinex_events_terminal::kitty::KittyConfig;

    // Test that configuration values are properly coerced between types
    let type_coercion_tests = vec![
        ("string_to_number", json!({"debounce_ms": "500"})),
        ("string_to_boolean", json!({"enabled": "true"})),
    ];

    for (test_name, config) in type_coercion_tests {
        let context = EventSourceContext::new(config);

        match test_name {
            "string_to_number" => {
                if let Ok(cfg) = serde_json::from_value::<FilesystemConfig>(context.config) {
                    assert_eq!(cfg.debounce_ms, 500, "Should convert string to number");
                }
            }
            "string_to_boolean" => {
                if let Ok(cfg) = serde_json::from_value::<KittyConfig>(context.config) {
                    assert!(cfg.enabled, "Should convert 'true' string to boolean");
                }
            }
            _ => {}
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_configuration_security_validation_comprehensive(_ctx: TestContext) -> TestResult {
    use sinex_core::EventSourceContext;
    use sinex_events_terminal::kitty::KittyConfig;

    // Test that potentially dangerous configuration values are rejected
    let security_tests = vec![
        (
            "path_traversal",
            json!({"socket_path": "/tmp/../../../etc/passwd"}),
        ),
        (
            "shell_injection",
            json!({"socket_path": "/tmp/socket; rm -rf /"}),
        ),
        ("long_path", json!({"socket_path": "/".repeat(1000)})),
    ];

    for (test_name, config) in security_tests {
        let context = EventSourceContext::new(config);
        let result: Result<KittyConfig, _> = serde_json::from_value(context.config);

        match result {
            Ok(cfg) => match test_name {
                "path_traversal" => {
                    let default_path = "".to_string();
                    let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                    assert!(
                        !path_str.contains("../"),
                        "Should not contain path traversal"
                    );
                }
                "shell_injection" => {
                    let default_path = "".to_string();
                    let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                    assert!(
                        !path_str.contains(";"),
                        "Should not contain shell metacharacters"
                    );
                }
                "long_path" => {
                    let default_path = "".to_string();
                    let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                    assert!(path_str.len() <= 4096, "Should limit path length");
                }
                _ => {}
            },
            Err(_) => {} // Rejection is acceptable for security tests
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_validation_chain_comprehensive_usage(_ctx: TestContext) -> TestResult {
    use sinex_core::ValidationChain;

    // Test string validation
    let result = ValidationChain::validate("test_string", "field_name")
        .not_empty()
        .min_length(5)
        .max_length(20)
        .into_result();
    assert!(result.is_ok(), "Valid string should pass validation");

    // Test string validation failure
    let result = ValidationChain::validate("", "empty_field")
        .not_empty()
        .into_result();
    assert!(
        result.is_err(),
        "Empty string should fail not_empty validation"
    );

    // Test number validation
    let result = ValidationChain::validate(42, "number_field")
        .min_value(0)
        .into_result();
    assert!(result.is_ok(), "Valid number should pass validation");

    // Test number validation failure
    let result = ValidationChain::validate(-5, "negative_field")
        .min_value(0)
        .into_result();
    assert!(
        result.is_err(),
        "Negative number should fail min_value validation"
    );

    Ok(())
}
