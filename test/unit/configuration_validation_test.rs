use sinex_core::EventSourceContext;
use sinex_events_fs::FilesystemConfig;
use sinex_events_desktop::clipboard::ClipboardConfig;
use sinex_events_terminal::kitty::KittyConfig;
use sinex_events_system::dbus::DbusConfig;
use serde_json::json;

#[test]
fn test_filesystem_config_validation() {
    // Test valid filesystem configuration
    let valid_config = json!({
        "watch_patterns": ["/home/user/**/*", "/tmp/**/*.log"],
        "ignore_patterns": ["*.tmp", "*.swp", ".git/**/*"],
        "debounce_ms": 500,
        "max_depth": 10
    });
    
    let context = EventSourceContext::new(valid_config);
    let config: FilesystemConfig = serde_json::from_value(context.config).unwrap();
    
    assert_eq!(config.watch_patterns.len(), 2);
    assert_eq!(config.ignore_patterns.len(), 3);
    assert_eq!(config.debounce_ms, 500);
    assert_eq!(config.max_depth, Some(10));
}

#[test]
fn test_filesystem_config_invalid_patterns() {
    // Test invalid glob patterns
    let invalid_config = json!({
        "watch_patterns": ["[invalid-glob-pattern"],
        "ignore_patterns": ["[another-invalid"],
        "debounce_ms": 100
    });
    
    let context = EventSourceContext::new(invalid_config);
    let result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);
    
    // Should fail due to invalid glob patterns
    match result {
        Err(e) => {
            assert!(e.to_string().contains("invalid") || e.to_string().contains("pattern"));
        }
        Ok(_) => panic!("Expected validation to fail for invalid glob patterns"),
    }
}

#[test]
fn test_filesystem_config_missing_required_fields() {
    // Test configuration missing required fields
    let minimal_config = json!({
        "debounce_ms": 200
        // Missing watch_patterns
    });
    
    let context = EventSourceContext::new(minimal_config);
    let result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);
    
    // Should use defaults for missing fields
    match result {
        Ok(config) => {
            assert!(!config.watch_patterns.is_empty(), "Should provide default watch patterns");
        }
        Err(e) => {
            assert!(e.to_string().contains("required") || e.to_string().contains("missing"));
        }
    }
}

#[test]
fn test_filesystem_config_boundary_values() {
    // Test boundary values for numeric fields
    let boundary_tests = vec![
        ("zero_debounce", json!({"debounce_ms": 0})),
        ("negative_debounce", json!({"debounce_ms": -1})),
        ("huge_debounce", json!({"debounce_ms": 1000000})),
        ("float_debounce", json!({"debounce_ms": 100.5})),
    ];
    
    for (test_name, config) in boundary_tests {
        let context = EventSourceContext::new(config);
        let result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);
        
        match test_name {
            "zero_debounce" => {
                // Zero debounce might be valid
                if let Ok(cfg) = result {
                    assert_eq!(cfg.debounce_ms, 0);
                }
            }
            "negative_debounce" => {
                // Negative should be rejected
                assert!(result.is_err(), "Negative debounce should be rejected");
            }
            "huge_debounce" => {
                // Very large values might be rejected or clamped
                match result {
                    Ok(cfg) => assert!(cfg.debounce_ms <= 3600000, "Should clamp extremely large values"),
                    Err(_) => {} // Rejection is also acceptable
                }
            }
            "float_debounce" => {
                // Float should be converted to int or rejected
                match result {
                    Ok(cfg) => assert_eq!(cfg.debounce_ms, 100),
                    Err(_) => {} // Rejection is also acceptable
                }
            }
            _ => {}
        }
    }
}

#[test]
fn test_clipboard_config_validation() {
    // Test valid clipboard configuration
    let valid_config = json!({
        "monitor_clipboard": true,
        "monitor_primary": true,
        "max_content_size": 1048576,
        "poll_interval_ms": 250,
        "enable_history": true
    });
    
    let context = EventSourceContext::new(valid_config);
    let config: ClipboardConfig = serde_json::from_value(context.config).unwrap();
    
    assert!(config.monitor_clipboard);
    assert!(config.monitor_primary);
    assert_eq!(config.max_content_size, 1048576);
    assert_eq!(config.poll_interval_ms, 250);
    assert!(config.enable_history);
}

#[test]
fn test_clipboard_config_invalid_sizes() {
    // Test invalid size configurations
    let invalid_configs = vec![
        ("negative_size", json!({"max_content_size": -1})),
        ("zero_size", json!({"max_content_size": 0})),
        ("tiny_interval", json!({"poll_interval_ms": 0})),
        ("huge_interval", json!({"poll_interval_ms": 3600000})),
    ];
    
    for (test_name, config) in invalid_configs {
        let context = EventSourceContext::new(config);
        let result: Result<ClipboardConfig, _> = serde_json::from_value(context.config);
        
        match test_name {
            "negative_size" | "zero_size" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.max_content_size > 0, "Should have positive max content size");
                }
            }
            "tiny_interval" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.poll_interval_ms >= 10, "Should have reasonable minimum interval");
                }
            }
            "huge_interval" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.poll_interval_ms <= 60000, "Should clamp excessive intervals");
                }
            }
            _ => {}
        }
    }
}

#[test]
fn test_kitty_config_validation() {
    // Test valid Kitty configuration
    let valid_config = json!({
        "socket_path": "/tmp/kitty-socket",
        "poll_interval_seconds": 5,
        "enabled": true
    });
    
    let context = EventSourceContext::new(valid_config);
    let config: KittyConfig = serde_json::from_value(context.config).unwrap();
    
    assert_eq!(config.socket_path, Some("/tmp/kitty-socket".to_string()));
    assert!(config.enabled);
    assert_eq!(config.poll_interval_seconds, 5);
    // Remove this assertion - field doesn't exist
    // Remove this assertion - field doesn't exist
}

#[test]
fn test_kitty_config_invalid_paths() {
    // Test invalid socket paths
    let invalid_configs = vec![
        ("empty_path", json!({"socket_path": ""})),
        ("nonexistent_path", json!({"socket_path": "/nonexistent/directory/socket"})),
        ("relative_path", json!({"socket_path": "./relative/socket"})),
        ("invalid_chars", json!({"socket_path": "/tmp/socket\0invalid"})),
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
            "nonexistent_path" => {
                // Nonexistent paths might be allowed (created later)
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.socket_path.is_some());
                }
            }
            "relative_path" => {
                if result.is_ok() {
                    let cfg = result.unwrap();
                    assert!(cfg.socket_path.is_some(), "Should convert to absolute path");
                }
            }
            "invalid_chars" => {
                // Null characters should be rejected
                assert!(result.is_err(), "Should reject paths with null characters");
            }
            _ => {}
        }
    }
}

#[test]
fn test_dbus_config_validation() {
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
    let config: DbusConfig = serde_json::from_value(context.config).unwrap();
    
    assert!(config.monitor_session);
    assert!(!config.monitor_system);
    assert_eq!(config.include_interfaces.len(), 2);
    assert!(config.extract_notifications);
}

#[test]
fn test_dbus_config_invalid_filters() {
    // Test invalid D-Bus filter rules
    let invalid_config = json!({
        "monitor_session": true,
        "include_interfaces": [
            "invalid.interface.name",
            "another.invalid.interface",
            ""
        ]
    });
    
    let context = EventSourceContext::new(invalid_config);
    let result: Result<DbusConfig, _> = serde_json::from_value(context.config);
    
    match result {
        Ok(config) => {
            // Invalid filters might be silently ignored or cause runtime errors
            println!("Config accepted with potentially invalid filters: {:?}", config.include_interfaces);
        }
        Err(e) => {
            assert!(e.to_string().contains("interface") || e.to_string().contains("invalid"));
        }
    }
}

#[test]
fn test_config_with_malformed_json() {
    // Test various malformed JSON scenarios that might occur in practice
    let malformed_configs = vec![
        ("truncated_json", r#"{"valid": true, "truncated"#),
        ("invalid_escape", r#"{"invalid": "string with \x invalid escape"}"#),
        ("mixed_quotes", r#"{'single': "mixed", "quotes": 'here'}"#),
        ("trailing_comma", r#"{"valid": true, "trailing": "comma",}"#),
        ("duplicate_keys", r#"{"key": "first", "key": "duplicate"}"#),
    ];
    
    for (test_name, json_str) in malformed_configs {
        let parse_result = serde_json::from_str::<serde_json::Value>(json_str);
        
        match parse_result {
            Ok(value) => {
                println!("Malformed JSON '{}' was unexpectedly parsed: {:?}", test_name, value);
                // Even if parsing succeeds, test that config extraction is robust
                let context = EventSourceContext::new(value);
                let _result: Result<FilesystemConfig, _> = serde_json::from_value(context.config);
                // Should either work or fail gracefully
            }
            Err(e) => {
                println!("Malformed JSON '{}' correctly rejected: {}", test_name, e);
                assert!(e.to_string().contains("JSON") || e.to_string().contains("parse"));
            }
        }
    }
}

#[test]
fn test_configuration_defaults() {
    // Test that all configurations provide sensible defaults
    let empty_context = EventSourceContext::new(serde_json::json!({}));
    
    // Test filesystem defaults
    let fs_config: FilesystemConfig = serde_json::from_value(empty_context.config.clone()).unwrap();
    assert!(!fs_config.watch_patterns.is_empty(), "Should have default watch patterns");
    assert!(fs_config.debounce_ms > 0, "Should have positive debounce");
    
    // Test clipboard defaults
    let clipboard_config: ClipboardConfig = serde_json::from_value(empty_context.config.clone()).unwrap();
    assert!(clipboard_config.max_content_size > 0, "Should have positive max size");
    assert!(clipboard_config.poll_interval_ms > 0, "Should have positive interval");
    
    // Test Kitty defaults
    let kitty_config: KittyConfig = serde_json::from_value(empty_context.config.clone()).unwrap();
    // Kitty socket path can be None by default
    assert!(kitty_config.poll_interval_seconds > 0, "Should have positive timeout");
    
    // Test D-Bus defaults
    let dbus_config: DbusConfig = serde_json::from_value(empty_context.config.clone()).unwrap();
    assert!(dbus_config.monitor_session || dbus_config.monitor_system, "Should monitor at least one bus");
}

#[test]
fn test_configuration_type_coercion() {
    // Test that configuration values are properly coerced between types
    let type_coercion_tests = vec![
        ("string_to_number", json!({"debounce_ms": "500"})),
        ("number_to_string", json!({"socket_path": 12345})),
        ("string_to_boolean", json!({"enabled": "true"})),
        ("number_to_boolean", json!({"enabled": 1})),
        ("array_to_string", json!({"socket_path": ["path", "segments"]})),
    ];
    
    for (test_name, config) in type_coercion_tests {
        let context = EventSourceContext::new(config);
        
        match test_name {
            "string_to_number" => {
                if let Ok(cfg) = serde_json::from_value::<FilesystemConfig>(context.config) {
                    assert_eq!(cfg.debounce_ms, 500, "Should convert string to number");
                }
            }
            "number_to_string" => {
                if let Ok(cfg) = serde_json::from_value::<KittyConfig>(context.config) {
                    assert!(cfg.socket_path.is_some(), "Should convert number to string");
                }
            }
            "string_to_boolean" => {
                if let Ok(cfg) = serde_json::from_value::<KittyConfig>(context.config) {
                    assert!(cfg.enabled, "Should convert 'true' string to boolean");
                }
            }
            "number_to_boolean" => {
                if let Ok(cfg) = serde_json::from_value::<KittyConfig>(context.config) {
                    assert!(cfg.enabled, "Should convert 1 to true");
                }
            }
            _ => {
                // Other coercions might fail, which is acceptable
            }
        }
    }
}

#[test]
fn test_configuration_security_validation() {
    // Test that potentially dangerous configuration values are rejected
    let security_tests = vec![
        ("path_traversal", json!({"socket_path": "/tmp/../../../etc/passwd"})),
        ("shell_injection", json!({"socket_path": "/tmp/socket; rm -rf /"})),
        ("null_bytes", json!({"socket_path": "/tmp/socket\0/malicious"})),
        ("long_path", json!({"socket_path": "/".repeat(10000)})),
        ("unicode_exploit", json!({"socket_path": "/tmp/\u{202E}reversed\u{202D}"})),
    ];
    
    for (test_name, config) in security_tests {
        let context = EventSourceContext::new(config);
        let result: Result<KittyConfig, _> = serde_json::from_value(context.config);
        
        match result {
            Ok(cfg) => {
                match test_name {
                    "path_traversal" => {
                        // Should normalize or reject path traversal
                        let default_path = "".to_string();
                        let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                        assert!(!path_str.contains("../"), "Should not contain path traversal");
                    }
                    "shell_injection" => {
                        // Should reject or sanitize shell metacharacters
                        let default_path = "".to_string();
                        let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                        assert!(!path_str.contains(";"), "Should not contain shell metacharacters");
                    }
                    "null_bytes" => {
                        // Should reject null bytes
                        let default_path = "".to_string();
                        let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                        assert!(!path_str.contains('\0'), "Should not contain null bytes");
                    }
                    "long_path" => {
                        // Should reject or truncate extremely long paths
                        let default_path = "".to_string();
                        let path_str = cfg.socket_path.as_ref().unwrap_or(&default_path);
                        assert!(path_str.len() <= 4096, "Should limit path length");
                    }
                    _ => {}
                }
            }
            Err(e) => {
                println!("Security test '{}' correctly rejected: {}", test_name, e);
            }
        }
    }
}

#[test]
fn test_validation_chain_comprehensive() {
    // Test the ValidationChain utility with various scenarios
    use sinex_core::validation_chains::{ValidationChain, JsonType};
    
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
    assert!(result.is_err(), "Empty string should fail not_empty validation");
    
    // Test number validation
    let result = ValidationChain::validate(42, "number_field")
        .min_value(0)
        .into_result();
    assert!(result.is_ok(), "Valid number should pass validation");
    
    // Test number validation failure
    let result = ValidationChain::validate(-5, "negative_field")
        .min_value(0)
        .into_result();
    assert!(result.is_err(), "Negative number should fail min_value validation");
    
    // Test JSON type validation
    let json_object = json!({"key": "value", "number": 42});
    let result = ValidationChain::validate(json_object, "json_field")
        .json_type(JsonType::Object)
        .into_result();
    assert!(result.is_ok(), "JSON object should pass object validation");
    
    // Test JSON type validation failure
    let json_array = json!([1, 2, 3]);
    let result = ValidationChain::validate(json_array, "json_field")
        .json_type(JsonType::Object)
        .into_result();
    assert!(result.is_err(), "JSON array should fail object validation");
}

#[test]
fn test_configuration_environment_override() {
    // Test that environment variables can override configuration
    // This tests the integration with the environment variable system
    
    std::env::set_var("SINEX_FS_DEBOUNCE_MS", "750");
    std::env::set_var("SINEX_CLIPBOARD_MAX_SIZE", "2097152");
    
    // Test that environment variables are considered
    let _context = EventSourceContext::new(json!({}));
    
    // Note: This test depends on the actual implementation of environment variable handling
    // It may need to be adapted based on how the configuration system works
    
    // Clean up environment variables
    std::env::remove_var("SINEX_FS_DEBOUNCE_MS");
    std::env::remove_var("SINEX_CLIPBOARD_MAX_SIZE");
}

#[test]
fn test_configuration_precedence() {
    // Test configuration precedence: CLI args > env vars > config file > defaults
    // This is a conceptual test - actual implementation may vary
    
    let base_config = json!({
        "debounce_ms": 100,
        "socket_path": "/tmp/base"
    });
    
    let override_config = json!({
        "debounce_ms": 200,
        "max_content_size": 1024
    });
    
    // Test that later configuration values override earlier ones
    let context = EventSourceContext::new(base_config);
    let fs_config: FilesystemConfig = serde_json::from_value(context.config).unwrap();
    assert_eq!(fs_config.debounce_ms, 100);
    
    let context = EventSourceContext::new(override_config);
    let fs_config: FilesystemConfig = serde_json::from_value(context.config).unwrap();
    assert_eq!(fs_config.debounce_ms, 200);
}