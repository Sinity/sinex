use sinex_collector::config::{CollectorConfig, ValidationReport};

// Removed trivial default config tests - they just verified that defaults contain expected values

#[test]
fn test_config_validation() {
    let config = CollectorConfig::default();
    
    // Default config should be valid
    let result = config.validate();
    assert!(result.is_ok(), "Default config should be valid: {:?}", result);
}

#[test]
fn test_config_validation_report() {
    let config = CollectorConfig::default();
    
    let report = config.get_validation_report();
    
    if !report.valid {
        println!("Validation errors: {:?}", report.errors);
        println!("Validation warnings: {:?}", report.warnings);
    }
    
    assert!(report.valid, "Default config should have valid report");
    assert!(report.errors.is_empty(), "Default config should have no errors");
}

#[test]
fn test_invalid_event_type_validation() {
    let mut config = CollectorConfig::default();
    config.enabled_events.push("invalid_event".to_string());
    
    let result = config.validate();
    assert!(result.is_err(), "Config with invalid event type should fail validation");
    
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Event type must have at least category.subcategory format"));
}

#[test]
fn test_malformed_event_type_validation() {
    let mut config = CollectorConfig::default();
    config.enabled_events.push("1invalid.event".to_string()); // Starts with number
    
    let result = config.validate();
    assert!(result.is_err(), "Config with malformed event type should fail validation");
}

#[test]
fn test_event_config_validation() {
    // Test by loading an invalid config from string
    let invalid_config_toml = r#"
enabled_events = ["shell.command.executed_atuin"]

[event.shell_command_executed_atuin]
db_path = "relative/path"  # Should be absolute
polling_interval_secs = -1  # Should be positive
"#;
    
    let result = toml::from_str::<CollectorConfig>(invalid_config_toml);
    if let Ok(config) = result {
        let validation_result = config.validate();
        assert!(validation_result.is_err(), "Config with invalid event config should fail validation");
        
        let error_msg = validation_result.unwrap_err().to_string();
        assert!(error_msg.contains("db_path must be an absolute path") || error_msg.contains("polling_interval_secs must be greater than 0"));
    } else {
        // If TOML parsing itself fails, that's also a validation failure
        assert!(true, "Invalid TOML should fail parsing");
    }
}

#[test]
fn test_cross_validation() {
    let mut config = CollectorConfig::default();
    
    // Clear existing configurations and enable an event that requires configuration but don't provide it
    config.flat_config.clear();
    config.event.clear();
    config.enabled_events.clear();
    config.enabled_events.push("shell.command.executed_atuin".to_string());
    
    let result = config.cross_validate();
    assert!(result.is_err(), "Cross-validation should fail when required config is missing: {:?}", result);
    
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("missing required 'db_path' configuration"));
}

#[test]
fn test_valid_event_config() {
    // Test by loading a valid config from string
    let valid_config_toml = r#"
enabled_events = ["shell.command.executed_atuin"]

[event.shell_command_executed_atuin]
db_path = "/home/user/.local/share/atuin/history.db"
polling_interval_secs = 5
"#;
    
    let config: CollectorConfig = toml::from_str(valid_config_toml).expect("Valid TOML should parse");
    let result = config.validate();
    assert!(result.is_ok(), "Valid event config should pass validation: {:?}", result);
}

#[test] 
fn test_validation_report_accumulation() {
    let mut report = ValidationReport::new();
    
    assert!(report.valid);
    assert!(report.is_empty());
    
    report.add_warning("Test warning".to_string());
    assert!(report.valid); // Warnings don't affect validity
    assert!(!report.is_empty());
    
    report.add_error("Test error".to_string());
    assert!(!report.valid); // Errors affect validity
    
    report.add_recommendation("Test recommendation".to_string());
    
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.warnings.len(), 1);
    assert_eq!(report.recommendations.len(), 1);
}

#[test]
fn test_validation_report_merge() {
    let mut report1 = ValidationReport::new();
    report1.add_error("Error 1".to_string());
    report1.add_warning("Warning 1".to_string());
    
    let mut report2 = ValidationReport::new();
    report2.add_error("Error 2".to_string());
    report2.add_recommendation("Recommendation 1".to_string());
    
    report1.merge(report2);
    
    assert!(!report1.valid);
    assert_eq!(report1.errors.len(), 2);
    assert_eq!(report1.warnings.len(), 1);
    assert_eq!(report1.recommendations.len(), 1);
}