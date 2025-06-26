/// Integration Examples for New Core Abstractions
/// This file demonstrates how the four new abstractions work together
/// in realistic scenarios and validates their integration.

use sinex_core::{
    ErrorContext, ErrorInfo, ResultExt,
    ConfigExtractor, ConfigValidator, parse_duration,
    ValidationChain, MultiValidator,
    ChannelSenderExt, ChannelReceiverExt, ChannelMonitor, monitored_channel,
    RawEvent, RawEventBuilder, EventSender, Timestamp, JsonValue, ConfigValue
};
use serde_json::json;
use tokio::time::{timeout, Duration};
use std::collections::HashMap;

#[tokio::test]
async fn test_comprehensive_error_handling_workflow() -> Result<(), Box<dyn std::error::Error>> {
    // Demonstrate ErrorContext with database operations
    let result = simulate_database_operation()
        .await
        .with_error_context("user", "12345")?
        .with_error_context("operation", "user_profile_update")?;
        
    assert!(result.is_ok());
    
    // Demonstrate structured error building
    let error_info = ErrorInfo::builder()
        .error_type("validation_failed")
        .user_facing_message("Please check your email format")
        .add_context("field", "email")
        .add_context("value", "invalid@")
        .add_context("validator", "email_format")
        .add_detail("Email must contain a valid domain")
        .build();
    
    assert_eq!(error_info.error_type, "validation_failed");
    assert_eq!(error_info.context.len(), 3);
    
    Ok(())
}

#[tokio::test]
async fn test_configuration_extraction_and_validation() -> Result<(), Box<dyn std::error::Error>> {
    // Create test configuration
    let config_str = r#"
        database.timeout_seconds = 30
        database.max_connections = 100
        database.url = "postgresql://localhost/test"
        features.enable_monitoring = true
        cache.ttl = "5m"
        cache.max_size = "128MB"
    "#;
    
    let config: toml::Value = toml::from_str(config_str)?;
    
    // Demonstrate ConfigExtractor usage
    let timeout = config.require_u64("database.timeout_seconds")?;
    let max_conn = config.require_u64("database.max_connections")?;
    let db_url = config.require_string("database.url")?;
    
    assert_eq!(timeout, 30);
    assert_eq!(max_conn, 100);
    assert_eq!(db_url, "postgresql://localhost/test");
    
    // Demonstrate optional extraction
    let debug_mode = config.optional_bool("debug.enabled").unwrap_or(false);
    assert_eq!(debug_mode, false);
    
    // Demonstrate duration parsing
    let cache_ttl = parse_duration(&config.require_string("cache.ttl")?)?;
    assert_eq!(cache_ttl, Duration::from_secs(300)); // 5 minutes
    
    // Demonstrate validation with constraints
    let validator = ConfigValidator::new(&config);
    let validated_timeout = validator
        .require_u64("database.timeout_seconds")?
        .min_value(1)?
        .max_value(300)?
        .value();
    
    assert_eq!(validated_timeout, 30);
    
    Ok(())
}

#[tokio::test]
async fn test_validation_chain_comprehensive() -> Result<(), Box<dyn std::error::Error>> {
    // Test string validation chain
    let email = "user@example.com";
    let validated_email = ValidationChain::validate(email, "email")
        .not_empty()
        .min_length(5)
        .max_length(254)
        .matches_regex(&regex::Regex::new(r"^[^@]+@[^@]+\.[^@]+$")?)
        .into_result()?;
    
    assert_eq!(validated_email, email);
    
    // Test numeric validation
    let port = 8080u16;
    let validated_port = ValidationChain::validate(port, "port")
        .min_value(1024)
        .max_value(65535)
        .into_result()?;
    
    assert_eq!(validated_port, port);
    
    // Test multi-validator for complex objects
    let user_data = json!({
        "email": "test@example.com",
        "age": 25,
        "name": "John Doe"
    });
    
    let mut validator = MultiValidator::new();
    
    // Validate email field
    if let Some(email) = user_data.get("email").and_then(|v| v.as_str()) {
        ValidationChain::validate(email, "email")
            .not_empty()
            .matches_regex(&regex::Regex::new(r"^[^@]+@[^@]+\.[^@]+$")?)
            .add_to_multi_validator(&mut validator);
    }
    
    // Validate age field
    if let Some(age) = user_data.get("age").and_then(|v| v.as_u64()) {
        ValidationChain::validate(age, "age")
            .min_value(18)
            .max_value(120)
            .add_to_multi_validator(&mut validator);
    }
    
    let validation_result = validator.into_result();
    assert!(validation_result.is_ok());
    
    Ok(())
}

#[tokio::test]
async fn test_enhanced_channel_operations() -> Result<(), Box<dyn std::error::Error>> {
    // Create monitored channel for events
    let (tx, mut rx) = monitored_channel::<RawEvent>("test_events", 100);
    let mut monitor = ChannelMonitor::new();
    
    // Test enhanced sender operations
    let event = RawEventBuilder::new("test", "channel_test", json!({"data": "test"}))
        .build();
    
    // Send with backpressure handling
    let send_result = tx.send_with_retry(event.clone(), 3, Duration::from_millis(10)).await;
    assert!(send_result.is_ok());
    
    // Test receiver operations with timeout
    let received = timeout(Duration::from_millis(100), rx.recv_with_context("test_receiver"))
        .await??;
    
    assert_eq!(received.source, "test");
    assert_eq!(received.event_type, "channel_test");
    
    // Test channel monitoring
    monitor.record_send("test_channel", true);
    monitor.record_receive("test_channel", 1);
    
    let stats = monitor.get_stats("test_channel");
    assert_eq!(stats.messages_sent, 1);
    assert_eq!(stats.messages_received, 1);
    
    Ok(())
}

#[tokio::test]  
async fn test_cross_abstraction_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Simulate a realistic event processing pipeline using all abstractions
    
    // 1. Configuration extraction
    let config_str = r#"
        event_source.name = "integration_test"
        event_source.batch_size = 50
        event_source.timeout = "30s"
        monitoring.enable_stats = true
    "#;
    
    let config: toml::Value = toml::from_str(config_str)?;
    
    let source_name = config.require_string("event_source.name")?;
    let batch_size = config.require_u64("event_source.batch_size")?;
    let timeout_duration = parse_duration(&config.require_string("event_source.timeout")?)?;
    let monitoring_enabled = config.optional_bool("monitoring.enable_stats").unwrap_or(false);
    
    // 2. Input validation
    let validated_batch_size = ValidationChain::validate(batch_size, "batch_size")
        .min_value(1)
        .max_value(1000)
        .into_result()?;
        
    let validated_name = ValidationChain::validate(source_name.as_str(), "source_name")
        .not_empty()
        .min_length(3)
        .max_length(50)
        .matches_regex(&regex::Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*$")?)
        .into_result()?;
    
    // 3. Channel setup with monitoring
    let (tx, mut rx) = monitored_channel::<RawEvent>("events", validated_batch_size as usize);
    let mut monitor = if monitoring_enabled {
        Some(ChannelMonitor::new())
    } else {
        None
    };
    
    // 4. Event processing with error handling
    let process_result = async {
        // Simulate event creation
        let event = RawEventBuilder::new(&validated_name, "test_event", json!({
            "batch_size": validated_batch_size,
            "timeout_ms": timeout_duration.as_millis()
        })).build();
        
        // Send with enhanced channel operations
        tx.send_with_retry(event, 3, Duration::from_millis(10))
            .await
            .with_error_context("operation", "event_send")?
            .with_error_context("source", &validated_name)?;
        
        // Receive with monitoring
        let received_event = timeout(timeout_duration, 
            rx.recv_with_context("test_processor")
        ).await
            .with_error_context("operation", "event_receive")?
            .with_error_context("timeout", &format!("{}ms", timeout_duration.as_millis()))?;
        
        // Update monitoring stats
        if let Some(ref mut mon) = monitor {
            mon.record_send("events", true);
            mon.record_receive("events", 1);
        }
        
        Ok::<RawEvent, Box<dyn std::error::Error>>(received_event)
    }.await;
    
    // 5. Verify the complete pipeline worked
    let final_event = process_result?;
    assert_eq!(final_event.source, validated_name);
    assert_eq!(final_event.event_type, "test_event");
    
    // Verify monitoring if enabled
    if let Some(monitor) = monitor {
        let stats = monitor.get_stats("events");
        assert_eq!(stats.messages_sent, 1);
        assert_eq!(stats.messages_received, 1);
    }
    
    Ok(())
}

// Helper function to simulate database operations
async fn simulate_database_operation() -> Result<String, Box<dyn std::error::Error>> {
    // Simulate successful operation
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok("operation_successful".to_string())
}

#[tokio::test]
async fn test_error_context_with_nested_operations() -> Result<(), Box<dyn std::error::Error>> {
    let result = async {
        nested_operation_level_1()
            .await
            .with_error_context("level", "1")?
            .with_error_context("caller", "test_function")?;
        
        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;
    
    assert!(result.is_ok());
    Ok(())
}

async fn nested_operation_level_1() -> Result<(), Box<dyn std::error::Error>> {
    nested_operation_level_2()
        .await
        .with_error_context("level", "2")?;
    Ok(())
}

async fn nested_operation_level_2() -> Result<(), Box<dyn std::error::Error>> {
    // Simulate successful nested operation
    Ok(())
}

#[tokio::test]
async fn test_validation_with_custom_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let data = "custom_validation_test";
    
    let result = ValidationChain::validate(data, "custom_field")
        .not_empty()
        .custom_predicate(|s| s.contains("validation"), "must contain 'validation'")
        .custom_predicate(|s| s.len() > 10, "must be longer than 10 characters")
        .into_result()?;
    
    assert_eq!(result, data);
    Ok(())
}