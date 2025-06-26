//! Simple test to verify the new abstractions work in isolation
//! 
//! This tests only the new abstractions without depending on other crates

use std::time::Duration;
use tokio::sync::mpsc;

// Import only the new abstractions we're testing
use sinex_core::{
    ErrorContext, ValidationChain, ConfigExtractor, ConfigValidator, 
    ChannelSenderExt, ChannelReceiverExt, CoreError, Result, ConfigValue,
    JsonValue, RawEvent, RawEventBuilder
};
use serde_json::json;
use regex::Regex;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🧪 Testing New Abstractions in Isolation\n");
    
    // Test 1: Error Context
    test_error_context().await?;
    
    // Test 2: Config Extraction  
    test_config_extraction().await?;
    
    // Test 3: Validation Chains
    test_validation_chains().await?;
    
    // Test 4: Channel Extensions
    test_channel_extensions().await?;
    
    println!("✅ All new abstractions work correctly!");
    Ok(())
}

async fn test_error_context() -> Result<()> {
    println!("🚨 Testing Error Context...");
    
    let event_id = sinex_ulid::Ulid::new();
    let timestamp = chrono::Utc::now();
    
    let error = CoreError::database("Connection failed")
        .with_event_id(event_id)
        .with_timestamp(timestamp)
        .with_operation("test_operation")
        .with_context("retry_count", 3)
        .build();
    
    let error_str = error.to_string();
    assert!(error_str.contains("Connection failed"));
    assert!(error_str.contains("retry_count: 3"));
    
    println!("  ✅ Error context building works");
    Ok(())
}

async fn test_config_extraction() -> Result<()> {
    println!("📋 Testing Config Extraction...");
    
    let config_toml = r#"
        app_name = "test_app"
        port = 8080
        debug = true
        
        [database]
        url = "postgresql://localhost/test"
        pool_size = 10
    "#;
    
    let config: ConfigValue = toml::from_str(config_toml)
        .map_err(|e| CoreError::Configuration(format!("Config parse error: {}", e)))?;
    
    // Test extraction
    let app_name = config.require_str("app_name")?;
    let port = config.require_u64("port")?;
    let debug = config.require_bool("debug")?;
    let db_url = config.require_str("database.url")?;
    let missing = config.optional_str("missing_field");
    
    assert_eq!(app_name, "test_app");
    assert_eq!(port, 8080);
    assert_eq!(debug, true);
    assert_eq!(db_url, "postgresql://localhost/test");
    assert!(missing.is_none());
    
    // Test validation
    let validator = ConfigValidator::new()
        .require("app_name")
        .validate_range("port", 1..=65535)
        .build();
    
    validator(&config)?;
    
    println!("  ✅ Config extraction and validation work");
    Ok(())
}

async fn test_validation_chains() -> Result<()> {
    println!("🔗 Testing Validation Chains...");
    
    // Test string validation
    let email = "test@example.com".to_string();
    let email_regex = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();
    
    let email_result = ValidationChain::validate(email, "email")
        .not_empty()
        .min_length(5)
        .matches_regex(&email_regex)
        .into_result();
    
    assert!(email_result.is_ok());
    
    // Test numeric validation
    let port = 8080;
    let port_result = ValidationChain::validate(port, "port")
        .min(1)
        .max(65535)
        .into_result();
    
    assert!(port_result.is_ok());
    
    // Test JSON validation
    let json_data = json!({
        "name": "test",
        "count": 42
    });
    
    let json_result = ValidationChain::validate(json_data, "data")
        .has_field("name")
        .has_field("count")
        .field_type("name", sinex_core::validation_chains::JsonType::String)
        .field_type("count", sinex_core::validation_chains::JsonType::Number)
        .into_result();
    
    assert!(json_result.is_ok());
    
    // Test error accumulation
    let bad_email = "".to_string();
    let bad_result = ValidationChain::validate(bad_email, "email")
        .not_empty()
        .min_length(5)
        .into_result();
    
    assert!(bad_result.is_err());
    
    println!("  ✅ Validation chains work correctly");
    Ok(())
}

async fn test_channel_extensions() -> Result<()> {
    println!("📡 Testing Channel Extensions...");
    
    let (tx, mut rx) = mpsc::channel::<String>(5);
    
    // Test send with context
    tx.send_or_log("test message".to_string(), "test_context").await?;
    
    // Test timeout send
    tx.send_timeout("timeout test".to_string(), Duration::from_millis(100)).await?;
    
    // Test receiving
    let msg1 = rx.recv().await;
    let msg2 = rx.recv().await;
    
    assert_eq!(msg1, Some("test message".to_string()));
    assert_eq!(msg2, Some("timeout test".to_string()));
    
    // Test batch operations
    for i in 1..=3 {
        tx.send_or_log(format!("batch_{}", i), "batch_test").await?;
    }
    
    drop(tx); // Close sender
    
    let batch = rx.recv_batch(2, Duration::from_millis(100)).await;
    assert_eq!(batch.len(), 2);
    
    let remaining = rx.drain_all().await;
    assert_eq!(remaining.len(), 1);
    
    println!("  ✅ Channel extensions work correctly");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_all_abstractions() {
        assert!(test_error_context().await.is_ok());
        assert!(test_config_extraction().await.is_ok());
        assert!(test_validation_chains().await.is_ok());
        assert!(test_channel_extensions().await.is_ok());
    }

    #[test]
    fn test_raw_event_builder() {
        let event = RawEventBuilder::new("test_source", "test.event", json!({"test": "data"}))
            .with_host("test_host")
            .with_ingestor_version("1.0.0")
            .build();

        assert_eq!(event.source, "test_source");
        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.host, "test_host");
        assert_eq!(event.ingestor_version, Some("1.0.0".to_string()));
        assert_eq!(event.payload["test"], "data");
    }
}