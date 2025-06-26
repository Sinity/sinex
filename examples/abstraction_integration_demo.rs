//! Integration test demonstrating all new abstractions working together
//! 
//! This example shows how the new utility abstractions integrate with each other
//! and provides examples of their usage patterns.

use sinex_core::{
    ErrorContext, ValidationChain, ConfigExtractor, ConfigValidator, 
    ChannelSenderExt, ChannelReceiverExt, CoreError, Result, ConfigValue,
    EventSender, EventReceiver, RawEvent, RawEventBuilder, JsonValue
};
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc;
use regex::Regex;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🧪 Sinex Abstraction Integration Demo\n");
    
    // 1. Configuration Extraction and Validation
    demonstrate_config_abstractions().await?;
    
    // 2. Error Context Building
    demonstrate_error_contexts().await?;
    
    // 3. Validation Chains
    demonstrate_validation_chains().await?;
    
    // 4. Channel Extensions
    demonstrate_channel_extensions().await?;
    
    // 5. Combined Workflow
    demonstrate_integrated_workflow().await?;
    
    println!("✅ All abstractions integration demo completed successfully!");
    Ok(())
}

async fn demonstrate_config_abstractions() -> Result<()> {
    println!("📋 Config Extraction & Validation Demo");
    println!("=====================================");
    
    // Create a sample configuration using TOML format
    let config_toml = r#"
        app_name = "sinex-demo"
        max_connections = 100
        timeout_seconds = 30
        debug_enabled = true
        allowed_hosts = ["localhost", "127.0.0.1"]
        
        [database]
        url = "postgresql://localhost/sinex_dev"
        pool_size = 50
        
        [logging]
        level = "info"
        file_path = "/var/log/sinex.log"
    "#;
    
    let config: ConfigValue = toml::from_str(config_toml)
        .map_err(|e| CoreError::Configuration(format!("Failed to parse config: {}", e)))?;
    
    // Demonstrate config extraction
    println!("🔍 Extracting configuration values:");
    let app_name = config.require_str("app_name")?;
    let max_connections = config.require_u64("max_connections")?;
    let timeout = config.require_u64("timeout_seconds")?;
    let debug = config.require_bool("debug_enabled")?;
    let db_url = config.require_str("database.url")?;
    let log_level = config.str_or("logging.level", "warn");
    
    println!("  - App Name: {}", app_name);
    println!("  - Max Connections: {}", max_connections);
    println!("  - Timeout: {}s", timeout);
    println!("  - Debug Enabled: {}", debug);
    println!("  - Database URL: {}", db_url);
    println!("  - Log Level: {}", log_level);
    
    // Demonstrate config validation
    println!("\n🔒 Validating configuration:");
    let validator = ConfigValidator::new()
        .require("app_name")
        .require("database.url") 
        .validate_range("max_connections", 1..=1000)
        .validate_range("timeout_seconds", 1..=300)
        .validate_regex("logging.level", r"^(trace|debug|info|warn|error)$")
        .build();
    
    match validator(&config) {
        Ok(()) => println!("  ✅ Configuration validation passed"),
        Err(e) => println!("  ❌ Configuration validation failed: {}", e),
    }
    
    println!();
    Ok(())
}

async fn demonstrate_error_contexts() -> Result<()> {
    println!("🚨 Error Context Building Demo");
    println!("==============================");
    
    // Demonstrate rich error context building
    let event_id = sinex_ulid::Ulid::new();
    let timestamp = chrono::Utc::now();
    
    let error = CoreError::database("Connection timeout")
        .with_event_id(event_id)
        .with_timestamp(timestamp)
        .with_operation("event_insertion")
        .with_context("retry_attempt", 3)
        .with_source("Network unreachable")
        .with_source("DNS resolution failed")
        .build();
    
    println!("🔍 Rich error with context:");
    println!("  {}", error);
    
    // Demonstrate error chaining
    let validation_error = CoreError::validation("Invalid email format")
        .with_field("email", "not-an-email")
        .with_context("validation_rule", "email_format")
        .build();
    
    println!("\n📧 Validation error with field context:");
    println!("  {}", validation_error);
    
    // Demonstrate path-based error
    let io_error = CoreError::io_error("/var/log/sinex.log")
        .with_operation("write")
        .with_context("log_level", "error")
        .build();
        
    println!("\n📁 IO error with path context:");
    println!("  {}", io_error);
    
    println!();
    Ok(())
}

async fn demonstrate_validation_chains() -> Result<()> {
    println!("🔗 Validation Chains Demo");
    println!("=========================");
    
    // String validation
    println!("📝 String validation:");
    let email = "user@example.com".to_string();
    let email_regex = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();
    
    let result = ValidationChain::validate(email, "email")
        .not_empty()
        .min_length(5)
        .max_length(50)
        .matches_regex(&email_regex)
        .is_valid_url() // This will fail for email, demonstrating error accumulation
        .into_result();
    
    match result {
        Ok(value) => println!("  ✅ Email validation passed: {}", value),
        Err(e) => println!("  ⚠️  Email validation issues: {}", e),
    }
    
    // Numeric validation
    println!("\n🔢 Numeric validation:");
    let port = 8080;
    let port_result = ValidationChain::validate(port, "port")
        .min(1)
        .max(65535)
        .range(1024..49152)
        .into_result();
    
    match port_result {
        Ok(value) => println!("  ✅ Port validation passed: {}", value),
        Err(e) => println!("  ❌ Port validation failed: {}", e),
    }
    
    // JSON validation
    println!("\n📄 JSON validation:");
    let json_data = json!({
        "user_id": 123,
        "username": "alice",
        "email": "alice@example.com",
        "settings": {
            "theme": "dark",
            "notifications": true
        }
    });
    
    let json_result = ValidationChain::validate(json_data, "user_data")
        .has_field("user_id")
        .has_field("username")
        .has_field("email")
        .field_type("user_id", sinex_core::validation_chains::JsonType::Number)
        .field_type("username", sinex_core::validation_chains::JsonType::String)
        .max_depth(3)
        .max_size(1024)
        .into_result();
    
    match json_result {
        Ok(_) => println!("  ✅ JSON validation passed"),
        Err(e) => println!("  ❌ JSON validation failed: {}", e),
    }
    
    println!();
    Ok(())
}

async fn demonstrate_channel_extensions() -> Result<()> {
    println!("📡 Channel Extensions Demo");
    println!("=========================");
    
    let (tx, mut rx): (EventSender, EventReceiver) = mpsc::channel(10);
    
    // Demonstrate sending with context
    println!("📤 Sending events with context:");
    
    for i in 1..=5 {
        let event = RawEventBuilder::new(
            "demo", 
            "test.event", 
            json!({"index": i, "message": format!("Test event {}", i)})
        ).build();
        
        tx.send_or_log(event, &format!("demo_event_{}", i)).await?;
        println!("  ✅ Sent event {}", i);
    }
    
    // Close the sender to demonstrate batch receiving
    drop(tx);
    
    // Demonstrate batch receiving
    println!("\n📥 Batch receiving events:");
    let events = rx.recv_batch(3, Duration::from_millis(100)).await;
    println!("  📦 Received batch of {} events:", events.len());
    
    for (i, event) in events.iter().enumerate() {
        println!("    {}. {} -> {} at {}", 
            i + 1, event.source, event.event_type, event.ts_ingest);
    }
    
    // Drain remaining events
    let remaining = rx.drain_all().await;
    if !remaining.is_empty() {
        println!("  🗑️  Drained {} remaining events", remaining.len());
    }
    
    println!();
    Ok(())
}

async fn demonstrate_integrated_workflow() -> Result<()> {
    println!("🔄 Integrated Workflow Demo");
    println!("===========================");
    
    // Simulate a complete workflow using all abstractions
    println!("🎯 Processing user registration event...");
    
    // 1. Parse and validate configuration
    let config_toml = r#"
        validation_rules = ["email_required", "password_strength"]
        max_username_length = 50
        allowed_domains = ["example.com", "test.org"]
        
        [email_validation]
        pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        max_length = 100
    "#;
    
    let config: ConfigValue = toml::from_str(config_toml)?;
    let max_username_len = config.require_u64("max_username_length")? as usize;
    let email_pattern = config.require_str("email_validation.pattern")?;
    let email_regex = Regex::new(email_pattern)
        .map_err(|e| CoreError::Configuration(format!("Invalid email regex: {}", e)))?;
    
    // 2. Create user data to validate
    let user_data = json!({
        "username": "alice_cooper",
        "email": "alice@example.com",
        "password": "SecurePass123!",
        "profile": {
            "first_name": "Alice",
            "last_name": "Cooper",
            "age": 25
        }
    });
    
    // 3. Validate user data using validation chains
    let username = user_data["username"].as_str().unwrap_or("").to_string();
    let email = user_data["email"].as_str().unwrap_or("").to_string();
    
    let username_result = ValidationChain::validate(username, "username")
        .not_empty()
        .min_length(3)
        .max_length(max_username_len)
        .custom(|s| s.chars().all(|c| c.is_alphanumeric() || c == '_'), 
                "must contain only alphanumeric characters and underscores")
        .into_result();
    
    let email_result = ValidationChain::validate(email, "email")
        .not_empty()
        .matches_regex(&email_regex)
        .into_result();
    
    let json_result = ValidationChain::validate(user_data.clone(), "user_data")
        .has_field("username")
        .has_field("email") 
        .has_field("password")
        .field_type("username", sinex_core::validation_chains::JsonType::String)
        .field_type("email", sinex_core::validation_chains::JsonType::String)
        .max_depth(3)
        .into_result();
    
    // 4. Handle validation results with rich error context
    match (username_result, email_result, json_result) {
        (Ok(username), Ok(email), Ok(_)) => {
            println!("  ✅ User validation passed:");
            println!("    - Username: {}", username);
            println!("    - Email: {}", email);
            
            // 5. Create and send success event
            let event = RawEventBuilder::new(
                "user_registration",
                "user.registration.success",
                json!({
                    "username": username,
                    "email": email,
                    "validation_timestamp": chrono::Utc::now(),
                    "validation_rules_applied": ["username_format", "email_format", "json_structure"]
                })
            ).build();
            
            // Create a channel and send the event
            let (tx, mut rx) = mpsc::channel(1);
            tx.send_or_log(event, "user_registration_success").await?;
            
            if let Some(sent_event) = rx.recv().await {
                println!("    📧 Success event created: {} -> {}", 
                    sent_event.source, sent_event.event_type);
            }
        }
        (username_res, email_res, json_res) => {
            // Collect all validation errors with context
            let mut error_builder = CoreError::validation("User registration validation failed")
                .with_operation("user_registration")
                .with_timestamp(chrono::Utc::now());
            
            if let Err(e) = username_res {
                error_builder = error_builder.with_source(format!("Username: {}", e));
            }
            if let Err(e) = email_res {
                error_builder = error_builder.with_source(format!("Email: {}", e));
            }
            if let Err(e) = json_res {
                error_builder = error_builder.with_source(format!("JSON structure: {}", e));
            }
            
            let final_error = error_builder.build();
            println!("  ❌ User validation failed:");
            println!("    {}", final_error);
        }
    }
    
    println!();
    Ok(())
}