use crate::common::prelude::*;
use sinex_core::{CoreError, Result as CoreResult};
use std::io;

#[sinex_test]
async fn test_core_error_from_io_error(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let io_err = io::Error::new(io::ErrorKind::NotFound, "File not found");
    let core_err: CoreError = io_err.into();
    
    match core_err {
        CoreError::Io(msg) => assert!(msg.contains("File not found")),
        _ => panic!("Expected CoreError::Io variant"),
    }
    Ok(())
}

#[sinex_test]
async fn test_core_error_from_serde_json_error(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let json_str = r#"{"invalid": json}"#;
    let json_err = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let core_err: CoreError = json_err.into();
    
    match core_err {
        CoreError::Serialization(msg) => assert!(!msg.is_empty()),
        _ => panic!("Expected CoreError::Serialization variant"),
    }
    Ok(())
}

#[sinex_test]
async fn test_error_chain_propagation(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    fn inner_operation() -> CoreResult<String> {
        Err(CoreError::Database("Connection lost".to_string()))
    }
    
    fn middle_operation() -> CoreResult<String> {
        inner_operation().map_err(|e| CoreError::Other(format!("Middle layer: {}", e)))
    }
    
    fn outer_operation() -> CoreResult<String> {
        middle_operation().map_err(|e| CoreError::Other(format!("Outer layer: {}", e)))
    }
    
    let result = outer_operation();
    assert!(result.is_err());
    
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Outer layer"));
    assert!(error_msg.contains("Middle layer"));
    assert!(error_msg.contains("Connection lost"));
    Ok(())
}

// Test error propagation in async context
#[derive(Debug)]
struct FailingEventSource;

#[async_trait]
impl EventSource for FailingEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "failing_source";
    
    async fn initialize(_ctx: EventSourceContext) -> CoreResult<Self> {
        // Simulate initialization failure
        Err(CoreError::Configuration("Missing required field".to_string()))
    }
    
    async fn stream_events(&mut self, _tx: mpsc::Sender<RawEvent>) -> CoreResult<()> {
        Err(CoreError::Io("Stream failed".to_string()))
    }
}

#[sinex_test]
async fn test_event_source_error_propagation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let ctx_local = event_sources::test_context(json!({}));
    let result = FailingEventSource::initialize(ctx_local).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        CoreError::Configuration(msg) => pretty_assertions::assert_eq!(msg, "Missing required field"),
        _ => panic!("Expected Configuration error"),
    }
    Ok(())
}

#[sinex_test]
async fn test_validation_error_propagation(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    fn validate_event_type(event_type: &str) -> CoreResult<()> {
        if event_type.is_empty() {
            return Err(CoreError::Validation("Event type cannot be empty".to_string()));
        }
        if !event_type.contains('.') {
            return Err(CoreError::Validation("Event type must contain a dot separator".to_string()));
        }
        Ok(())
    }
    
    // Test empty event type
    let result = validate_event_type("");
    assert!(matches!(result, Err(CoreError::Validation(msg)) if msg.contains("empty")));
    
    // Test invalid format
    let result = validate_event_type("invalid");
    assert!(matches!(result, Err(CoreError::Validation(msg)) if msg.contains("dot separator")));
    
    // Test valid event type
    let result = validate_event_type("system.startup");
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_error_display_implementation(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let errors = vec![
        (CoreError::Database("Connection timeout".to_string()), "Database error: Connection timeout"),
        (CoreError::Serialization("Invalid JSON".to_string()), "Serialization error: Invalid JSON"),
        (CoreError::Validation("Invalid input".to_string()), "Validation error: Invalid input"),
        (CoreError::Configuration("Missing config".to_string()), "Configuration error: Missing config"),
        (CoreError::Io("File not found".to_string()), "IO error: File not found"),
        (CoreError::Other("Unknown error".to_string()), "Other error: Unknown error"),
    ];
    
    for (error, expected) in errors {
        pretty_assertions::assert_eq!(error.to_string(), expected);
    }
    Ok(())
}

// Test error propagation across thread boundaries
#[sinex_test]
async fn test_error_propagation_across_tasks(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::task;
    
    let handle = task::spawn(async {
        // Simulate work that fails
        Err::<String, CoreError>(CoreError::Database("Task failed".to_string()))
    });
    
    let result = handle.await;
    assert!(result.is_ok()); // Join succeeded
    
    let inner_result = result.unwrap();
    assert!(inner_result.is_err());
    assert!(matches!(inner_result, Err(CoreError::Database(_))));
    Ok(())
}

// Test error recovery patterns
#[sinex_test]
async fn test_error_recovery_with_fallback(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    fn operation_with_fallback() -> Result<String> {
        let primary_result = Err::<String, CoreError>(CoreError::Io("Primary failed".to_string()));
        
        primary_result.or_else(|_| {
            // Try fallback
            Ok("Fallback value".to_string())
        })
    }
    
    let result = operation_with_fallback();
    assert!(result.is_ok());
    pretty_assertions::assert_eq!(result.unwrap(), "Fallback value");
    Ok(())
}

// Test nested Result handling
#[sinex_test]
async fn test_nested_result_error_propagation(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    fn parse_config(data: &str) -> Result<serde_json::Value> {
        serde_json::from_str(data).map_err(|e| anyhow::anyhow!("Config parse error: {}", e))
    }
    
    fn validate_config(config: serde_json::Value) -> Result<serde_json::Value> {
        if config.get("required_field").is_none() {
            return Err(anyhow::anyhow!("Missing required_field"));
        }
        Ok(config)
    }
    
    fn load_and_validate_config(data: &str) -> Result<serde_json::Value> {
        parse_config(data).and_then(validate_config)
    }
    
    // Test with invalid JSON
    let result = load_and_validate_config("{invalid}");
    assert!(result.is_err(), "Expected JSON parse error for invalid syntax");
    
    // Test with valid JSON but missing field
    let result = load_and_validate_config(r#"{}"#);
    assert!(result.is_err(), "Expected validation error for missing required field");
    
    // Test with valid config
    let result = load_and_validate_config(r#"{"required_field": "value"}"#);
    assert!(result.is_ok(), "Expected valid config to pass validation");
    Ok(())
}