# Migration Examples: Channel Helpers & Validation Chains

This document provides concrete before/after examples for migrating existing patterns to use the new channel helpers and validation chains.

## Channel Helpers Migration Examples

### Example 1: Basic Channel Error Handling

**Before** (found in 28+ locations):
```rust
// crate/sinex-events/src/clipboard.rs:352
tx.send(event).await.map_err(|_| sinex_core::CoreError::Other(
    "Channel closed".to_string()
))?;
```

**After**:
```rust
use sinex_core::ChannelSenderExt;

tx.send_or_log(event, "clipboard_monitor").await?;
```

**Benefits:**
- ✅ Automatic context in error messages
- ✅ Structured logging with source information
- ✅ Consistent error handling pattern
- ✅ Reduced boilerplate (1 line vs 3 lines)

### Example 2: Event Source with Monitoring

**Before** (typical pattern):
```rust
// crate/sinex-events/src/filesystem.rs
pub struct FilesystemMonitor {
    // ... other fields
}

impl EventSource for FilesystemMonitor {
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        loop {
            let event = self.create_event(event_type, payload);
            tx.send(event).await.map_err(|_| 
                sinex_core::CoreError::Other("Channel closed".to_string())
            )?;
        }
    }
}
```

**After**:
```rust
use sinex_core::{ChannelSenderExt, monitored_channel, MonitoredEventSender};

pub struct FilesystemMonitor {
    // ... other fields
    metrics: Option<ChannelStats>,  // Optional metrics tracking
}

impl EventSource for FilesystemMonitor {
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        loop {
            let event = self.create_event(event_type, payload);
            
            // Automatic context and metrics
            tx.send_or_log(event, "filesystem_monitor").await?;
            
            // Optional: track metrics for health monitoring
            if let Some(stats) = tx.stats() {
                if stats.queue_depth > 1000 {
                    tracing::warn!("Filesystem monitor queue depth: {}", stats.queue_depth);
                }
            }
        }
    }
}
```

**Benefits:**
- ✅ Built-in metrics collection
- ✅ Queue depth monitoring
- ✅ Automatic error context
- ✅ Health monitoring capabilities

### Example 3: Backpressure Handling

**Before** (no backpressure handling):
```rust
// High-throughput event source
async fn process_events(&mut self, tx: EventSender) -> Result<()> {
    for event in self.event_stream() {
        tx.send(event).await.map_err(|_| 
            sinex_core::CoreError::Other("Channel closed".to_string())
        )?;
    }
    Ok(())
}
```

**After**:
```rust
use sinex_core::{ChannelSenderExt, BackpressureManager};

async fn process_events(&mut self, tx: EventSender) -> Result<()> {
    let mut backpressure = BackpressureManager::new(1000, 500); // high/low watermarks
    
    for event in self.event_stream() {
        // Check queue depth and apply backpressure if needed
        let queue_depth = estimate_queue_depth(); // from monitoring
        backpressure.check_and_wait(queue_depth).await;
        
        tx.send_or_log(event, "high_throughput_source").await?;
    }
    Ok(())
}
```

**Benefits:**
- ✅ Prevents memory exhaustion
- ✅ Automatic exponential backoff
- ✅ Configurable watermarks
- ✅ System stability under load

## Validation Chains Migration Examples

### Example 1: Field Validation

**Before** (found in sinex-db/validation.rs):
```rust
// Verbose validation with separate error handling
let path = payload.get("path")
    .ok_or_else(|| ValidationError::MissingField { 
        field: "path".to_string() 
    })?
    .as_str()
    .ok_or_else(|| ValidationError::InvalidType {
        field: "path".to_string(),
        expected: "string".to_string(),
        actual: format!("{:?}", payload.get("path")),
    })?;

if path.is_empty() {
    return Err(ValidationError::InvalidValue {
        field: "path".to_string(),
        reason: "cannot be empty".to_string(),
    });
}

if !is_safe_path(path) {
    return Err(ValidationError::InvalidValue {
        field: "path".to_string(),
        reason: "contains unsafe characters".to_string(),
    });
}
```

**After**:
```rust
use sinex_core::ValidationChain;

let path = ValidationChain::validate(
    payload.get("path").and_then(|v| v.as_str()).unwrap_or(""),
    "path"
)
.not_empty()
.is_path_safe()
.into_result()?;
```

**Benefits:**
- ✅ Fluent, readable API
- ✅ Built-in security validations
- ✅ Reduced boilerplate (4 lines vs 20+ lines)
- ✅ Consistent error messages

### Example 2: Multi-Field Validation with Error Accumulation

**Before** (stops at first error):
```rust
// Individual field validation - stops at first error
let name = validate_name(&payload)?;
let age = validate_age(&payload)?;
let email = validate_email(&payload)?;
```

**After**:
```rust
use sinex_core::{ValidationChain, MultiValidator};

let validator = MultiValidator::new()
    .add(ValidationChain::validate(
        payload.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "name"
    ).not_empty().min_length(2))
    .add(ValidationChain::validate(
        payload.get("age").and_then(|v| v.as_u64()).unwrap_or(0) as i32,
        "age"
    ).min(0).max(150))
    .add(ValidationChain::validate(
        payload.get("email").and_then(|v| v.as_str()).unwrap_or(""),
        "email"
    ).not_empty().is_valid_email());

// Validate all fields and collect ALL errors
match validator.validate_all() {
    Ok(_) => {
        // All validations passed
        let name = payload["name"].as_str().unwrap();
        let age = payload["age"].as_u64().unwrap() as i32;
        let email = payload["email"].as_str().unwrap();
    }
    Err(errors) => {
        // Show user ALL validation errors at once
        let error_msg = errors.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ValidationError::SchemaValidation(error_msg));
    }
}
```

**Benefits:**
- ✅ Shows all validation errors at once
- ✅ Better user experience
- ✅ Composable validation logic
- ✅ Reusable validation patterns

### Example 3: JSON Security Validation

**Before** (basic validation only):
```rust
// Basic JSON validation
if !payload.is_object() {
    return Err(ValidationError::InvalidType {
        field: "payload".to_string(),
        expected: "object".to_string(),
        actual: "other".to_string(),
    });
}
```

**After**:
```rust
use sinex_core::ValidationChain;

let validated_payload = ValidationChain::validate(payload.clone(), "payload")
    .has_field("required_field")
    .field_type("required_field", JsonType::String)
    .max_depth(5)  // Prevent deeply nested JSON attacks
    .max_size(1024 * 1024)  // Prevent large payload attacks  
    .no_excessive_expansion()  // Prevent billion laughs attacks
    .into_result()?;
```

**Benefits:**
- ✅ Built-in security validations
- ✅ Protection against JSON-based attacks
- ✅ Configurable limits
- ✅ Comprehensive validation in one chain

### Example 4: Custom Validation with Context

**Before** (manual custom validation):
```rust
fn validate_username(username: &str) -> Result<(), ValidationError> {
    if username.len() < 3 {
        return Err(ValidationError::InvalidValue {
            field: "username".to_string(),
            reason: "too short".to_string(),
        });
    }
    if !username.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(ValidationError::InvalidValue {
            field: "username".to_string(),
            reason: "invalid characters".to_string(),
        });
    }
    if reserved_usernames().contains(&username.to_lowercase()) {
        return Err(ValidationError::InvalidValue {
            field: "username".to_string(),
            reason: "reserved username".to_string(),
        });
    }
    Ok(())
}
```

**After**:
```rust
use sinex_core::ValidationChain;

let username = ValidationChain::validate(input_username, "username")
    .min_length(3)
    .custom(
        |s| s.chars().all(|c| c.is_alphanumeric() || c == '_'),
        "must contain only letters, numbers, and underscores"
    )
    .custom(
        |s| !reserved_usernames().contains(&s.to_lowercase()),
        "username is reserved"
    )
    .into_result()?;
```

**Benefits:**
- ✅ Combines built-in and custom validations
- ✅ Clear error messages
- ✅ Composable validation logic
- ✅ Easy to test and maintain

## Advanced Migration Patterns

### Pattern 1: Event Source with Full Monitoring

```rust
use sinex_core::{
    ChannelSenderExt, monitored_channel, BackpressureManager,
    ValidationChain, JsonType
};

pub struct AdvancedEventSource {
    backpressure: BackpressureManager,
    event_count: u64,
}

impl EventSource for AdvancedEventSource {
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        loop {
            // Create raw event data
            let raw_data = self.collect_event_data().await?;
            
            // Validate event data before processing
            let validated_payload = ValidationChain::validate(raw_data, "event_payload")
                .has_field("timestamp")
                .field_type("timestamp", JsonType::String)
                .has_field("event_type")
                .field_type("event_type", JsonType::String)
                .max_depth(3)
                .max_size(64 * 1024)  // 64KB limit
                .into_result()?;
            
            let event = RawEventBuilder::new("advanced_source", "data.collected", validated_payload)
                .build();
            
            // Handle backpressure based on queue depth
            let queue_depth = self.estimate_queue_depth();
            self.backpressure.check_and_wait(queue_depth).await;
            
            // Send with monitoring and context
            tx.send_or_log(event, &format!("advanced_source_event_{}", self.event_count)).await?;
            self.event_count += 1;
            
            // Health check every 1000 events
            if self.event_count % 1000 == 0 {
                tracing::info!("Advanced source processed {} events", self.event_count);
            }
        }
    }
}
```

### Pattern 2: Validation Pipeline

```rust
use sinex_core::{ValidationChain, MultiValidator};

pub fn validate_event_pipeline(raw_event: &RawEvent) -> Result<(), ValidationError> {
    // Event-level validation
    let event_validator = ValidationChain::validate(raw_event.clone(), "event")
        .has_valid_source()
        .has_valid_event_type()
        .payload_is_object();
    
    // Payload-specific validation based on event type
    let payload_validator = match raw_event.event_type.as_str() {
        "file.created" | "file.modified" => {
            ValidationChain::validate(raw_event.payload.clone(), "file_event_payload")
                .has_field("path")
                .field_type("path", JsonType::String)
        }
        "user.login" => {
            ValidationChain::validate(raw_event.payload.clone(), "login_event_payload")
                .has_field("username")
                .field_type("username", JsonType::String)
                .has_field("success")
                .field_type("success", JsonType::Bool)
        }
        _ => {
            // Generic validation for unknown event types
            ValidationChain::validate(raw_event.payload.clone(), "generic_payload")
                .max_depth(5)
                .max_size(1024 * 1024)
        }
    };
    
    // Combine all validations
    MultiValidator::new()
        .add(event_validator)
        .add(payload_validator)
        .validate_all()
        .map_err(|errors| {
            ValidationError::SchemaValidation(
                errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")
            )
        })?;
    
    Ok(())
}
```

## Migration Checklist

### For Channel Helpers:
- [ ] Add `use sinex_core::ChannelSenderExt;` to imports
- [ ] Replace `tx.send(event).await.map_err(...)` with `tx.send_or_log(event, "context").await`
- [ ] Add context strings that identify the event source
- [ ] Consider adding backpressure management for high-throughput sources
- [ ] Update health monitoring to use channel statistics

### For Validation Chains:
- [ ] Add `use sinex_core::ValidationChain;` to imports  
- [ ] Replace verbose validation blocks with fluent chains
- [ ] Add security validations (path safety, JSON limits, etc.)
- [ ] Use `MultiValidator` for multi-field validation with error accumulation
- [ ] Update error handling to work with accumulated errors

### Testing Migration:
- [ ] Verify all existing tests still pass
- [ ] Add tests for new validation and channel patterns
- [ ] Test error cases and edge conditions
- [ ] Performance test high-throughput scenarios with backpressure

## Expected Benefits

After migration, you should see:

1. **Reduced Code Duplication**: ~70% reduction in repetitive patterns
2. **Better Error Messages**: Context-aware error reporting
3. **Improved Monitoring**: Built-in metrics and health monitoring
4. **Enhanced Security**: Built-in validations for common attack vectors
5. **Better Performance**: Backpressure handling prevents memory issues
6. **Easier Testing**: Simpler patterns are easier to unit test
7. **Improved Maintainability**: Consistent patterns across the codebase