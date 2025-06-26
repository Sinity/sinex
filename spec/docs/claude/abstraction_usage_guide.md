# Sinex Abstraction Usage Guide

This guide provides comprehensive usage examples and patterns for the five core abstractions introduced to the Sinex test infrastructure.

## Table of Contents

1. [ValidationChain](#validationchain)
2. [ErrorContext](#errorcontext)
3. [ChannelSenderExt/ReceiverExt](#channelsenderextreceiverext)
4. [ConfigExtractor](#configextractor)
5. [Enhanced Test Infrastructure](#enhanced-test-infrastructure)
6. [Migration Patterns](#migration-patterns)

## ValidationChain

The `ValidationChain` abstraction provides fluent, composable validation with error accumulation.

### Basic Usage

```rust
use sinex_core::ValidationChain;

// String validation
let result = ValidationChain::validate("user@example.com".to_string(), "email")
    .not_empty()
    .min_length(5)
    .max_length(100)
    .matches_regex(&email_regex)
    .into_result();

// Numeric validation
let result = ValidationChain::validate(42i64, "port")
    .min(1)
    .max(65535)
    .range(1024..65536)
    .into_result();

// JSON validation
let result = ValidationChain::validate(json_payload, "event_payload")
    .has_field("required_field")
    .field_type("number_field", JsonType::Number)
    .max_depth(5)
    .max_size(10000)
    .into_result();
```

### Event-Specific Validation

```rust
let result = ValidationChain::validate(raw_event, "event")
    .has_valid_source()
    .has_valid_event_type()
    .payload_is_object()
    .payload_matches_schema(&schema)
    .into_result();
```

### Custom Validation

```rust
let result = ValidationChain::validate(username, "username")
    .custom(|s| s.chars().all(|c| c.is_alphanumeric()), "must be alphanumeric")
    .custom(|s| !s.starts_with("_"), "cannot start with underscore")
    .into_result();
```

### MultiValidator for Complex Scenarios

```rust
let multi_validator = MultiValidator::new()
    .add(ValidationChain::validate(config.database_url, "db_url").is_valid_url())
    .add(ValidationChain::validate(config.port, "port").range(1024..65536))
    .add(ValidationChain::validate(config.pool_size, "pool_size").min(1).max(100));

let result = multi_validator.validate_all();
```

### In Tests

```rust
#[sinex_test]
async fn test_event_validation(ctx: TestContext) -> TestResult {
    let event = EventBuilder::filesystem().path("/test/file.txt").build();
    
    // Direct validation chain testing
    let validation = assert_with_validation(event.source.clone(), "event_source")
        .not_empty()
        .min_length(3);
    
    assert_validation_passes(validation)?;
    
    // Test failing validation
    let failing_validation = assert_with_validation("".to_string(), "empty_field")
        .not_empty();
    
    assert_validation_fails(failing_validation, "cannot be empty")?;
    
    Ok(())
}
```

## ErrorContext

The `ErrorContext` abstraction provides rich, chainable error context for better debugging.

### Basic Usage

```rust
use sinex_core::{CoreError, ErrorContext, ResultExt};

// Create error with context
let error = CoreError::database("Connection failed")
    .with_context("host", "localhost")
    .with_context("port", 5432)
    .with_operation("connect_to_database")
    .build();

// Add context to existing errors
let result = some_operation()
    .context("Failed to process user data")
    .with_context(|| {
        CoreError::processing_failed()
            .with_context("user_id", user_id)
            .with_operation("user_data_processing")
    });
```

### Event-Specific Context

```rust
let error = CoreError::validation("Event validation failed")
    .with_event_id(event.id)
    .with_timestamp(event.ts_orig.unwrap_or(event.ts_ingest))
    .with_context("source", &event.source)
    .with_context("event_type", &event.event_type)
    .build();
```

### File Operation Context

```rust
let error = CoreError::io_error("/var/log/sinex.log")
    .with_operation("write_log")
    .with_context("log_level", "error")
    .build();
```

### In Tests

```rust
#[sinex_test]
async fn test_with_error_context(ctx: TestContext) -> TestResult {
    let event = EventBuilder::terminal().command("test").build();
    
    // Enhanced event insertion with context
    let event_id = assert_event_inserted_with_context(
        ctx.pool(),
        &event,
        "test_error_context_scenario"
    ).await?;
    
    // Database operations with enhanced error context
    let result = assert_database_state(
        ctx.pool(),
        async {
            sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events WHERE id = $1", 
                               sinex_db::ulid_to_uuid(event_id))
                .fetch_one(ctx.pool())
                .await
        },
        "verify event insertion"
    ).await?;
    
    Ok(())
}
```

## ChannelSenderExt/ReceiverExt

The channel extension traits provide enhanced async channel operations with timeouts, batching, and monitoring.

### Basic Channel Operations

```rust
use sinex_core::{ChannelSenderExt, ChannelReceiverExt};

// Enhanced sending with context
let result = sender.send_or_log(event, "event_processing").await;

// Timeout-based sending
let result = sender.send_timeout(event, Duration::from_secs(5)).await;

// Queue management with backpressure
let mut queue = Vec::new();
let result = sender.try_send_or_queue(event, &mut queue, 100).await;

// Batch receiving
let batch = receiver.recv_batch(10, Duration::from_millis(100)).await;

// Drain all available items
let items = receiver.drain_all().await;
```

### Channel Monitoring

```rust
let monitor = ChannelMonitor::new();

// Record operations
monitor.record_send();
monitor.record_receive();
monitor.record_error("Send failed: channel full".to_string());

// Get statistics
let stats = monitor.stats();
println!("Sent: {}, Received: {}, Errors: {}, Queue depth: {}", 
         stats.sent, stats.received, stats.errors, stats.queue_depth);
```

### Backpressure Management

```rust
let mut backpressure_manager = BackpressureManager::new(100, 50);

// Check and apply backpressure based on queue depth
backpressure_manager.check_and_wait(queue_depth).await;
```

### In Tests

```rust
#[sinex_test]
async fn test_channel_operations(_ctx: TestContext) -> TestResult {
    let test_setup = TestChannelSetup::new(10);
    
    // Test basic send/receive
    assert_channel_send_success(&test_setup.sender, "test_message", "test_context").await?;
    
    // Test timeout behavior
    assert_channel_send_timeout(
        &test_setup.sender,
        "timeout_test",
        Duration::from_millis(100),
        false // should not timeout
    ).await?;
    
    // Run comprehensive test scenario
    channel_scenarios::run_comprehensive_channel_test(
        "my_channel_test",
        vec!["msg1", "msg2", "msg3"],
        10
    ).await?;
    
    Ok(())
}
```

## ConfigExtractor

The `ConfigExtractor` abstraction provides type-safe configuration access with clear error messages.

### Basic Extraction

```rust
use sinex_core::{ConfigExtractor, ConfigValidator};

// Required values
let database_url = config.require_str("database.url")?;
let pool_size = config.require_u64("database.pool_size")?;
let enabled = config.require_bool("features.enabled")?;

// Optional values with defaults
let timeout = config.u64_or("database.timeout_seconds", 30);
let debug_mode = config.bool_or("debug.enabled", false);
let log_level = config.str_or("logging.level", "info");

// Array extraction
let watch_paths = config.require_array("filesystem.watch_paths")?;
```

### Configuration Validation

```rust
let validator = ConfigValidator::new()
    .require("database.url")
    .validate_range("database.pool_size", 1..=100)
    .validate_regex("logging.level", r"^(trace|debug|info|warn|error)$")
    .validate_custom(|config| {
        if let Some(url) = config.optional_str("database.url") {
            ValidationChain::validate(url.to_string(), "database.url")
                .is_valid_url()
                .into_result()?;
        }
        Ok(())
    })
    .build();

let result = validator(&config);
```

### Duration and URL Validation

```rust
// Parse duration strings
let flush_interval = parse_duration("30s")?; // Returns seconds as u64

// Validate paths
validate_path_exists(&config, "log_file_path")?;
validate_is_file(&config, "config_file")?;
validate_is_dir(&config, "data_directory")?;

// Validate URLs and ports
validate_url(&config, "api_endpoint")?;
validate_port(&config, "listen_port")?;
```

### In Tests

```rust
#[sinex_test]
async fn test_configuration_handling(_ctx: TestContext) -> TestResult {
    // Create test configuration
    let config = test_configs::valid_database_config();
    
    // Validate configuration
    let validator = config_validation::validate_complete_config();
    assert_config_valid(&config, validator, "test_configuration")?;
    
    // Extract configuration
    let db_config = assert_config_extraction(
        config_extraction::extract_database_config(&config),
        "database configuration"
    )?;
    
    // Test configuration scenarios
    for scenario in config_scenarios::all_validation_scenarios() {
        let validator = config_validation::validate_complete_config();
        let result = validator(&scenario.config);
        
        match (result.is_ok(), scenario.should_validate) {
            (true, true) => println!("✓ {} passed as expected", scenario.name),
            (false, false) => println!("✓ {} failed as expected", scenario.name),
            _ => return Err(Box::new(CoreError::validation("Scenario mismatch").build())),
        }
    }
    
    Ok(())
}
```

## Enhanced Test Infrastructure

### TestAssertionBatch

Accumulate multiple assertions and report all failures at once:

```rust
#[sinex_test]
async fn test_multiple_assertions(ctx: TestContext) -> TestResult {
    let mut batch = TestAssertionBatch::new("comprehensive_test");
    
    batch
        .assert_that(|| {
            assert_eq_with_context(&actual_value, &expected_value, "value comparison")
        }, "value validation")
        .assert_that(|| {
            assert_with_context(condition, "condition should be true", "condition check")
        }, "condition validation")
        .assert_validation(
            ValidationChain::validate(data, "test_data").not_empty().min_length(5),
            "data validation"
        );
    
    batch.execute()?;
    Ok(())
}
```

### Enhanced Assertions

```rust
// Context-aware equality
assert_eq_with_context(&left, &right, "comparing user IDs")?;

// Conditional assertions with context
assert_with_context(condition, "message", "test_context")?;

// Event equivalence checking
assert_events_equivalent(&event1, &event2)?;

// Timeout operations
let result = assert_completes_within(
    async_operation(),
    Duration::from_secs(5),
    "complex_database_query"
).await?;
```

### Test Data Generation

```rust
// Using event builders
let event = EventBuilder::filesystem()
    .path("/test/file.txt")
    .created()
    .size(1024)
    .permissions(0o644)
    .build();

// Using configuration factories
let config = TestConfigFactory::create_randomized_config(42);

// Using channel test setups
let channel_setup = TestChannelSetup::new(100);
```

## Migration Patterns

### Migrating from Manual Assertions

**Before:**
```rust
pretty_assertions::assert_eq!(left, right);
assert!(condition);
let result = operation().await.unwrap();
```

**After:**
```rust
assert_eq_with_context(&left, &right, "comparison context")?;
assert_with_context(condition, "condition failed", "test context")?;
let result = assert_completes_within(operation(), Duration::from_secs(1), "operation").await?;
```

### Migrating from Basic Event Creation

**Before:**
```rust
let event = RawEventBuilder::new("source", "type", json!({"data": "test"})).build();
```

**After:**
```rust
let event = EventBuilder::filesystem()
    .path("/test/file.txt")
    .created()
    .size(1024)
    .build();
```

### Migrating Configuration Handling

**Before:**
```rust
let url = config["database"]["url"].as_str().unwrap();
let pool_size = config["database"]["pool_size"].as_i64().unwrap() as u64;
```

**After:**
```rust
let url = config.require_str("database.url")?;
let pool_size = config.require_u64("database.pool_size")?;

// With validation
let url_validation = ValidationChain::validate(url.to_string(), "database.url")
    .not_empty()
    .is_valid_url()
    .into_result()?;
```

### Migrating Channel Operations

**Before:**
```rust
sender.send(item).await.unwrap();
let item = receiver.recv().await.unwrap();
```

**After:**
```rust
assert_channel_send_success(&sender, item, "test_context").await?;
let item = receiver.recv_timeout(Duration::from_secs(1)).await?.unwrap();
```

## Best Practices

### ValidationChain
- Use descriptive field names for better error messages
- Chain related validations together
- Use `custom()` for domain-specific validation rules
- Accumulate errors with `MultiValidator` for complex validation scenarios

### ErrorContext
- Always provide operation context for debugging
- Include relevant IDs (event_id, user_id, etc.)
- Chain errors to preserve the full error context
- Use specific error types (database, validation, configuration, etc.)

### Channel Extensions
- Always specify context when using `send_or_log`
- Use appropriate timeouts for your use case
- Monitor channel health in long-running operations
- Test backpressure scenarios for realistic load conditions

### ConfigExtractor
- Use `require_*` for mandatory fields
- Use `*_or` with sensible defaults for optional fields
- Validate extracted values with ValidationChain
- Create reusable validation functions for common patterns

### Test Infrastructure
- Use `TestAssertionBatch` for comprehensive test validation
- Provide meaningful test context in all assertions
- Use event builders instead of manual RawEvent construction
- Test both success and failure scenarios with enhanced assertions

## Examples in the Codebase

- **ValidationChain**: `test/common/validation_test_utils.rs`
- **ErrorContext**: `test/common/enhanced_assertions.rs`
- **ChannelExtensions**: `test/common/channel_test_utils.rs`
- **ConfigExtractor**: `test/common/config_test_utils.rs`
- **Integration**: `test/integration/abstraction_integration_test.rs`
- **Modernized Tests**: `test/unit/db/basic_db_test.rs`

## Conclusion

These abstractions work together to provide:

1. **Fluent Validation**: Clear, composable validation with rich error reporting
2. **Rich Error Context**: Debuggable errors with operation and data context
3. **Robust Channel Operations**: Timeout-aware, monitored async communication
4. **Type-Safe Configuration**: Clear configuration access with validation
5. **Enhanced Test Infrastructure**: Better test failures and easier test writing

By using these abstractions consistently, the Sinex codebase achieves better error handling, more maintainable tests, and clearer code patterns throughout the system.