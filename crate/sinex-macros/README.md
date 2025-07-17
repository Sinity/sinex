# Sinex Macros

Procedural macros for the Sinex codebase that reduce boilerplate and improve maintainability across satellite architectures.

## Overview

This crate provides code generation macros focused on common patterns in the Sinex satellite architecture:

- **Event type registration and handling**
- **Validation chain construction**
- **Configuration struct generation**
- **Stream processor implementations**
- **Database query helpers**
- **Error context enrichment**
- **Satellite processing patterns**

## Satellite Processing Macros

### `#[derive(SatelliteProcessor)]`

Generates common methods for StatefulStreamProcessor implementations with lifecycle management.

#### Features:
- Basic initialization and configuration loading
- Checkpoint management helpers
- Error handling with exponential backoff
- Heartbeat emission utilities
- Health check implementations

#### Usage:
```rust
use sinex_macros::SatelliteProcessor;

#[derive(Debug, Default, SatelliteProcessor)]
pub struct FilesystemProcessor {
    config: FilesystemConfig,
    #[checkpoint_state]
    last_scan_time: Option<DateTime<Utc>>,
}

// Generated methods available:
async fn main() {
    let mut processor = FilesystemProcessor::new();
    
    // Basic processor information
    println!("Processor: {}", processor.processor_name());
    
    // Health check
    let is_healthy = processor.health_check().await?;
    
    // Retry logic with exponential backoff
    let result = processor.execute_with_retry(
        || async { /* operation */ Ok(42) },
        max_retries: 3,
        base_delay_ms: 100,
    ).await?;
}
```

### `#[derive(EventHandler)]`

Generates event processing methods with validation, filtering, and retry logic.

#### Features:
- Event validation and filtering
- Batch processing with configurable sizes
- Retry logic with exponential backoff
- Error context and logging
- Payload extraction and type safety

#### Usage:
```rust
use sinex_macros::EventHandler;

#[derive(Debug, Default, EventHandler)]
pub struct FileEventHandler {
    batch_size: usize,
}

// Generated methods available:
async fn main() {
    let handler = FileEventHandler::default();
    
    // Process events with retry logic
    let events = vec![/* events */];
    let results = handler.process_events_with_retry(events, 3).await?;
    
    // Extract payloads from raw events
    let payload = handler.extract_payload::<FilePayload>(&raw_event)?;
    
    // Check if event should be processed
    if handler.should_process_event(&raw_event) {
        // Process the event
    }
}
```

### `#[derive(SatelliteConfig)]`

Generates configuration management with hierarchical loading and validation.

#### Features:
- Hierarchical loading (CLI > env > file > default)
- Comprehensive validation with custom validators
- Environment variable loading with type conversion
- Default value handling
- Configuration merging and overlaying

#### Usage:
```rust
use sinex_macros::SatelliteConfig;

#[derive(Debug, Default, SatelliteConfig)]
pub struct FilesystemConfig {
    #[config(env = "WATCH_PATTERNS", default = "vec![]")]
    pub watch_patterns: Vec<String>,
    
    #[config(env = "DEBOUNCE_MS", default = 500, validate = "positive")]
    pub debounce_ms: u64,
}

// Generated methods available:
fn main() {
    // Load from environment variables
    let config = FilesystemConfig::from_env();
    
    // Load from file
    let config = FilesystemConfig::from_file("config.toml")?;
    
    // Hierarchical loading
    let config = FilesystemConfig::load()?;
    
    // Validation
    assert!(config.validate().is_ok());
    assert!(config.is_valid());
    
    // JSON serialization
    let json = config.to_json()?;
    let from_json = FilesystemConfig::from_json(&json)?;
}
```

### `#[derive(PayloadExtractor)]`

Generates payload extraction methods with type safety and validation.

#### Features:
- Type-safe payload extraction with comprehensive error handling
- Validation with custom validators
- Schema validation support
- Multiple payload format support (JSON, TOML, etc.)
- Automatic type conversion and coercion

#### Usage:
```rust
use sinex_macros::PayloadExtractor;

#[derive(Debug, Default, PayloadExtractor)]
pub struct FileCreatedExtractor {
    schema: Option<serde_json::Value>,
}

// Generated methods available:
fn main() {
    let extractor = FileCreatedExtractor::default();
    
    // Extract payload from JSON
    let payload: FileCreatedPayload = extractor.extract_payload(&json_value)?;
    
    // Extract with validation
    let validated = extractor.extract_and_validate::<FileCreatedPayload>(&json_value)?;
    
    // Extract from raw event
    let from_event = extractor.extract_from_event::<FileCreatedPayload>(&raw_event)?;
    
    // Batch extraction
    let batch = extractor.extract_batch::<FileCreatedPayload>(&events)?;
    
    // Check if extraction is possible
    if extractor.can_extract(&json_value) {
        // Safe to extract
    }
}
```

## Core Macros

### `#[with_context]`

Automatic error context enrichment that adds function name, module path, and operation context to errors.

```rust
use sinex_macros::with_context;

#[with_context]
fn read_config() -> Result<String, std::io::Error> {
    std::fs::read_to_string("config.toml")
}

#[with_context(operation = "database_insert")]
async fn insert_event(event: &RawEvent) -> Result<(), CoreError> {
    // function body
}
```

### `event_registry!`

Generates event type registries with automatic constant generation.

```rust
use sinex_macros::event_registry;

event_registry! {
    sources {
        FILESYSTEM => "fs",
        SHELL => "shell",
    }
    
    events {
        filesystem => FILESYSTEM {
            FILE_CREATED => "file.created" with FileCreatedPayload,
            FILE_MODIFIED => "file.modified" with FileModifiedPayload,
        },
    }
}
```

### `#[typed_event_envelope]`

Generates typed event envelope implementations with automatic to_json_event() methods.

```rust
use sinex_macros::typed_event_envelope;

#[typed_event_envelope]
pub enum EventEnvelope {
    FileCreated(TypedRawEvent<FileCreatedPayload>),
    FileModified(TypedRawEvent<FileModifiedPayload>),
}
```

### `validation_chain!`

Creates fluent validation chains with concise syntax.

```rust
use sinex_macros::validation_chain;

validation_chain! {
    username: String => {
        not_empty(),
        min_length(3),
        max_length(50),
    },
    port: u16 => {
        in_range(1, 65535),
    },
}
```

### `config_struct!`

Generates configuration structs with validation and defaults.

```rust
use sinex_macros::config_struct;

config_struct! {
    pub struct DatabaseConfig {
        #[config(env = "DATABASE_URL", validate = "not_empty")]
        pub url: String,
        
        #[config(env = "DATABASE_MAX_CONNECTIONS", default = 10)]
        pub max_connections: u32,
    }
}
```

### `#[stream_processor]`

Generates StatefulStreamProcessor implementations with reduced boilerplate.

```rust
use sinex_macros::stream_processor;

#[stream_processor(
    processor_type = "ingestor",
    checkpoint_type = "external",
    source = "filesystem"
)]
pub struct FilesystemWatcher {
    config: FilesystemConfig,
    #[state]
    last_scan_time: Option<DateTime<Utc>>,
}
```

### Database Helper Macros

#### `db_query!`

Generates database query helpers with automatic ULID/UUID conversion.

```rust
use sinex_macros::db_query;

db_query! {
    async fn get_event_by_id(pool: &PgPool, id: Ulid) -> Option<RawEvent> {
        "SELECT * FROM raw.events WHERE id = $1"
    }
}
```

#### `db_transaction!`

Generates database transaction helpers with automatic rollback handling.

```rust
use sinex_macros::db_transaction;

db_transaction! {
    async fn insert_multiple_events(pool: &PgPool, events: Vec<RawEvent>) -> Result<(), CoreError> {
        for event in events {
            // Insert logic here
        }
    }
}
```

## Design Principles

### Flexibility and Composability

The macros are designed to be flexible and composable rather than overly specific:

- **Common Patterns**: Focus on patterns that appear across multiple satellite implementations
- **Extensibility**: Generated code can be extended and overridden by specific implementations
- **Type Safety**: All generated code maintains Rust's type safety guarantees
- **Zero Runtime Overhead**: All code generation happens at compile time

### Error Handling

All macros generate comprehensive error handling:

- **Exponential Backoff**: Retry operations with configurable backoff strategies
- **Error Context**: Rich error context with operation details and debugging information
- **Graceful Degradation**: Fallback mechanisms for non-critical failures
- **Validation**: Comprehensive validation with custom validators

### Performance

The macros are designed for performance:

- **Batch Processing**: Efficient batch processing for large data sets
- **Memory Efficiency**: Minimal memory allocation and efficient data structures
- **Concurrent Processing**: Support for concurrent and parallel processing patterns
- **Lazy Evaluation**: Lazy evaluation where appropriate to minimize resource usage

## Testing

The crate includes comprehensive tests for all macros:

```bash
# Run all tests
cargo test

# Run satellite macro tests specifically
cargo test satellite_macros_test

# Run with verbose output
cargo test -- --nocapture
```

## Examples

### Complete Satellite Implementation

```rust
use sinex_macros::{SatelliteProcessor, EventHandler, SatelliteConfig, PayloadExtractor};

#[derive(Debug, Default, SatelliteConfig)]
pub struct MyConfig {
    #[config(env = "BATCH_SIZE", default = 100)]
    pub batch_size: usize,
    
    #[config(env = "RETRY_COUNT", default = 3)]
    pub retry_count: u32,
}

#[derive(Debug, Default, EventHandler)]
pub struct MyEventHandler;

#[derive(Debug, Default, PayloadExtractor)]
pub struct MyPayloadExtractor;

#[derive(Debug, Default, SatelliteProcessor)]
pub struct MyProcessor {
    config: MyConfig,
    handler: MyEventHandler,
    extractor: MyPayloadExtractor,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = MyConfig::load()?;
    
    // Create processor
    let mut processor = MyProcessor::new();
    
    // Health check
    let is_healthy = processor.health_check().await?;
    println!("Processor healthy: {}", is_healthy);
    
    // Process events with retry
    let events = vec![/* events */];
    let handler = MyEventHandler::default();
    let results = handler.process_events_with_retry(events, config.retry_count).await?;
    
    // Extract payloads
    let extractor = MyPayloadExtractor::default();
    let payloads = extractor.extract_batch::<MyPayload>(&raw_events)?;
    
    Ok(())
}
```

## Future Enhancements

### Planned Features

- **Advanced Attribute Support**: More sophisticated attribute-based configuration
- **Custom Validators**: Easy definition of custom validation functions
- **Schema Generation**: Automatic generation of JSON schemas for validation
- **Performance Optimizations**: Further optimizations for large-scale processing
- **Integration Testing**: Enhanced integration testing with actual satellite services

### Contributing

The macros are designed to be extensible. To add new functionality:

1. Identify common patterns in satellite implementations
2. Design flexible interfaces that don't overfit to specific use cases
3. Add comprehensive tests for new functionality
4. Update documentation and examples
5. Ensure backward compatibility

## Dependencies

- `syn 2.0`: For parsing Rust syntax
- `quote 1.0`: For generating Rust code
- `proc-macro2 1.0`: For procedural macro utilities
- `serde 1.0`: For serialization support
- `tokio 1.0`: For async runtime support
- `chrono 0.4`: For timestamp handling
- `toml 0.8`: For configuration file support

## License

This crate is part of the Sinex project and follows the same licensing terms.