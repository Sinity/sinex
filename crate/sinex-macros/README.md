# sinex-macros

Procedural macros for Sinex error handling and context enrichment.

## Overview

This crate provides procedural macros that integrate with the Sinex error handling system to automatically add contextual information to errors. The primary macro is `#[with_context]`, which wraps functions and enriches any errors they return with additional context like function names, module paths, and custom metadata.

## Features

- **Automatic Error Context**: Add function name, module path, and operation context to errors automatically
- **Custom Operations**: Specify custom operation names for more descriptive error messages
- **Async Support**: Works seamlessly with both sync and async functions
- **Result Type Validation**: Compile-time validation that the macro is only applied to functions returning `Result<T, E>`
- **Zero Runtime Overhead**: All context enrichment happens only when errors occur

## Usage

### Basic Usage

The simplest usage adds function name and module path to any errors:

```rust
use sinex_macros::with_context;
use sinex_core::{CoreError, Result};

#[with_context]
fn read_config() -> Result<String> {
    std::fs::read_to_string("config.toml")
        .map_err(|e| CoreError::Io(e.to_string()))
}

// Error will include:
// - operation: read_config
// - function: read_config  
// - module: myapp::config
```

### Custom Operation Names

You can specify a custom operation name for more descriptive error context:

```rust
#[with_context(operation = "database_connection")]
async fn connect_to_db() -> Result<Connection> {
    // implementation
}

// Error will include:
// - operation: database_connection
// - function: connect_to_db
// - module: myapp::database
```

### Async Function Support

The macro works seamlessly with async functions:

```rust
#[with_context(operation = "user_authentication")]
async fn authenticate_user(token: &str) -> Result<User> {
    // async implementation
}
```

## Error Context Format

Errors enhanced by `#[with_context]` will include additional context in this format:

```
Original error message (operation: operation_name, function: function_name, module: module::path)
```

For example:
```
IO error: No such file or directory (operation: read_config, function: read_config, module: myapp::config)
```

## Integration with Sinex Core

This macro integrates with the `sinex-core` error handling system:

```rust
use sinex_core::{CoreError, Result, with_context};

#[with_context]
fn process_events() -> Result<()> {
    // Any CoreError returned will be automatically enriched
    Err(CoreError::Database("Connection lost".to_string()))
}
```

## Compilation Requirements

- Functions must return `Result<T, E>` where `E: Into<CoreError>`
- The macro performs compile-time validation and will produce helpful error messages for invalid usage

## Examples

See `sinex-core/examples/with_context_usage.rs` for comprehensive examples demonstrating:

- Basic error context addition
- Custom operation names
- Async function support
- Error conversion patterns
- Success case handling (no overhead)

## Implementation Details

The macro transforms functions by:

1. Wrapping the original function body in a closure
2. Adding `.map_err()` to catch and enrich any errors
3. Using the existing `ErrorContext` builder pattern from `sinex-core`
4. Preserving all original function attributes and async behavior

The transformation is zero-cost for success cases - no additional overhead is introduced when functions succeed.

## Future Enhancements

Planned features include:
- Custom context key-value pairs: `#[with_context(context = [("key", "value")])]`
- Conditional context based on error types
- Integration with structured logging
- Performance metrics collection