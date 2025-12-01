# sinex-error

Comprehensive error handling for the Sinex ecosystem.

This crate provides a unified error type ([`SinexError`]) that is used throughout
the Sinex system. It offers rich error context, categorization, serialization,
and seamless integration with both standard Rust error handling and the `anyhow` crate.

## Features

- **Rich Context**: Attach key-value pairs and source errors to provide detailed diagnostics
- **Categorization**: Errors are categorized by type (database, validation, network, etc.)
- **Serialization**: Full serde support for API responses and logging
- **Status Codes**: Automatic HTTP status code mapping for web services
- **Retryability**: Built-in classification of retryable vs permanent errors
- **Performance**: Zero-allocation error creation for common cases
- **Integration**: Seamless conversion from common error types (io, serde, sqlx, etc.)

## Examples

### Basic Usage

```rust
use crate::error::{SinexError, Result};

fn validate_email(email: &str) -> Result<()> {
if !email.contains('@') {
return Err(SinexError::validation("Invalid email format")
.wrap_err_with("email", email)
.wrap_err_with("reason", "missing @ symbol"));
}
Ok(())
}
```

### With Source Chain

```rust
use crate::error::SinexError;

let error = SinexError::service("Request processing failed")
.with_source("Database connection lost")
.with_source("Network timeout after 30s")
.wrap_err_with("request_id", "abc-123")
.wrap_err_with("retry_count", 3);

// Error display includes full context and source chain
println!("{}", error);
```

### Error Categorization

```rust
use crate::error::SinexError;

let network_error = SinexError::network("Connection refused");
assert!(network_error.is_retryable());
assert_eq!(network_error.status_code(), 500);

let validation_error = SinexError::validation("Invalid input");
assert!(validation_error.is_client_error());
assert_eq!(validation_error.status_code(), 400);
```

### Integration with anyhow

```rust
use crate::error::SinexError;
use color_eyre::eyre::Result;

fn process_data() -> Result<String> {
// SinexError automatically converts to color_eyre::eyre::Error
Err(SinexError::not_found("Data not found"))?
}
```
