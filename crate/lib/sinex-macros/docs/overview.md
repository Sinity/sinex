# sinex-macros

Procedural macros for Sinex codebase

This crate provides code generation macros to reduce boilerplate and improve
maintainability across the Sinex codebase. The macros focus on common patterns
that would benefit from automation:

- Event type registration and handling
- Validation chain construction
- Configuration struct generation
- Stream processor implementations
- Database query helpers
- Error context enrichment

# Usage

## Basic usage (adds function name and module path):
```rust
use sinex_macros::with_context;
use sinex_core::types::error::{SinexError, Result};

#[with_context]
fn read_config() -> Result<String> {
std::fs::read_to_string("config.toml")
.map_err(|e| SinexError::io(e.to_string()))
}
```

## Examples

### Error Context Enrichment
```rust
#[with_context(operation = "database_insert")]
async fn insert_event(pool: &PgPool, event: &RawEvent) -> Result<()> {
// function body
}
```

### Event Registry Generation
```rust
event_registry! {
sources {
FILESYSTEM => sinex_events::sources::FS,
SHELL => "shell",
}

events {
filesystem => FILESYSTEM {
FILE_CREATED => event_types::file::CREATED with FileCreatedPayload,
FILE_MODIFIED => event_types::file::MODIFIED with FileModifiedPayload,
},
}
}
```

### Configuration Struct Generation
```rust
config_struct! {
pub struct DatabaseConfig {
#[config(env = "DATABASE_URL", validate = "not_empty")]
pub url: String,

#[config(env = "DATABASE_MAX_CONNECTIONS", default = 10)]
pub max_connections: u32,
}
}
```
