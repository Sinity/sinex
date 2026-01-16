# sinex-macros

Procedural macros for Sinex codebase

This crate provides code generation macros to reduce boilerplate and improve
maintainability across the Sinex codebase. The macros focus on common patterns
that would benefit from automation:

- Error context enrichment (`with_context`)
- Event type registration (`event_registry`, `typed_event_envelope`)
- Database query helpers (`db_query`, `db_transaction`)
- Typed IDs and schema validation (`define_id_type`, `EventPayload`, `ValidateRecord`)

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

### Schema Validation
```rust
use sinex_macros::ValidateRecord;
use sqlx::FromRow;

#[derive(FromRow, ValidateRecord)]
#[validate_against(sinex_schema::Events)]
pub struct EventRecord {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    // ...
}
```
