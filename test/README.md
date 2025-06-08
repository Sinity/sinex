# Test Infrastructure

## Quick Start

```bash
# All tests
cargo test

# Integration tests only  
cargo test --test integration

# Isolated ephemeral environment
nix run .#ephemeral test
```

## Test Types

- **Unit tests**: `cargo test --lib` - Fast, no database
- **Integration tests**: `cargo test --test integration` - Full system, isolated database  
- **Ephemeral tests**: `nix run .#ephemeral test` - Complete isolation, fresh PostgreSQL

## Test Organization

```
test/
├── database/     # Schema, migrations, ULID
├── pipeline/     # Event processing, workers  
├── agent/        # Manifests, heartbeats
├── reliability/  # Error handling, failures
├── runtime/      # Event sink, validation
└── common/       # Shared utilities
```

## Writing Tests

**Database tests** - Use `#[sqlx::test]` for automatic cleanup:
```rust
#[sqlx::test]
async fn test_something(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Test with isolated database
    Ok(())
}
```

**Unit tests** - Standard `#[test]` or `#[tokio::test]`:
```rust
#[test] 
fn test_logic() {
    // Fast, no dependencies
}
```

## Test Utilities

`use crate::common;` provides:
- `events::*` - Pre-built test events
- `assertions::*` - Common test assertions  
- `generators::*` - Test data generation

## Rules

1. Database tests use `#[sqlx::test]` - never manual pools
2. Integration tests are isolated - no shared state
3. Tests are self-contained - no external dependencies
4. Use ephemeral environment for debugging