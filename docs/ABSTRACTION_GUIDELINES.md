# Sinex Abstraction Guidelines

This document outlines the key abstractions used in Sinex and how to use them properly.

## Core Abstractions

### 1. Query Builder (Instead of Raw SQL)

**Use**: `QueryBuilder` from `crate/sinex-db/src/query_builder.rs`  
**Avoid**: Direct `sqlx::query!()` or `sqlx::query_as!()`

```rust
// ❌ Wrong
let event = sqlx::query_as!(
    Event,
    "SELECT * FROM core.events WHERE id = $1",
    event_id.to_uuid()  // Manual ULID conversion
)
.fetch_one(pool)
.await?;

// ✅ Correct
let event = EventQueries::get_by_id(event_id)  // Automatic ULID handling
    .fetch_one(pool)
    .await
    .context(CoreError::NotFound { entity: "event".to_string() })?;
```

**Benefits**:
- Automatic ULID↔UUID conversion
- Type-safe parameter binding
- Consistent error handling

### 2. Error Handling (Instead of anyhow)

**Use**: `CoreError` from `crate/sinex-error/src/lib.rs`  
**Avoid**: `anyhow!()`, raw `unwrap()`, `expect()` in production code

```rust
// ❌ Wrong
anyhow!("Failed to process event: {}", event_id)

// ✅ Correct
CoreError::Processing {
    event_id,
    reason: "validation failed".to_string(),
}
```

**Common Variants**:
- `CoreError::Database { operation }`
- `CoreError::NotFound { entity }`
- `CoreError::Validation { field, reason }`
- `CoreError::Internal { message }`

### 3. String Constants (Instead of Literals)

**Use**: Constants from `crate/sinex-events/src/constants.rs`  
**Avoid**: Hardcoded strings like `"process.heartbeat"`, `"core.events"`

```rust
// ❌ Wrong
if event.event_type == "process.heartbeat" {
    // ...
}

// ✅ Correct
use sinex_events::constants::event_types::sinex::PROCESS_HEARTBEAT;

if event.event_type == PROCESS_HEARTBEAT {
    // ...
}
```

**Available Constants**:
- `event_types::*` - Event type constants by domain
- `sources::*` - Event source identifiers
- `services::*` - Service names

### 4. Validation (Instead of Manual Checks)

**Use**: `ValidationChain` from `crate/sinex-validation/src/validation_chains.rs`  
**Avoid**: Manual validation logic

```rust
// ❌ Wrong
if name.is_empty() {
    return Err(anyhow!("Name cannot be empty"));
}

// ✅ Correct
ValidationChain::validate(&name, "name")
    .not_empty()
    .min_length(3)
    .into_result()?;
```

## Enforcement

### Pre-commit Hook

Install the pre-commit hook to catch violations before committing:

```bash
ln -s ../../scripts/check-abstractions.sh .git/hooks/pre-commit
```

### CI/CD

GitHub Actions will run abstraction checks on all PRs. See `.github/workflows/abstractions.yml`.

### Clippy Configuration

The `clippy.toml` file disallows certain methods:

```toml
disallowed-methods = [
    { path = "sqlx::query", reason = "Use QueryBuilder from sinex-db" },
    { path = "anyhow::anyhow", reason = "Use CoreError from sinex-error" },
]
```

### Migration Tool

For existing code, use the migration script:

```bash
# Dry-run by default
./scripts/migrate-to-abstractions.py crate/

# Actually apply changes
./scripts/migrate-to-abstractions.py crate/ --apply
```

## When to Use Abstractions

These abstractions should be used in all production code. The benefits include:

1. **Consistency** - Everyone uses the same patterns
2. **Safety** - Automatic ULID conversion prevents bugs
3. **Maintainability** - Changes only need to happen in one place
4. **Discoverability** - Constants make available options clear

## Examples

See the `examples/` directory for comprehensive examples:

- `query_patterns.rs` - QueryBuilder usage patterns
- `error_handling.rs` - CoreError best practices
- `using_constants.rs` - String constant usage

## FAQ

**Q: What if QueryBuilder doesn't support my query?**  
A: First check if it can be composed using existing methods. If not, add the functionality to QueryBuilder.

**Q: Are there performance implications?**  
A: No. The abstractions compile to the same code as raw implementations.

**Q: What about tests?**  
A: Tests should also use abstractions to ensure they test real behavior.