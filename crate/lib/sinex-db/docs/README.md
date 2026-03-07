# sinex-db

Database layer utilities, repository pattern implementations, and test database infrastructure.

## Overview

This crate provides:
- **Repository pattern** - Type-safe database access via `DbPoolExt`
- **Test database pool** - 64-database parallel testing infrastructure
- **TimescaleDB integration** - Hypertable partitioning and time-series queries

## Documentation

| File | Description |
|------|-------------|
| [patterns.md](./patterns.md) | Repository trait, DbPoolExt, SQLX compile-time validation |
| [diagrams.md](./diagrams.md) | Schema visualization, repository architecture diagrams |

## Quick Start

```rust
use sinex_db::{DbPoolExt, EventRepository};

// Ergonomic repository access
let event = pool.events().get_by_id(event_id).await?;
let materials = pool.source_materials().search(query).await?;

// Compile-time validated queries
let events = sqlx::query_as!(
    EventRecord,
    r#"SELECT id as "id!: Uuid", source, payload
       FROM core.events WHERE source = $1"#,
    source
).fetch_all(&pool).await?;
```

## Key Concepts

### Repository Pattern
Repositories borrow the connection pool with lifetime `'a`, ensuring:
- No connection leaks (compile-time enforced)
- Zero-cost abstraction (inlined at compile time)
- Clear ownership semantics

### Test Database Pool
64 pre-created databases for parallel testing:
- PostgreSQL advisory locks coordinate access
- Template database with migration fingerprinting
- Automatic cleanup on test completion

## See Also

- Schema definitions: `crate/lib/sinex-schema/docs/`
- Core types: `crate/lib/sinex-primitives/docs/`
