# Repository Pattern Implementation

This module provides a clean, type-safe interface to the database using a hybrid approach:

- Direct `sqlx` queries for static, performance-critical operations
- SeaQuery for dynamic query building

Each repository follows the same pattern and provides both approaches where appropriate.

## Architecture

All repositories implement common traits for consistency:

- `Repository<T>`: Basic CRUD operations
- `TransactionSupport`: Transaction-aware operations
- `BatchRepository<T>`: Efficient batch operations

## Usage

Access repositories through the `DbPoolExt` trait:

```rust
use sinex_core::DbPoolExt;

let events = pool.events().get_recent(100).await?;
let checkpoint = pool.checkpoints().get_latest("node").await?;
```
