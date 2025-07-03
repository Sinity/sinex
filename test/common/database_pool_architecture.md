# Universal Database Pool Architecture

## Overview

The Sinex test suite uses a universal database pool system that provides near-instant test database acquisition with perfect isolation. This replaces the previous heterogeneous approach with a single, fast, bulletproof system.

## Key Features

- **Pre-created pool**: Databases created from template on startup
- **<5ms acquisition**: Near-instant database availability 
- **TRUNCATE cleanup**: Fast cleanup without dropping databases
- **Automatic scaling**: Pool expands under load up to max_size
- **Perfect isolation**: Each test gets its own database
- **Zero configuration**: Works out of the box with #[sinex_test]

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 Pool Manager                     │
│  ┌──────────────┐  ┌──────────────┐            │
│  │  Available   │  │   In Use     │            │
│  │  Databases   │  │  Databases   │            │
│  │    (LIFO)    │  │              │            │
│  └──────────────┘  └──────────────┘            │
│         ↓                  ↑                    │
│    acquire()          return on drop            │
└─────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────┐
│              Template Database                   │
│  - All migrations applied                        │
│  - Optimized for fast copying                    │  
│  - No expensive indexes                          │
│  - Autovacuum disabled                          │
└─────────────────────────────────────────────────┘
```

## Usage

### Basic Test with #[sinex_test]

```rust
#[sinex_test]
async fn test_something(ctx: TestContext) -> TestResult {
    // Database already available via ctx
    let event = ctx.filesystem_event("/test/file.txt");
    ctx.insert_event(&event).await?;
    
    // Perfect isolation - other tests can't see this data
    ctx.wait_for_event_count(1).await?;
    
    Ok(())
    // Database automatically cleaned and returned to pool
}
```

### Direct Pool Usage

```rust
use crate::common::database_pool;

#[tokio::test]
async fn test_direct_usage() -> TestResult {
    let db = database_pool::acquire_database().await?;
    
    // Use db.pool() for queries
    queries::insert_event(db.pool(), &event).await?;
    
    Ok(())
    // Database returned to pool on drop
}
```

## Performance Characteristics

| Operation | Old System | New System | Improvement |
|-----------|------------|------------|-------------|
| Database Setup | 300-500ms | 5-20ms | 15-100x faster |
| Cleanup | 100-200ms | 5-10ms | 10-40x faster |
| Total Overhead | 400-700ms | 10-30ms | 13-70x faster |

## Configuration

The pool configures itself based on system resources:

- **min_size**: CPU count (optimal parallelism)
- **max_size**: CPU count × 2 (allows bursting)
- **Template**: Created once, reused for all databases
- **Cleanup**: TRUNCATE CASCADE (preserves structure)

## Migration Guide

### From TestDatabase

```rust
// OLD
let test_db = TestDatabase::create("my_test").await?;
let pool = &test_db.pool;

// NEW  
#[sinex_test]
async fn my_test(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
```

### From Transaction Isolation

```rust
// OLD
test_with_transaction!(my_test, tx, {
    sqlx::query!("...").execute(&mut *tx).await?;
});

// NEW
#[sinex_test]
async fn my_test(ctx: TestContext) -> TestResult {
    sqlx::query!("...").execute(ctx.pool()).await?;
}
```

### From Shared Pool

```rust
// OLD
let pool = get_shared_test_pool().await?;

// NEW
#[sinex_test]
async fn my_test(ctx: TestContext) -> TestResult {
    // Just use ctx.pool()
}
```

## How It Works

1. **First Test**: Creates template database with all migrations
2. **Pool Init**: Pre-creates N databases from template (N = CPU count)
3. **Test Starts**: Acquires clean database from pool (<5ms)
4. **Test Runs**: Full isolation, can commit/rollback as needed
5. **Test Ends**: Database cleaned with TRUNCATE and returned to pool
6. **Scaling**: If pool exhausted, creates new databases on demand

## Best Practices

1. **Always use #[sinex_test]** for database tests
2. **Don't manually manage cleanup** - it's automatic
3. **Write tests assuming empty database** - no shared state
4. **Use TestContext helpers** for common operations
5. **Let the pool scale** - don't worry about exhaustion

## Troubleshooting

### Tests are slow
- Check if template database exists: `SELECT datname FROM pg_database WHERE datname LIKE 'sinex_test_template_%'`
- Verify pool is initialized: Look for "🚀 Initializing database pool" in output
- Check pool stats in test output

### Database not clean
- Verify TRUNCATE permissions on all tables
- Check for tables not in cleanup list
- Look for failed cleanup messages in output

### Pool exhaustion
- Normal under heavy load - pool will expand
- Check max_size if consistently exhausted
- Consider increasing max_size for stress tests

## Implementation Details

- Uses PostgreSQL template databases for fast copying
- LIFO queue for better cache locality
- Automatic health checks before reuse
- Graceful handling of corrupted databases
- Statistics tracking for debugging