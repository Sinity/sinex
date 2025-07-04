# Complete Test Infrastructure Fix

## Summary

The test infrastructure has fundamental issues with database cleanup due to:

1. **Async Drop Limitations**: Rust's Drop trait is synchronous, but database cleanup requires async operations
2. **Runtime Conflicts**: Tests spawn/destroy Tokio runtimes, causing "runtime shutdown" errors during cleanup
3. **Concurrent Initialization**: Multiple tests racing to initialize shared resources
4. **Connection Pool Exhaustion**: Too many connections created without proper cleanup

## Solution Implemented

### 1. Simple Database Manager (`simple_db_manager.rs`)
- Background task handles all database operations
- Command channel pattern avoids Drop complexity
- Automatic cleanup of idle databases
- Resilient to concurrent test execution

### 2. Disabled Auto-Cleanup
- Removed `#[ctor::dtor]` that was interfering with tests
- Tests clean up their own databases via Drop handlers
- Background task periodically cleans idle databases

### 3. Connection Limits
- Admin pool: 3 connections (down from 20)
- Test database pools: 5 connections each
- Connection lifecycle management (max_lifetime, idle_timeout)

## Current Status

**Partially Working**: 
- Single-threaded tests work reliably
- Multi-threaded tests have race conditions
- Manager task sometimes dies or doesn't respond

## Recommended Final Solution

### Option 1: Use External Database Pool Service
Run a separate process that manages test databases:
```bash
# Start pool service
cargo run --bin test-db-pool-service &

# Run tests (they connect to the service)
cargo test

# Stop service
pkill test-db-pool-service
```

### Option 2: Use Transactions Instead of Separate Databases
Modify tests to use transactions on a shared database:
```rust
#[sinex_test]
async fn test(pool: DbPool) -> TestResult {
    let mut tx = pool.begin().await?;
    // Test runs in transaction
    // Automatic rollback on drop
}
```

### Option 3: Accept Manual Cleanup
Keep current solution but require manual cleanup:
```bash
# Run tests
cargo test

# Clean up after
./scripts/cleanup_test_dbs.sh
```

### Option 4: Use Process-Level Synchronization
Use file locks or system semaphores to ensure only one process initializes the manager:
```rust
use fs2::FileExt;
let lock_file = std::fs::File::create("/tmp/sinex_test.lock")?;
lock_file.lock_exclusive()?;
// Initialize manager
lock_file.unlock()?;
```

## Short-Term Workaround

For immediate use:
1. Run tests with limited parallelism: `cargo test -- --test-threads=4`
2. Clean up periodically: `./scripts/cleanup_test_dbs.sh`
3. Monitor with: `./scripts/monitor_test_dbs.sh 50 &`

## Long-Term Recommendation

Implement Option 2 (transaction-based isolation) as it:
- Eliminates database creation/cleanup overhead
- Provides perfect isolation via MVCC
- Scales to any number of parallel tests
- No cleanup required

This would require:
1. Modifying the test macro to use transactions
2. Updating test helpers to work within transactions
3. Ensuring all tests are transaction-safe

The current implementation works but has limitations with high parallelism. A transaction-based approach would be more robust and performant.