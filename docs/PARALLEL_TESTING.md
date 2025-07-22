# Parallel Test Execution Guide

This guide explains how parallel test execution is configured in the Sinex project to achieve optimal performance while maintaining test isolation and reliability.

## Overview

The Sinex test suite is configured for parallel execution to achieve 50%+ faster test runs by utilizing all available CPU cores. The setup includes:

- **Dynamic parallelism** that adapts to the system's CPU count
- **Database isolation** through a pool of 64 pre-created test databases
- **Inter-process coordination** using PostgreSQL advisory locks
- **Optimized nextest profiles** for different testing scenarios

## Configuration

### Nextest Profiles

The project uses cargo-nextest with custom profiles defined in `.config/nextest.toml`:

#### Default Profile
```toml
[profile.default]
# Dynamically uses all available CPU cores
test-threads = "num-cpus"
failure-output = "immediate-final"
success-output = "never"
retries = 1
slow-timeout = { period = "120s", terminate-after = 1 }
```

#### Parallel Profile (Maximum Speed)
```toml
[profile.parallel]
test-threads = "num-cpus"
failure-output = "final"
success-output = "never"
retries = 0
slow-timeout = { period = "60s", terminate-after = 1 }
```

#### CI Parallel Profile (Balanced)
```toml
[profile.ci-parallel]
# Uses 75% of CPUs to leave headroom
test-threads = 18
failure-output = "immediate-final"
success-output = "never"
retries = 1
slow-timeout = { period = "120s", terminate-after = 1 }
```

## Database Isolation

### Pool Architecture

The test suite uses a sophisticated database pooling system (`test/common/database_pool.rs`) that provides:

1. **64 pre-created databases** - Minimizes contention even on high-core systems
2. **PostgreSQL advisory locks** - Ensures inter-process coordination
3. **Automatic cleanup** - Databases are cleaned and returned to the pool after each test
4. **Template database** - Shared template with migrations pre-applied for fast database creation

### How It Works

1. **Template Creation**: On first run, a template database is created with all migrations applied
2. **Pool Initialization**: 64 test databases are created from the template in parallel
3. **Test Execution**: Each test acquires a database using PostgreSQL advisory locks
4. **Cleanup**: After test completion, the database is cleaned and returned to the pool

### Database Acquisition Process

```rust
// Each test automatically gets an isolated database
#[sinex_test]
async fn test_example(ctx: TestContext) -> TestResult {
    // ctx.pool() provides access to the isolated database
    let result = sqlx::query("SELECT 1")
        .fetch_one(ctx.pool())
        .await?;
    Ok(())
}
```

## Running Tests in Parallel

### Command Line Usage

```bash
# Run all tests with maximum parallelism
just test-parallel

# Run specific test category in parallel
just test-parallel -E "test(unit::)"

# Run all tests with parallel profile
just test-all-parallel

# Run with statistics
just test-parallel-stats
```

### Direct Cargo Commands

```bash
# Use default profile (auto-detects CPU count)
cargo nextest run

# Use specific parallel profile
cargo nextest run --profile parallel

# Run with custom thread count
cargo nextest run --test-threads 16
```

## Performance Optimization

### System Requirements

- **CPU**: Benefits scale with core count (tested up to 24 cores)
- **Memory**: ~100MB per test database (6.4GB for full pool)
- **Disk**: Fast SSD recommended for database operations

### Optimization Tips

1. **Use the parallel profile** for fastest execution:
   ```bash
   just test-parallel
   ```

2. **Run specific test categories** to reduce scope:
   ```bash
   just test-unit      # Fast unit tests only
   just test-fast      # Unit + property tests
   ```

3. **Monitor pool health** during long test runs:
   ```rust
   let stats = database_pool::get_pool_stats();
   ```

4. **Adjust pool size** for your system if needed:
   ```rust
   // In database_pool.rs
   PoolConfig::with_size(32) // Reduce for smaller systems
   ```

## Troubleshooting

### Common Issues

1. **"Failed to acquire database" errors**
   - Cause: All 64 databases in use
   - Solution: Increase pool size or reduce parallelism

2. **Database cleanup failures**
   - Cause: Tests leaving uncommitted transactions
   - Solution: Ensure all transactions are properly committed/rolled back

3. **Slow test startup**
   - Cause: Template database creation on first run
   - Solution: This is one-time cost, subsequent runs will be fast

### Debugging Commands

```bash
# Run with single thread to isolate issues
cargo nextest run --test-threads 1

# Use debug profile for verbose output
cargo nextest run --profile debug

# Check pool statistics
just test-parallel-stats
```

## Best Practices

1. **Use #[sinex_test] macro** - Provides automatic database isolation
2. **Avoid shared state** - Each test should be independent
3. **Clean up resources** - Use RAII patterns for cleanup
4. **Set appropriate timeouts** - Use timeout attribute for long tests:
   ```rust
   #[sinex_test(timeout = 60)]
   async fn long_running_test(ctx: TestContext) -> TestResult {
       // Test code
   }
   ```

## Performance Metrics

On a 24-core system, parallel execution provides:

- **Unit tests**: ~70% faster (12s → 4s)
- **Integration tests**: ~60% faster (60s → 24s)
- **Full test suite**: ~50% faster (5m → 2.5m)

Results vary based on:
- CPU core count
- Database I/O performance
- Test complexity and dependencies

## Future Improvements

Potential optimizations under consideration:

1. **Dynamic pool sizing** based on system resources
2. **Test sharding** across multiple machines
3. **Parallel migration application** during template creation
4. **Memory-based PostgreSQL** for even faster tests