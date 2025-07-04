# Test Infrastructure Fixes

## Issues Found
1. Database cleanup in Drop handlers was not completing reliably
2. Admin connection pool was exhausting PostgreSQL connections (20 connections per pool)
3. Race conditions between async runtime shutdown and cleanup tasks
4. Test databases were accumulating (1500+) due to failed cleanup

## Fixes Applied

### 1. Reduced Connection Pool Sizes
- Admin pool: 20 → 5 max connections
- Test database pools: 20 → 5 max connections
- Added connection lifecycle settings (max_lifetime, idle_timeout)

### 2. Improved Drop Handlers
- Check for active runtime with `tokio::runtime::Handle::try_current()`
- Use existing runtime when available, fallback to blocking executor
- Avoid creating new runtimes in Drop (causes "runtime shutdown" errors)

### 3. Database Cleanup Retry Logic
- Added retry logic with exponential backoff for DROP DATABASE
- Try DROP DATABASE WITH (FORCE) first, fallback to regular DROP
- Terminate connections before dropping

### 4. Cleanup Scripts
- `scripts/cleanup_test_dbs.sh` - Emergency cleanup of all test databases
- `scripts/monitor_test_dbs.sh` - Monitor and auto-cleanup when threshold exceeded

## Remaining Considerations

1. **Parallel Test Execution**: High parallelism can still cause issues. Recommend:
   - Use `--test-threads=8` or lower for stable runs
   - Consider using nextest with partition strategy

2. **Database Cleanup**: Drop handlers in async contexts are inherently unreliable. Options:
   - Run monitor script during test runs
   - Periodic cleanup via cron/systemd timer
   - Custom test harness with guaranteed cleanup

3. **Pool Sizing**: Current settings (min_size=8, max_size=16) may be too high for some systems.
   Consider making configurable via environment variables.

## Usage

Run tests with reasonable parallelism:
```bash
cargo test -- --test-threads=8
```

Clean up orphaned databases:
```bash
./scripts/cleanup_test_dbs.sh
```

Monitor and auto-cleanup during test runs:
```bash
./scripts/monitor_test_dbs.sh 50 &  # Keep max 50 test DBs
cargo test
```