# Failure Mode Testing

This directory contains comprehensive tests for various failure scenarios that the Sinex system might encounter in production. These tests are designed to ensure the system degrades gracefully and recovers appropriately when things go wrong.

## Test Categories

### 1. Channel Backpressure (`channel_backpressure_test.rs`)

Tests how the system handles event channel overflow when event sources produce data faster than it can be consumed.

**Key scenarios tested:**
- Fast producer with slow consumer leading to dropped events
- Memory pressure from large event payloads
- Event source recovery after simulated crashes

**What we verify:**
- Events are dropped in a controlled manner when channels fill
- Memory usage is bounded even under pressure
- The system continues operating after event source failures

### 2. Configuration Reload (`config_reload_test.rs`)

Tests configuration changes during active system operation.

**Key scenarios tested:**
- Config reload during batch processing
- Invalid configuration rejection
- Timing of config application
- Config reload during shutdown

**What we verify:**
- Configuration changes don't corrupt in-flight data
- Invalid configs are rejected without affecting current operation
- Proper timing ensures consistency

### 3. Network Timeouts (`network_timeout_test.rs`)

Tests database connection timeout scenarios and network reliability issues.

**Key scenarios tested:**
- Connection timeouts under various network conditions
- Retry logic with exponential backoff
- Connection pool behavior under timeout conditions

**What we verify:**
- Operations timeout appropriately instead of hanging
- Retry logic works with proper backoff
- Connection pool doesn't exhaust under adverse conditions

### 4. Worker Orphans (`worker_orphan_test.rs`)

Tests detection and recovery of orphaned workers and work items.

**Key scenarios tested:**
- Worker crash while holding work items
- Work recovery from dead workers
- Zombie worker prevention

**What we verify:**
- Orphaned work items are detected via heartbeat timeout
- Work is successfully recovered and reprocessed
- Dead workers can't continue claiming work

### 5. Connection Pool Exhaustion (`connection_pool_test.rs`)

Tests behavior when database connection pools are exhausted.

**Key scenarios tested:**
- Steady load vs burst load patterns
- Connection leak detection
- Deadlock prevention in connection acquisition

**What we verify:**
- Pool limits are enforced
- Connection leaks are detected and reported
- Deadlocks are prevented or resolved

### 6. Filesystem Failures (`filesystem_failures_test.rs`)

Tests filesystem-related failure scenarios during event monitoring.

**Key scenarios tested:**
- Disk full conditions
- Permission changes during monitoring
- Filesystem unmount/remount
- Symbolic link edge cases
- Rapid file creation/deletion

**What we verify:**
- Graceful handling of disk space exhaustion
- Recovery after permission issues
- Resilience to filesystem availability changes
- Safe handling of symlink loops and broken links

### 7. Database Failures (`database_failures_test.rs`)

Tests database-specific failure modes and recovery.

**Key scenarios tested:**
- Transaction rollback on constraint violations
- Schema migration failures
- Database restart resilience
- Large result set handling

**What we verify:**
- Transactions maintain ACID properties
- Failed migrations don't leave partial changes
- Connection pools recover after database restarts
- Memory-efficient handling of large queries

### 8. Performance Degradation (`performance_degradation_test.rs`)

Tests detection and handling of gradual performance degradation.

**Key scenarios tested:**
- Memory leak detection
- CPU throttling detection
- I/O saturation handling
- Resource usage pattern analysis

**What we verify:**
- Memory leaks are detected through growth patterns
- CPU throttling is identified via processing time increases
- I/O saturation is detected through latency metrics
- Resource patterns (steady, bursty, growing) are correctly identified

## Design Philosophy

These tests follow several key principles:

1. **Realistic Scenarios**: Each test simulates a real failure mode that could occur in production
2. **Measurable Degradation**: Tests track metrics to verify graceful degradation
3. **Recovery Verification**: Tests confirm the system recovers after issues resolve
4. **No Catastrophic Failure**: The system should never completely fail, only degrade

## Running the Tests

```bash
# Run all failure mode tests
cargo test --test integration failure_modes::

# Run specific test file
cargo test --test integration failure_modes::channel_backpressure

# Run with output to see degradation metrics
cargo test --test integration failure_modes:: -- --nocapture
```

## Adding New Failure Tests

When adding new failure mode tests:

1. Identify a realistic failure scenario
2. Define metrics to track degradation
3. Implement both the failure and recovery phases
4. Verify the system continues operating (degraded is OK, stopped is not)
5. Document the scenario and what's being verified

## Key Patterns

### Metric Tracking
```rust
let metric = Arc::new(AtomicU64::new(0));
// Pass to concurrent tasks and track
```

### Timeout Testing
```rust
match timeout(Duration::from_millis(500), operation).await {
    Ok(result) => // Normal operation
    Err(_) => // Timeout occurred
}
```

### Phased Testing
```rust
// Phase 1: Normal operation (baseline)
// Phase 2: Introduce failure
// Phase 3: Verify degraded operation
// Phase 4: Remove failure
// Phase 5: Verify recovery
```

## Integration with Sinex

These tests specifically target Sinex components:

- **UnifiedCollector**: Channel backpressure, config reload
- **Event Sources**: Not separate processes, but event streams within the collector
- **Workers**: Orphan detection, queue processing
- **Database Layer**: Connection pools, transactions
- **File Monitoring**: Filesystem event source resilience

The tests ensure that Sinex's event-driven architecture can handle real-world failure conditions without data loss or system crashes.