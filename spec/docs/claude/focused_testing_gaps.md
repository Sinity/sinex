# Focused Testing Gaps for Sinex - Real Issues Only

## Executive Summary

Current test coverage focuses heavily on basic functionality but misses critical race conditions, resource exhaustion scenarios, and failure cascades that could cause data loss or system failure in production.

## Critical Missing Tests by Priority

### 1. ULID Generation Edge Cases (CRITICAL)

**Current Gap**: The monotonic ULID generator has basic concurrency tests but lacks:
- **Clock regression handling**: What happens when system time goes backwards (NTP adjustments)?
- **Counter overflow at millisecond boundary**: The counter could theoretically overflow if >4 billion ULIDs generated in one millisecond
- **Multi-process collision**: Process ID is only 16 bits, collision possible with >65K processes

**Required Tests**:
```rust
// Critical: System clock goes backwards
test_ulid_backwards_clock() {
    - Generate ULID at time T
    - Set system clock to T-1 hour  
    - Generate another ULID
    - Verify second ULID > first (monotonicity preserved)
}

// Critical: Counter approaching u32::MAX
test_ulid_counter_near_overflow() {
    - Artificially set counter to u32::MAX - 10
    - Generate 20 ULIDs rapidly
    - Verify graceful handling at overflow boundary
}
```

### 2. Worker Queue Race Conditions (CRITICAL)

**Current Gap**: Missing tests for:
- **Double claim prevention**: The SELECT FOR UPDATE SKIP LOCKED pattern needs adversarial testing
- **Worker crash mid-processing**: Events could get stuck in "processing" state forever
- **Batch claim atomicity**: What if connection drops after claiming but before processing?

**Required Tests**:
```rust
// Critical: Simulate actual PostgreSQL race
test_promotion_queue_serialization() {
    - Insert 1 high-value event
    - Start 50 workers simultaneously (more than connection pool)
    - Use sub-millisecond synchronization
    - Verify exactly 1 successful claim
    - Verify no partial updates in database
}

// Critical: Worker dies holding claimed items  
test_worker_crash_recovery() {
    - Worker claims 100 items
    - Kill -9 the worker process
    - Verify items return to queue after timeout
    - Verify no data corruption
}
```

### 3. Channel Overflow & Backpressure (HIGH)

**Current Gap**: The collector uses bounded channels (10,000) but no tests for:
- **Producer faster than consumer**: What happens when events pile up?
- **Memory growth**: Does the system degrade gracefully or OOM?
- **Event dropping**: Are events lost silently?

**Required Tests**:
```rust
// High: Fast producer, slow consumer
test_channel_backpressure() {
    - Mock slow database (add 100ms delay per insert)
    - Generate 50K events/second from sources
    - Monitor channel queue depth
    - Verify either:
      a) Backpressure applied to sources, OR
      b) Events dropped with metrics, OR  
      c) Memory grows bounded
}
```

### 4. Database Connection Pool Exhaustion (HIGH)

**Current Gap**: No tests for connection pool starvation scenarios

**Required Tests**:
```rust
// High: All connections blocked
test_connection_pool_deadlock() {
    - Start transaction with exclusive table lock
    - Attempt pool_size + 10 concurrent operations
    - Verify operations timeout cleanly
    - Verify no permanent deadlock
    - Check recovery after lock release
}
```

### 5. File Descriptor Leaks (MEDIUM)

**Current Gap**: File watching could leak FDs on hot reload or errors

**Required Tests**:
```rust
// Medium: FD leak on config reload
test_file_watcher_cleanup() {
    - Configure watching 1000 files
    - Trigger 100 config reloads
    - Check FD count stays constant
    - Verify no inotify watch leaks
}
```

### 6. Partial Write Failures (MEDIUM)

**Current Gap**: Batch inserts could leave partial data on failure

**Required Tests**:
```rust
// Medium: Batch insert atomicity
test_partial_batch_failure() {
    - Insert batch of 1000 events
    - Trigger OOM/connection loss at event 500
    - Verify all-or-nothing behavior
    - Check no orphaned records
}
```

## Test Implementation Priority

### Week 1 Sprint (Prevent Data Loss)
1. ULID backwards clock test
2. Promotion queue double-claim test  
3. Worker crash recovery test
4. Channel backpressure test

### Week 2 Sprint (Prevent System Failure)
1. Connection pool exhaustion test
2. FD leak detection test
3. Partial batch failure test
4. Config reload memory leak test

## Key Testing Principles

1. **Test actual failure modes**, not hypothetical ones
2. **Use real timing/concurrency**, not mocked
3. **Measure resource usage**, don't assume
4. **Verify data integrity**, not just "no crash"
5. **Test recovery paths**, not just happy paths

## Missing Test Infrastructure

To properly test these scenarios, we need:

1. **Fault injection hooks** in production code
2. **Resource monitoring** in test harness  
3. **Precise timing control** for race conditions
4. **Database introspection** for integrity checks

## Definition of Done

A test is complete when:
- It reliably reproduces the failure scenario
- It verifies both failure handling AND recovery
- It measures impact (data loss, resource usage)
- It runs in < 10 seconds (or is marked as slow)
- It includes clear documentation of what it protects against