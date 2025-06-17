# Comprehensive Test Coverage Analysis and Plan for Sinex

## Current Test Coverage Analysis

### 1. Well-Tested Areas
- **Database Operations**: ULID conversions, basic queries, schema validation
- **Adversarial Testing**: Race conditions, resource exhaustion, security attacks
- **Event Sources**: Basic functionality for filesystem, terminal, window manager
- **Worker Processing**: Concurrent processing, lifecycle management
- **Integration Tests**: Collector configuration, database integration

### 2. Critical Untested Areas

#### A. Core Module Gaps (crate/)
1. **sinex-core**
   - EventRegistry: No tests for the registry implementation
   - EventSourceContext: Missing context propagation tests
   - Error propagation between components
   - Event source lifecycle management

2. **sinex-db**
   - Connection pool exhaustion scenarios
   - Transaction rollback edge cases
   - Query timeout handling
   - Concurrent schema validation conflicts

3. **sinex-collector**
   - Hot-reload race conditions during config update
   - Multi-source coordination failures
   - Channel overflow when sources produce faster than consumption
   - Graceful shutdown with pending events

4. **sinex-worker**
   - DLQ (Dead Letter Queue) promotion logic
   - Backoff calculation edge cases
   - Worker coordination across multiple instances
   - Partial processing failure recovery

5. **sinex-ulid**
   - Clock skew handling
   - System time changes (NTP adjustments)
   - Multi-process ULID collision scenarios
   - Performance under extreme load

#### B. Missing Critical Integration Tests

### 3. High-Risk Areas Requiring Tests

## Testing Plan for Real Issues

### Phase 1: Race Condition Tests (Priority: CRITICAL)

#### 1.1 ULID Generation Race Conditions
```rust
// Test: Concurrent ULID generation across process boundaries
// Risk: Process ID collision, counter overflow
test_multi_process_ulid_generation() {
    - Fork multiple processes
    - Generate ULIDs simultaneously
    - Check for collisions and ordering violations
}

// Test: System clock adjustment during generation
// Risk: ULIDs going backwards in time
test_ulid_clock_skew_handling() {
    - Generate ULID
    - Adjust system time backwards
    - Generate another ULID
    - Verify monotonicity maintained
}
```

#### 1.2 Worker Queue Race Conditions
```rust
// Test: Multiple workers claiming same promotion queue item
// Risk: Duplicate processing, data corruption
test_promotion_queue_double_claim() {
    - Insert high-value event
    - Start 10 workers simultaneously
    - Verify exactly one successful claim
    - Check no partial updates
}

// Test: Worker crash during processing
// Risk: Event stuck in processing state forever
test_worker_crash_recovery() {
    - Worker claims event
    - Kill worker mid-processing
    - Verify event returns to queue after timeout
    - Ensure new worker can reclaim
}
```

#### 1.3 Collector Channel Overflow
```rust
// Test: Event sources producing faster than database can consume
// Risk: Memory exhaustion, event loss
test_backpressure_handling() {
    - Configure slow database (add artificial delay)
    - Generate 100K events/second
    - Monitor memory usage
    - Verify no events lost
    - Check graceful degradation
}
```

### Phase 2: Resource Exhaustion Tests (Priority: HIGH)

#### 2.1 Database Connection Pool Exhaustion
```rust
// Test: All connections blocked by slow queries
// Risk: Complete system deadlock
test_connection_pool_starvation() {
    - Start transaction holding locks
    - Attempt maximum concurrent operations
    - Verify timeout handling
    - Check recovery after lock release
}
```

#### 2.2 File Descriptor Exhaustion
```rust
// Test: Watching too many files/directories
// Risk: System resource limits hit
test_file_watcher_limits() {
    - Create directory with 10K files
    - Add to watch list
    - Monitor file descriptor usage
    - Verify graceful degradation
    - Test recovery after cleanup
}
```

#### 2.3 Memory Leaks in Long-Running Processes
```rust
// Test: Memory growth over extended operation
// Risk: OOM killer, performance degradation
test_24_hour_memory_stability() {
    - Run collector for 24 hours
    - Generate steady event stream
    - Monitor RSS/heap growth
    - Force config reloads
    - Check for unbounded growth
}
```

### Phase 3: Data Corruption Edge Cases (Priority: HIGH)

#### 3.1 Partial Write Failures
```rust
// Test: Database fails mid-transaction
// Risk: Inconsistent state
test_partial_batch_insert_failure() {
    - Insert batch of 1000 events
    - Fail at event 500
    - Verify rollback complete
    - Check no partial data remains
}
```

#### 3.2 JSON Schema Validation Attacks
```rust
// Test: Malicious schemas causing DoS
// Risk: CPU exhaustion, regex bombs
test_malicious_json_schema() {
    - Submit schema with exponential regex
    - Submit deeply nested schema
    - Submit schema with circular references
    - Verify bounded validation time
}
```

### Phase 4: Security Vulnerability Tests (Priority: MEDIUM)

#### 4.1 SQL Injection via ULID
```rust
// Test: Malformed ULIDs attempting injection
// Risk: Database compromise
test_ulid_sql_injection_attempts() {
    - Generate ULIDs with SQL fragments
    - Test all query paths
    - Verify proper parameterization
}
```

#### 4.2 Path Traversal in File Monitoring
```rust
// Test: Escape configured watch directories
// Risk: Unauthorized file access
test_path_traversal_attacks() {
    - Configure watch on /tmp
    - Attempt to access ../../../etc/passwd
    - Verify access denied
    - Check symlink handling
}
```

### Phase 5: Integration Failure Points (Priority: MEDIUM)

#### 5.1 Multi-Component Failure Cascades
```rust
// Test: Database down, then collector crash
// Risk: Data loss, corrupted state
test_cascading_failures() {
    - Stop database
    - Accumulate events in collector
    - Crash collector
    - Restart both
    - Verify event recovery
}
```

#### 5.2 Network Partition During Processing
```rust
// Test: Database connection lost mid-operation
// Risk: Hanging transactions, unclear state
test_network_partition_handling() {
    - Start large batch operation
    - Simulate network failure (iptables)
    - Verify timeout behavior
    - Check cleanup on reconnect
}
```

### Phase 6: Performance Boundary Tests (Priority: LOW)

#### 6.1 Event Size Limits
```rust
// Test: Maximum payload size handling
// Risk: Memory exhaustion, truncation
test_giant_event_payloads() {
    - Generate 100MB JSON payload
    - Verify rejection or handling
    - Test compression boundaries
}
```

#### 6.2 Sustained Maximum Throughput
```rust
// Test: System at 100% capacity for hours
// Risk: Degradation, queue buildup
test_sustained_max_throughput() {
    - Generate events at measured max rate
    - Run for 4 hours
    - Monitor latency percentiles
    - Check for queue growth
}
```

## Implementation Priority

### Week 1: Critical Race Conditions
- ULID generation races
- Worker queue double-claiming
- Channel overflow handling

### Week 2: Resource Exhaustion
- Connection pool starvation
- File descriptor limits
- Memory leak detection

### Week 3: Data Integrity
- Partial write failures
- Schema validation attacks
- Transaction consistency

### Week 4: Security & Integration
- Injection attempts
- Path traversal
- Cascading failures

## Test Infrastructure Requirements

1. **Multi-Process Test Harness**: For testing process-boundary races
2. **Resource Monitor**: Track FDs, memory, connections during tests
3. **Fault Injection Framework**: Simulate network/disk/process failures
4. **Performance Baseline System**: Detect regressions

## Success Metrics

- Zero data loss under any failure scenario
- No resource leaks in 24-hour test runs
- Graceful degradation at resource limits
- Recovery time < 30 seconds for any failure
- No security vulnerabilities in fuzzing

## Missing Test Utilities Needed

1. **Chaos injection library** for random failures
2. **Resource usage tracker** for leak detection
3. **Multi-process test coordinator**
4. **Performance regression detector**