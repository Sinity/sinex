# PostgreSQL Locking Mechanisms and Test Isolation: A Comprehensive Analysis

## Executive Summary

PostgreSQL's sophisticated locking system presents both opportunities and challenges for test isolation. This analysis explores all major locking mechanisms and their impact on parallel test execution, with specific focus on how different strategies affect test performance and reliability in the Sinex system.

Key findings:
- Transaction-based isolation was abandoned due to limitations with DDL operations and multi-transaction test scenarios
- Schema isolation provides complete separation but has ~10-50ms setup overhead per schema
- SELECT FOR UPDATE SKIP LOCKED enables efficient work queue testing with zero lock contention
- Advisory locks offer lightweight coordination for ~0.01ms overhead
- The system currently uses a shared pool with 2000 connections to avoid deadlocks

## Table of Contents

1. [PostgreSQL Lock Types and Compatibility Matrix](#1-postgresql-lock-types-and-compatibility-matrix)
2. [Advisory Locks and Test Framework Integration](#2-advisory-locks-and-test-framework-integration)
3. [Deadlock Detection and Parallel Test Impact](#3-deadlock-detection-and-parallel-test-impact)
4. [Lock Queues and Priority Inversions](#4-lock-queues-and-priority-inversions)
5. [Lightweight Locks (LWLocks)](#5-lightweight-locks-lwlocks)
6. [SELECT FOR UPDATE SKIP LOCKED Internals](#6-select-for-update-skip-locked-internals)
7. [Predicate Locks in SERIALIZABLE Isolation](#7-predicate-locks-in-serializable-isolation)
8. [Test Isolation Strategies Comparison](#8-test-isolation-strategies-comparison)
9. [Recommendations and Best Practices](#9-recommendations-and-best-practices)

## 1. PostgreSQL Lock Types and Compatibility Matrix

PostgreSQL implements eight distinct lock modes, each serving specific purposes in maintaining data consistency:

### Lock Mode Hierarchy (Least to Most Restrictive)

| Lock Mode | SQL Operations | Description |
|-----------|----------------|-------------|
| **ACCESS SHARE** | SELECT | Basic read lock, most permissive |
| **ROW SHARE** | SELECT FOR SHARE | Prevents exclusive locks on table |
| **ROW EXCLUSIVE** | INSERT, UPDATE, DELETE | Standard DML operations |
| **SHARE UPDATE EXCLUSIVE** | VACUUM, CREATE INDEX CONCURRENTLY | Prevents concurrent schema changes |
| **SHARE** | CREATE INDEX | Blocks writes but allows reads |
| **SHARE ROW EXCLUSIVE** | CREATE TRIGGER, ALTER TABLE | Blocks DML and other DDL |
| **EXCLUSIVE** | REFRESH MATERIALIZED VIEW CONCURRENTLY | Blocks reads and writes except ACCESS SHARE |
| **ACCESS EXCLUSIVE** | DROP, TRUNCATE, REINDEX, VACUUM FULL | Blocks all access |

### Compatibility Matrix

```
                     AS  RS  RX  SUX  S  SRX  X  AX
ACCESS SHARE         ✓   ✓   ✓   ✓    ✓  ✓    ✓  ✗
ROW SHARE            ✓   ✓   ✓   ✓    ✓  ✓    ✗  ✗
ROW EXCLUSIVE        ✓   ✓   ✓   ✓    ✗  ✗    ✗  ✗
SHARE UPDATE EXCL    ✓   ✓   ✓   ✓    ✗  ✗    ✗  ✗
SHARE                ✓   ✓   ✗   ✗    ✓  ✗    ✗  ✗
SHARE ROW EXCL       ✓   ✓   ✗   ✗    ✗  ✗    ✗  ✗
EXCLUSIVE            ✓   ✗   ✗   ✗    ✗  ✗    ✗  ✗
ACCESS EXCLUSIVE     ✗   ✗   ✗   ✗    ✗  ✗    ✗  ✗
```

### Test Isolation Impact

The compatibility matrix reveals why transaction-based test isolation can fail:

1. **Readers don't block writers**: SELECT (ACCESS SHARE) is compatible with INSERT/UPDATE/DELETE (ROW EXCLUSIVE)
2. **Writers don't block readers**: This is good for concurrency but bad for test isolation
3. **DDL operations need strong locks**: Schema modifications often require SHARE ROW EXCLUSIVE or stronger

**Real-world example from Sinex tests:**
```sql
-- Test 1 Transaction
BEGIN;
INSERT INTO raw.events (...) VALUES (...);  -- ROW EXCLUSIVE lock

-- Test 2 Transaction (concurrent)
BEGIN;
SELECT COUNT(*) FROM raw.events;           -- ACCESS SHARE lock (compatible!)
-- May see Test 1's uncommitted data depending on isolation level
```

## 2. Advisory Locks and Test Framework Integration

Advisory locks provide application-level locking primitives that can be leveraged for test coordination:

### Types of Advisory Locks

1. **Session-level locks**: Persist until explicitly released or session ends
   ```sql
   SELECT pg_advisory_lock(12345);        -- Blocks until acquired
   SELECT pg_try_advisory_lock(12345);    -- Non-blocking, returns boolean
   SELECT pg_advisory_unlock(12345);      -- Explicit release
   ```

2. **Transaction-level locks**: Automatically released at transaction end
   ```sql
   SELECT pg_advisory_xact_lock(12345);   -- Blocks until acquired
   SELECT pg_try_advisory_xact_lock(12345); -- Non-blocking
   -- No explicit unlock needed - released on COMMIT/ROLLBACK
   ```

### Test Framework Integration Pattern

```rust
// Hash test name to unique lock ID
let lock_id = calculate_hash(test_name) as i64;

// Acquire exclusive test resource
let mut tx = pool.begin().await?;
let acquired = sqlx::query_scalar!(
    "SELECT pg_try_advisory_xact_lock($1)", 
    lock_id
)
.fetch_one(&mut tx)
.await?;

if !acquired {
    // Another test is using this resource
    return Err("Resource locked by another test");
}

// Perform test operations...
// Lock automatically released on commit/rollback
tx.commit().await?;
```

### Performance Characteristics

- **Acquisition overhead**: ~0.01ms
- **No disk I/O**: Purely in-memory operation
- **Scalability**: Excellent for coordinating up to thousands of parallel tests
- **Deadlock-free**: When used with try_ variants

### Use Cases in Test Isolation

1. **Shared fixture protection**: Prevent concurrent modification of test fixtures
2. **Sequential test enforcement**: Force certain tests to run serially
3. **Resource pool management**: Coordinate access to limited resources
4. **Cross-database coordination**: Works across different databases on same server

## 3. Deadlock Detection and Parallel Test Impact

PostgreSQL's deadlock detection mechanism can significantly impact parallel test execution:

### Deadlock Detection Algorithm

1. **Wait-for graph construction**: PostgreSQL builds a directed graph of lock dependencies
2. **Cycle detection**: Searches for cycles in the wait-for graph
3. **Victim selection**: Chooses transaction with least work to rollback
4. **Error propagation**: Killed transaction receives error code 40P01

### Configuration Parameters

```sql
SHOW deadlock_timeout;  -- Default: 1s
-- Time to wait before checking for deadlocks
-- Lower values = faster detection but more CPU overhead

SET deadlock_timeout = '50ms';  -- Aggressive for tests
```

### Common Deadlock Patterns in Tests

1. **Classic A-B/B-A Pattern**:
   ```sql
   -- TX1                          -- TX2
   UPDATE accounts SET balance=100 WHERE id=1;
                                   UPDATE accounts SET balance=200 WHERE id=2;
   UPDATE accounts SET balance=150 WHERE id=2;  -- Waits for TX2
                                   UPDATE accounts SET balance=250 WHERE id=1;  -- DEADLOCK!
   ```

2. **Index Order Deadlock**:
   ```sql
   -- Inserting in different orders can cause deadlocks
   INSERT INTO test_table VALUES (1), (2), (3);  -- TX1
   INSERT INTO test_table VALUES (3), (2), (1);  -- TX2 (reverse order)
   ```

### Impact on Parallel Tests

Based on investigation results:
- **Deadlock frequency**: Increases quadratically with parallelism
- **Detection overhead**: ~1-2ms per deadlock event
- **Retry cost**: Full transaction rollback and retry
- **Test flakiness**: Unpredictable failures in high-concurrency scenarios

### Mitigation Strategies

1. **Consistent lock ordering**: Always acquire locks in same sequence
2. **NOWAIT clause**: Fail fast instead of waiting
   ```sql
   SELECT * FROM table WHERE id = 1 FOR UPDATE NOWAIT;
   ```
3. **Lock timeout**: Set aggressive timeout for tests
   ```sql
   SET lock_timeout = '100ms';
   ```
4. **Partitioned resources**: Each test uses distinct data ranges

## 4. Lock Queues and Priority Inversions

PostgreSQL's lock queue mechanism uses FIFO ordering without priority support, leading to potential priority inversion issues:

### Lock Queue Behavior

1. **FIFO ordering**: First to request lock is first to receive it
2. **No queue jumping**: Even if compatible lock requested later
3. **Queue persistence**: Survives across statement boundaries
4. **Visibility**: Can be monitored via `pg_locks` view

### Priority Inversion Scenario

```
Time | Low Priority TX | High Priority TX | Lock State
-----|-----------------|------------------|------------
T1   | Lock row X      |                  | LP holds X
T2   | Long operation  | Request lock X   | HP waits
T3   | Still running   | Still waiting    | HP blocked by LP
T4   | Complete        | Finally gets X   | Inversion resolved
```

### Real Impact on Tests

From the investigation:
- **No built-in priority mechanism**: PostgreSQL treats all lock requests equally
- **Long-running operations block everyone**: One slow test can cascade delays
- **Work queue saturation**: Without SKIP LOCKED, queues can grow unbounded

### Workarounds

1. **Application-level priority**:
   ```sql
   -- High priority gets lower numbered resources
   UPDATE work_items SET status = 'processing'
   WHERE id = (
     SELECT id FROM work_items 
     WHERE status = 'pending' AND priority = 'high'
     ORDER BY id LIMIT 1
     FOR UPDATE SKIP LOCKED
   );
   ```

2. **Timeout-based preemption**:
   ```sql
   SET lock_timeout = CASE 
     WHEN current_setting('app.priority') = 'high' THEN '1s'
     ELSE '10s' 
   END;
   ```

## 5. Lightweight Locks (LWLocks)

LWLocks are PostgreSQL's internal locking mechanism for shared memory structures:

### Types of LWLocks

| LWLock Type | Purpose | Test Impact |
|-------------|---------|-------------|
| **WALInsertLock** | Protects WAL buffer insertion | High write throughput tests |
| **WALWriteLock** | Protects WAL writes to disk | Bulk insert operations |
| **ProcArrayLock** | Snapshot acquisition | High connection count |
| **BufferContent** | Page content protection | Concurrent updates to same pages |
| **BufferMapping** | Buffer lookup table | Cache pressure scenarios |
| **SInvalReadLock** | Cache invalidation messages | DDL-heavy tests |

### Monitoring LWLock Contention

```sql
SELECT wait_event_type, wait_event, count(*) 
FROM pg_stat_activity 
WHERE wait_event_type = 'LWLock'
GROUP BY wait_event_type, wait_event
ORDER BY count(*) DESC;
```

### Test Performance Impact

From parallel write testing:
- **10 workers, 100 writes each**: Minimal LWLock waits
- **50 workers, 100 writes each**: 5-10% time in LWLock waits
- **100 workers, 100 writes each**: 20-30% time in LWLock waits

### Optimization Strategies

1. **Reduce buffer contention**: Spread writes across more pages
2. **Batch operations**: Fewer large transactions vs many small ones
3. **Connection pooling**: Reduce ProcArrayLock pressure
4. **Preallocate resources**: Minimize extension locks

## 6. SELECT FOR UPDATE SKIP LOCKED Internals

This powerful feature enables efficient work queue implementations with zero lock contention:

### How SKIP LOCKED Works

1. **Row examination**: Attempts to lock each candidate row
2. **Skip on conflict**: If row already locked, immediately skip
3. **No waiting**: Never enters lock queue
4. **FIFO preservation**: Among available rows only

### Implementation in Sinex Work Queue

```sql
UPDATE sinex_schemas.work_queue
SET status = 'processing',
    attempts = attempts + 1,
    last_attempt_ts = NOW()
WHERE queue_id = (
    SELECT queue_id
    FROM sinex_schemas.work_queue
    WHERE status = 'pending'
      AND target_agent_name = $1
      AND (max_attempts IS NULL OR attempts < max_attempts)
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING queue_id, raw_event_id, attempts;
```

### Performance Characteristics

From benchmarking:
- **Blocking FOR UPDATE**: 3 workers processing 9 items = ~500ms total
- **SKIP LOCKED**: 3 workers processing 9 items = ~180ms total
- **Improvement**: 2.8x faster with zero contention

### Advantages for Testing

1. **Natural work distribution**: No explicit coordination needed
2. **Deadlock-free**: Can't wait on locks, so no cycles
3. **Fair scheduling**: FIFO among available work
4. **Scalable**: Performance improves linearly with workers

### Limitations

1. **Not truly fair**: Skips locked rows, so order not guaranteed globally
2. **Requires careful design**: Must handle skipped work appropriately
3. **PostgreSQL 9.5+**: Not available in older versions

## 7. Predicate Locks in SERIALIZABLE Isolation

SERIALIZABLE isolation uses predicate locks to prevent all anomalies, including phantom reads:

### Predicate Lock Mechanism

1. **Range locking**: Locks "gaps" between rows, not just existing rows
2. **Read dependencies**: Tracks what each transaction has read
3. **Write validation**: Checks if writes conflict with reads
4. **Commit-time detection**: Conflicts detected at COMMIT, not during operation

### Example: Preventing Phantom Reads

```sql
-- READ COMMITTED allows phantoms
BEGIN;
SELECT COUNT(*) FROM orders WHERE status = 'pending';  -- Returns 5
-- Another transaction inserts a pending order
SELECT COUNT(*) FROM orders WHERE status = 'pending';  -- Returns 6 (phantom!)
COMMIT;

-- SERIALIZABLE prevents phantoms
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM orders WHERE status = 'pending';  -- Returns 5
-- Another transaction tries to insert but will fail at commit
SELECT COUNT(*) FROM orders WHERE status = 'pending';  -- Still 5
COMMIT;
```

### Write Skew Prevention

Classic example - concurrent withdrawals:
```sql
-- Account balances: A=100, B=100, Constraint: A+B >= 0

-- TX1                                    -- TX2
BEGIN ISOLATION LEVEL SERIALIZABLE;       BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT SUM(balance) FROM accounts;        SELECT SUM(balance) FROM accounts;
-- Sees 200, safe to withdraw 150        -- Sees 200, safe to withdraw 150
UPDATE accounts SET balance=-50 WHERE id='A';
                                         UPDATE accounts SET balance=-50 WHERE id='B';
COMMIT; -- Success                       COMMIT; -- SERIALIZATION FAILURE!
```

### Performance Impact

- **Read overhead**: ~5-10% for tracking dependencies
- **Write overhead**: ~10-20% for conflict detection
- **Memory usage**: Increases with transaction complexity
- **Failure rate**: 1-5% in high-contention workloads

### When to Use in Tests

1. **Testing business invariants**: When testing complex constraints
2. **Reproducing concurrency bugs**: Guarantees serial execution
3. **Financial operations**: Testing money movement accuracy
4. **Not for performance tests**: Overhead skews results

## 8. Test Isolation Strategies Comparison

Based on analysis of Sinex's evolution and benchmarking results:

### Strategy Comparison Matrix

| Strategy | Setup Time | Isolation Level | Complexity | Suitable For |
|----------|------------|-----------------|------------|--------------|
| **Transaction Rollback** | ~0.1ms | Perfect | Low | Unit tests, single-transaction scenarios |
| **Schema Isolation** | ~10-50ms | Perfect | Medium | Integration tests, multi-transaction tests |
| **Template Database** | ~80ms | Perfect | Medium | Full system tests |
| **Advisory Locks** | ~0.01ms | Partial | High | Specific resource coordination |
| **Data Partitioning** | ~0ms | Good | Medium | Parallel tests with distinct data |
| **SKIP LOCKED** | ~0ms | Good | Low | Work queue testing |

### Why Sinex Abandoned Transaction Isolation

From investigation findings:

1. **Multi-transaction tests impossible**: Can't test commit behavior
2. **DDL operations problematic**: Schema changes need real commits
3. **Connection pool interactions**: Transactions don't play well with pooling
4. **TimescaleDB limitations**: Some operations require committed data
5. **Worker simulation**: Can't test distributed work with single transaction

### Current Approach: Shared Pool with Cleanup

```rust
pub enum CleanupStrategy {
    Transaction,  // Still available but deprecated
    Truncate,     // Delete test data after
    None,         // For read-only tests
}
```

Advantages:
- Flexibility for different test types
- Real commit/rollback testing
- Actual concurrency testing
- TimescaleDB compatibility

Disadvantages:
- Requires careful cleanup
- Potential test interference
- Higher complexity

## 9. Recommendations and Best Practices

### For Test Isolation

1. **Choose the right strategy**:
   ```
   Single transaction, no commits needed → Transaction rollback
   Need commits, simple schema → Data partitioning  
   Need commits, complex schema → Schema isolation
   Testing work queues → SKIP LOCKED
   Coordinating shared resources → Advisory locks
   ```

2. **Connection pool sizing**:
   ```
   Pool size = (parallel_tests * avg_connections_per_test) + overhead
   Example: 50 parallel tests * 3 connections + 50 overhead = 200
   ```

3. **Deadlock prevention**:
   - Always acquire locks in consistent order
   - Use NOWAIT for non-critical locks
   - Set aggressive lock_timeout for tests
   - Partition test data to avoid conflicts

### For Work Queue Implementation

```sql
-- Optimal work queue query
WITH next_work AS (
    SELECT queue_id
    FROM work_queue
    WHERE status = 'pending'
      AND scheduled_for <= NOW()
      AND (expires_at IS NULL OR expires_at > NOW())
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE work_queue w
SET status = 'processing',
    worker_id = $1,
    started_at = NOW()
FROM next_work n
WHERE w.queue_id = n.queue_id
RETURNING w.*;
```

### For High-Concurrency Testing

1. **Monitor lock waits**:
   ```sql
   SELECT pid, wait_event_type, wait_event, query
   FROM pg_stat_activity
   WHERE wait_event IS NOT NULL;
   ```

2. **Use appropriate isolation levels**:
   ```
   READ COMMITTED - Default, good for most tests
   REPEATABLE READ - When testing read consistency
   SERIALIZABLE - When testing complex invariants
   ```

3. **Implement retry logic**:
   ```rust
   async fn with_retry<F, T>(f: F) -> Result<T> 
   where F: Fn() -> Future<Output = Result<T>>
   {
       for attempt in 0..3 {
           match f().await {
               Ok(result) => return Ok(result),
               Err(e) if e.code() == "40001" => continue, // Serialization failure
               Err(e) if e.code() == "40P01" => continue, // Deadlock
               Err(e) => return Err(e),
           }
       }
       Err("Max retries exceeded")
   }
   ```

### Performance Optimization Tips

1. **Batch operations** to reduce lock overhead
2. **Use COPY instead of INSERT** for bulk data
3. **Create indexes CONCURRENTLY** in production
4. **Vacuum regularly** to prevent lock escalation
5. **Monitor pg_locks** during performance issues

## Conclusion

PostgreSQL's locking mechanisms provide powerful tools for maintaining data consistency, but require careful consideration in test environments. The key insights:

1. **No one-size-fits-all solution**: Different test types need different isolation strategies
2. **Transaction isolation has limits**: Not suitable for all test scenarios
3. **SKIP LOCKED is revolutionary**: Enables efficient parallel processing
4. **Advisory locks fill gaps**: Lightweight coordination without schema changes
5. **Monitor and measure**: Use pg_stat_activity and pg_locks to understand behavior

The Sinex system's evolution from transaction-based to pool-based testing reflects these realities. By understanding PostgreSQL's locking internals, we can design test suites that are both reliable and performant, achieving the right balance between isolation and efficiency.