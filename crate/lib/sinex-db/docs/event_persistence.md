# Event Persistence Architecture

The event persistence layer in `sinex-core` manages the storage of immutable system events in PostgreSQL. It is optimized for high-throughput ingestion while maintaining referential integrity.

## Insertion Paths

The system provides two distinct paths for event insertion to balance throughput and safety:

### 1. Single Event Path (`insert()`)
Used for low-volume API calls and test helpers.
- **Safety**: Performs full provenance validation and synthesis cycle detection.
- **Atomicity**: Uses `REPEATABLE READ` transactions to ensure consistent views during cycle checks.
- **Consistency**: High. Guaranteed DAG (Directed Acyclic Graph) invariants.

### 2. Stream Batch Path (`insert_stream_batch()`)
Used by `sinex-ingestd` for high-volume JetStream consumption.
- **Optimization**: Uses `ON CONFLICT DO NOTHING` for idempotent deduplication.
- **Performance**: High. Bypasses application-level cycle checks for maximum throughput.
- **Warning**: Batch operations risk introducing circular synthesis dependencies if upstream validation is bypassed.

## Synthesis Cycle Detection

Circular dependencies in event synthesis (where event A depends on B, which depends on A) would break timeline traversals. The system implements a recursive CTE check:

```sql
WITH RECURSIVE parents AS (
    SELECT id, source_event_ids FROM core.events WHERE id = ANY($1)
    UNION
    SELECT e.id, e.source_event_ids FROM core.events e
    JOIN parents p ON e.id = ANY(p.source_event_ids)
)
SELECT EXISTS (SELECT 1 FROM parents WHERE $2 = ANY(source_event_ids))
```

This check is performed in the single-event path but is currently omitted in the high-throughput batch path for performance reasons.

## Transaction Strategy

- **Retries**: Uses `with_retry_transaction_idempotent()` to handle transient serialization failures common in high-concurrency PostgreSQL environments.
- **Isolation**: Defaults to `READ COMMITTED`, promoting to `REPEATABLE READ` only when structural integrity (like cycle detection) must be verified.
- **Batching**: Transactions are managed at the batch level to minimize commit overhead.