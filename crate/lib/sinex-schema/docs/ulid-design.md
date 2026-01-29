# ULID Implementation Design

## Overview

ULIDs (Universally Unique Lexicographically Sortable Identifiers) are the primary key type used throughout Sinex. They provide time-ordering properties and excellent index efficiency.

## Why ULID over UUID?

- **Time-ordering**: Natural chronological sort without additional columns
- **Index efficiency**: Sequential inserts minimize B-tree fragmentation
- **Timestamp extraction**: Can derive creation time from ID
- **Global uniqueness**: Safe for distributed systems

## PostgreSQL Integration

### Extension Setup

Requires the `pgx_ulid` extension:

```sql
CREATE EXTENSION pgx_ulid;
```

### Table Usage

Use the native ULID type in tables:

```sql
CREATE TABLE my_table (
    id ULID PRIMARY KEY DEFAULT gen_ulid()
);
```

### ULID-UUID Casting for Foreign Keys

ULIDs seamlessly cast to UUIDs for foreign key relationships:

```rust
// Cast ULID to UUID when querying
let events = sqlx::query!(
    r#"
    SELECT
        id::uuid as "event_id!",
        source,
        event_type
    FROM core.events
    WHERE id = $1::uuid
    "#,
    event_id.to_uuid()  // ULID provides to_uuid() method
)
.fetch_all(pool)
.await?;
```

Database schema supports ULID-UUID relationships:

```sql
-- Foreign key constraints handle ULID-UUID casting
ALTER TABLE core.event_relations
    ADD CONSTRAINT fk_event_relations_from_event
    FOREIGN KEY (from_event_id)
    REFERENCES core.events(id::uuid);
```

## Clock Regression Handling

### Decision

**We handle clock regression by not caring about it.**

Instead, we:

1. Use standard monotonic ULID generation
2. Rely on the operating system to maintain reasonable time
3. Recommend (but not require) chrony for time synchronization
4. Accept that minor clock regressions may occasionally cause out-of-order ULIDs

### Rationale

1. **Complexity vs Benefit**: Elaborate solutions add significant complexity for a rare edge case
2. **Performance Impact**: Complex monotonic generators require synchronization that slows ULID generation
3. **OS Responsibility**: Timekeeping is the operating system's job, not the application's
4. **Real-world Impact**: With modern NTP clients (chrony), significant clock regression is extremely rare
5. **Failure Mode**: If time goes backwards, having slightly out-of-order events is preferable to refusing to operate

### Consequences

**Positive:**

- Simple, fast ULID generation with no synchronization overhead
- No complex time validation logic to maintain
- System continues operating even during time anomalies
- Clear separation of concerns (OS handles time, app handles events)

**Negative:**

- Events may occasionally have out-of-order ULIDs during clock regression
- No application-level detection of time anomalies
- Relies on proper OS configuration for time accuracy

**Mitigations:**

- Document that Sinex requires a properly synchronized system clock
- Recommend chrony with `makestep 1 3` configuration
- The `ts_ingest` derived from ULID provides a consistent timestamp even if system time is wrong
- Database indexes on both `id` and `ts_ingest` allow efficient querying by either order

## Implementation Details

### Monotonic Generation

ULIDs generated within the same millisecond have their random component incremented to ensure strict ordering. This prevents ordering violations during high-frequency generation.

### Format

- **Timestamp**: 48 bits (milliseconds since Unix epoch)
- **Random**: 80 bits
- **Total**: 128 bits (same as UUID)
- **Encoding**: Crockford's Base32 (26 characters)

### Performance

ULID generation is extremely fast:

- Zero-copy conversion to/from UUID
- Simple bit manipulation for timestamp extraction
- Minimal synchronization overhead (only during same-millisecond generation)
