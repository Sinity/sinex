# Sinex ULID Implementation

Time-ordered, globally unique identifiers for the Sinex system.

This module provides ULID (Universally Unique Lexicographically Sortable Identifier) support with
PostgreSQL integration via the `pgx_ulid` extension.

## Architectural Decision: ULID Primary Keys (ADR-001)

**Status**: Implemented  
**Decision Date**: 2024-03-11  
**Implementation Date**: 2025-07-17

### Context

Sinex requires a robust primary key strategy for high-volume, time-ordered data. The strategy must
address:

1. **Index Efficiency** – minimise B-tree bloat and fragmentation.
2. **Time-Ordering** – keys should be naturally sortable by time.
3. **Global Uniqueness** – support distributed generation.
4. **Performance** – efficient generation and comparison.
5. **Developer Experience** – good ecosystem support.

### Decision

Use ULIDs via the `pgx_ulid` PostgreSQL extension for all primary keys.

### Rationale

1. **Best of Both Worlds** – time-ordering benefits with native PostgreSQL support.
2. **Performance** – 30 % faster generation than UUIDs in benchmarks.
3. **Rich Features** – timestamp casting, monotonic generation.
4. **Binary Storage** – efficient 16-byte storage (same as UUID).
5. **Ecosystem Alignment** – `pgx_ulid` is written in Rust and fits the stack.

### Alternatives Considered

| Option         | Pros                                     | Cons                                  | Decision |
|----------------|------------------------------------------|---------------------------------------|----------|
| UUIDv4         | Standard, widely supported               | Random = poor index locality          | ❌        |
| UUIDv7         | Time-ordered, standard                   | Less mature ecosystem                 | ❌        |
| Custom ULID    | No dependencies                          | Complex implementation                | ❌        |
| `pgx_ulid`     | All ULID benefits + native PG            | External dependency                   | ✅ **Chosen** |

### Consequences

**Positive**:

- Sequential inserts improve index performance.
- Natural time-based partitioning.
- Extract timestamp from the ID.
- Sortable across distributed systems.

**Negative**:

- Requires `pgx_ulid` extension installation.
- 26-character string representation (vs 36 for UUID).

## ULID Structure

```text
 01AN4Z07BY      79KA1307SR9X4MV3
|----------|    |----------------|
   Timestamp          Randomness
     48bits             80bits
```

## Usage Examples

```rust
use sinex_schema::ulid::Ulid;

// Generate new ULID
let id = Ulid::new();
println!("Generated: {}", id);

// Extract timestamp
let timestamp = id.timestamp();
println!("Created at: {}", timestamp);
```

### PostgreSQL Integration

```sql
CREATE EXTENSION pgx_ulid;

CREATE TABLE events (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    data JSONB
);
```

```rust
# use sinex_schema::ulid::Ulid;
# use sqlx::PgPool;
let id = Ulid::new();

sqlx::query!(
    "INSERT INTO events (id, data) VALUES ($1, $2)",
    id.as_uuid(),  // Convert to UUID for parameter binding
    serde_json::json!({ "event": "test" })
)
.execute(&pool)
.await?;
# Ok::<_, Box<dyn std::error::Error>>(())
```

## Monotonic Generation

This implementation includes monotonic generation to handle high-frequency ID generation within the
same millisecond:

```rust
# use sinex_schema::ulid::Ulid;
let id1 = Ulid::new();
let id2 = Ulid::new();
let id3 = Ulid::new();

assert!(id1 < id2);
assert!(id2 < id3);
```

## SQLx Integration & Casting Helpers

Rust code interacts with PostgreSQL through `sqlx`, which transmits ULIDs as UUID bytes. Keep the
following rules in mind:

1. **Parameter Binding** – pass `Ulid::as_uuid()` (or `Ulid::from(uuid)`) when binding parameters; let
   Postgres convert via `pgx_ulid`.
2. **Single Values** – cast inside SQL with `ulid_from_uuid($1)` when the query expects a `ULID`.
3. **Arrays** – convert arrays using `ARRAY(SELECT ulid_from_uuid(elem) FROM unnest($1::uuid[]) AS elem)`
   to satisfy compile-time checking.
4. **Batch Inserts** – SQLx cannot bind 2-D arrays; insert events inside a transaction loop rather
   than attempting a single `UNNEST`.

PostgreSQL helper functions:

```sql
CREATE OR REPLACE FUNCTION ulid_from_uuid(uuid) RETURNS ULID
    LANGUAGE sql IMMUTABLE STRICT
    AS 'SELECT $1::ULID';

CREATE OR REPLACE FUNCTION ulid_to_uuid(ULID) RETURNS uuid
    LANGUAGE sql IMMUTABLE STRICT
    AS 'SELECT $1::uuid';
```

Example insert with casting:

```rust
sqlx::query!(
    r#"
    INSERT INTO core.events (id, source_event_ids)
    VALUES (
        ulid_from_uuid($1),
        CASE
            WHEN $2::uuid[] IS NULL THEN NULL
            ELSE ARRAY(SELECT ulid_from_uuid(elem) FROM unnest($2::uuid[]) AS elem)
        END
    )
    "#,
    event_id.as_uuid(),
    source_event_ids.map(|ids| ids.iter().map(Ulid::as_uuid).collect::<Vec<_>>())
)
.execute(&pool)
.await?;
```

When adding new repositories or queries, prefer `sqlx::query!` macros to keep compile-time checking
intact and ensure the relevant casts are applied.
