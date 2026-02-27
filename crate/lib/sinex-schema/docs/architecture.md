# Sinex Schema Architecture

This document explains the key architectural decisions and design patterns used in the `sinex-schema` crate.

## Overview

The `sinex-schema` crate serves as the single source of truth for the Sinex database schema. It provides:

- **Schema definitions** using `sea-query` for type-safe SQL generation
- **ULID support** with PostgreSQL integration via UUID conversion
- **Migration management** with a squashed base migration and incremental updates
- **Record structs** for database result deserialization
- **Validation constraints** embedded directly in schema definitions

## Core Design Principles

### 1. Single Source of Truth

All table definitions, constraints, indexes, and relationships are defined programmatically in Rust. This eliminates drift between code and database schema by ensuring they are generated from the same source.

```rust
// Schema definition drives both migration SQL and query types
impl Events {
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .col(ColumnDef::new(Events::Id).custom(Alias::new("ULID")).primary_key())
            // ... rest of schema
    }
}
```

### 2. Time-Ordered Primary Keys via ULID

**Decision**: Use ULIDs instead of UUID v4 for all primary keys.

**Rationale**:
- **Sequential inserts**: Improve B-tree index performance by avoiding random UUID fragmentation
- **Timestamp extraction**: Can derive creation time from ID without additional columns
- **Global uniqueness**: Safe for distributed generation across nodes
- **Lexicographic ordering**: Natural sort order matches temporal order

```rust
// ULID provides both uniqueness AND temporal ordering
let event_id = Ulid::new();
let timestamp = event_id.timestamp(); // Extract creation time
```

### 3. PostgreSQL Integration Strategy

**Challenge**: Rust ULID types vs PostgreSQL UUID storage

**Solution**: Transparent conversion layer with zero-copy operations

```rust
// Application uses ULIDs
let ulid = Ulid::new();

// Database stores as UUID
sqlx::query!("INSERT INTO events (id, ...) VALUES ($1, ...)", ulid.as_uuid())

// Conversion is zero-copy (same 16 bytes)
```

### 4. Constraint-Driven Design

Critical business rules are encoded as database constraints, not just application logic:

```sql
-- The Provenance XOR Invariant
CHECK (
    (source_material_id IS NOT NULL AND source_event_ids IS NULL) OR 
    (source_material_id IS NULL AND source_event_ids IS NOT NULL)
)
```

This ensures data integrity even if application bugs exist.

## Key Components

### ULID Implementation (`src/ulid.rs`)

The ULID implementation includes several sophisticated features:

#### Monotonic Generation
```rust
// Handles high-frequency generation within same millisecond
let ulid1 = Ulid::new();
let ulid2 = Ulid::new(); // Generated microseconds later
assert!(ulid1 < ulid2);  // Still maintains order
```

#### Clock Regression Handling
Rather than complex time validation, the implementation accepts that:
- Minor clock regressions are rare with modern NTP (chrony)
- Slight ordering violations are preferable to application crashes
- The derived `ts_ingest` timestamp provides consistent ordering

#### PostgreSQL Type Integration
```rust
// Seamless integration with SQLx
impl Type<Postgres> for Ulid {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("ulid")
    }
}
```

### Schema Definitions (`src/schema/`)

Each module defines one logical domain:

- **events.rs**: Core event log (heart of the system)
- **blobs.rs**: Content-addressed storage metadata
- **source_materials.rs**: File system source tracking
- **operations.rs**: System operations and node coordination

#### Design Pattern: Enum + TableDef + Record

```rust
// 1. Enum for column references
#[derive(Iden, Copy, Clone)]
pub enum Events {
    Table,
    Id,
    Source,
    // ...
}

// 2. TableDef trait for metadata
impl TableDef for Events {
    fn table_name() -> &'static str { "events" }
    fn schema_name() -> &'static str { "core" }
    fn primary_key() -> &'static str { "id" }
}

// 3. Record struct for query results
#[derive(Debug, FromRow)]
pub struct EventRecord {
    pub id: Ulid,
    pub source: String,
    // ...
}
```

### Migration Strategy (`src/migrations/`)

**Approach**: Single squashed base migration + incremental updates

**Benefits**:
- New deployments get optimal schema immediately
- No complex migration chains for fresh installs
- Historical changes preserved in git history
- Future changes are incremental ALTER statements

```rust
// m20241028_000001_create_canonical_schema.rs - Creates everything
// m20250816_122538_add_associated_blob_ids.rs - Adds one column
```

### Conversion Utilities (`src/ulid_conversions.rs`)

Provides ergonomic conversion between ULID and database UUID types:

```rust
// Direct functions
let db_uuid = ulid_to_uuid(ulid);
let restored = uuid_to_ulid(db_uuid);

// Extension traits
let db_uuid = ulid.to_db();
let db_uuids = ulids.to_uuid_vec();

// Optional handling
let maybe_uuid = opt_to_db(maybe_ulid);
```

## Advanced Features

### TimescaleDB Integration

The events table is converted to a TimescaleDB hypertable for time-series performance:

```sql
SELECT create_hypertable(
    'core.events', 
    by_range('id', partition_func => 'public.ulid_to_timestamptz'::regproc)
);
```

This enables:
- Automatic time-based partitioning
- Optimized time-range queries
- Efficient data retention policies

### Vector Search Support

Via pgvector extension for embedding similarity search:

```sql
CREATE EXTENSION vector;
-- Embedding columns use vector(1536) for OpenAI embeddings
```

### Content-Addressed Storage

The blobs table implements content-addressed storage patterns:

```rust
// Decomposed git-annex key for efficient querying
pub struct BlobRecord {
    pub annex_backend: String,    // "SHA256E"
    pub content_hash: String,     // The actual hash
    pub size_bytes: i64,          // File size
    pub checksum_blake3: Option<String>, // Fast dedup hash
}
```

### JSON Schema Validation

Integration with `pg_jsonschema` for payload validation:

```sql
-- Event payloads validated against registered schemas
ALTER TABLE core.events ADD CONSTRAINT valid_payload 
CHECK (validate_json_schema(payload_schema_id::text, payload));
```

## Testing Strategy

### Multi-Level Testing

1. **Unit Tests**: Individual function behavior
2. **Property Tests**: Concurrent generation, edge cases
3. **Integration Tests**: Actual database operations
4. **Schema Tests**: Constraint validation, index performance

### Test Database Isolation

Using `sinex-test-utils` for parallel test execution:

```rust
let ctx = TestContext::new().await;  // Fresh database
let pool = ctx.db().pool();          // Isolated connection pool
```

### Property-Based Testing

Critical for ULID generation under stress:

```rust
sinex_proptest! {
    fn concurrent_ulids_are_unique_and_ordered(
        num_threads in 2usize..=8,
        ulids_per_thread in 10usize..=100
    ) {
        // Test concurrent generation maintains invariants
    }
}
```

## Performance Characteristics

### ULID Generation

- **Efficient generation** with thread-safe implementation
- **Monotonic guarantee** with minimal synchronization overhead
- **Zero-copy conversion** to/from UUID

### Database Operations

- **Sequential inserts** optimize B-tree performance
- **Time-range queries** leverage natural ULID ordering
- **Index efficiency** from clustered primary key

### Memory Usage

- **16 bytes per ULID** (same as UUID)
- **Zero allocation** conversion operations
- **Efficient collections** via specialized traits

## Security Considerations

### ID Predictability

ULIDs include timestamps, making them slightly more predictable than random UUIDs. However:

- 80 bits of randomness still provide strong uniqueness guarantees
- Timestamp leakage is generally acceptable for Sinex's use case
- Benefits of time-ordering outweigh minor predictability increase

### Database Constraints

Security-critical invariants are enforced at the database level:

```sql
-- Prevent privilege escalation via constraint validation
CHECK (user_role IN ('admin', 'user', 'readonly'))

-- Prevent data corruption via referential integrity
FOREIGN KEY (source_event_ids) REFERENCES core.events(id)
```

## Future Evolution

### Schema Versioning

The current approach supports evolution through:

1. **New migrations** for schema changes
2. **Feature flags** for optional functionality
3. **Record struct versioning** for API compatibility

### Extension Points

The architecture supports future enhancements:

- **Custom ULID generators** for specific use cases
- **Additional database backends** through trait abstractions
- **Schema validation** extensions via pluggable validators

## Conclusion

The sinex-schema crate provides a robust foundation for Sinex's data layer through:

- **Type-safe schema definitions** that prevent SQL drift
- **Time-ordered ULIDs** that optimize both performance and developer experience  
- **Comprehensive testing** that validates behavior under stress
- **Clear architectural patterns** that support long-term maintenance

This design enables Sinex to handle high-volume, time-ordered data efficiently while maintaining strong consistency guarantees and developer productivity.
