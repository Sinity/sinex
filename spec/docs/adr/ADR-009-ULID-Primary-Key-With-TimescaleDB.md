# ADR-009: ULID Primary Key Strategy with TimescaleDB

**Status**: Proposed  
**Date**: 2025-01-06  
**Decision makers**: Engineering Team  
**Technical lead**: TBD  

## Context

Sinex uses ULID (Universally Unique Lexicographically Sortable Identifier) as the primary key for all entities. ULIDs provide:
- Time-ordered sorting (first 48 bits encode millisecond timestamp)
- Globally unique identifiers without coordination
- K-sortable properties ideal for distributed systems
- Compatibility with UUID storage in PostgreSQL

However, when integrating with TimescaleDB for time-series optimization, we encountered a fundamental constraint: PostgreSQL requires that unique constraints on partitioned tables must include all partitioning columns. This created a conflict with our desire to maintain ULID as the sole primary key.

### The Problem

TimescaleDB typically partitions by a timestamp column (e.g., `ts_ingest`). To maintain referential integrity, it would require:
```sql
PRIMARY KEY (id, ts_ingest)  -- Composite key required!
```

This violates our design principle of using ULID as the single source of truth for identity and time.

## Decision Drivers

1. **Maintain ULID as sole primary key** - Core architectural principle
2. **Performance requirements** - Sub-100ms query latency for analytics
3. **Storage efficiency** - Minimize redundant data
4. **Query simplicity** - Avoid complex joins or timestamp extraction
5. **TimescaleDB features** - Compression, continuous aggregates, retention policies
6. **Operational simplicity** - Easy to understand and maintain

## Considered Options

### Option 1: Accept Composite Key (Baseline)

Accept TimescaleDB's requirement and use composite primary key `(id, ts_ingest)`.

**Pros:**
- Straightforward TimescaleDB setup
- Best query performance for time-based operations
- Full TimescaleDB feature compatibility

**Cons:**
- Violates single primary key principle
- Redundant timestamp storage
- Complex foreign key relationships
- Conceptual model mismatch

### Option 2: Custom Partition Function (ULID-Only)

Use TimescaleDB's `partition_func` parameter to partition by extracting timestamp from ULID:

```sql
CREATE FUNCTION ulid_to_timestamptz(ulid_val ULID) 
RETURNS TIMESTAMPTZ AS $$
    RETURN ulid_val::timestamp;
$$ LANGUAGE plpgsql IMMUTABLE;

SELECT create_hypertable('events',
    by_range('id', 
        partition_func => 'ulid_to_timestamptz',
        partition_interval => INTERVAL '1 day'
    )
);
```

**Pros:**
- Maintains ULID as sole primary key
- No redundant columns
- Clean conceptual model

**Cons:**
- Timestamp extraction overhead in queries
- Cannot use index-only scans for time queries
- Poor performance for aggregations

### Option 3: Generated Column Optimization (Recommended)

Add a GENERATED column for timestamp while keeping ULID as sole primary key:

```sql
CREATE TABLE events (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    ts_computed TIMESTAMPTZ GENERATED ALWAYS AS (id::timestamp) STORED,
    -- other columns
);
```

**Pros:**
- ULID remains sole primary key
- Direct timestamp access (no extraction overhead)
- Enables index-only scans
- Automatic maintenance by PostgreSQL
- Best query performance

**Cons:**
- 8 bytes storage overhead per row
- Additional indexes needed

### Option 4: Native PostgreSQL Partitioning

Use PostgreSQL's native range partitioning on ULID values:

```sql
CREATE TABLE events (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    -- other columns
) PARTITION BY RANGE (id);

-- Create partitions for ULID ranges corresponding to time periods
CREATE TABLE events_2024_01 PARTITION OF events
    FOR VALUES FROM ('01HK3SXKE0000000000000000') 
    TO ('01HMQNB400000000000000000');
```

**Pros:**
- No dependency on TimescaleDB
- ULID as sole primary key
- Native PostgreSQL feature

**Cons:**
- Manual partition management
- No automatic compression
- Missing TimescaleDB features

### Option 5: Materialized Views

Keep simple table structure and use materialized views for analytics:

```sql
CREATE MATERIALIZED VIEW events_analytics AS
SELECT *, (id::timestamp) as ts_computed
FROM events;
```

**Pros:**
- Original table remains simple
- Can optimize view for queries

**Cons:**
- Data duplication
- Refresh lag
- Complex maintenance

## Benchmark Results

We conducted extensive benchmarks comparing the approaches:

### Test Environment
- PostgreSQL 16.9 with TimescaleDB 2.x
- 50,000 test events
- Various query patterns

### Performance Comparison

| Metric | Composite Key | ULID-Only | Generated Column | Plain Table |
|--------|--------------|-----------|------------------|-------------|
| Insert Performance | 917 ms | 837 ms ✅ | 842 ms | 897 ms |
| Storage Size | 56 KB | 48 KB ✅ | 56 KB | 6.7 MB |
| Hourly Aggregation | 26 ms ✅ | 76 ms | 26 ms ✅ | 46 ms |
| Complex Analytics | 10 ms ✅ | 135 ms | 12 ms | 23 ms |
| Range Query | 1.5 ms ✅ | 1.7 ms | 1.5 ms ✅ | 1.5 ms |
| PK Lookup | 22 μs | 44 μs | 44 μs | 14 μs ✅ |

### Adversarial Testing

We tested worst-case scenarios:

1. **Out-of-order inserts**: Random timestamp ULIDs were 5.5x slower than sequential
2. **Aggregation queries**: ULID-only was 3-13x slower due to extraction overhead
3. **No index-only scans**: ULID-only couldn't use covering indexes

### Optimization Results

The Generated Column approach (Option 3) with optimizations:
- Eliminated timestamp extraction overhead
- Enabled index-only scans
- Maintained ULID as sole primary key
- Only 8 bytes overhead per row

## Decision

**We recommend Option 3: Generated Column Optimization**

This approach provides the best balance of:
- Architectural purity (ULID-only primary key)
- Query performance (matches composite key)
- Storage efficiency (minimal overhead)
- Operational simplicity (PostgreSQL manages the column)

## Implementation

### Schema Definition

```sql
-- Create optimized table structure
CREATE TABLE raw.events (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    ts_computed TIMESTAMPTZ GENERATED ALWAYS AS (id::timestamp) STORED,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    ts_orig TIMESTAMPTZ,
    host TEXT NOT NULL,
    ingestor_version TEXT,
    payload_schema_id ULID REFERENCES sinex_schemas.event_payload_schemas(id),
    payload JSONB NOT NULL
);

-- Create hypertable partitioned by ULID
SELECT create_hypertable(
    'raw.events',
    by_range('id', 
        partition_func => 'ulid_to_timestamptz',
        partition_interval => INTERVAL '1 week'
    )
);

-- Create indexes on generated column
CREATE INDEX idx_events_ts_computed ON raw.events (ts_computed);
CREATE INDEX idx_events_source_ts ON raw.events (source, ts_computed);
```

### Migration Strategy

For existing tables:
```sql
-- Add generated column (non-blocking)
ALTER TABLE raw.events 
ADD COLUMN ts_computed TIMESTAMPTZ 
GENERATED ALWAYS AS (id::timestamp) STORED;

-- Create indexes concurrently
CREATE INDEX CONCURRENTLY idx_events_ts_computed 
ON raw.events (ts_computed);
```

### Query Patterns

```sql
-- Time-range queries use generated column
SELECT * FROM events 
WHERE ts_computed >= NOW() - INTERVAL '1 hour';

-- Aggregations are fast
SELECT date_trunc('hour', ts_computed), COUNT(*)
FROM events
GROUP BY 1;

-- Can still query by ULID if needed
SELECT * FROM events
WHERE id >= '01HMQNB400000000000000000'::ulid;
```

## Consequences

### Positive

1. **Maintains architectural integrity** - ULID as sole primary key
2. **Optimal query performance** - Matches composite key approach
3. **Future-proof** - Easy to add continuous aggregates
4. **Developer friendly** - Simple mental model
5. **Index flexibility** - Can create various indexes on ts_computed

### Negative

1. **Storage overhead** - 8 bytes per row (acceptable)
2. **Index maintenance** - Additional indexes to maintain
3. **Migration required** - Existing tables need schema change

### Neutral

1. **PostgreSQL dependency** - GENERATED columns require PG 12+
2. **Visible technical column** - ts_computed exposed in schema

## Mitigation Strategies

1. **For out-of-order inserts**: Use larger chunk intervals (weekly/monthly)
2. **For analytics workloads**: Create continuous aggregates
3. **For storage concerns**: Enable TimescaleDB compression after 30 days

## References

1. [TimescaleDB create_hypertable documentation](https://docs.timescale.com/api/latest/hypertable/create_hypertable/)
2. [PostgreSQL Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
3. [ULID Specification](https://github.com/ulid/spec)
4. Internal benchmarks: `benchmark_ulid_approaches.sql`, `adversarial_benchmark.sql`
5. Related ADRs: ADR-001 (Primary Key Strategy), ADR-005 (Vector Index Type)

## Appendix: Benchmark Queries

### Aggregation Performance Test
```sql
-- Test timestamp extraction overhead
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    date_trunc('hour', ts_computed) as hour,  -- or id::timestamp
    source,
    COUNT(*) as event_count
FROM events
WHERE ts_computed >= NOW() - INTERVAL '24 hours'
GROUP BY 1, 2;
```

### Storage Comparison
```sql
SELECT 
    pg_size_pretty(pg_total_relation_size('events')) as total_size,
    pg_size_pretty(pg_table_size('events')) as table_size,
    pg_size_pretty(pg_indexes_size('events')) as index_size;
```

## Status

This ADR is in PROPOSED status pending:
1. Team review and feedback
2. Performance validation in staging environment
3. Migration plan approval

Once approved, we will update status to ACCEPTED and proceed with implementation.