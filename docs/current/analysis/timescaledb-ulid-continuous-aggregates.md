# TimescaleDB + ULID + Continuous Aggregates: Deep Analysis

## Current Configuration

### Hypertable Setup

The `core.events` hypertable is currently configured with:

```sql
SELECT create_hypertable(
    'core.events',
    by_range('id', partition_func => 'public.ulid_to_timestamptz'::regproc),
    if_not_exists => TRUE
);
```

**Time Dimension**: `id` (ULID type)
**Partition Function**: `ulid_to_timestamptz()` - extracts timestamp from ULID

### Table Schema

```sql
core.events (
    id          ULID PRIMARY KEY,           -- ULID (sortable by timestamp)
    ts_orig     TIMESTAMPTZ NOT NULL,       -- Original event timestamp
    ts_ingest   TIMESTAMPTZ NOT NULL,       -- Ingestion timestamp
    source      TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB,
    ...
)
```

### Why ULID as Time Dimension?

**Benefits**:
1. **Single column for ID + Time**: ULID embeds timestamp in first 48 bits
2. **Deterministic partitioning**: Same ULID always maps to same chunk
3. **Primary key = partition key**: Natural alignment

**Problems**:
1. **Continuous aggregates broken**: TimescaleDB continuous aggregates require native timestamp type
2. **Cannot use `time_bucket()`**: Function expects timestamp, not ULID
3. **Implicit conversion overhead**: Every query requires `ulid_to_timestamptz()` call

## TimescaleDB Continuous Aggregates Requirements

### What Continuous Aggregates Need

From TimescaleDB documentation:

> Continuous aggregates require a hypertable with a **native timestamp column** as the time dimension.
> User-defined types with partition functions are not supported for continuous aggregates.

**Why?**: Continuous aggregates use `time_bucket()` internally, which operates on:
- `TIMESTAMP`
- `TIMESTAMPTZ`
- `DATE`

**Not supported**:
- User-defined types (like ULID)
- Integer time dimensions
- Custom partition functions

### Current Error

When trying to create continuous aggregates:

```sql
CREATE MATERIALIZED VIEW current_window_focus
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ts_ingest) AS bucket,
    ...
FROM core.events
...
```

**Result**: Migration skipped because hypertable check fails:
```sql
SELECT 1 FROM timescaledb_information.dimensions
WHERE hypertable_schema = 'core'
  AND hypertable_name = 'events'
  AND column_name = 'ts_ingest'
  AND dimension_type = 'Time'
```

Returns 0 rows because time dimension is `id`, not `ts_ingest`.

## Reconfiguration Options

### Option 1: Migrate to ts_ingest Time Dimension (RECOMMENDED)

**Approach**: Recreate hypertable with `ts_ingest` as time dimension, keep `id` as primary key.

**SQL**:
```sql
-- Step 1: Decompress and drop hypertable
SELECT decompress_chunk(c.chunk_schema || '.' || c.chunk_name)
FROM timescaledb_information.chunks c
WHERE hypertable_name = 'events';

-- Step 2: Drop hypertable (keeps table data)
SELECT drop_hypertable('core.events', if_exists => TRUE);

-- Step 3: Recreate with ts_ingest as time dimension
SELECT create_hypertable(
    'core.events',
    by_range('ts_ingest'),
    if_not_exists => TRUE
);

-- Step 4: Reconfigure chunk interval
SELECT set_chunk_time_interval('core.events', INTERVAL '7 days');
```

**Impact**:
- ✅ **Enables continuous aggregates** - `time_bucket(ts_ingest)` works
- ✅ **Keeps ULID primary key** - `id` remains unique identifier
- ✅ **No data loss** - Table and data unchanged
- ⚠️ **Repart it ions data** - All chunks recreated based on `ts_ingest`
- ⚠️ **Downtime required** - Brief interruption during migration

**Queries After Migration**:
```sql
-- Partition by ts_ingest (natural for continuous aggregates)
SELECT time_bucket('1 hour', ts_ingest), COUNT(*)
FROM core.events
WHERE ts_ingest >= NOW() - INTERVAL '24 hours'
GROUP BY 1;

-- Still queryable by id (ULID)
SELECT * FROM core.events WHERE id = 'some-ulid';

-- Range queries by ts_ingest (partition-aligned)
SELECT * FROM core.events
WHERE ts_ingest BETWEEN '2026-01-20' AND '2026-01-21';
```

### Option 2: Add ts_ingest as Secondary Dimension (NOT SUPPORTED)

TimescaleDB 2.x does not support multiple time dimensions on the same hypertable.

### Option 3: Create Separate Continuous Aggregate Hypertable (COMPLEX)

Create a second hypertable for aggregated data:

```sql
CREATE TABLE core.events_aggregates (
    bucket      TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL,
    value       FLOAT,
    metadata    JSONB
);

SELECT create_hypertable('core.events_aggregates', by_range('bucket'));
```

**Problems**:
- Requires custom ETL to populate aggregates table
- Duplicates data and storage
- Loses TimescaleDB automatic refresh
- Not true continuous aggregates

### Option 4: Use PostgreSQL Materialized Views (CURRENT FALLBACK)

Standard PostgreSQL materialized views without continuous refresh:

```sql
CREATE MATERIALIZED VIEW current_window_focus AS
SELECT
    date_trunc('hour', ts_ingest) AS bucket,
    payload->>'workspace' AS workspace,
    ...
FROM core.events
GROUP BY bucket, workspace;

-- Manual refresh required
REFRESH MATERIALIZED VIEW CONCURRENTLY current_window_focus;
```

**Limitations**:
- No automatic refresh
- Full table scan on refresh (not incremental)
- No `time_bucket()` optimization
- Manual management required

## Recommended Migration Path

### Phase 1: Analysis

```sql
-- 1. Check current chunk distribution
SELECT
    chunk_schema || '.' || chunk_name AS chunk,
    range_start,
    range_end,
    num_rows
FROM timescaledb_information.chunks
WHERE hypertable_name = 'events'
ORDER BY range_start;

-- 2. Check compression status
SELECT
    chunk_name,
    compression_status,
    uncompressed_heap_size,
    compressed_heap_size
FROM timescaledb_information.chunks c
JOIN timescaledb_information.compression_settings cs
    ON c.chunk_schema = cs.hypertable_schema
WHERE c.hypertable_name = 'events';

-- 3. Estimate downtime
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename = 'events';
```

### Phase 2: Backup

```bash
# Full backup before migration
pg_dump -Fc -t core.events -f events_backup_$(date +%Y%m%d_%H%M%S).dump sinex_dev

# Or TimescaleDB-specific backup
timescaledb-backup --type full --dest /backup/sinex/
```

### Phase 3: Migration Script

```sql
BEGIN;

-- Disable triggers (if any)
ALTER TABLE core.events DISABLE TRIGGER ALL;

-- Decompress all chunks
DO $$
DECLARE
    chunk record;
BEGIN
    FOR chunk IN
        SELECT chunk_schema || '.' || chunk_name AS chunk_name
        FROM timescaledb_information.chunks
        WHERE hypertable_name = 'events'
          AND is_compressed
    LOOP
        EXECUTE format('SELECT decompress_chunk(%L)', chunk.chunk_name);
    END LOOP;
END $$;

-- Drop hypertable (KEEPS TABLE AND DATA)
SELECT drop_hypertable('core.events', if_exists => TRUE);

-- Recreate with ts_ingest as time dimension
SELECT create_hypertable(
    'core.events',
    by_range('ts_ingest'),
    if_not_exists => TRUE,
    migrate_data => TRUE  -- Important: migrates existing data to chunks
);

-- Reconfigure chunk interval
SELECT set_chunk_time_interval('core.events', INTERVAL '7 days');

-- Re-enable triggers
ALTER TABLE core.events ENABLE TRIGGER ALL;

COMMIT;
```

### Phase 4: Verify

```sql
-- 1. Check hypertable dimensions
SELECT * FROM timescaledb_information.dimensions
WHERE hypertable_name = 'events';
-- Expected: column_name='ts_ingest', dimension_type='Time'

-- 2. Check chunk distribution
SELECT
    chunk_name,
    range_start,
    range_end,
    num_rows
FROM timescaledb_information.chunks
WHERE hypertable_name = 'events'
ORDER BY range_start DESC
LIMIT 10;

-- 3. Test continuous aggregate creation
CREATE MATERIALIZED VIEW test_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts_ingest) AS bucket,
    COUNT(*) AS event_count
FROM core.events
GROUP BY bucket
WITH NO DATA;

-- Should succeed without errors

-- Cleanup test
DROP MATERIALIZED VIEW test_cagg;
```

### Phase 5: Apply Continuous Aggregates

```bash
# Rerun migration 017 (will now succeed)
cargo xtask db migrate
```

## Performance Impact Analysis

### Current (ULID Time Dimension)

**Query**: Get events from last hour
```sql
SELECT * FROM core.events
WHERE ulid_to_timestamptz(id) >= NOW() - INTERVAL '1 hour';
```

**Plan**:
- Sequential scan or index scan on `id`
- Function call `ulid_to_timestamptz()` for every row
- No partition pruning (requires function evaluation)

### After (ts_ingest Time Dimension)

**Query**: Get events from last hour
```sql
SELECT * FROM core.events
WHERE ts_ingest >= NOW() - INTERVAL '1 hour';
```

**Plan**:
- **Partition pruning**: Only scans chunks with ts_ingest in range
- Direct timestamp comparison (no function calls)
- Index scan on `ix_events_ts_ingest`

**Chunk pruning example**:
```
Append
  -> Index Scan on _hyper_1_42_chunk  (chunk for last hour)
  -> Index Scan on _hyper_1_43_chunk  (chunk for current hour)
(2 chunks scanned, 1000+ chunks pruned)
```

### Continuous Aggregate Performance

**Without continuous aggregates** (current):
```sql
-- Full table scan every query
SELECT
    date_trunc('hour', ts_ingest) AS bucket,
    COUNT(*)
FROM core.events
WHERE ts_ingest >= NOW() - INTERVAL '24 hours'
GROUP BY bucket;
```

**With continuous aggregates**:
```sql
-- Pre-computed, incremental refresh
SELECT bucket, event_count
FROM sinex_telemetry.current_system_state
WHERE bucket >= NOW() - INTERVAL '24 hours';
```

**Speedup**: 10-1000x depending on data volume and query complexity.

## Migration Risks

### Low Risk
- ✅ Data loss: None (migration preserves all data)
- ✅ Schema changes: None (table structure unchanged)
- ✅ Rollback: Can recreate ULID hypertable if needed

### Medium Risk
- ⚠️ **Downtime**: Brief (minutes) during migration
- ⚠️ **Chunk recreation**: All chunks recreated (I/O intensive)
- ⚠️ **Query breakage**: Queries using `ulid_to_timestamptz(id)` still work but lose partition pruning

### High Risk (if not careful)
- ❌ **Application outage**: Must coordinate with running services
- ❌ **Index rebuild time**: Large tables may take hours
- ❌ **Disk space**: Temporary 2x storage during migration

## Rollback Plan

If migration fails or causes issues:

```sql
-- Step 1: Drop new hypertable
SELECT drop_hypertable('core.events', if_exists => TRUE);

-- Step 2: Recreate ULID hypertable
SELECT create_hypertable(
    'core.events',
    by_range('id', partition_func => 'public.ulid_to_timestamptz'::regproc),
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Step 3: Reconfigure chunk interval
SELECT set_chunk_time_interval('core.events', INTERVAL '7 days');

-- Step 4: Restore from backup if data issues
pg_restore -d sinex_dev events_backup_YYYYMMDD_HHMMSS.dump
```

## Alternative: Hybrid Approach

If downtime is unacceptable, consider:

1. **Create new table** `core.events_v2` with `ts_ingest` time dimension
2. **Dual-write** to both tables during transition
3. **Backfill** historical data to `events_v2`
4. **Switch reads** to `events_v2` gradually
5. **Drop old table** once stable

**Complexity**: High
**Downtime**: Zero
**Duration**: Weeks

## Recommended Next Steps

1. ✅ **Commit current work** - Done
2. **Create test migration** in development environment
3. **Measure migration time** on production-sized dataset
4. **Test continuous aggregates** creation and refresh
5. **Benchmark query performance** before/after
6. **Schedule maintenance window** for production migration
7. **Execute migration** with rollback plan ready

## References

- [TimescaleDB Continuous Aggregates Documentation](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [TimescaleDB Hypertables Documentation](https://docs.timescale.com/use-timescale/latest/hypertables/)
- [TimescaleDB Migration Guide](https://docs.timescale.com/migrate/latest/)
