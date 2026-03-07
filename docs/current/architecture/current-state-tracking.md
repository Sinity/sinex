# Current State Tracking

## Overview

Sinex uses **TimescaleDB continuous aggregates** and **PostgreSQL materialized views** to track current state efficiently, addressing the architectural gap where event sourcing tracks "what happened" but not "what is."

## Architecture Decision

**Rationale**: After evaluating streaming databases (Materialize, RisingWave), we determined that TimescaleDB continuous aggregates provide sufficient freshness and overlap significantly with streaming database capabilities, while avoiding operational overhead.

See: `docs/current/analysis/streaming-database-evaluation.md`

## State Tracking Approaches

| Approach | Use Case | Freshness | Example |
|----------|----------|-----------|---------|
| **Continuous Aggregates** | Time-series current state | 5-10 min | "What window is focused right now?" |
| **Materialized Views** | Entity-level current state | Manual refresh | "What's the current state of entity X?" |
| **Synthesis Events** | Business-meaningful derivations | Real-time | "User became idle" |

## Continuous Aggregates (Time-Series State)

### `current_window_focus`

Tracks workspace/window activity in 5-minute buckets.

**Query**: Get current focused window

```sql
SELECT
    workspace,
    window_class,
    window_title,
    last_focus_time
FROM sinex_telemetry.current_window_focus
WHERE bucket >= NOW() - INTERVAL '30 minutes'
ORDER BY bucket DESC
LIMIT 1;
```

**Refresh**: Every 5 minutes, 3-hour lag window

**Schema**:

- `bucket` - 5-minute time bucket
- `workspace` - Workspace name
- `window_class` - Window class (application)
- `window_title` - Window title
- `last_focus_time` - Timestamp of last focus event
- `focus_event_count` - Number of focus events in bucket

### `command_frequency_hourly`

Tracks shell command execution patterns.

**Query**: Get most frequently executed commands in last 24 hours

```sql
SELECT
    command,
    shell,
    SUM(total_executions) AS total_executions,
    AVG(avg_duration_ms) AS avg_duration_ms,
    SUM(failed_executions) AS total_failures
FROM sinex_telemetry.command_frequency_hourly
WHERE bucket >= NOW() - INTERVAL '24 hours'
GROUP BY command, shell
ORDER BY total_executions DESC
LIMIT 20;
```

**Refresh**: Every 10 minutes, 3-hour lag window

**Schema**:

- `bucket` - 1-hour time bucket
- `command` - Command executed
- `shell` - Shell name (bash, zsh, fish)
- `total_executions` - Total command executions
- `successful_executions` - Executions with exit code 0
- `failed_executions` - Executions with non-zero exit code
- `avg_duration_ms` - Average command duration

### `file_activity_summary`

Tracks filesystem activity patterns by directory.

**Query**: Get most active directories in last 24 hours

```sql
SELECT
    directory,
    event_type,
    SUM(total_events) AS total_events,
    SUM(unique_files) AS unique_files
FROM sinex_telemetry.file_activity_summary
WHERE bucket >= NOW() - INTERVAL '24 hours'
GROUP BY directory, event_type
ORDER BY total_events DESC
LIMIT 20;
```

**Refresh**: Every 10 minutes, 3-hour lag window

**Schema**:

- `bucket` - 1-hour time bucket
- `directory` - Directory path
- `event_type` - Event type (file.created, file.modified, file.deleted)
- `total_events` - Total filesystem events
- `unique_files` - Number of unique files affected

### `current_system_state`

Tracks system resource usage trends.

**Query**: Get current system load

```sql
SELECT
    avg_cpu_percent,
    max_cpu_percent,
    avg_memory_percent,
    max_memory_percent,
    current_active_units
FROM sinex_telemetry.current_system_state
WHERE bucket >= NOW() - INTERVAL '1 hour'
ORDER BY bucket DESC
LIMIT 1;
```

**Refresh**: Every 5 minutes, 3-hour lag window

**Schema**:

- `bucket` - 5-minute time bucket
- `avg_cpu_percent` - Average CPU usage
- `max_cpu_percent` - Peak CPU usage
- `avg_memory_percent` - Average memory usage
- `max_memory_percent` - Peak memory usage
- `avg_disk_percent` - Average disk usage
- `current_active_units` - Number of active systemd units
- `sample_count` - Number of samples in bucket

## Materialized Views (Entity-Level State)

### `current_entity_state`

Tracks last known state for each entity in the knowledge graph.

**Query**: Get current state of entity

```sql
SELECT
    entity_id,
    entity_type,
    entity_name,
    metadata,
    updated_at
FROM entities.current_entity_state
WHERE entity_id = 'some-entity-id';
```

**Refresh**: Manual via `REFRESH MATERIALIZED VIEW entities.current_entity_state;`

**Schema**:

- `entity_id` - Entity UUIDv7 (unique index)
- `entity_type` - Entity type (person, project, document, etc.)
- `entity_name` - Entity name
- `metadata` - JSON metadata
- `created_at` - Entity creation timestamp
- `updated_at` - Last update timestamp

**Indexes**:

- `ix_current_entity_state_entity_id` (unique)

### `current_device_state`

Tracks current state of systemd units and devices.

**Query**: Get active systemd units

```sql
SELECT
    unit_name,
    unit_type,
    state,
    sub_state,
    last_update
FROM sinex_telemetry.current_device_state
WHERE state = 'active'
ORDER BY unit_name;
```

**Refresh**: Manual via `REFRESH MATERIALIZED VIEW sinex_telemetry.current_device_state;`

**Schema**:

- `unit_name` - Unit name
- `unit_type` - Unit type (service, timer, socket, etc.)
- `state` - Current state (active, inactive, failed)
- `sub_state` - Sub-state (running, exited, etc.)
- `last_update` - Last state change timestamp

**Indexes**:

- `ix_current_device_state_unit_name`
- `ix_current_device_state_state`

## Convenience Views

### `recent_activity_summary`

Combines multiple current state sources for dashboard use.

**Query**: Get recent activity snapshot

```sql
SELECT
    activity_type,
    context,
    detail,
    timestamp
FROM sinex_telemetry.recent_activity_summary
ORDER BY timestamp DESC;
```

**Returns**:

- Latest window focus
- Current CPU load
- Top 5 recent commands

## Refresh Strategies

### Continuous Aggregates

Continuous aggregates are automatically refreshed by TimescaleDB according to their refresh policies.

**Default policies**:

- 5-minute buckets: Refresh every 5 minutes
- 1-hour buckets: Refresh every 10 minutes
- Lag window: 3 hours (allows late-arriving events)

**Manual refresh** (if needed):

```sql
CALL refresh_continuous_aggregate('sinex_telemetry.current_window_focus', NULL, NULL);
```

### Materialized Views

Materialized views require manual refresh or periodic refresh via cron/scheduler.

**Manual refresh**:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY entities.current_entity_state;
REFRESH MATERIALIZED VIEW CONCURRENTLY sinex_telemetry.current_device_state;
```

**Periodic refresh** (recommended):

```bash
# Add to crontab or systemd timer
*/10 * * * * psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY entities.current_entity_state;"
*/5 * * * * psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY sinex_telemetry.current_device_state;"
```

## Performance Considerations

### Query Performance

Continuous aggregates pre-compute common queries, providing:

- **Sub-second latency** for current state queries
- **No full table scans** on core.events
- **Retention via explicit lifecycle operations** (no automatic TimescaleDB retention policy)

**Example query plan**:

```sql
EXPLAIN ANALYZE
SELECT * FROM sinex_telemetry.current_window_focus
WHERE bucket >= NOW() - INTERVAL '1 hour';
```

### Storage Overhead

Continuous aggregates use additional storage:

- **5-minute buckets**: ~1-2% of raw events storage
- **1-hour buckets**: ~0.1-0.5% of raw events storage
- **Total overhead**: Typically <5% of total database size

### Refresh Load

Continuous aggregates refresh incrementally:

- **Refresh window**: Only processes new data since last refresh
- **Background refresh**: Does not block queries or writes
- **Concurrency**: Multiple aggregates can refresh in parallel

## Synthesis Events vs Continuous Aggregates

| Aspect | Synthesis Events | Continuous Aggregates |
|--------|------------------|----------------------|
| **Purpose** | Business-meaningful derivations | Query optimization |
| **Examples** | "User became idle", "Project milestone reached" | "Current focused window", "Command frequency" |
| **Provenance** | Full event lineage via parent_event_ids | No provenance tracking |
| **Audit trail** | Immutable events in core.events | Refreshable materialized state |
| **Use case** | Downstream processing, analytics | Current state queries |

**When to use synthesis events**:

- Derivation has business meaning (e.g., "session started")
- Need audit trail of when derivation occurred
- Downstream nodes will process this event
- State change should be observable as an event

**When to use continuous aggregates**:

- Pure query optimization (e.g., "sum of events in last hour")
- No business meaning beyond current state
- Only used for queries, not processing
- Refreshable without loss of semantics

**Example**: User session tracking

- **Synthesis event**: `session.started` (when user becomes active)
- **Continuous aggregate**: `active_session_count` (count of active sessions)

Both can coexist: synthesis events provide business semantics, continuous aggregates optimize queries over those events.

## Schema Apply and Maintenance

### Applying Declarative Schema

```bash
# Apply declarative schema (includes continuous aggregates)
xtask db apply

# Verify continuous aggregates exist
psql -c "SELECT view_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'sinex_telemetry';"

# Verify refresh policies
psql -c "SELECT view_name, schedule_interval, start_offset, end_offset FROM timescaledb_information.continuous_aggregate_policies WHERE view_schema = 'sinex_telemetry';"
```

### Manual Refresh

If you need to force a refresh (e.g., after backfilling events):

```sql
-- Refresh all continuous aggregates
CALL refresh_continuous_aggregate('sinex_telemetry.current_window_focus', NULL, NULL);
CALL refresh_continuous_aggregate('sinex_telemetry.command_frequency_hourly', NULL, NULL);
CALL refresh_continuous_aggregate('sinex_telemetry.file_activity_summary', NULL, NULL);
CALL refresh_continuous_aggregate('sinex_telemetry.current_system_state', NULL, NULL);

-- Refresh all materialized views
REFRESH MATERIALIZED VIEW CONCURRENTLY entities.current_entity_state;
REFRESH MATERIALIZED VIEW CONCURRENTLY sinex_telemetry.current_device_state;
```

### Monitoring

Check continuous aggregate health:

```sql
-- View last refresh time
SELECT
    view_name,
    materialized_only,
    compression_enabled,
    materialization_hypertable_schema,
    materialization_hypertable_name
FROM timescaledb_information.continuous_aggregates
WHERE view_schema = 'sinex_telemetry';

-- Check refresh policy status
SELECT
    application_name,
    state,
    wait_event_type,
    wait_event,
    query
FROM pg_stat_activity
WHERE query LIKE '%refresh_continuous_aggregate%';
```

## Scale Triggers

The current PostgreSQL + TimescaleDB approach is sufficient until one of the
following thresholds is sustainably exceeded:

1. **Event ingestion rate > 50K/sec** sustained — continuous aggregate refresh
   will lag behind and backpressure will propagate into the ingest pipeline.
2. **Sub-second freshness required** — NATS-driven automata + periodic continuous
   aggregate refresh cannot achieve < 1s lag; a streaming database (RisingWave)
   would be required.
3. **Complex cross-hypertable joins** — multiple hypertables joined in a single
   materialized view are not well-supported by TimescaleDB continuous aggregates.
4. **Multi-hypertable real-time materialized views** — would require a dedicated
   incremental view maintenance layer.

When any of these triggers fires, evaluate RisingWave (see
[Streaming Database Evaluation](../analysis/streaming-database-evaluation.md))
before committing to the architecture change.

## See Also

- [Streaming Database Evaluation](../analysis/streaming-database-evaluation.md) - Architecture decision rationale
- [TimescaleDB Continuous Aggregates Documentation](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [PostgreSQL Materialized Views Documentation](https://www.postgresql.org/docs/current/rules-materializedviews.html)
