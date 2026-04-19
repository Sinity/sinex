# Current State Tracking

## Overview

Sinex uses a mix of **TimescaleDB continuous aggregates**, **event-time SQL views**, and
**PostgreSQL materialized views** to track current state efficiently, addressing the
architectural gap where event sourcing tracks "what happened" but not "what is."

## Architecture Decision

**Rationale**: After evaluating streaming databases (Materialize, RisingWave), we determined that TimescaleDB continuous aggregates provide sufficient freshness and overlap significantly with streaming database capabilities, while avoiding operational overhead.

**Compressed conclusion**:
- keep PostgreSQL + TimescaleDB as the active current-state substrate,
- use continuous aggregates for ingest-time operational telemetry,
- use live event-time views for user-facing activity timelines,
- use materialized views for entity-level current-state projections,
- reserve synthesis events for business-meaningful derived facts, not generic state mirrors,
- revisit dedicated streaming databases only if freshness or scale requirements exceed what this stack can realistically satisfy.

## State Tracking Approaches

| Approach | Use Case | Freshness | Example |
|----------|----------|-----------|---------|
| **Event-Time Views** | User-facing activity timelines | Immediate | "What window was focused then?" |
| **Continuous Aggregates** | Ingest-time operational telemetry | 5-10 min | "How is ingestd behaving?" |
| **Materialized Views** | Entity-level current state | Manual refresh | "What's the current state of entity X?" |
| **Synthesis Events** | Business-meaningful derivations | Real-time | "User became idle" |

## Event-Time Activity Views

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

**Freshness**: Live view, keyed by `ts_orig`

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

**Freshness**: Live view, keyed by `ts_orig`

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

**Freshness**: Live view, keyed by `ts_orig`

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

**Freshness**: Live view, keyed by `ts_orig`

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

### Event-Time Views

The activity read models (`current_window_focus`, `command_frequency_hourly`,
`file_activity_summary`, `current_system_state`, and `recent_activity_summary`) are ordinary
views over `core.events`. They stay fresh immediately and respect `ts_orig`, which makes
historical imports visible without waiting for a refresh policy.

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

The event-time activity views avoid import-time drift and "empty until first refresh" behavior.
The operator-facing hourly views now aggregate directly from indexed `core.events` rows using
`ts_coided`, which keeps the surfaces live immediately after inserts and avoids refresh-policy lag.

- **Fresh rows immediately visible** after ingest without waiting for background policies
- **No extra storage tier** for operator telemetry rollups
- **Query-time aggregation cost** instead of precomputed materialization

**Example query plan**:

```sql
EXPLAIN ANALYZE
SELECT * FROM sinex_telemetry.current_window_focus
WHERE bucket >= NOW() - INTERVAL '1 hour';
```

### Storage Overhead

The operator hourly views add no extra storage because they are ordinary views over `core.events`.
Only `sinex_telemetry.current_device_state` remains materialized and consumes additional storage.

### Refresh Load

Only `sinex_telemetry.current_device_state` needs an explicit refresh path. The operator hourly
views always reflect the current event store state because they do not materialize.

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
# Schema apply happens through preflight. If you need to force a reapply:
xtask reset --yes --schema
xtask check

# Verify the current materialized/read-model split
psql -c "SELECT matviewname FROM pg_matviews WHERE schemaname = 'sinex_telemetry';"
psql -c "SELECT table_name FROM information_schema.views WHERE table_schema = 'sinex_telemetry' ORDER BY table_name;"
```

### Manual Refresh

If you need to force a refresh of the remaining materialized view:

```sql
-- Refresh materialized views
REFRESH MATERIALIZED VIEW CONCURRENTLY entities.current_entity_state;
REFRESH MATERIALIZED VIEW CONCURRENTLY sinex_telemetry.current_device_state;
```

### Monitoring

Check the telemetry read-model surface:

```sql
-- Operator telemetry `_1h` relations are ordinary views
SELECT
    table_name
FROM information_schema.views
WHERE table_schema = 'sinex_telemetry'
  AND table_name LIKE '%_1h'
ORDER BY table_name;

-- `current_device_state` is still materialized
SELECT
    matviewname
FROM pg_matviews
WHERE schemaname = 'sinex_telemetry';
```

## Scale Triggers

The current PostgreSQL + TimescaleDB approach is sufficient until one of the
following thresholds is sustainably exceeded:

1. **Event ingestion rate > 50K/sec** sustained — hourly view scans over raw events
   will become too expensive for dashboards without dedicated persisted rollups.
2. **Sub-second freshness required with bounded query cost** — current views are fresh,
   but a streaming database or dedicated telemetry table would be required to precompute
   that surface cheaply.
3. **Complex cross-hypertable joins** — multiple hypertables joined in a single
   materialized view are not well-supported by TimescaleDB continuous aggregates.
4. **Multi-hypertable real-time materialized views** — would require a dedicated
   incremental view maintenance layer.

When any of these triggers fires, re-open the streaming-database question using the decision
summary above before committing to the architecture change.

## See Also

- [TimescaleDB Continuous Aggregates Documentation](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [PostgreSQL Materialized Views Documentation](https://www.postgresql.org/docs/current/rules-materializedviews.html)
