# Analytics Service

`AnalyticsService` provides read-only rollups and time-series analysis for dashboards and telemetry. It leverages `TimescaleDB` hypertable capabilities to perform efficient aggregations over high-volume event data.

## API Surface

| Method | Description |
|--------|-------------|
| `get_event_count_by_source` | Counts events grouped by `source` with optional time-window filtering. |
| `get_source_statistics` | Detailed per-source metrics: event/type/host counts and average ingest delay. |
| `get_event_count_by_type` | Counts events grouped by `event_type`. |
| `get_events_over_time` | Buckets events into fixed intervals (e.g., 5m, 1h) using `time_bucket`. |
| `get_top_commands` | Frequency analysis of terminal commands extracted from JSON payloads. |
| `activity_heatmap` | Produces high-level activity density buckets. |
| `list_replay_operations` | Diagnostic listing of replay state machine transitions. |

## Implementation Details

### Resource Protection
To prevent long-running analytical queries from starving the primary ingestion pool, the service uses an aggressive **40ms connection acquisition timeout**. If the database pool is under heavy load, analytics requests fail fast rather than blocking.

### Time Bucketing
The service uses `TimescaleDB`'s `time_bucket` function for all temporal aggregations. When possible, it prefers `ts_orig` (event occurrence) but falls back to `ts_coided` (arrival time) to ensure consistent bucketing.

### Ingest Delay Analysis
The `avg_ingest_delay` metric computes the delta between `ts_orig` and `ts_coided`. This is a critical observability metric used to detect backpressure or processing lag in the node fleet.

## Safety & Performance

- **Limit Clamping**: All statistical queries are clamped to `MAX_LIMIT` (5000) to prevent OOM on the gateway or client.
- **Read-Only Enforced**: All queries are strictly `SELECT` operations; any state-changing operations (like replay management) are delegated to the core state machine.
- **Partial Scans**: Where possible, queries are scoped by `ts_orig` to leverage hypertable partition pruning.