# Analytics Service

`AnalyticsService` exposes the read-only rollups used by the CLI and gateway.
All methods execute against `sinex-core` repositories and return lightweight
structures for direct serialization.

## API Surface

| Method | Description |
|--------|-------------|
| `get_event_count_by_source(start?, end?)` | Counts events grouped by `source`. Applies optional time-window filtering (start handled in SQL, end filtered client-side). |
| `get_event_count_by_type(start?, end?)` | Counts events grouped by `event_type`. Falls back to all-time counts when no range is provided. |
| `get_events_over_time(start, end, interval_minutes)` | Buckets events into fixed intervals using `get_events_over_time` repository helpers. |
| `get_top_commands(start?, end?, limit)` | Returns the most frequent terminal commands for the requested window. |
| `activity_heatmap(bucket_size_minutes, limit)` | Produces high-level activity buckets (e.g., for heatmaps). |

All functions return plain maps or `(timestamp, count)` tuples suitable for
JSON-RPC responses.

## Error Handling

Errors are surfaced as `SinexError::service(...)` with the originating
repository operation annotated in the context.

See `docs/architecture/SystemOperations_And_Integrity_Architecture.md` for the
dashboards that consume these rollups.
