# Loop 012 - Concrete Issues

1) `current_health` collapses multiple components into one row.
- The view groups by `source` only and `source = 'sinex'` for health events, so only the most recent health status across all components is returned.
- File: `crate/lib/sinex-schema/src/migrations/m20250117_000011_add_self_observation_aggregates.rs`.

2) Self-observation event types are missing from `schemas/v1`.
- No JSON schema files exist for metrics payloads (`metric.counter`, `request.stats`, `rate_limit.exceeded`, etc.), so schema validation does not apply to internal telemetry.
- Directories: `schemas/v1/` (no matching entries).

3) `gateway_stats_1h` includes event types with no aggregate columns.
- The view filters `rate_limit.exceeded` and `replay.stats` but only aggregates fields present on `request.stats`, so those event types do not contribute to the metrics.
- File: `crate/lib/sinex-schema/src/migrations/m20250117_000011_add_self_observation_aggregates.rs`.
