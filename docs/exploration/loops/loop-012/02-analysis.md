# Loop 012 - Self-Observation Schema Coverage vs Aggregates

Scope
- Self-observation payload definitions in `sinex-core`.
- Schema bundle coverage under `schemas/v1`.
- Timescale aggregates in `m20250117_000011_add_self_observation_aggregates`.

Event Types (from `crate/lib/sinex-core/src/types/events/payloads/metrics.rs`)
- `metric.counter` (source `sinex`)
- `metric.gauge` (source `sinex`)
- `metric.histogram` (source `sinex`)
- `stream.stats` (source `sinex.ingestd`)
- `assembly.stats` (source `sinex.ingestd`)
- `request.stats` (source `sinex.gateway`)
- `rate_limit.exceeded` (source `sinex.gateway`)
- `health.status` (source `sinex`)
- `pool.stats` (source `sinex.gateway`)
- `processing.stats` (source `sinex.node`)
- `replay.stats` (source `sinex.gateway`)

Schema Bundle Coverage (`schemas/v1`)
- No JSON schema files were found for the above self-observation event types in `schemas/v1`.
- The only match under `schemas/v1/sinex` is `process.heartbeat.json` (unrelated to self-observation metrics).
- Practical effect: `EventValidator` will return `SchemaValidationOutcome::NoSchema` and allow these events without schema validation.

Aggregate Coverage (Timescale Views)
- `sinex_telemetry.gateway_stats_1h`
  - Filters event types: `request.stats`, `rate_limit.exceeded`, `replay.stats`.
  - Aggregates fields only from `request.stats` payload (`total_requests`, `rate_limited_requests`, `avg_latency_ms`, `p99_latency_ms`).
  - `rate_limit.exceeded` and `replay.stats` events are included in the filter but do not contribute fields.
- `sinex_telemetry.stream_stats_1h` uses `stream.stats` payload fields.
- `sinex_telemetry.assembly_stats_1h` uses `assembly.stats` payload fields.
- `sinex_telemetry.node_stats_1h` uses `processing.stats` payload fields.
- `sinex_telemetry.metric_counters_1h` aggregates `metric.counter` values only.
- `sinex_telemetry.current_health` uses `health.status` payload fields.

Findings
- Schema coverage for self-observation event types is absent from `schemas/v1`, so these payloads bypass schema validation when validation is enabled.
- `gateway_stats_1h` aggregates only `request.stats` payloads, but the filter includes `rate_limit.exceeded` and `replay.stats` without dedicated fields.
- `current_health` view groups by `source` only, which collapses multiple component statuses into a single row (latest event wins).
- Only `metric.counter` has a continuous aggregate; `metric.gauge` and `metric.histogram` are not aggregated (direct queries only).

Risks
- Lack of schema validation for self-observation events can allow malformed telemetry into core.events without detection.
- `current_health` does not preserve per-component status history, risking false “healthy” readings when multiple components report.
- Gateway aggregate metrics may be interpreted as complete, but rate-limit and replay stats are not summarized in the view.

Opportunities
- Add schema entries for self-observation event types or explicitly document that internal telemetry bypasses schema validation.
- Update `current_health` to group by `component` (or source + component) to preserve per-component status.
- Add aggregates for `metric.gauge`/`metric.histogram` or document that they require raw queries.
- Consider adding explicit aggregate columns for `rate_limit.exceeded` and `replay.stats` payload fields.
