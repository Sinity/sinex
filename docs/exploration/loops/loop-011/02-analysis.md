# Loop 011 - Self-Observation Emission Volume vs Rate Limiting

Scope
- Self-observation event emission and rate limiting in `sinex-node-sdk`.
- High-frequency emitters in `sinex-gateway` and `sinex-ingestd`.

Primary Components
- `SelfObserver` and `SelfObservationTask` in `crate/lib/sinex-node-sdk/src/self_observation.rs`.
- `GatewayMetrics` in `crate/core/sinex-gateway/src/gateway_metrics.rs`.
- Ingest stats emission in `crate/core/sinex-ingestd/src/service.rs`.

Emission Mechanics
- `SelfObserver::publish_event` enforces a single, shared rate limiter via `last_emission` and `min_interval`.
  - Any emission from the component updates `last_emission`.
  - If `last_emission.elapsed()` is below `min_interval`, the method logs a debug line and drops the emission.
  - Default `min_interval` is 1s (configurable via `SINEX_SELF_OBSERVATION_INTERVAL_SECS`).
- This limiter is component-wide, not per metric or per event type.
- Emissions publish to NATS without waiting for an ack (best-effort), logging on failure.

Gateway Emission Paths
- `GatewayMetrics::spawn_emission_task` emits aggregated stats every 10 seconds via `emit_gateway_stats`.
- `GatewayMetrics::record_rate_limited` spawns a task that emits `RateLimitExceededPayload` per rate-limited request.
- Both use the same `SelfObserver` instance, so per-request emissions share the same limiter as the 10-second aggregates.

Ingestd Emission Paths
- `IngestionService::run` spawns a 60-second interval task emitting `emit_node_processing_stats`.
- Ingestd uses a single `SelfObserver` for these emissions.
- No other high-frequency emitters in ingestd were found in this pass.

Findings
- The rate limiter is global per component. A burst of rate-limit events can suppress periodic aggregates if they occur within the same 1-second window.
- `GatewayMetrics::record_rate_limited` calls `emit_rate_limit_exceeded` with `requests_in_window` and `limit` set to 0, and `method` as `None`. The payload lacks real context for alerting or forensics.
- Emissions are best-effort (no ack) and do not provide any internal counter for dropped events due to rate limiting.

Risks
- High-rate bursts can cause missing aggregate telemetry (10s snapshots) due to the shared limiter.
- Rate limit events can be underreported during incident spikes, masking the true rate of throttling.
- The current per-request emitter fills payloads with placeholder values, reducing the usefulness of these events.

Opportunities
- Split the limiter: per-metric or per-event-type rate limiters so aggregates are not suppressed by per-request events.
- Allow `emit_rate_limit_exceeded` to bypass or use a distinct limiter.
- Track a local counter for dropped telemetry events so gaps are visible in aggregate stats.
- Populate `RateLimitExceededPayload` with the actual window count, limit, and method when available.
