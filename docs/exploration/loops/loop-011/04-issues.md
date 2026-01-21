# Loop 011 - Concrete Issues

1) Rate limit payloads are emitted with placeholder values.
- `GatewayMetrics::record_rate_limited` passes `requests_in_window = 0`, `limit = 0`, and `method = None` into `emit_rate_limit_exceeded`.
- File: `crate/core/sinex-gateway/src/gateway_metrics.rs`.

2) Shared rate limiter can suppress aggregate stats emissions.
- `SelfObserver::publish_event` uses a single `last_emission` across all event types per component, so per-request rate-limit emissions can suppress 10s aggregate metrics.
- Files: `crate/lib/sinex-node-sdk/src/self_observation.rs`, `crate/core/sinex-gateway/src/gateway_metrics.rs`.
