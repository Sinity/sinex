# Loop 014 - Concrete Issues

1) Rate-limit telemetry lacks data because the limiter exposes only allow/deny.
- `TokenRateLimiter::check` returns `Result<(), ()>` with no request count or remaining budget, so `RateLimitExceededPayload.requests_in_window` cannot be populated.
- File: `crate/core/sinex-gateway/src/rate_limit.rs`.

2) Rate-limit telemetry omits available method context.
- `handle_rpc` has `request.method` at the rate-limit check, but `record_rate_limited` does not accept a method argument, so `RateLimitExceededPayload.method` is always `None`.
- Files: `crate/core/sinex-gateway/src/rpc_server.rs`, `crate/core/sinex-gateway/src/gateway_metrics.rs`.

3) Rate-limit telemetry omits limit configuration.
- `RateLimitConfig` values exist but are not exposed to the metrics emitter, so `RateLimitExceededPayload.limit` remains `0`.
- Files: `crate/core/sinex-gateway/src/rate_limit.rs`, `crate/core/sinex-gateway/src/gateway_metrics.rs`.
