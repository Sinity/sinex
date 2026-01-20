# Loop 014 - Rate Limiter State vs Telemetry Payload Completeness

Scope
- Per-token rate limiting in `sinex-gateway`.
- Telemetry fields for `rate_limit.exceeded` events.

Rate Limiter Implementation
- `TokenRateLimiter::check` returns `Result<(), ()>` and uses `governor::RateLimiter::check()`.
- The rate limiter stores only:
  - `RateLimitConfig` (requests/sec, burst, idle timeout, enabled).
  - `DashMap<String, TokenEntry>` with `RateLimiter` + `last_access`.
- No method for retrieving per-token counters, window counts, or remaining budget.

Telemetry Emission Site
- In `handle_rpc` (`crate/core/sinex-gateway/src/rpc_server.rs`), rate limiting occurs before request validation.
- The handler has:
  - `token` from headers.
  - `request.method` (string) available even before validation.
- It passes only `token_prefix` into `GatewayMetrics::record_rate_limited`.
- `record_rate_limited` emits `RateLimitExceededPayload` with `requests_in_window = 0`, `limit = 0`, `method = None`.

Field Coverage vs Available Context
- `token_prefix`: available in `handle_rpc`.
- `method`: available as `request.method` before validation.
- `limit`: partially available via `RateLimitConfig` (requests_per_second, burst_size), but the metrics emitter does not have access to the config.
- `requests_in_window`: not available; `governor`’s `check()` does not expose per-token window count.

Findings
- The telemetry payload is currently under-specified because the rate limiter exposes only allow/deny.
- The gateway has access to request.method at rejection, but `record_rate_limited` does not accept it.
- Rate limit configuration is not exposed by `TokenRateLimiter`, so the telemetry layer cannot populate `limit`.

Risks
- Rate limit telemetry events are mostly placeholders, reducing their audit/debug value.
- Operators may misinterpret zeroed fields as actual measurements.

Opportunities
- Extend `TokenRateLimiter` to expose configuration (limit/burst) for telemetry.
- Include `method` in `record_rate_limited` and populate `RateLimitExceededPayload.method`.
- If precise request counts are required, track per-token counters in the rate limiter or wrap `governor` with explicit accounting.
