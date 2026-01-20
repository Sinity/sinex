# Loop 014 - Rate Limiter State vs Telemetry Payload Completeness

Goal
- Determine whether gateway rate limiting code exposes enough data to populate `RateLimitExceededPayload` fields accurately.
- Identify missing hooks or state needed to emit `requests_in_window`, `limit`, and `method`.

Process
1) Inspect `TokenRateLimiter` and its configuration to see what state is tracked per token.
2) Trace how `rpc_server` enforces rate limits and what data it has at the rejection site.
3) Compare available data to `RateLimitExceededPayload` fields.
4) Note any missing data paths or API gaps that prevent accurate emission.

Deliverables
- Mapping of rate limit enforcement site to available context.
- Findings on missing data fields.
- Concrete issues and potential improvements.
