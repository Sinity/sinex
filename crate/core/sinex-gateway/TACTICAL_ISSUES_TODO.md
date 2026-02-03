# Remaining Tactical Issues - sinex-gateway

This document tracks tactical issues that require more extensive implementation beyond quick fixes.

## Issue 125 (CRITICAL): RPC Dispatcher Completely Unimplemented
**Status**: Out of scope for gateway crate
**Location**: Binary references cli.rs but dispatcher is in runtime crate
**Notes**: This appears to be an architectural issue in another crate. The gateway RPC server is functional and provides all methods via `dispatch_rpc_method`. If there's a separate dispatcher binary needed, it should be implemented in the appropriate crate.

## Issue 127 (HIGH): Replay Control Silently Disabled on NATS Failure
**Status**: Partially addressed
**Implementation**:
- Replay control status is now exposed in the enhanced health endpoint (Issue 146)
- Health endpoint shows degraded state when replay control is disabled
- Further work: Add monitoring alerts based on this health status

## Issue 133 (MEDIUM): No Metrics on Load Shedding
**Status**: Requires metrics framework
**Implementation needed**:
- Add metrics crate (e.g., `prometheus` or `metrics`)
- Create custom tower layer that wraps LoadShedLayer
- Instrument rejection events with counter metric
- Expose via /metrics endpoint (see Issue 147)

## Issue 140 (MEDIUM): No Service-Level Caching
**Status**: Requires architectural design
**Implementation needed**:
- Evaluate caching strategy (in-memory, distributed, TTL policies)
- Add caching layer (e.g., `moka`, `mini-moka`, or Redis client)
- Wrap service methods with cache lookup/store
- Add cache invalidation logic
- Make cache configuration optional via env vars

## Issue 141 (MEDIUM): No Request Tracing
**Status**: Requires tracing framework integration
**Implementation needed**:
- Already using `tracing` crate, but need OpenTelemetry integration
- Add `tracing-opentelemetry` and `opentelemetry` dependencies
- Initialize OpenTelemetry tracer with OTLP exporter
- Propagate trace context through request headers
- Add trace_id to structured logs
- Configuration: OTEL_EXPORTER_OTLP_ENDPOINT env var

## Issue 142 (MEDIUM): No Token Rotation Support
**Status**: **IMPLEMENTED** ✓
**Implementation**: Token file watching with notify crate is already implemented in GatewayAuth::start_file_watcher

## Issue 143 (MEDIUM): No Rate Limiting Per Token
**Status**: Requires rate limiting framework
**Implementation needed**:
- Add rate limiting crate (e.g., `governor`, `tower-governor`)
- Extract token identity from Authorization header
- Create per-token rate limiter map with LRU eviction
- Apply limits in auth middleware before handler dispatch
- Make limits configurable via env vars (requests per second, burst size)

## Issue 145 (MEDIUM): No Replay Control Metrics
**Status**: Requires metrics framework (same as Issue 133)
**Implementation needed**:
- Add prometheus metrics for replay operations:
  - Counter: replay_operations_total{state="planning|approved|executing|completed|failed|cancelled"}
  - Gauge: replay_operations_active
  - Histogram: replay_operation_duration_seconds
- Instrument ReplayStateMachine state transitions
- Expose via /metrics endpoint

## Issue 147 (MEDIUM): No Prometheus Metrics Endpoint
**Status**: Requires metrics framework
**Implementation needed**:
- Add `prometheus` crate dependency
- Create global metrics registry
- Add `/metrics` route to rpc_server router
- Handler returns text/plain with Prometheus format
- Integrate with Issues 133, 145 for specific metrics

## Issue 149 (MEDIUM): No Graceful Degradation on DB Failure
**Status**: Requires retry/circuit breaker framework
**Implementation needed**:
- Add retry logic with exponential backoff for transient failures
- Consider using `tower` retry layer or `backon` crate
- Implement circuit breaker pattern to prevent cascade failures
- Add connection health checks before operations
- Expose degraded state in health endpoint (partially done in Issue 146)
- Make retry configuration tunable via env vars

## Implementation Priority
1. **Metrics Framework** (Issues 133, 145, 147) - foundational for observability
2. **Request Tracing** (Issue 141) - critical for debugging distributed systems
3. **Rate Limiting** (Issue 143) - important for security and stability
4. **Service Caching** (Issue 140) - performance optimization
5. **DB Retry Logic** (Issue 149) - reliability improvement
