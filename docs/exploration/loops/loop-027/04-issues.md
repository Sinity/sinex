Concrete issues to handle
- SelfObserver rate limiting is global per observer; frequent emissions can suppress other telemetry types. Consider per-metric or per-event-type rate limits to avoid starving critical metrics (`crate/lib/sinex-node-sdk/src/self_observation.rs:132-178`).
- Ingestd telemetry omits `nats_errors` and uses placeholder values for avg latency and queue depth (None/0). Add instrumentation or remove fields to avoid misleading metrics (`crate/core/sinex-ingestd/src/service.rs:185-214`).
- Gateway rate_limit_exceeded emits zeros for limits/counts; this makes downstream aggregates misleading. Propagate actual limiter values (`crate/core/sinex-gateway/src/gateway_metrics.rs:120-140`).
- SelfObservationTask helper is unused; if intended, wire it into ingestd/gateway or remove to reduce dead surface area (`crate/lib/sinex-node-sdk/src/self_observation.rs:480-520`).
- Heartbeat observability depends on journald ingestion; if that ingestor is disabled, there is no fallback telemetry path. Consider optional SelfObserver heartbeat emission or a warning when journald ingestion is disabled (`crate/lib/sinex-node-sdk/src/heartbeat.rs:318-380`).
