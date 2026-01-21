Observability coverage audit

Summary
- Sinex has two parallel observability channels: SelfObserver telemetry (NATS events under `sinex.telemetry.*`) and journald-based heartbeats (structured JSON logs ingested as events).
- Only ingestd and gateway currently emit SelfObserver telemetry; many telemetry APIs and continuous aggregates exist but are unused.
- Several emitted metrics are placeholders or missing (latency/queue depth), and some counters are tracked but never emitted.

Self-observation telemetry (NATS events)
- Implementation: `crate/lib/sinex-node-sdk/src/self_observation.rs` defines SelfObserver, rate limits emissions via a shared `last_emission`/`min_interval`, and publishes to `sinex.telemetry.<component>`.
  - This rate limiter is global across all event types for the observer, so frequent emissions can suppress other telemetry types.
- Aggregates: migration `crate/lib/sinex-schema/src/migrations/m20250117_000011_add_self_observation_aggregates.rs` creates materialized views for stream_stats_1h, assembly_stats_1h, node_stats_1h, gateway_stats_1h.
  - Only gateway stats and node processing stats are emitted today, so other aggregates likely remain empty.

Ingestd
- Logging: `IngestStats::log_stats` reports counts and rates every 60s (`crate/core/sinex-ingestd/src/service.rs:445-489`).
- Telemetry: emits `emit_node_processing_stats` with events_processed, dropped, errors in a background task (`crate/core/sinex-ingestd/src/service.rs:185-214`).
  - Missing fields: avg_latency is `None`, queue_depth is `0` (commented as not tracked), and `nats_errors` is tracked but not emitted.

Gateway
- Telemetry: `GatewayMetrics` emits aggregated stats (`emit_gateway_stats`) every 10s (`crate/core/sinex-gateway/src/gateway_metrics.rs:188-245`).
  - P99 latency is always `None` because no histogram is tracked.
  - Rate limit exceeded events are emitted with placeholder values (`emit_rate_limit_exceeded(&token, 0, 0, None)`), so limits/counts are not reported (`crate/core/sinex-gateway/src/gateway_metrics.rs:120-140`).
- No explicit log/metric mapping for individual RPC failures beyond tracing.

Node SDK heartbeat channel (journald logs)
- HeartbeatEmitter writes structured JSON to stdout and logs a summary via tracing (`crate/lib/sinex-node-sdk/src/heartbeat.rs:318-376`).
  - This relies on the journald ingestor path to turn logs into events; if journald ingestion is disabled, heartbeat events won’t propagate.
  - Heartbeat includes uptime, memory, CPU, errors_count, last_error_message, version/git hash, and metadata.

Other component emissions
- BlobManager emits `storage.statistics` events into the event channel with `try_send` and logs on drop (`crate/lib/sinex-node-sdk/src/annex/blob_manager.rs:644-695`).
- Health aggregator emits component/system health report events via EventSender but does not integrate with SelfObserver metrics (`crate/nodes/sinex-health-automaton/src/lib.rs:170-220`).

Unused or underused telemetry APIs
- No call sites for `emit_stream_stats`, `emit_assembly_stats`, `emit_health_status`, `emit_pool_stats`, or `emit_replay_stats` outside the SelfObserver implementation (see `crate/lib/sinex-node-sdk/src/self_observation.rs`).
- SelfObservationTask is defined but not used anywhere (background emission helper in `crate/lib/sinex-node-sdk/src/self_observation.rs:480-520`).

