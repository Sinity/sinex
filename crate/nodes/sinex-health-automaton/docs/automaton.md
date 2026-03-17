# Health Aggregator

`lib.rs` implements `ScopeReconcilerNode` that turns raw telemetry
into aggregated health events. The binary entrypoint uses `node_entrypoint!`
for the standardized CLI/lifecycle wiring (`src/main.rs`). It uses `ScopeReconcilerNodeAdapter`
to manage per-source health state and emit reconciliation events.

- Consolidates event streams from multiple sources (scopes).
- Tracks and reconciles health state per scope.
- Publishes health.status events when thresholds are breached.
