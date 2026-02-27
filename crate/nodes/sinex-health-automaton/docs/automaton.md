# Health Aggregator

`lib.rs` implements `AutomatonNode` that turns raw telemetry
into aggregated health events. The binary entrypoint uses `node_entrypoint!`
for the standardized CLI/lifecycle wiring (`src/main.rs`).

- Consolidates event streams from multiple sources.
- Applies rolling window computations for SLA checks.
- Publishes alerts when thresholds are breached.
