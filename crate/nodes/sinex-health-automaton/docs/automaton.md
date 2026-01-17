# Health Aggregator

`lib.rs` implements the `StatefulStreamProcessor` that turns raw telemetry
into aggregated health events. The binary entrypoint uses `processor_main!`
for the standardized CLI/lifecycle wiring (`src/main.rs`).

- Consolidates event streams from multiple sources.
- Applies rolling window computations for SLA checks.
- Publishes alerts when thresholds are breached.
