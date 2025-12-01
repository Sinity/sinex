# Health Automaton

`automaton.rs` implements the `StatefulStreamProcessor` that turns raw telemetry
into aggregated health events.

- Consolidates event streams from multiple sources.
- Applies rolling window computations for SLA checks.
- Publishes alerts when thresholds are breached.
