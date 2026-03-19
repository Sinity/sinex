# sinex-health-automaton

The health aggregator collects signals from nodes and services to produce
health summaries. It exposes derived events that power operational dashboards.

## Input Events

Consumes `health.status` events emitted by the self-observation infrastructure
when component health status changes (healthy -> degraded -> failed).

These events are produced by:
- `sinex-node-sdk::self_observation::SelfObserver` when health transitions occur
- Any component using `HealthStatusPayload` to report status changes

## Processing

- Tracks component health state over time
- Aggregates key metrics (latency, queue depth, failure counts)
- Maintains recent event history per component

## Output Events

Emits `health.aggregated_report` payloads containing aggregated health data
across all monitored components, consumed by gateways and operators.

See `README.md#deployment--operations` for the operator path and
`crate/lib/sinex-primitives/src/events/payloads/metrics.rs` for the
`HealthStatusPayload` schema.
