# sinex-health-automaton

The health aggregator collects signals from nodes and services to produce
health summaries. It exposes derived events that power operational dashboards.

- Pulls activity from analytics, ingestion, and system nodes.
- Aggregates key metrics (latency, queue depth, failure counts).
- Emits `HealthReport` payloads consumed by gateways and operators.

See `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md` for the
health model.
