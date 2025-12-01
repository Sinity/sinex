# sinex-health-aggregator

The health aggregator collects signals from satellites and services to produce
health summaries. It exposes derived events that power operational dashboards.

- Pulls activity from analytics, ingestion, and system satellites.
- Aggregates key metrics (latency, queue depth, failure counts).
- Emits `HealthReport` payloads consumed by gateways and operators.

See `docs/architecture/SystemOperations_And_Integrity_Architecture.md` for the
health model.
