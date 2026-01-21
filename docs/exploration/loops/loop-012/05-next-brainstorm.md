# Loop 012 - Next Analysis Ideas

- Self-observation consumers: trace where `sinex_telemetry.*` views are queried and validate expectations.
- Schema generation pipeline: confirm why internal telemetry schemas are missing from `schemas/v1`.
- Event provenance semantics for self-observation: confirm synthetic ULIDs align with ingestion policies.
- Gateway rate-limit telemetry: compare `TokenRateLimiter` state to emitted payload fields.
- Replay stats aggregation: define a dedicated view if replay metrics are expected in dashboards.
