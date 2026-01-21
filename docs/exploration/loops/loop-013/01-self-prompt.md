# Loop 013 - Schema Generation Pipeline vs Telemetry Coverage

Goal
- Determine whether self-observation payloads should appear in `schemas/v1` based on the schema generation pipeline.
- Identify reasons telemetry schemas might be missing (stale artifacts, feature gating, or registry omissions).

Process
1) Inspect `sinex-schema` CLI generator to confirm output structure and data sources.
2) Inspect `schema_registry::generate_all_schemas` and inventory registration for EventPayloads.
3) Verify that metrics payloads in `sinex-core` are annotated with `EventPayload` and thus should be registered.
4) Compare expected output paths (`schemas/v1/<source>/<event_type>.json`) with actual schema bundle contents.
5) Document mismatches and likely root causes.

Deliverables
- Pipeline summary (registry -> generator -> bundle layout).
- Evidence whether telemetry payloads should appear.
- Concrete issues and next steps.
