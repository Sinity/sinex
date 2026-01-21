# Loop 012 - Self-Observation Schema Coverage vs Aggregates

Goal
- Verify that self-observation event types (metrics payloads) are represented in schema bundles and Timescale aggregates.
- Identify mismatches between emitted payload fields and aggregate views.

Process
1) Locate metrics payload definitions in `sinex-core` and record event_type/source pairs.
2) Scan `schemas/` bundle for these event types to check schema coverage.
3) Inspect migration `m20250117_000011_add_self_observation_aggregates` for aggregate views and filters.
4) Compare aggregate columns to payload fields and note mismatches or missing coverage.
5) Capture concrete issues (missing schemas, aggregation bugs, view grouping errors).

Deliverables
- Event-type mapping summary.
- Coverage and mismatch findings.
- Concrete issues list.
