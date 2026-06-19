# OpenTelemetry Projection Boundary

Sinex may project selected runtime and telemetry read models into
OpenTelemetry-compatible DTOs. OpenTelemetry is not Sinex's internal truth
model.

## Boundary

Canonical Sinex truth remains:

```text
RawMaterial
ProducerRun / Parser
EventIntent / Candidate
AdmissionOutcome
AdmittedEvent
Projection / Artifact
Proposal / Judgment
Operation
View
```

OTel projection is allowed only after that state already exists in Sinex read
surfaces such as telemetry RPC responses, operation views, debt views, runtime
health, and source/package coverage.

## Current Implementation

`sinex_primitives::otel_projection` defines Sinex-owned DTOs that are shaped for
the OpenTelemetry metrics data model:

- resource attributes;
- metric names, units, kinds, and aggregation temporality;
- number data points with attributes;
- explicit disclosure boundary metadata.

The first renderer maps `TelemetryGatewayStatsResponse` buckets into a metrics
projection. Operators can request it with:

```text
sinexctl metrics telemetry gateway-stats --otel --format json
```

Table output summarizes the projection. JSON/YAML output emits the full DTO.
This is not an OTLP exporter and does not require an OTel collector to run
Sinex.

## Attribute Discipline

Projected attributes should use stable refs, ids, counts, timings, and bounded
aggregate coordinates:

```text
service.name
sinex.source
sinex.package_id
sinex.mode_id
sinex.event_contract_id
sinex.admission_policy_id
sinex.operation_id
sinex.producer_run_id
sinex.parser_id
sinex.outcome
sinex.debt_kind
sinex.target
sinex.telemetry.bucket
sinex.telemetry.source_surface
```

Do not export raw event payloads, raw material bytes, OCR/transcript text,
email bodies, browser URLs, command text, DLQ payloads, or private log bodies
unless an operator-controlled disclosure/export policy explicitly allows
that destination and exposes the decision in the projection.

## Use OTel Export When

- the target is an external metrics/traces/logs system;
- the data is already available through Sinex read surfaces;
- the export can use bounded aggregate values and stable refs;
- local Sinex operation must continue if the external collector is absent.

## Use Sinex Views Instead When

- the operator needs source/package readiness, debt, or operation actions;
- the data contains raw personal material or unbounded diagnostic logs;
- the workflow needs authority, proposal, judgment, finalization, or mutation;
- the consumer needs canonical event/material/projection semantics rather than
  external observability interoperability.

## References

The OTel metrics projection follows the OpenTelemetry metrics data-model shape:
resources, metrics, data points, and attributes. The OpenTelemetry metrics data
model is stable and supports importing pre-aggregated timeseries from existing
systems. Semantic conventions define common attribute names for standardized
domains; Sinex-specific attributes stay under the `sinex.*` namespace until a
standard convention exists.

- https://opentelemetry.io/docs/specs/otel/metrics/data-model/
- https://opentelemetry.io/docs/concepts/semantic-conventions/
