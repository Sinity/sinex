# Loop 015 - Schema Path Conventions for Dotted Names

Scope
- Schema generator path sanitization.
- Existing `schemas/v1` directory structure.

Path Sanitization
- `sanitize_component` replaces `/`, `\`, and spaces. It does not replace dots.
- Result: dotted source and event_type strings should remain dotted in directory and file names.

Observed Schema Layout
- `schemas/v1` contains directories with dots (e.g., `canonical.terminal`), confirming dots are preserved.
- No directories matching `sinex.gateway`, `sinex.ingestd`, or `sinex.node` are present.
- No files for telemetry event types (e.g., `metric.counter.json`, `request.stats.json`) are present.

Expected Layout for Telemetry Schemas
Given the current generator behavior, telemetry schemas should appear as:
- `schemas/v1/sinex/metric.counter.json`
- `schemas/v1/sinex/metric.gauge.json`
- `schemas/v1/sinex/metric.histogram.json`
- `schemas/v1/sinex/health.status.json`
- `schemas/v1/sinex.gateway/request.stats.json`
- `schemas/v1/sinex.gateway/rate_limit.exceeded.json`
- `schemas/v1/sinex.gateway/replay.stats.json`
- `schemas/v1/sinex.ingestd/stream.stats.json`
- `schemas/v1/sinex.ingestd/assembly.stats.json`
- `schemas/v1/sinex.node/processing.stats.json`

Findings
- Dotted names are supported in schema paths; missing telemetry schemas are not due to sanitization.
- The absence of telemetry schema files is consistent with a stale schema bundle rather than a path mapping issue.

Risks
- Schema bundle drift persists even for internal telemetry events, enabling schema-less validation.

Opportunities
- Regenerate schema bundles to verify telemetry schema paths and ensure the generator emits them as expected.
