# Event Validator

`validator.rs` centralises structural, plausibility, payload, and schema checks
before events hit storage. `AdmissionService` calls the full event validator on
the production admission route, so envelope sanity, object payload shape,
domain-specific required fields, temporal bounds, UUID timestamp drift, and
provenance checks run before persistence as well as registered JSON Schema
validation.

- Resolves schema metadata through `sinex-schema` and caches lookups.
- Applies per-event validation and accumulates `ValidationStats`.
- Surfaces actionable failure messages for producers while preserving security
  boundaries.

## Strict Validation

`sinexd::event_engine` supports a stricter schema gate for environments that want
schema coverage to be mandatory instead of best-effort.

- default behavior is permissive: events without registered schemas are accepted
- with strict validation enabled, schema-less events are rejected before persistence
- with strict validation enabled, a registered schema that is unavailable from
  the compiled cache is also rejected before persistence
- this is an event-engine behavior/config knob, not a system-wide architectural mode

Schema compilation failures are retained as validator diagnostics for the most
recent load (`IngestEventValidator::get_schema_compilation_failures`). They are
not silently erased into an unregistered event family. In permissive mode, a
compiled-cache miss remains accepted for compatibility, but the diagnostic
identifies the affected registered schema for operator repair; strict mode is
fail-closed for both missing bindings and missing compiled schemas.

### Configuration

- NixOS: `services.sinex.core.event_engine.strictValidation = true`
- direct/manual run: `SINEX_EVENT_ENGINE_STRICT_VALIDATION=true`
- default: `false`

### Effective Behavior

| `strict_validation` | `validate_schemas` | Result |
|---------------------|--------------------|--------|
| `false` | `false` | accept all events without schema validation |
| `false` | `true` | validate events that have schemas; accept schema-less events |
| `true` | `false` | reject schema-less events; accept events that do have schemas without schema validation |
| `true` | `true` | reject schema-less events and validate the rest against schemas |

Recommended deployed posture:

- `strictValidation = true`
- `validateSchemas = true`

### Operational Guidance

- use permissive mode during rapid schema iteration or partial schema rollout
- enable strict mode once all expected event families have registered schemas
- watch validation failures and `no_schema` style drift before flipping production

Whenever schema contracts change, update this documentation alongside the
validation flows so operators understand the active guardrails.
