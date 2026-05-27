# Event Validator

`validator.rs` centralises payload and schema checks before events hit storage.

- Resolves schema metadata through `sinex-schema` and caches lookups.
- Applies per-event validation and accumulates `ValidationStats`.
- Surfaces actionable failure messages for nodes while preserving security
  boundaries.

## Strict Validation

`sinex-ingestd` supports a stricter schema gate for environments that want
schema coverage to be mandatory instead of best-effort.

- default behavior is permissive: events without registered schemas are accepted
- with strict validation enabled, schema-less events are rejected before persistence
- this is an ingestd behavior/config knob, not a system-wide architectural mode

### Configuration

- NixOS: `services.sinex.core.ingestd.strictValidation = true`
- direct/manual run: `SINEX_INGESTD_STRICT_VALIDATION=true`
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
