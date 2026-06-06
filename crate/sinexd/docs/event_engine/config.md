# Configuration

`config.rs` exposes the strongly typed configuration for the ingestion daemon,
including helper functions for defaults, validation, and CLI/env overrides.

Current binary startup (`main.rs`) constructs config via `EventEngineConfig::from_args`
(CLI + environment). That is the canonical runtime path today, and it matches how
the NixOS module deploys the service.

EventEngine does not treat TOML/figment loading as a co-equal deployment path. For deployed
systems, use typed NixOS options; for direct/manual runs, use env vars and CLI flags.

Document any new knobs here and keep the examples in sync with
`README.md#deployment--operations` and `nixos/modules/README.md`.

EventEngine environment overrides are prefixed with `SINEX_EVENT_ENGINE_`.

## Transport Security Knobs

- `nats_require_tls` (default: false): When true, event_engine refuses to start unless
  `nats_url` uses `tls://` or `wss://`. Set via `SINEX_NATS_REQUIRE_TLS=1` or the
  matching runtime config path.

On NixOS, prefer the typed transport surface:

- `services.sinex.runtime.nats.servers`
- `services.sinex.runtime.nats.tls.requireTls`
- `services.sinex.runtime.nats.tls.caCertFile`
- `services.sinex.runtime.nats.tls.clientCertFile`
- `services.sinex.runtime.nats.tls.clientKeyFile`

The module exports the matching `SINEX_NATS_*` variables for event_engine and runtime modules.

## `JetStream` Consumer Knobs

- `consumer_fetch_max_messages` (default: 100): Max messages per pull batch. Set via
  `SINEX_EVENT_ENGINE_CONSUMER_FETCH_MAX_MESSAGES` or `event_engine.consumer_fetch_max_messages`.
- `consumer_max_ack_pending` (default: 100): Max in-flight (unacked) messages for the primary
  event_engine consumer. Set via `SINEX_EVENT_ENGINE_CONSUMER_MAX_ACK_PENDING` or
  `event_engine.consumer_max_ack_pending`.
- `material_slices_max_ack_pending` (default: 1000): Max in-flight messages for the material
  slices consumer. Set via `SINEX_EVENT_ENGINE_MATERIAL_SLICES_MAX_ACK_PENDING` or
  `event_engine.material_slices_max_ack_pending`.

## Validation Knobs

- `strict_validation` (default: false): Reject events that do not have registered schemas.
  Set via `services.sinex.core.event_engine.strictValidation` on NixOS or
  `SINEX_EVENT_ENGINE_STRICT_VALIDATION=true` for direct/manual runs.
- `validate_schemas` works independently: strict mode controls whether schema presence is
  mandatory, while schema validation controls whether present schemas are enforced.

See `validator.md` for the behavioral matrix and rollout guidance.
