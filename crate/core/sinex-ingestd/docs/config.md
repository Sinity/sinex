# Configuration

`config.rs` exposes the strongly typed configuration for the ingestion daemon,
including helper functions for defaults, validation, and CLI/env overrides.

Current binary startup (`main.rs`) constructs config via `IngestdConfig::from_args`
(CLI + environment). That is the canonical runtime path today, and it matches how
the NixOS module deploys the service.

Ingestd does not treat TOML/figment loading as a co-equal deployment path. For deployed
systems, use typed NixOS options; for direct/manual runs, use env vars and CLI flags.

Document any new knobs here and keep the examples in sync with
`OPERATIONS.md`.

Ingestd environment overrides are prefixed with `SINEX_INGESTD_`.

## Transport Security Knobs

- `nats_require_tls` (default: false): When true, ingestd refuses to start unless
  `nats_url` uses `tls://` or `wss://`. Set via `SINEX_NATS_REQUIRE_TLS=1` or the
  matching runtime config path.

On NixOS, prefer the typed transport surface:

- `services.sinex.nodes.nats.servers`
- `services.sinex.nodes.nats.tls.requireTls`
- `services.sinex.nodes.nats.tls.caCertFile`
- `services.sinex.nodes.nats.tls.clientCertFile`
- `services.sinex.nodes.nats.tls.clientKeyFile`

The module exports the matching `SINEX_NATS_*` variables for ingestd and node services.

## `JetStream` Consumer Knobs

- `consumer_fetch_max_messages` (default: 100): Max messages per pull batch. Set via
  `SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES` or `ingestd.consumer_fetch_max_messages`.
- `consumer_max_ack_pending` (default: 100): Max in-flight (unacked) messages for the primary
  ingestd consumer. Set via `SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING` or
  `ingestd.consumer_max_ack_pending`.
- `material_slices_max_ack_pending` (default: 1000): Max in-flight messages for the material
  slices consumer. Set via `SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING` or
  `ingestd.material_slices_max_ack_pending`.

## Validation Knobs

- `strict_validation` (default: false): Reject events that do not have registered schemas.
  Set via `services.sinex.core.ingestd.strictValidation` on NixOS or
  `SINEX_INGESTD_STRICT_VALIDATION=true` for direct/manual runs.
- `validate_schemas` works independently: strict mode controls whether schema presence is
  mandatory, while schema validation controls whether present schemas are enforced.

See `validator.md` for the behavioral matrix and rollout guidance.
