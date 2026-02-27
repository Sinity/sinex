# Configuration

`config.rs` exposes the strongly typed configuration for the ingestion daemon,
including helper functions for defaults, validation, and CLI/env overrides.

Current binary startup (`main.rs`) constructs config via `IngestdConfig::from_args`
(CLI + environment). Figment loading helpers (`load`, `load_from_path`) remain
available for tests and tooling paths.

Document any new knobs here and keep the examples in sync with
`docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`.

Ingestd environment overrides are prefixed with `SINEX_INGESTD_`.

## Transport Security Knobs

- `nats_require_tls` (default: false): When true, ingestd refuses to start unless
  `nats_url` uses `tls://` or `wss://`. Set via `SINEX_NATS_REQUIRE_TLS=1` or the
  config file key `ingestd.nats.require_tls`.

## `JetStream` Consumer Knobs

- `consumer_fetch_max_messages` (default: 100): Max messages per pull batch. Set via
  `SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES` or `ingestd.consumer_fetch_max_messages`.
- `consumer_max_ack_pending` (default: 100): Max in-flight (unacked) messages for the primary
  ingestd consumer. Set via `SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING` or
  `ingestd.consumer_max_ack_pending`.
- `material_slices_max_ack_pending` (default: 1000): Max in-flight messages for the material
  slices consumer. Set via `SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING` or
  `ingestd.material_slices_max_ack_pending`.
