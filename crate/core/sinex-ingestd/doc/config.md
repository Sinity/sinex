# Configuration

`config.rs` and `figment_config.rs` expose strongly typed configuration for the
ingestion daemon.

- `config.rs` defines the runtime `IngestdConfig` structure and helpers for
  defaults, validation, and environment overrides.
- `figment_config.rs` adapts the configuration to Figment so the binary can load
  layered sources (`just`, environment, files).

Document any new knobs here and keep the examples in sync with
`docs/architecture/SystemOperations_And_Integrity_Architecture.md`.
