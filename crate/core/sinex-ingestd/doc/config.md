# Configuration

`config.rs` exposes the strongly typed configuration for the ingestion daemon,
including helper functions for defaults, validation, CLI overrides, and Figment
loading from layered sources (files + environment + defaults).

Document any new knobs here and keep the examples in sync with
`docs/architecture/SystemOperations_And_Integrity_Architecture.md`.
