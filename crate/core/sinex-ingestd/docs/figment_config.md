# Figment Configuration

`IngestdConfig::load` (in `config.rs`) bridges the ingestion configuration into
Figment so multiple sources (defaults, configuration files, environment
variables) can be merged.

- Defines canonical keys and default values.
- Documents how to override settings when running locally (`just ingestd`) or in
  production.
- Keeps the Figment profile aligned with the runtime `IngestdConfig`.
