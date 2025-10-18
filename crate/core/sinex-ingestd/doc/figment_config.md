# Figment Configuration

`figment_config.rs` bridges the ingestion configuration into Figment so multiple
sources (environment variables, configuration files, defaults) can be merged.

- Defines canonical keys and default values.
- Documents how to override settings when running locally (`just ingestd`) or in
  production.
- Keeps the Figment profile aligned with `IngestdConfig`.
