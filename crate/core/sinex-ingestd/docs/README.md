# sinex-ingestd Documentation

## Overview

`sinex-ingestd` is the ingestion daemon that receives events from nodes, validates them, writes them to PostgreSQL, and relays them to streaming sinks.

## Key Responsibilities

- Consume JetStream events/materials from nodes and enforce schema validation
- Persist events and source material through the repositories in `sinex-core`
- Publish derived data to JetStream so downstream services receive updates
- Coordinate schema migrations by integrating with `sinex-schema`

## Documentation

- `architecture.md` – Service role and separation rationale
- `diagrams.md` – Visual architecture diagrams (NATS topology, pipeline flow)
- `patterns.md` – Event sourcing, idempotency, backpressure patterns
- `pipeline-design.md` – Future event pipeline design patterns
- `environment.md` – Ingestd-specific environment variables
- `transport_security.md` – NATS TLS and authentication requirements
- `config.md` – Configuration options
- `service.md` – Service architecture
- `validator.md` – Event validation
- `schema_sync.md` – Schema synchronization
- `figment_config.md` – Figment configuration patterns

## See Also

- Global architecture: `docs/current/architecture/Core_Architecture.md`
- Operations: `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`
- Global config: `docs/current/configuration/environment-variables.md`
