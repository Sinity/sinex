# sinex-ingestd Documentation

## Overview

`sinex-ingestd` is the ingestion daemon that receives events from nodes, validates them, writes them to PostgreSQL, and relays them to streaming sinks.

## Key Responsibilities

- Consume JetStream events/materials from nodes and enforce schema validation
- Persist events and source material through the repositories in `sinex-core`
- Publish derived data to JetStream so downstream services receive updates
- Coordinate schema migrations by integrating with `sinex-schema`

## Architecture & Deep Dives

- `ingestion_pipeline.md` – JetStream consumer, batch persistence, confirmation logic, and DLQ (NEW)
- `material_assembly.md` – Source material reconstruction, WAL-based recovery, and git-annex integration (NEW)
- `architecture.md` – Service role and separation rationale
- `diagrams.md` – Visual architecture diagrams (NATS topology, pipeline flow)

## Operational Documentation

- `config.md` – Configuration options and defaults
- `validator.md` – Event schema validation rules
- `environment.md` – Ingestd-specific environment variables
- `transport_security.md` – NATS TLS and authentication requirements
- `schema_sync.md` – Schema synchronization details

## Legacy / Reference

- `patterns.md` – Event sourcing, idempotency, backpressure patterns
- `pipeline-design.md` – Future event pipeline design patterns
- `service.md` – Service architecture
- `figment_config.md` – Figment configuration patterns

## See Also

- Global architecture: `docs/current/architecture/Core_Architecture.md`
- Operations: `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`
- Global config: `docs/current/configuration/environment-variables.md`