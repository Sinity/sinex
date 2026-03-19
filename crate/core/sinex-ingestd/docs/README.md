# sinex-ingestd Documentation

## Overview

`sinex-ingestd` is the ingestion daemon that receives events from nodes, validates them, writes them to `PostgreSQL`, and relays them to streaming sinks.

## Key Responsibilities

- Consume `JetStream` events/materials from nodes and enforce schema validation
- Persist events and source material through the repositories in `sinex-db`
- Publish derived data to `JetStream` so downstream services receive updates
- Coordinate schema migrations by integrating with `sinex-schema`

## Architecture & Deep Dives

- `ingestion_pipeline.md` – `JetStream` consumer, batch persistence, confirmation logic, and DLQ (NEW)
- `material_assembly.md` – Source material reconstruction, WAL-based recovery, and git-annex integration (NEW)
- `architecture.md` – Service role and separation rationale
- `diagrams.md` – Visual architecture diagrams (NATS topology, pipeline flow)

## Operational Documentation

- `config.md` – Configuration options and defaults
- `validator.md` – Event schema validation rules
- `environment.md` – Ingestd-specific environment variables
- `transport_security.md` – NATS TLS and authentication requirements
- `schema_sync.md` – Schema synchronization details
- `schema_gitops.md` – Repo-driven schema sync and source management

## Reference

- `patterns.md` – Event sourcing, idempotency, backpressure patterns
- `pipeline-design.md` – Future event pipeline design patterns
- `service.md` – Service architecture

## See Also

- Global architecture: `README.md#architecture`
- Deployment and operations: `README.md#deployment--operations`
- Deployment config: `nixos/modules/README.md`
