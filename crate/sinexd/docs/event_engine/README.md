# sinexd Event Engine Documentation

## Overview

`sinexd::event_engine` receives events from source contracts and automata, validates
them, writes them to PostgreSQL, and relays confirmations to streaming sinks.

## Key Responsibilities

- Consume `JetStream` events/materials from sources and enforce schema validation
- Persist events and source material through the repositories in `sinex-db`
- Publish derived data to `JetStream` so downstream services receive updates
- Coordinate schema migrations by integrating with `sinex-schema`

## Architecture & Deep Dives

- `ingestion_pipeline.md` – `JetStream` consumer, batch persistence, confirmation logic, and DLQ (NEW)
- `material_assembly.md` – Source material reconstruction, WAL-based recovery, and content-store integration (NEW)
- `architecture.md` – Service role and separation rationale
- `diagrams.md` – Visual architecture diagrams (NATS topology, pipeline flow)

## Operational Documentation

- `config.md` – Configuration options and defaults
- `validator.md` – Event schema validation rules
- `environment.md` – event-engine-specific environment variables
- `transport_security.md` – NATS TLS and authentication requirements
- `schema_sync.md` – Schema synchronization details

## Reference

- `patterns.md` – Event sourcing, idempotency, backpressure patterns
- `pipeline-design.md` – Future event pipeline design patterns
- `service.md` – Service architecture

## See Also

- Global architecture: `README.md#architecture`
- Deployment and operations: `README.md#deployment--operations`
- Deployment config: `nixos/modules/README.md`
