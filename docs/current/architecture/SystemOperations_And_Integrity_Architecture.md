Status: canonical
Last Verified: 2026-03-12 (manual review)
> **Purpose:** Cross-cutting operational invariants, integrity rules, and deployment-facing runbook summaries.
# System Operations & Integrity Architecture

Implementation status: operational, with specific hardening gaps called out below.

This document owns the global operational policy for Sinex. Detailed mechanics for journald ingestion, checkpoints, replay execution, and node runtime behavior live in crate docs.

## Core Invariants

- Single writer: normal canonical event persistence flows through `sinex-ingestd`; DB role separation still needs further hardening.
- Immutability: events are append-only; lifecycle changes are explicit operations, not background retention.
- Provenance: derived events carry `source_event_ids`, temporal metadata, and replay metadata.
- Time/order: `UUIDv7` IDs provide ordering; `ts_orig` and `ts_coided` are distinct and intentional.
- Material integrity: blobs are content-addressed and referenced stably.
- Operational trace: long-running lifecycle/replay operations are recorded in `operations_log`.

## Operational Model

- services run as separate systemd units with NixOS-managed configuration and dependency ordering
- resource isolation and hardening come from the deployment layer
- observability is journald-first: services emit structured logs, `sinex-system-ingestor` ingests the relevant journal stream, and health/query surfaces consume those events
- nodes and derived automata rely on checkpointed recovery
- replay, archive, and restore are explicit control-plane operations

## Integrity Controls

- `PostgreSQL` constraints, schema validation, and `UUIDv7` ordering are active today
- schema convergence is handled through `sinex-schema` declarative `apply()`
- payload schema changes must carry short changelog notes in payload-type docs
- immutable raw history is preserved even when replay or lifecycle operations reshape the live set

## Current Hardening Gaps

- distinct per-service `PostgreSQL` login roles are not yet the default deployment path
- NATS TLS is available but not yet universal everywhere
- NATS subject-level authorization is not yet in place
- secret delivery remains uneven across some services
- syscall filtering is not yet applied in the NixOS service modules
- application-level access auditing is still weak

## Runbook Summary

Disaster recovery
- use `pgBackRest` for `PostgreSQL` base + WAL archiving
- version NixOS config in Git
- keep annex blobs on redundant remotes
- for full recovery: rebuild host, restore Postgres, reinitialize annex, restart services, verify event flow

Daily operations
- verify service health, recent event counts, error scans, and DB disk usage
- check `JetStream` consumer lag and DLQ state
- apply schema through `sinex-schema`; `SQLx` checks use the live database

Troubleshooting
- ingestion failures: inspect ingestd logs, schema IDs, payload validity, and retry state
- node issues: check NATS connectivity, checkpoint state, and service status
- DB issues: inspect pool saturation, hot-path indexes, and slow-query plans

## Canonical Downward Links

- observability and checkpoints: `crate/lib/sinex-node-sdk/docs/observability.md`
- system-level journal/systemd ingestion: `crate/nodes/sinex-system-ingestor/docs/README.md`
- gateway replay and coordination: `crate/core/sinex-gateway/docs/replay_control.md`, `crate/core/sinex-gateway/docs/coordination.md`
- current security posture: `docs/current/security.md`
