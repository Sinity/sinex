Status: canonical\
Last Verified: 2025-12-02 (manual review)
> **Purpose:** Operational reference for observability, integrity, and service orchestration (keep in sync with the systemd/NixOS modules).
# System Operations & Integrity Architecture: Ensuring a Resilient and Maintainable Sinex Deployment

*   **Version:** 2.0
*   **Date:** 2025-07-17
*   **Implementation Status:** ✅ **OPERATIONAL** - node orchestration operational, journald heartbeat pattern working, unified `Node` + `IngestorNode`/`AutomatonNode` runtime in place, basic security in place
*   **Purpose:** This document describes the operational architecture for ensuring the Sinex system's health, security, and maintainability. It focuses on the working operational patterns rather than planned features.
*   **Scope:** Covers operational observability, security measures, and service orchestration as currently implemented.

## 1. Introduction & Guiding Principles

### 1.1. Importance of Operational Robustness for Long-Term Data Systems

Sinex is intended to run continuously over long periods while capturing and serving critical user context. That requirement mandates architecture that is operationally robust, secure, resilient to failures, and maintainable over time. This document outlines the strategies used to meet those non-functional requirements.

### 1.2. Core Principles for System Operations & Integrity

*   **System Resilience:** The system must be able to withstand component failures, data corruption, and external disruptions, with well-defined recovery mechanisms.
*   **Data Integrity & Durability:** User data is sacrosanct. Measures for preventing data loss, ensuring consistency, and verifying integrity are paramount.
*   **Security & Privacy by Design:** Proactive measures to protect sensitive personal data and ensure user control.
*   **Maintainability & Evolvability:** The system architecture and operational practices should facilitate updates, bug fixes, and the graceful evolution of features and technologies.
*   **Meta-Observability as a First-Class Concern:** The system's own operational health is treated as critical data, enabling self-monitoring and diagnostics.
*   **Automation:** Operational tasks (backups, checks, deployments) should be automated as much as possible.

### 1.3. Core Invariants

- Single writer: Only `sinex-ingestd` persists events to `core.events`.
- Immutability: Events are append-only; corrections emit new events with provenance.
- Provenance: Derived events record sources via `source_event_ids`/`associated_blob_ids`.
- Time/order: UUIDv7 IDs provide global ordering; `ts_coided` and `ts_orig` are tracked rigorously.
- Material integrity: Blobs are content-addressed; references are stable.
- Operational trace: Long-running operations are recorded in `operations_log`.

## 2. Operational Observability Architecture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Unified observability through journald integration

The node constellation implements an elegant observability pattern where systemd's journald serves as the universal collection point for all operational data.

### 2.1. Journald Heartbeat Pattern

*   **Structured Logging:** ✅ **OPERATIONAL** - nodes emit structured JSON logs to stdout/stderr, automatically captured by systemd
*   **Journald Bridge:** ✅ **OPERATIONAL** - `sinex-system-ingestor` monitors journald and ingests all Sinex-related logs as events
*   **Automatic Service Discovery:** ✅ **OPERATIONAL** - systemd service metadata automatically tracked through journal entries
*   **Health Inference:** ✅ **OPERATIONAL** - Regular log output creates implicit heartbeat pattern without explicit heartbeat events
*   **Meta-Observability:** ✅ **OPERATIONAL** - System health becomes queryable Sinex data, enabling self-analysis and alerting
*   **Unified Monitoring:** ✅ **OPERATIONAL** - All system components (`PostgreSQL`, message bus, nodes) monitored through single journald channel

### 2.2. Operational Metrics

*   **node Service Health:** ✅ **OPERATIONAL** - systemd service status, restart counts, resource usage per node
*   **Event Processing Pipeline:** ✅ **OPERATIONAL** - NATS `JetStream` lag, durable consumer positions, checkpoint ages, DLQ sizes
*   **Ingestion Hub Performance:** ✅ **OPERATIONAL** - ingestd throughput, batch sizes, validation failures, NATS consumer lag/latency
*   **Automaton Processing:** ✅ **OPERATIONAL** - Processing rates, error rates, checkpoint intervals per automaton

## 3. Security and Service Orchestration Architecture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Basic security and service orchestration working

### 3.1. Service Orchestration

*   **node Service Orchestration:** ✅ **OPERATIONAL** - All nodes run as independent systemd services with NixOS-managed configuration
*   **Resource Isolation:** ✅ **OPERATIONAL** - Per-service memory and CPU limits enforced through systemd cgroups
*   **Service Dependencies:** ✅ **OPERATIONAL** - Proper startup ordering ensures ingestd and NATS available before nodes
*   **Configuration Management:** ✅ **OPERATIONAL** - Declarative NixOS configuration with service orchestration

### 3.2. Basic Security Measures

*   **Process Isolation:** ✅ **OPERATIONAL** - systemd service isolation with independent user contexts
*   **Local-Operation Boundary:** ✅ **OPERATIONAL** - Data capture and processing stay on the host unless explicitly configured otherwise
*   **Filesystem Permissions:** ✅ **OPERATIONAL** - Appropriate file system permissions and socket access controls
*   **Database Access Control:** ✅ **OPERATIONAL** - `PostgreSQL` access controlled through Unix socket authentication

## 4. Data Integrity and Configuration Management

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Basic data integrity and configuration management working

### 4.1. Configuration Management

*   **NixOS Configuration:** ✅ **OPERATIONAL** - Entire NixOS configuration (flakes, modules) is version-controlled in Git
*   **Declarative Services:** ✅ **OPERATIONAL** - All node services defined declaratively in NixOS configuration
*   **Service Configuration:** ✅ **OPERATIONAL** - Consistent configuration management across all services
*   **Reproducible Builds:** ✅ **OPERATIONAL** - Nix ensures reproducible service deployments

### 4.2. Data Integrity

*   **`PostgreSQL` Constraints:** ✅ **OPERATIONAL** - Database constraints (PK, FK, UNIQUE) implemented
*   **Event Schema Validation:** ✅ **OPERATIONAL** - `pg_jsonschema` validation for event payloads
*   **UUIDv7 Consistency:** ✅ **OPERATIONAL** - Time-ordered UUIDv7 primary keys ensure data consistency
*   **Immutable Event Log:** ✅ **OPERATIONAL** - Raw events table provides immutable audit trail

## 5. Performance and Scalability Architecture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Core performance and scalability patterns working

### 5.1. Scalability Patterns

*   **Horizontal Scaling:** ✅ **OPERATIONAL** - NATS `JetStream` durable consumers enable horizontal scaling of automaton processing
*   **Asynchronous Processing:** ✅ **OPERATIONAL** - Batch processing and asynchronous operations implemented
*   **`TimescaleDB` Partitioning:** ✅ **OPERATIONAL** - Automatic time-based partitioning for efficient queries
*   **Checkpoint-Based Recovery:** ✅ **OPERATIONAL** - Reliable state management enables service scaling

### 5.2. Schema Evolution

*   **JSONB Flexibility:** ✅ **OPERATIONAL** - Event payloads use JSONB for schema flexibility
*   **Migrations:** ✅ **OPERATIONAL** - Database schema and migrations via `sinex-schema` (sea-orm-migration)
*   **Event Schema Validation:** ✅ **OPERATIONAL** - GitOps-driven schema validation enables evolution
*   **Immutable Event Log:** ✅ **OPERATIONAL** - Raw events preserve history during schema changes
*   **Schema Change Notes:** ✅ **REQUIRED** - Every payload schema change must include a short changelog block in the payload type docs describing breaking vs additive changes.

## 6. Operational Excellence Summary

### 6.1. Operational Architecture Benefits

**Zero-Configuration Observability:**
- Journald-based monitoring with automatic service discovery
- Real-time health inference from service activity
- Self-monitoring through event stream integration
- Unified logging without external dependencies

**Service Orchestration:**
- Declarative NixOS configuration management
- Independent node services with proper isolation
- Automatic service dependencies and startup ordering
- Resource management through systemd cgroups

**Data Integrity:**
- Immutable event log with complete audit trail
- Database constraints and schema validation
- Time-ordered UUIDv7 primary keys for consistency
- Version-controlled configuration management

**Performance and Scalability:**
- Horizontal scaling through NATS `JetStream` durable consumers
- `TimescaleDB` partitioning for efficient queries
- Asynchronous processing with checkpoint recovery
- JSONB flexibility for schema evolution

This operational architecture provides a robust foundation for the Sinex system, focusing on proven patterns that are currently operational rather than speculative future features.

## 7. Runbooks (Summary)

Disaster Recovery (summary)
- Backups: Use `pgBackRest` for `PostgreSQL` base + WAL archiving; version NixOS config in Git; store annex blobs on redundant remotes.
- Full host recovery: Rebuild NixOS from config; restore Postgres with `pgbackrest restore` (latest or PITR); reinitialize `git-annex` and sync content; start services and verify.
- Logical error recovery: Restore to a temporary instance at time T; dump specific tables/rows; apply to production after review.

Daily Ops
- Health check: verify services; recent event counts; error scans; DB disk usage.
- Queue/lag: check `JetStream` consumer lag; DLQs; retry transient failures.
- Migrations: apply via `sinex-schema` (sea-orm-migration); `SQLx` compile-time checks always hit the live database (no offline cache).

Troubleshooting
- Ingestion failures: inspect ingestd logs; validate schema IDs and payloads; requeue batches.
- node issues: ensure NATS connectivity; check checkpoints; restart the unit with `systemctl`.
- DB issues: connection pool saturation, missing indexes on hot paths; run focused EXPLAINs on slow queries.
