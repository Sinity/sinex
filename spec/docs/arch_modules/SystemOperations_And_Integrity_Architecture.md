# System Operations & Integrity Architecture: Ensuring a Resilient and Maintainable Exocortex

*   **Version:** 1.3
*   **Date:** 2025-07-16
*   **Implementation Status:** 🚧 **55% IMPLEMENTED** - Satellite orchestration operational, journald heartbeat pattern working, StatefulStreamProcessor interface implemented, basic security in place
*   **Purpose:** This document describes the architectural approaches for ensuring the Sinex system's operational health, security, data integrity, resilience, and long-term maintainability. It covers meta-observability, security measures, backup and disaster recovery, performance and scalability considerations, and release engineering with the satellite constellation architecture.
*   **Primary Sources:** STAD (System Technical Architecture Document) Part V; Vision Document Part VI.

## 1. Introduction & Guiding Principles

### 1.1. Importance of Operational Robustness for a Lifelong Archive

The Sinex is envisioned as a lifelong cognitive partner. This long-term aspiration mandates an architecture that is not only feature-rich but also operationally robust, secure, resilient to failures, and maintainable over decades. This document outlines the architectural strategies to achieve these critical non-functional requirements.

### 1.2. Core Principles for System Operations & Integrity

*   **System Resilience:** The system must be able to withstand component failures, data corruption, and external disruptions, with well-defined recovery mechanisms.
*   **Data Integrity & Durability:** User data is sacrosanct. Measures for preventing data loss, ensuring consistency, and verifying integrity are paramount.
*   **Security & Privacy by Design:** Proactive measures to protect sensitive personal data and ensure user control.
*   **Maintainability & Evolvability:** The system architecture and operational practices should facilitate updates, bug fixes, and the graceful evolution of features and technologies.
*   **Meta-Observability as a First-Class Concern:** The system's own operational health is treated as critical data, enabling self-monitoring and diagnostics.
*   **Automation:** Operational tasks (backups, checks, deployments) should be automated as much as possible.

## 2. Meta-Observability Architecture (Vision VI.1)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Comprehensive observability stack not developed

The Sinex treats its own operational data as a first-class data stream, ingested into `raw.events`.

### 2.1. Philosophy: Sinex Operational Data *is* Sinex Data

All system health metrics, agent performance data, errors, and logs are captured within Sinex itself. This allows the system's analytical and agentic capabilities to be applied to its own functioning, enabling self-diagnosis, adaptive optimization, and transparent reporting to the user.

### 2.2. Satellite Constellation Observability

*   **Satellite Service Health:** ✅ **OPERATIONAL** - systemd service status, restart counts, resource usage per satellite
*   **Event Processing Pipeline:** ✅ **OPERATIONAL** - Redis Streams lag, consumer group positions, checkpoint ages, DLQ sizes
*   **Ingestion Hub Performance:** ✅ **OPERATIONAL** - ingestd throughput, batch sizes, validation failures, gRPC latency
*   **Automaton Processing:** ✅ **OPERATIONAL** - Processing rates, error rates, checkpoint intervals per automaton
*   **Database Performance:** 🚧 **BASIC** - Connection stats, query performance, TimescaleDB chunk management
*   **Host System Resources:** 🚧 **BASIC** - CPU, memory, disk I/O captured through journald integration

### 2.3. Journald Heartbeat Pattern: Operational Elegance

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Unified observability through journald integration

The satellite constellation implements an elegant observability pattern where systemd's journald serves as the universal collection point for all operational data.

*   **Structured Logging:** ✅ **OPERATIONAL** - Satellites emit structured JSON logs to stdout/stderr, automatically captured by systemd
*   **Journald Bridge:** ✅ **OPERATIONAL** - `sinex-system-satellite` monitors journald and ingests all Sinex-related logs as events
*   **Automatic Service Discovery:** ✅ **OPERATIONAL** - systemd service metadata automatically tracked through journal entries
*   **Health Inference:** ✅ **OPERATIONAL** - Regular log output creates implicit heartbeat pattern without explicit heartbeat events
*   **Meta-Observability:** ✅ **OPERATIONAL** - System health becomes queryable Sinex data, enabling self-analysis and alerting
*   **Unified Monitoring:** ✅ **OPERATIONAL** - All system components (PostgreSQL, Redis, satellites) monitored through single journald channel

### 2.4. Utilization for Self-Management and User Awareness

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Dashboard and alerting systems not implemented

*   **Dashboards (Grafana):** ❌ **NOT IMPLEMENTED** - Would visualize all key operational metrics and log summaries.
*   **Alerting Agents:** ❌ **NOT IMPLEMENTED** - Specialized Sinex agents would monitor critical `sinex.*` meta-event patterns and generate notifications.
*   **Capacity Planning:** ❌ **NOT IMPLEMENTED** - Long-term analysis of resource usage trends not implemented.
*   **Referenced TIMs:**
    *   `[TIM-ObservabilityStackSetup.md](docs/tims/operations/TIM-ObservabilityStackSetup.md)` for Prometheus, Grafana, Loki/Promtail setup, and application instrumentation for `/metrics`.

## 3. Security, Privacy, and Data Sovereignty Architecture (Vision VI.2)

> **🚧 IMPLEMENTATION STATUS: 20% IMPLEMENTED** - Basic filesystem permissions, comprehensive security not implemented

Protecting the user's cognitive core is paramount.

### 3.1. Access Control & Authentication Architecture

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Basic permissions working, granular access control not implemented

*   **PostgreSQL Roles:** ❌ **NOT IMPLEMENTED** - Granular roles for different access patterns not implemented. Currently using basic database access.
*   **Satellite Service Orchestration:** ✅ **OPERATIONAL** - All satellites run as independent systemd services with NixOS-managed configuration
*   **Resource Isolation:** ✅ **OPERATIONAL** - Per-service memory and CPU limits enforced through systemd cgroups
*   **Service Dependencies:** ✅ **OPERATIONAL** - Proper startup ordering ensures ingestd and Redis available before satellites
*   **API Endpoint Authentication:** ❌ **NOT IMPLEMENTED** - Network-facing APIs with authentication not implemented.

### 3.2. Encryption Architecture

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Some filesystem encryption, comprehensive encryption not implemented

*   **At Rest:**
    *   **Full-Disk Encryption (LUKS):** 🚧 **USER DEPENDENT** - Recommended for the host machine, not enforced by Sinex.
    *   **PostgreSQL Data Directory:** ❌ **NOT IMPLEMENTED** - Specific database encryption not configured.
    *   **`git-annex` Blobs:** ❌ **NOT IMPLEMENTED** - Git-annex encryption not configured.
*   **In Transit:** ❌ **NOT IMPLEMENTED** - TLS for remote communication not implemented (no remote APIs yet).
*   **Secrets Management (`agenix`) (ADR-006):** ❌ **NOT IMPLEMENTED** - Agenix secrets management not implemented.
*   **Field-Level & Searchable Encryption (`pgsodium`):** ❌ **NOT IMPLEMENTED** - Database field-level encryption not implemented.

### 3.3. Consent & Control Architecture for Sensitive Data

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Privacy controls not implemented

*   **Explicit Opt-In:** ❌ **NOT IMPLEMENTED** - Sensitive data ingestors would be disabled by default and require explicit user enablement.
*   **Clear UI Indicators:** ❌ **NOT IMPLEMENTED** - Visual notification when sensitive capture is active not implemented.
*   **Global Pause/Resume:** ❌ **NOT IMPLEMENTED** - Controls to pause data ingestion not implemented.
*   **Configurable Redaction Policies:** ❌ **NOT IMPLEMENTED** - User-defined redaction rules not implemented.
*   **Privacy Zones/Tags:** ❌ **NOT IMPLEMENTED** - Privacy tagging system not implemented.

### 3.4. Data Export and Deletion Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Data export/deletion not implemented

*   **Export:** ❌ **NOT IMPLEMENTED** - `exo` CLI and UIs would provide robust data export in open formats.
*   **Deletion ("Right to be Forgotten"):** ❌ **NOT IMPLEMENTED** - Logical deletion, cryptographic erasure, and selective physical deletion not implemented.

### 3.5. Process Sandboxing Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Process sandboxing not implemented

Hardening Sinex agents and services against vulnerabilities.
*   **Systemd `SystemCallFilter` (`seccomp-bpf`):** ❌ **NOT IMPLEMENTED** - Syscall whitelisting and `NoNewPrivileges` not configured.
*   **AppArmor (NixOS):** ❌ **NOT IMPLEMENTED** - Mandatory Access Control profiles not implemented.
*   **`evdev` Keyboard Capture Specific Mitigations:** ❌ **NOT IMPLEMENTED** - Privilege separation for input capture not implemented.
*   **Referenced TIMs:**
    *   `[TIM-ProcessSandboxing.md](docs/tims/operations/TIM-ProcessSandboxing.md)` for `seccomp` and AppArmor setup/profile writing.

## 4. Backup, Disaster Recovery, and Data Integrity Architecture (Vision VI.3)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Backup and DR systems not implemented

Ensuring data permanence and recoverability.

### 4.1. PostgreSQL Backup Strategy (`pgBackRest`)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - pgBackRest not configured

`pgBackRest` is the chosen tool for PostgreSQL backups.
*   **Architecture:** ❌ **NOT IMPLEMENTED** - WAL archiving, automated backups, encryption, and PITR capability not configured.
*   **Configuration:** ❌ **NOT IMPLEMENTED** - `pgbackrest.conf` configuration not implemented.
*   **Operations:** ❌ **NOT IMPLEMENTED** - Scheduled backups and retention policies not implemented.
*   **Referenced TIMs:**
    *   `[TIM-PostgreSQLBackupDR_pgBackRest.md](docs/tims/operations/TIM-PostgreSQLBackupDR_pgBackRest.md)` for `pgBackRest` setup, configuration, backup/restore commands, and S3 lifecycle policies.

### 4.2. `git-annex` Backup Strategy

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Git-annex backup not configured

*   **Architecture:** ❌ **NOT IMPLEMENTED** - Multiple git-annex remotes, sync operations, and metadata backup not configured.
*   **Referenced TIMs:**
    *   `[TIM-GitAnnexLargeFileMgmt.md](docs/tims/operations/TIM-GitAnnexLargeFileMgmt.md)` for annex remote setup.

### 4.3. NixOS Configuration Backup

> **✅ IMPLEMENTATION STATUS: WORKING** - NixOS configuration is version-controlled

The entire NixOS configuration (flakes, modules) is version-controlled in Git. Agenix secrets management not yet implemented.
*   **Referenced TIMs:**
    *   `[TIM-ReleaseEngineeringCICD.md](docs/tims/operations/TIM-ReleaseEngineeringCICD.md)` (implicitly, as NixOS config is part of codebase).

### 4.4. Disaster Recovery Plan Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - DR plan not documented or tested

A documented, periodically tested DR plan covers full host failure, database corruption, etc.
*   **Key Steps:** ❌ **NOT IMPLEMENTED** - Formal DR procedures not documented.
*   **Automated Test Restores:** ❌ **NOT IMPLEMENTED** - Test restore procedures not implemented.
*   **Referenced TIMs:**
    *   `[TIM-PostgreSQLBackupDR_pgBackRest.md](docs/tims/operations/TIM-PostgreSQLBackupDR_pgBackRest.md)` (Section 6) for test restore procedures.
    *   A future dedicated `TIM-DisasterRecoveryPlan.md` could consolidate detailed steps from UG Appendix G.

### 4.5. Data Integrity Check Architecture

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Basic constraints working, advanced integrity checking not implemented

Proactive measures to detect and report data corruption or inconsistencies.
*   **`git-annex`:** ❌ **NOT IMPLEMENTED** - Regular `git annex fsck` automation not implemented.
*   **PostgreSQL:** ✅ **BASIC WORKING** - Basic database constraints (PK, FK, UNIQUE) implemented. `pg_jsonschema` validation partial.
*   **Link Integrity:** ❌ **NOT IMPLEMENTED** - Periodic scanning for broken links not implemented.
*   **Orphaned Data Detection:** ❌ **NOT IMPLEMENTED** - Orphaned data detection agents not implemented.

## 5. Performance, Scalability, and Schema Evolution Architecture (Vision VI.4)

> **🚧 IMPLEMENTATION STATUS: 25% IMPLEMENTED** - Basic database setup, optimization and scalability not implemented

Ensuring Sinex can grow gracefully.

### 5.1. Database Performance Tuning & Management Strategy

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Performance tuning not implemented

*   **Regular Maintenance:** ❌ **NOT IMPLEMENTED** - Automated `VACUUM ANALYZE` and TimescaleDB chunk management not configured.
*   **Index Monitoring:** ❌ **NOT IMPLEMENTED** - Index usage tracking and bloat monitoring not implemented.
*   **Query Optimization:** ❌ **NOT IMPLEMENTED** - Query performance monitoring and optimization not implemented.

### 5.2. Agent & Ingestion Scalability Architecture

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Basic async processing, advanced scalability not implemented

*   **Asynchronous Processing & Batching:** ✅ **BASIC WORKING** - Basic asynchronous processing implemented.
*   **Connection Pooling:** ❌ **NOT IMPLEMENTED** - Database connection pooling not configured.
*   **Parallelization:** 🚧 **PARTIAL** - Basic `SKIP LOCKED` pattern implemented but multiple worker instances not deployed.
*   **Resource Limits:** ❌ **NOT IMPLEMENTED** - Systemd cgroups and resource limits not configured.

### 5.3. Schema Evolution Strategy

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Basic migrations working, formal strategy not implemented

*   **`raw.events.payload` (JSONB):** ✅ **WORKING** - JSONB flexibility implemented for schema evolution.
*   **Domain Tables & Core Schema:** ✅ **BASIC WORKING** - Basic SQL migrations using sqlx. Formal migration tools not adopted.
*   **Promotion Agent Versioning:** ❌ **NOT IMPLEMENTED** - Version-aware agents and historical data reprocessing not implemented.
*   **Impact Logging:** ❌ **NOT IMPLEMENTED** - Schema change logging and impact analysis not implemented.

## 6. Federation and Multi-Device Coherence Architecture (Future Vision) (Vision VI.5)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Multi-device sync not developed

Enabling a distributed Sinex across multiple user devices or potentially (very cautiously) between trusted user instances.
*   **Core Principles:** Local-first operation for each instance. Eventual consistency between instances. User controls synchronization policies.
*   **Technical Enablers Already in Design:** Global ULIDs for unique IDs. Consistent timestamping (NTP). `git-annex` for distributed blob management. CRDTs (Yjs for text) for conflict-free data merging.
*   **Synchronization Tools & Mechanisms (Conceptual):**
    *   **LiteFS:** For replicating SQLite databases used by specific components (e.g., Atuin history, local caches) across devices. Single-writer (primary leaseholder) model.
    *   **Syncthing:** For P2P synchronization of general files (e.g., PKM vault filesystem view, `git-annex` metadata, user data folders). Handles conflicts by creating `*.sync-conflict` files.
    *   **Custom Sync Agents:** For Exocortex-specific data types (e.g., `raw.events` stream, `core_entities` graph, Yjs updates).
        *   Use operation queues on each device for offline changes.
        *   On reconnect, exchange queued operations/deltas.
        *   Employ Hybrid Logical Clocks (HLCs) or Vector Clocks for causal ordering of distributed events/updates.
*   **Referenced TIMs:**
    *   `[TIM-MultiDeviceSyncArchitecture.md](docs/tims/operations/TIM-MultiDeviceSyncArchitecture.md)` for LiteFS, Syncthing, HLC/Vector Clock details, and CRDT sync concepts.

## 7. Release Engineering and CI/CD Architecture

> **🚧 IMPLEMENTATION STATUS: 40% IMPLEMENTED** - Basic Nix builds working, CI/CD not implemented

Automating the build, test, and release process for Sinex components.
*   **Nix Flakes (`flake.nix`):** ✅ **WORKING** - Primary mechanism for reproducible builds and development environments implemented. Defines package derivations for Sinex binaries and NixOS modules.
*   **Continuous Integration (CI - GitHub Actions):** ❌ **NOT IMPLEMENTED** - Automated CI/CD pipeline not set up.
    *   **Workflow:** Would be triggered on push/PR with Nix and Cachix integration.
    *   **Steps:** Would include flake check, build, test, security scan, and publish steps.
*   **Artifact Management:** ❌ **NOT IMPLEMENTED** - Cachix, container registries, and automated releases not configured.
*   **Referenced TIMs:**
    *   `[TIM-ReleaseEngineeringCICD.md](docs/tims/operations/TIM-ReleaseEngineeringCICD.md)` for `flake.nix` structure, GitHub Actions workflow YAML, CI pipeline steps, and artifact management.

