# System Operations & Integrity Architecture: Ensuring a Resilient and Maintainable Exocortex

*   **Version:** 1.0
*   **Date:** 2024-03-11
*   **Purpose:** This document describes the architectural approaches for ensuring the Sinnix Exocortex system's operational health, security, data integrity, resilience, and long-term maintainability. It covers meta-observability, security measures, backup and disaster recovery, performance and scalability considerations, and release engineering.
*   **Primary Sources:** STAD (System Technical Architecture Document) Part V; Vision Document Part VI.

## 1. Introduction & Guiding Principles

### 1.1. Importance of Operational Robustness for a Lifelong Archive

The Exocortex is envisioned as a lifelong cognitive partner. This long-term aspiration mandates an architecture that is not only feature-rich but also operationally robust, secure, resilient to failures, and maintainable over decades. This document outlines the architectural strategies to achieve these critical non-functional requirements.

### 1.2. Core Principles for System Operations & Integrity

*   **System Resilience:** The system must be able to withstand component failures, data corruption, and external disruptions, with well-defined recovery mechanisms.
*   **Data Integrity & Durability:** User data is sacrosanct. Measures for preventing data loss, ensuring consistency, and verifying integrity are paramount.
*   **Security & Privacy by Design:** Proactive measures to protect sensitive personal data and ensure user control.
*   **Maintainability & Evolvability:** The system architecture and operational practices should facilitate updates, bug fixes, and the graceful evolution of features and technologies.
*   **Meta-Observability as a First-Class Concern:** The system's own operational health is treated as critical data, enabling self-monitoring and diagnostics.
*   **Automation:** Operational tasks (backups, checks, deployments) should be automated as much as possible.

## 2. Meta-Observability Architecture (Vision VI.1)

The Exocortex treats its own operational data as a first-class data stream, ingested into `raw.events`.

### 2.1. Philosophy: Exocortex Operational Data *is* Exocortex Data

All system health metrics, agent performance data, errors, and logs are captured within the Exocortex itself. This allows the system's analytical and agentic capabilities to be applied to its own functioning, enabling self-diagnosis, adaptive optimization, and transparent reporting to the user.

### 2.2. Key Metrics & Events Captured (Architectural Overview)

*   **Ingestion Pipeline Health:** Throughput, latency, error rates, DLQ sizes per ingestor/agent.
*   **Agent Ecosystem Performance:** Agent uptime, processing errors, resource utilization (CPU, memory), LLM API call metrics (tokens, cost, latency via `sinex.agent.llm_api_call` events).
*   **Database Performance:** Slow query logs, connection stats, index bloat, disk I/O, replication lag (if any).
*   **Host System Resources:** CPU, memory, disk space, network I/O for the Exocortex host.
*   **Backup & Integrity Status:** Outcomes of backup jobs (`pgBackRest`, `git-annex`), `git annex fsck` results, database integrity check results.

### 2.3. Architectural Mechanisms for Ingesting Meta-Observability Data

*   **Journald Ingestion:** The `ingestor/journald_bridge` captures logs from all systemd units (Exocortex agents, PostgreSQL, etc.).
*   **Agent Self-Reporting:** Agents directly emit `sinex.agent.heartbeat`, `sinex.agent.error`, and other operational events to `raw.events`.
*   **Prometheus Integration:**
    *   Exocortex services expose `/metrics` endpoints in Prometheus text format.
    *   Prometheus scrapes these endpoints, as well as standard exporters like `node_exporter` (host metrics) and `postgres_exporter` (DB metrics).
    *   (Optional) A Prometheus remote-write receiver or a metrics-to-events bridge can ingest Prometheus metrics into `raw.events` if deep historical analysis of metrics as events is desired, though Prometheus's TSDB is primary for metric storage.
*   **Database Internal Monitoring:** PostgreSQL logs (slow queries, errors) captured by `journald` or Promtail. `postgres_exporter` provides internal statistics.

### 2.4. Utilization for Self-Management and User Awareness

*   **Dashboards (Grafana):** Visualize all key operational metrics and log summaries.
*   **Alerting Agents:** Specialized Exocortex agents can monitor critical `sinex.*` meta-event patterns (e.g., high error rates, failing heartbeats, low disk space) and generate user notifications via `NotificationDispatcher` or trigger automated responses.
*   **Capacity Planning:** Long-term analysis of resource usage trends (from metrics) informs capacity planning.
*   **Referenced TIMs:**
    *   `[TIM-ObservabilityStackSetup.md](docs/tims/operations/TIM-ObservabilityStackSetup.md)` for Prometheus, Grafana, Loki/Promtail setup, and application instrumentation for `/metrics`.

## 3. Security, Privacy, and Data Sovereignty Architecture (Vision VI.2)

Protecting the user's cognitive core is paramount.

### 3.1. Access Control & Authentication Architecture

*   **PostgreSQL Roles:** Granular roles (`exocortex_ingest_default`, `exocortex_agent_default_template` + per-agent roles, `exocortex_query_user`, `exocortex_admin_user`) define database access.
*   **Systemd User Services:** Agents run as dedicated, unprivileged system users (e.g., `sinnix-exo`) with restricted filesystem permissions (`ReadWritePaths`, `ProtectHome`).
*   **API Endpoint Authentication:** Network-facing APIs (mobile ingest, future web UI) use strong authentication (HTTPS, client certs, robust API keys/tokens).

### 3.2. Encryption Architecture

*   **At Rest:**
    *   **Full-Disk Encryption (LUKS):** Recommended for the host machine.
    *   **PostgreSQL Data Directory:** Filesystem-level encryption or native tablespace encryption.
    *   **`git-annex` Blobs:** `git-annex` native encryption (symmetric or GPG).
*   **In Transit:** TLS for all remote communication (LLM APIs, mobile ingest, future federation).
*   **Secrets Management (`agenix`) (ADR-006):** API keys, DB passwords, encryption master keys are managed declaratively and securely using `agenix`. Secrets encrypted in NixOS Git repo, decrypted at runtime to restricted paths (`/run/agenix.d/`).
    *   *Referenced TIMs:* `[TIM-SecretsManagementAgenix.md](docs/tims/operations/TIM-SecretsManagementAgenix.md)`.
*   **Field-Level & Searchable Encryption (`pgsodium`):**
    *   Sensitive data fields in PostgreSQL encrypted using `pgsodium` (libsodium `crypto_secretbox` AEAD cipher). Keys derived from a master root key (managed by `agenix` and `pgsodium.getkey_script`). Nonces stored alongside ciphertext.
    *   Searchable encryption via blind indexes (keyed hash of plaintext stored in unencrypted, indexed column; application filters false positives).
    *   *Referenced TIMs:* `[TIM-PostgreSQLSecurityEncryption.md](docs/tims/operations/TIM-PostgreSQLSecurityEncryption.md)`.

### 3.3. Consent & Control Architecture for Sensitive Data

*   **Explicit Opt-In:** Ingestors for highly sensitive data (raw keystrokes, continuous audio/video) are disabled by default and require explicit user enablement.
*   **Clear UI Indicators:** Persistent visual notification when sensitive capture is active.
*   **Global Pause/Resume:** Easy access to pause all/specific sensitive data ingestion.
*   **Configurable Redaction Policies:** User-defined rules for redacting sensitive patterns (PII, passwords) at ingest or early processing.
*   **Privacy Zones/Tags:** User can tag data as "highly private" to trigger stricter agent access policies or exclusion from external LLM processing.

### 3.4. Data Export and Deletion Architecture

*   **Export:** `exo` CLI and UIs provide robust data export in open formats.
*   **Deletion ("Right to be Forgotten"):**
    *   **Logical Deletion/Redaction:** Marking items as "archived"/"redacted," filtered from default views.
    *   **Cryptographic Erasure (Blobs):** Deleting per-blob encryption keys (if used).
    *   **Selective Physical Deletion:** Complex, involves specialized procedures for PG/annex, impacts perfect replayability.

### 3.5. Process Sandboxing Architecture

Hardening Exocortex agents and services against vulnerabilities.
*   **Systemd `SystemCallFilter` (`seccomp-bpf`):** Whitelists allowed Linux system calls per service. `NoNewPrivileges=true` is mandatory. Syscall profiles generated via `strace`.
*   **AppArmor (NixOS):** Mandatory Access Control (MAC) via per-program profiles defining allowed resource access (files, network, capabilities). Profiles developed using `aa-genprof`/`aa-logprof`.
*   **`evdev` Keyboard Capture Specific Mitigations:** Strict privilege separation is critical (minimal reader component -> IPC -> unprivileged processor agent).
*   **Referenced TIMs:**
    *   `[TIM-ProcessSandboxing.md](docs/tims/operations/TIM-ProcessSandboxing.md)` for `seccomp` and AppArmor setup/profile writing.

## 4. Backup, Disaster Recovery, and Data Integrity Architecture (Vision VI.3)

Ensuring data permanence and recoverability.

### 4.1. PostgreSQL Backup Strategy (`pgBackRest`)

`pgBackRest` is the chosen tool for PostgreSQL backups.
*   **Architecture:** Continuous WAL archiving (`archive_mode = on`, `archive_command` pushes to `pgBackRest` repo). Regular full, differential, and incremental backups to a dedicated repository (local or S3). Backups and WALs are encrypted. Point-in-Time Recovery (PITR) capability.
*   **Configuration:** `pgbackrest.conf` defines stanzas, repository paths, retention policies, compression (`zstd`), encryption. Stanza initialized with `stanza-create`.
*   **Operations:** Scheduled backups, `expire` for retention.
*   **Referenced TIMs:**
    *   `[TIM-PostgreSQLBackupDR_pgBackRest.md](docs/tims/operations/TIM-PostgreSQLBackupDR_pgBackRest.md)` for `pgBackRest` setup, configuration, backup/restore commands, and S3 lifecycle policies.

### 4.2. `git-annex` Backup Strategy

*   **Architecture:** Multiple `git-annex` remotes (external USB, NAS, encrypted cloud via `rclone`). Regular `git annex sync --content` and `git annex copy`. The Git repository metadata itself (symlinks, history) is also backed up (e.g., `git bundle` or push to private Git remote).
*   **Referenced TIMs:**
    *   `[TIM-GitAnnexLargeFileMgmt.md](docs/tims/operations/TIM-GitAnnexLargeFileMgmt.md)` for annex remote setup.

### 4.3. NixOS Configuration Backup

The entire NixOS configuration (flakes, modules, `agenix` secrets) is version-controlled in a private Git repository and backed up to a secure remote.
*   **Referenced TIMs:**
    *   `[TIM-ReleaseEngineeringCICD.md](docs/tims/operations/TIM-ReleaseEngineeringCICD.md)` (implicitly, as NixOS config is part of codebase).

### 4.4. Disaster Recovery Plan Architecture

A documented, periodically tested DR plan covers full host failure, database corruption, etc.
*   **Key Steps:** Provision new host (from NixOS config backup) -> Restore PostgreSQL (from `pgBackRest` base backup + WALs for PITR) -> Restore `git-annex` repo (metadata + content from remotes) -> Re-initialize services -> Verify.
*   **Automated Test Restores:** Systemd timer periodically executes a test restore of PostgreSQL to a temporary instance and verifies data.
*   **Referenced TIMs:**
    *   `[TIM-PostgreSQLBackupDR_pgBackRest.md](docs/tims/operations/TIM-PostgreSQLBackupDR_pgBackRest.md)` (Section 6) for test restore procedures.
    *   A future dedicated `TIM-DisasterRecoveryPlan.md` could consolidate detailed steps from UG Appendix G.

### 4.5. Data Integrity Check Architecture

Proactive measures to detect and report data corruption or inconsistencies.
*   **`git-annex`:** Regular `git annex fsck` (results logged as `sinex.data_integrity.annex_fsck_result` events).
*   **PostgreSQL:** Database constraints (PK, FK, UNIQUE). `pg_jsonschema` validation for `raw.events.payload`.
*   **Link Integrity:** Periodic agent scans for broken FKs or unresolved links in `core_entity_relations`, `core_artifact_links`, `event_relations`. Logs `sinex.data_integrity.broken_link_detected`.
*   **Orphaned Data Detection:** Agents identify unreferenced blobs, artifact contents, or entities. Trigger `sinex.data_cleanup.suggestion_created`.

## 5. Performance, Scalability, and Schema Evolution Architecture (Vision VI.4)

Ensuring the Exocortex can grow gracefully.

### 5.1. Database Performance Tuning & Management Strategy

*   **Regular Maintenance:** `VACUUM ANALYZE`. TimescaleDB chunk management (interval tuning, compression policies for `raw.events`).
*   **Index Monitoring:** Track usage, bloat. Identify inefficient queries (`EXPLAIN ANALYZE`).
*   **Query Optimization:** Rewrite queries, add/tune indexes, create materialized views if needed.

### 5.2. Agent & Ingestion Scalability Architecture

*   **Asynchronous Processing & Batching:** Most agents operate asynchronously, processing data in batches.
*   **Connection Pooling:** All database clients use connection pooling.
*   **Parallelization:** Multiple instances of stateless worker agents (e.g., `sinex-promo-worker`) can run concurrently, pulling from `promotion_queue` using `SKIP LOCKED`.
*   **Resource Limits:** Systemd cgroups enforce CPU/memory limits per agent.

### 5.3. Schema Evolution Strategy

*   **`raw.events.payload` (JSONB):** Offers flexibility; new fields can be added by ingestors without DDL changes to `raw.events` itself. Schema evolution managed by versioning JSON Schemas in `sinex_schemas.event_payload_schemas`.
*   **Domain Tables & Core Schema:** Changes managed via versioned SQL migration scripts. A formal migration tool (e.g., Sqitch, Diesel Migrations) is adopted post-Phase 2.5.
*   **Promotion Agent Versioning:** Promotion agents are version-aware. When a target domain table schema changes, the agent is updated. It can process new raw events to the new schema and optionally reprocess historical data to backfill changes.
*   **Impact Logging:** Schema changes (DDL migrations, `event_payload_schemas` updates) are logged as `sinex.schema.*` events, triggering review of dependent components.

## 6. Federation and Multi-Device Coherence Architecture (Future Vision) (Vision VI.5)

Enabling a distributed Exocortex across multiple user devices or potentially (very cautiously) between trusted user instances.
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

Automating the build, test, and release process for Exocortex components.
*   **Nix Flakes (`flake.nix`):** Primary mechanism for reproducible builds and development environments. Defines package derivations for all Exocortex binaries, NixOS modules for deployment, and `devShells`.
*   **Continuous Integration (CI - GitHub Actions):**
    *   **Workflow:** Triggered on push/PR. Installs Nix, uses Cachix for binary caching.
    *   **Steps:**
        1.  **Check:** `nix flake check` (validates flake, runs `checks` derivations including NixOS VM tests), linters (`rustfmt`, `clippy`, `shellcheck`), static analysis.
        2.  **Build:** `nix build .#packages.<system>.all`.
        3.  **Test:** Unit tests (`cargo test`), integration tests (DB tests with Testcontainers or GitHub Actions services), NixOS VM tests.
        4.  **Security Scan:** Dependency vulnerability checks (`cargo audit`, `vulnix`), SAST.
        5.  **Publish (on main/tags):** Push Nix artifacts to Cachix, build/push Docker images (if any), create GitHub Releases.
*   **Artifact Management:** Cachix for Nix binaries, Docker registries, GitHub Releases.
*   **Referenced TIMs:**
    *   `[TIM-ReleaseEngineeringCICD.md](docs/tims/operations/TIM-ReleaseEngineeringCICD.md)` for `flake.nix` structure, GitHub Actions workflow YAML, CI pipeline steps, and artifact management.

