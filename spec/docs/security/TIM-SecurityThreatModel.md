# TIM-SecurityThreatModel: Exocortex System Security Threat Model

*   **Purpose:** To document potential security threats to the Sinnix Exocortex system and outline corresponding mitigation strategies or references to where mitigations are detailed. This TIM formalizes and expands upon original Vision Document Appendix F.
*   **Source:** Derived from original Vision Document Appendix F, STAD, and general security best practices.
*   **Dependencies:** Relies on security measures detailed in various TIMs (e.g., `TIM-PostgreSQLSecurityEncryption.md`, `TIM-ProcessSandboxing.md`, `TIM-SecretsManagementAgenix.md`).

## 1. Introduction

This threat model adopts a STRIDE-like approach (Spoofing, Tampering, Repudiation, Information Disclosure, Denial of Service, Elevation of Privilege) tailored to the Exocortex context. The Exocortex is primarily a local-first, single-user system, which shapes the threat landscape (e.g., fewer external attack surfaces) but introduces significant risks related to the concentration of highly sensitive personal data.

**Key Assets to Protect:**
*   Raw event data (`raw.events`) containing user activity logs.
*   Personal Knowledge Management content (`core.artifacts`, `core.artifact_contents`, Yjs deltas).
*   Large file blobs (`git-annex` store).
*   Knowledge graph data (`core.entities`, `core.entity_relations`).
*   Configuration secrets (`agenix` store, e.g., API keys, `pgsodium` master key).
*   The integrity and availability of the Exocortex system itself.

## 2. Threat Categories, Specific Threats, and Mitigations

### 2.1. Information Disclosure (Confidentiality Breach)

*   **Threat 1.1: Unauthorized physical/filesystem access to PostgreSQL database files at rest.**
    *   **Asset:** Entire Exocortex structured dataset.
    *   **Impact:** Full exposure of all Exocortex data.
    *   **Mitigation:**
        1.  **Full-Disk Encryption (FDE):** LUKS on the host system is the primary defense. (Host-level)
        2.  **PostgreSQL Data Directory Permissions:** Ensure PGDATA directory is owned by `postgres` user and has restrictive permissions (e.g., `0700`). (OS-level)
        3.  **(Consider) PostgreSQL Native Tablespace Encryption:** If FDE is insufficient or for specific partitions. (DB-level, adds complexity)
    *   **Reference:** `SystemOperations_And_Integrity_Architecture.md` (Sec 3.2).

*   **Threat 1.2: Unauthorized physical/filesystem access to `git-annex` blob storage at rest.**
    *   **Asset:** Large binary files (documents, images, recordings).
    *   **Impact:** Exposure of potentially sensitive blob content.
    *   **Mitigation:**
        1.  **Full-Disk Encryption (FDE):** LUKS on the host system / annex remotes. (Host-level)
        2.  **`git-annex` Native Encryption:** Use `type=crypt` remotes (GPG or shared password) for all off-host or untrusted annex remotes. (Application-level)
    *   **Reference:** `TIM-GitAnnexLargeFileMgmt.md`, `SystemOperations_And_Integrity_Architecture.md` (Sec 3.2).

*   **Threat 1.3: Unauthorized access to decrypted secrets in `/run/agenix.d/` (or `/run/secrets/`).**
    *   **Asset:** API keys, `pgsodium` master key, DB passwords.
    *   **Impact:** Compromise of external services, ability to decrypt `pgsodium`-encrypted fields.
    *   **Mitigation:**
        1.  **`agenix` Permissions:** `agenix` NixOS module sets strict file permissions (e.g., `0400`, owner-readable only) on decrypted secret files.
        2.  **`tmpfs` for Runtime Secrets:** `/run` is typically a `tmpfs`, secrets are in-memory.
        3.  **Root Access Control:** Preventing unauthorized root access on the host is paramount.
    *   **Reference:** `TIM-SecretsManagementAgenix.md`.

*   **Threat 1.4: Data leakage via insufficiently protected Exocortex network services.**
    *   **Asset:** Data exposed by Prometheus, Grafana, Ollama API, future Web UI, mobile/IoT ingest endpoints.
    *   **Impact:** Unauthorized network access to metrics, logs, dashboards, LLM interactions, or ingested data.
    *   **Mitigation:**
        1.  **Bind to Localhost:** Default for all services not explicitly needing wider access.
        2.  **Firewall (NixOS `nftables`):** Restrict access to necessary ports/IPs if wider access is enabled.
        3.  **Authentication:** Strong authentication for Grafana (admin user), future Web UI (user login), mobile/IoT ingest API (TLS, API keys/tokens).
        4.  **TLS for all external-facing services.**
    *   **Reference:** `TIM-ObservabilityStackSetup.md`, `TIM-MobileIoTImplementation_ESP32.md` (for ingest endpoint security).

*   **Threat 1.5: `evdev` input capture leading to keylogging.**
    *   **Asset:** All user keystrokes, including passwords and sensitive messages.
    *   **Impact:** Severe privacy breach.
    *   **Mitigation:**
        1.  **Strict Privilege Separation:** Minimal `evdev` reader component forwards data via IPC to unprivileged processor. (Mandatory)
        2.  **User Opt-In & Clear Persistent Notification:** `evdev` capture disabled by default. (Mandatory)
        3.  **Prefer Higher-Level Capture:** Use compositor/AT-SPI2 input events as primary.
    *   **Reference:** `TIM-EvdevInterceptionTools.md`, `TIM-ProcessSandboxing.md`.

*   **Threat 1.6: Sensitive data in `raw.events.payload` or `core_artifact_contents` directly accessible via SQL to an authorized (but potentially over-privileged or compromised) user/agent.**
    *   **Asset:** PII, confidential notes, private communications.
    *   **Impact:** Privacy breach even with legitimate DB access if permissions are too broad.
    *   **Mitigation:**
        1.  **`pgsodium` Field-Level Encryption:** Encrypt specific sensitive JSONB fields within `payload` or specific columns in `core_artifact_contents` or domain tables.
        2.  **Granular PostgreSQL Roles:** Ensure query users and agents have the minimum necessary `SELECT` grants on sensitive tables/columns. Row Level Security (RLS) if different agents/users have different data visibility needs (more complex).
        3.  **Data Minimization & Redaction:** User-configurable redaction policies for ingestors capturing potentially sensitive data.
    *   **Reference:** `TIM-PostgreSQLSecurityEncryption.md`.

*   **Threat 1.7: Data exfiltration by a compromised Exocortex agent with network access.**
    *   **Asset:** Any data the compromised agent has read access to.
    *   **Impact:** Data theft.
    *   **Mitigation:**
        1.  **Process Sandboxing (AppArmor):** Restrict network destinations an agent can connect to.
        2.  **Least-Privilege DB Roles:** Limit data readable by each agent.
        3.  **Code Review & Trusted Binaries:** Ensure agent code is vetted. NixOS reproducible builds help ensure binary integrity.
        4.  **Network Monitoring/Egress Filtering (Host Level):** General host security practice.
    *   **Reference:** `TIM-ProcessSandboxing.md`.

*   **Threat 1.8: Privacy violation due to overly broad data sharing with external LLM APIs.**
    *   **Asset:** Sensitive textual content from notes, events, etc.
    *   **Impact:** Personal data processed by third-party LLM providers.
    *   **Mitigation:**
        1.  **User-Configurable Policies:** Define which data categories/tags are permissible to send to external LLMs.
        2.  **Prefer Local LLMs (Ollama):** Use for tasks involving highly sensitive data.
        3.  **Audit Logging:** `sinex.agent.llm_api_call` events log model used, calling agent.
        4.  **Data Minimization in Prompts:** Agents should construct prompts with only necessary context.
        5.  **(Future) Local PII Detection/Redaction:** Agent pre-processes text before sending to external LLM to remove/mask PII.
    *   **Reference:** `AgenticEcosystem_Architecture.md` (Section 3.2), `TIM-LLMResourceOrchestration.md`.

### 2.2. Tampering (Integrity Breach)

*   **Threat 2.1: Unauthorized or accidental modification/deletion of data in PostgreSQL.**
    *   **Asset:** All structured data.
    *   **Impact:** Corrupted knowledge base, loss of history, false records.
    *   **Mitigation:**
        1.  **`raw.events` Append-Only Principle:** Application logic enforces this (no `UPDATE`/`DELETE` on `raw.events`).
        2.  **Restricted DB Roles:** Ingestors only `INSERT` to `raw.events`. Agents have limited DML on their target tables.
        3.  **PostgreSQL Checksums:** Enable data block checksums (`data_checksums = on` at `initdb` time).
        4.  **Regular Backups with PITR (`pgBackRest`):** Allows recovery from logical corruption.
        5.  **Content Hashing for Artifacts/Blobs:** `core_artifact_contents.content_hash_blake3` and `core_blobs.content_blake3_hash` allow verification of content integrity against external tampering if files were re-imported.
    *   **Reference:** `TIM-PostgreSQLBackupDR_pgBackRest.md`.

*   **Threat 2.2: Tampering with `git-annex` blob content outside of `git-annex`'s control.**
    *   **Asset:** Large binary files.
    *   **Impact:** Corrupted files.
    *   **Mitigation:**
        1.  **`git-annex` Content Hashing:** `git-annex` keys are based on content hashes. Any tampering with an object file in `.git/annex/objects/` would make its hash not match the key, detectable by `git annex fsck`.
        2.  **Filesystem Permissions:** Restrict write access to the `.git/annex/objects/` directory.
    *   **Reference:** `TIM-GitAnnexLargeFileMgmt.md`.

*   **Threat 2.3: Tampering with Exocortex agent binaries or configurations on disk.**
    *   **Asset:** Agent code and behavior.
    *   **Impact:** Agent performs malicious actions, data corruption, system instability.
    *   **Mitigation:**
        1.  **NixOS Immutability:** Most of `/nix/store` (where binaries live) is read-only.
        2.  **Configuration Management:** NixOS configurations are version-controlled in Git. Changes require `nixos-rebuild`.
        3.  **File Integrity Monitoring (Host Level):** Tools like AIDE or Tripwire (if extreme security needed, generally overkill for personal system but good for servers).
        4.  **Restrict Write Access:** Standard OS file permissions on agent config files in `/etc/` or agent state dirs in `/var/lib/`.
    *   **Reference:** `TIM-ReleaseEngineeringCICD.md` (Nix builds), `TIM-SecretsManagementAgenix.md` (config).

### 2.3. Repudiation (Disputing Actions)

*   **Threat 3.1: User or agent denies performing an action or generating data.**
    *   **Asset:** Auditability, accountability.
    *   **Impact:** Difficulty in tracing origin of data or system changes.
    *   **Mitigation:**
        1.  **`raw.events` Logging:** `source`, `host`, `ingestor_version`, `ts_ingest`, `ts_orig` provide strong attribution for ingested data.
        2.  **`created_by_actor` Fields:** Used in `core.entity_relations`, `artifact_tags`, `event_annotations` to record who/what created the link/tag/annotation.
        3.  **Audit Trails for System Config:** Git history of NixOS configuration.
        4.  **Database Logs:** PostgreSQL logs (if enabled at sufficient level) can show user/application performing DML.
        5.  **(Future) Digital Signatures:** For critical agent-generated events or user attestations (advanced).

### 2.4. Denial of Service (Availability Breach)

*   **Threat 4.1: Resource exhaustion (disk space, CPU, memory, network bandwidth) by Exocortex components.**
    *   **Asset:** System availability.
    *   **Impact:** System becomes unresponsive or crashes.
    *   **Mitigation:**
        1.  **Monitoring:** Prometheus `node_exporter` and application metrics for resource usage. Alerts on high utilization.
        2.  **Systemd Resource Quotas:** `MemoryMax`, `CPUQuota` for agent services.
        3.  **Data Retention/Archival Policies:** For `raw.events` (TimescaleDB compression/tiering), `pgBackRest` backup expiration, `git-annex` drop policies for blobs.
        4.  **Efficient Agent Design:** Batch processing, careful resource use in agents.
    *   **Reference:** `TIM-ObservabilityStackSetup.md`, `TIM-TimescaleDBConfiguration.md`, `TIM-ExocortexDevelopmentPractices.md` (NixOS module patterns).

*   **Threat 4.2: PostgreSQL database overload or crash.**
    *   **Asset:** Database availability.
    *   **Impact:** Entire Exocortex becomes non-functional.
    *   **Mitigation:**
        1.  **Connection Pooling:** All clients use pooling.
        2.  **Query Optimization:** Regularly review and optimize slow queries.
        3.  **Resource Allocation for PostgreSQL:** Ensure adequate RAM, CPU, fast disk.
        4.  **Robust Backup/Restore (`pgBackRest`):** For quick recovery.
        5.  **(Future) High Availability Setup for PostgreSQL:** Streaming replication to a hot standby (adds complexity).
    *   **Reference:** `SystemOperations_And_Integrity_Architecture.md` (Sec 5.1), `TIM-PostgreSQLBackupDR_pgBackRest.md`.

*   **Threat 4.3: Ingestor flood overwhelming `raw.events` or `work_queue`.**
    *   **Asset:** Ingestion pipeline, processing capacity.
    *   **Impact:** Event loss (if queue overflows before persistence or DLQ), processing backlog.
    *   **Mitigation:**
        1.  **Rate Limiting:** On external-facing ingest endpoints (e.g., mobile ingest API).
        2.  **Systemd Resource Quotas for Ingestors.**
        3.  **Efficient `work_queue` Processing:** Scalable worker pattern.
        4.  **Backpressure:** If `work_queue` grows too large, system could signal ingestors to slow down (advanced).

*   **Threat 4.4: LLM API rate limiting or cost runaway causing dependent agent failure.**
    *   **Asset:** Availability of LLM-dependent agent features.
    *   **Impact:** Agents fail or become non-responsive. Unexpected high costs.
    *   **Mitigation:**
        1.  **Agent-Level Budgeting & Throttling:** Agents monitor their API usage/cost.
        2.  **Exponential Backoff & Retries for API calls.**
        3.  **LLM Router Fallbacks:** Switch to alternative (local or cheaper cloud) models if primary is unavailable or over budget.
        4.  Clear alerting to user on cost/rate limit issues.
    *   **Reference:** `TIM-LLMResourceOrchestration.md`.

### 2.5. Elevation of Privilege (Gaining Unauthorized Capabilities)

*   **Threat 5.1: Vulnerability in an Exocortex agent or its dependency allows attacker to gain higher privileges on the host system.**
    *   **Asset:** Host system integrity, user data.
    *   **Impact:** Full system compromise.
    *   **Mitigation:**
        1.  **Least Privilege Principle:** Agents run as dedicated unprivileged users. DB roles are minimal.
        2.  **Process Sandboxing (`seccomp-bpf`, AppArmor):** Strictly limit what agents can do. (Crucial)
        3.  **Regular Dependency Scanning & Updates:** `cargo audit` (Rust), `vulnix` (NixOS). Patch promptly.
        4.  **Secure Coding Practices.**
        5.  **Minimize Attack Surface:** Agents should not expose unnecessary network ports.
    *   **Reference:** `TIM-ProcessSandboxing.md`, `TIM-ReleaseEngineeringCICD.md`.

*   **Threat 5.2: SQL injection vulnerabilities in Exocortex components that construct SQL queries.**
    *   **Asset:** Database integrity and confidentiality.
    *   **Impact:** Data theft, modification, denial of service.
    *   **Mitigation:**
        1.  **Parameterized Queries/Prepared Statements:** Use exclusively (e.g., `sqlx` in Rust promotes this). **Never construct SQL by string concatenation with untrusted input.**
        2.  **Input Validation:** Validate all data used in queries, even if parameterized.

*   **Threat 5.3: Path traversal or command injection vulnerabilities if agents handle user-provided/external paths or execute external commands unsafely.**
    *   **Asset:** Filesystem integrity, arbitrary code execution.
    *   **Impact:** Data leakage, system compromise.
    *   **Mitigation:**
        1.  **Path Validation & Canonicalization:** Normalize and validate all path inputs. Confine file access to pre-approved base directories.
        2.  **Avoid Shelling Out with Untrusted Input:** If external commands must be run, do not pass unvalidated user input directly as command arguments. Use library functions that handle argument arrays safely.
        3.  **Contextual Escaping:** If generating scripts or commands that are then executed, ensure proper escaping of all variables.

## 3. Ongoing Security Practices

*   **Regular Software Updates:** Keep NixOS, PostgreSQL, all extensions, and all agent software dependencies up-to-date using a structured patching process.
*   **Periodic Permission Reviews:** Regularly review PostgreSQL roles, filesystem permissions, and AppArmor/seccomp profiles to ensure they still adhere to least privilege.
*   **Monitoring & Alerting:** Actively monitor system logs (Loki), metrics (Prometheus), and Exocortex meta-events for suspicious activity, security-related errors, or policy violations. Set up alerts for critical security events.
*   **Security Self-Assessment:** Periodically review this threat model against system changes and new known vulnerabilities. Conduct vulnerability scans if resources permit.
*   **Incident Response Plan (Conceptual):** Have a basic plan for how to respond to a suspected security incident (isolate, analyze, recover, learn).

This threat model provides a framework for systematically considering and mitigating security risks in the Exocortex. It should be a living document, updated as the system evolves.

