# System Operations & Integrity Architecture: Ensuring a Resilient and Maintainable Exocortex

*   **Version:** 2.0
*   **Date:** 2025-07-17
*   **Implementation Status:** ✅ **OPERATIONAL** - Satellite orchestration operational, journald heartbeat pattern working, StatefulStreamProcessor interface implemented, basic security in place
*   **Purpose:** This document describes the operational architecture for ensuring the Sinex system's health, security, and maintainability. It focuses on the working operational patterns rather than planned features.
*   **Scope:** Covers operational observability, security measures, and service orchestration as currently implemented.

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

## 2. Operational Observability Architecture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Unified observability through journald integration

The satellite constellation implements an elegant observability pattern where systemd's journald serves as the universal collection point for all operational data.

### 2.1. Journald Heartbeat Pattern

*   **Structured Logging:** ✅ **OPERATIONAL** - Satellites emit structured JSON logs to stdout/stderr, automatically captured by systemd
*   **Journald Bridge:** ✅ **OPERATIONAL** - `sinex-system-satellite` monitors journald and ingests all Sinex-related logs as events
*   **Automatic Service Discovery:** ✅ **OPERATIONAL** - systemd service metadata automatically tracked through journal entries
*   **Health Inference:** ✅ **OPERATIONAL** - Regular log output creates implicit heartbeat pattern without explicit heartbeat events
*   **Meta-Observability:** ✅ **OPERATIONAL** - System health becomes queryable Sinex data, enabling self-analysis and alerting
*   **Unified Monitoring:** ✅ **OPERATIONAL** - All system components (PostgreSQL, Redis, satellites) monitored through single journald channel

### 2.2. Operational Metrics

*   **Satellite Service Health:** ✅ **OPERATIONAL** - systemd service status, restart counts, resource usage per satellite
*   **Event Processing Pipeline:** ✅ **OPERATIONAL** - Redis Streams lag, consumer group positions, checkpoint ages, DLQ sizes
*   **Ingestion Hub Performance:** ✅ **OPERATIONAL** - ingestd throughput, batch sizes, validation failures, gRPC latency
*   **Automaton Processing:** ✅ **OPERATIONAL** - Processing rates, error rates, checkpoint intervals per automaton

## 3. Security and Service Orchestration Architecture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Basic security and service orchestration working

### 3.1. Service Orchestration

*   **Satellite Service Orchestration:** ✅ **OPERATIONAL** - All satellites run as independent systemd services with NixOS-managed configuration
*   **Resource Isolation:** ✅ **OPERATIONAL** - Per-service memory and CPU limits enforced through systemd cgroups
*   **Service Dependencies:** ✅ **OPERATIONAL** - Proper startup ordering ensures ingestd and Redis available before satellites
*   **Configuration Management:** ✅ **OPERATIONAL** - Declarative NixOS configuration with service orchestration

### 3.2. Basic Security Measures

*   **Process Isolation:** ✅ **OPERATIONAL** - systemd service isolation with independent user contexts
*   **Local-First Architecture:** ✅ **OPERATIONAL** - All data processing occurs locally, no external API dependencies
*   **Filesystem Permissions:** ✅ **OPERATIONAL** - Appropriate file system permissions and socket access controls
*   **Database Access Control:** ✅ **OPERATIONAL** - PostgreSQL access controlled through Unix socket authentication

## 4. Data Integrity and Configuration Management

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Basic data integrity and configuration management working

### 4.1. Configuration Management

*   **NixOS Configuration:** ✅ **OPERATIONAL** - Entire NixOS configuration (flakes, modules) is version-controlled in Git
*   **Declarative Services:** ✅ **OPERATIONAL** - All satellite services defined declaratively in NixOS configuration
*   **Service Configuration:** ✅ **OPERATIONAL** - Consistent configuration management across all services
*   **Reproducible Builds:** ✅ **OPERATIONAL** - Nix ensures reproducible service deployments

### 4.2. Data Integrity

*   **PostgreSQL Constraints:** ✅ **OPERATIONAL** - Database constraints (PK, FK, UNIQUE) implemented
*   **Event Schema Validation:** ✅ **OPERATIONAL** - `pg_jsonschema` validation for event payloads
*   **ULID Consistency:** ✅ **OPERATIONAL** - Time-ordered ULID primary keys ensure data consistency
*   **Immutable Event Log:** ✅ **OPERATIONAL** - Raw events table provides immutable audit trail

## 5. Performance and Scalability Architecture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Core performance and scalability patterns working

### 5.1. Scalability Patterns

*   **Horizontal Scaling:** ✅ **OPERATIONAL** - Redis consumer groups enable horizontal scaling of automaton processing
*   **Asynchronous Processing:** ✅ **OPERATIONAL** - Batch processing and asynchronous operations implemented
*   **TimescaleDB Partitioning:** ✅ **OPERATIONAL** - Automatic time-based partitioning for efficient queries
*   **Checkpoint-Based Recovery:** ✅ **OPERATIONAL** - Reliable state management enables service scaling

### 5.2. Schema Evolution

*   **JSONB Flexibility:** ✅ **OPERATIONAL** - Event payloads use JSONB for schema flexibility
*   **SQL Migrations:** ✅ **OPERATIONAL** - Database migrations using sqlx for schema evolution
*   **Event Schema Validation:** ✅ **OPERATIONAL** - GitOps-driven schema validation enables evolution
*   **Immutable Event Log:** ✅ **OPERATIONAL** - Raw events preserve history during schema changes

## 6. Operational Excellence Summary

### 6.1. Operational Architecture Benefits

**Zero-Configuration Observability:**
- Journald-based monitoring with automatic service discovery
- Real-time health inference from service activity
- Self-monitoring through event stream integration
- Unified logging without external dependencies

**Service Orchestration:**
- Declarative NixOS configuration management
- Independent satellite services with proper isolation
- Automatic service dependencies and startup ordering
- Resource management through systemd cgroups

**Data Integrity:**
- Immutable event log with complete audit trail
- Database constraints and schema validation
- Time-ordered ULID primary keys for consistency
- Version-controlled configuration management

**Performance and Scalability:**
- Horizontal scaling through Redis consumer groups
- TimescaleDB partitioning for efficient queries
- Asynchronous processing with checkpoint recovery
- JSONB flexibility for schema evolution

This operational architecture provides a robust foundation for the Sinex system, focusing on proven patterns that are currently operational rather than speculative future features.

