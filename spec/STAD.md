# Sinex Exocortex: System Technical Architecture Document (STAD) v1.1

> **📊 CURRENT IMPLEMENTATION MATURITY**:
>
> - 🏗️ **Foundation Phase** (~20% of vision complete)
> - ✅ **Event substrate operational** - PostgreSQL + TimescaleDB + ULID
> - ✅ **Basic ingestion working** - File/terminal/window events  
> - ✅ **Worker framework functional** - Promotion queue + agents
> - ⚠️ **Limited validation** - Schema registry exists, enforcement weak
> - ❌ **Missing major components** - PKM, AI/LLM integration, semantic search, user interfaces

**(A High-Level Architectural Map linking to detailed Architectural Modules, TIMs, and ADRs)**

## Preamble

- **Purpose:** This System Technical Architecture Document (STAD) provides a concise, high-level map of the Sinnix Exocortex system's architecture. It introduces the major architectural domains and directs readers to dedicated Architectural Module documents for comprehensive details, and to Technical Implementation Modules (TIMs) and Architectural Decision Records (ADRs) for specific implementation specifications and design choices.
- **Scope:** This STAD focuses on orienting the reader to the overall structure and key architectural pillars.
- **Relationship to Other Canonical Documents:**
  - **Vision/Charter (`VISION.md`):** The "Why" and high-level "What."
  - **SADI (`SADI.md`):** The central meta-document linking all documentation.
  - **Architectural Modules (`docs/arch_modules/`):** The primary detailed architectural descriptions for each domain.
  - **ADRs (`docs/adr/`):** The "Why these choices were made."
  - **TIMs (`docs/tims/`):** The "How to build specific parts."
- **Conventions:** Links guide to deeper information.

## 1. Exocortex System Overview

### 1.1. Mission and Core Architectural Goals

The Sinnix Exocortex aims to be a "sentient archive," augmenting human intellect by comprehensively capturing digital experiences and subjective states, structuring this data emergently, and enabling powerful query, analysis, and agentic assistance, all while prioritizing user agency and data sovereignty.

- **Full Vision:** `VISION_OR_CHARTER.md`, `SADI.md`.

### 1.2. High-Level Architectural Diagram

*(Imagine a C4 Level 1/2 diagram here, or refer to one in SADI.md, showing: Data Substrate, Ingestion & Telemetry, Agentic Ecosystem, User Interaction & Query, System Operations & Integrity).*
The system is built on these five interconnected architectural domains.

### 1.3. Key Architectural Principles

Core principles include Universal Capture, Emergent Structure, User Agency, Continuous Context, Feedback as Fuel, Meta-Observability, Local-First, and Security by Design.

- **Details:** `VISION_OR_CHARTER.md` (Part I), `SADI.md` (Part I).

## 2. Core Data Substrate Architecture

The Data Substrate is the Exocortex's foundation, built on PostgreSQL (enhanced by TimescaleDB, `pgx_ulid` ([ADR-001](docs/adr/ADR-001-PrimaryKeyStrategy.md)), `pgvector`, `pg_jsonschema`)). It features an immutable event log (`raw.events`) as the source of truth, with versioned JSON Schemas for payload validation (`sinex_schemas.event_payload_schemas`). A PostgreSQL-based queue (`sinex_schemas.promotion_queue`) with worker polling ([ADR-002](docs/adr/ADR-002-EventProcessingNotificationMechanism.md)) manages event processing, supported by a central Dead Letter Queue (`core.dead_letter_queue`). Structured knowledge emerges in `core_entities` and `core_entity_relations` (the Knowledge Graph), versioned `core_artifacts` (with Yjs CRDTs for PKM notes - [ADR-004](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)), universal `core_tags`, and semantic `artifact_embeddings` (HNSW index - [ADR-005](docs/adr/ADR-005-VectorIndexTypePgvector.md), CPU-first - [ADR-007](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)). Large binary blobs are managed by `git-annex` with metadata in `core_blobs`.

- **Detailed Architecture:** `[DataSubstrate_Architecture.md](docs/arch_modules/DataSubstrate_Architecture.md)`
- **Key Implementation Details:** `TIM-PrimaryKeyImplementation.md`, `TIM-TimescaleDBConfiguration.md`, `TIM-EventSchemaRegistry.md`, `TIM-EventIngestionProcessing.md`, `TIM-GitAnnexLargeFileMgmt.md`.

## 3. Ingestion & Telemetry Architecture

The Ingestion Layer is the system's sensory network, capturing diverse data streams into `raw.events` based on principles of layered fidelity and ambient capture. Ingestors are managed as systemd services, designed for idempotency, and use local DLQs as a fallback. Data is captured from the Desktop Environment (Hyprland IPC/Plugin - [ADR-003](docs/adr/ADR-003-HyprlandCompositorIntegrationPath.md); AT-SPI2; `evdev`; Clipboard), specific Applications (Browser extension + native host; Terminal via Atuin, Asciinema, Kitty RC - [ADR-008](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md); Neovim plugin; Email), the Filesystem (watchers + `git-annex` integration), user's PKM (DB-native with Yjs - [ADR-004](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)), Audio/Visual streams (PipeWire), Mobile/IoT devices (MQTT, ESP32 reference), and user-logged Meta-Cognitive/Subjective states.

- **Detailed Architecture:** `[IngestionArchitecture_And_TelemetrySources.md](docs/arch_modules/IngestionArchitecture_And_TelemetrySources.md)`
- **Key Implementation Details:** TIMs in `docs/tims/ingestors/`.

## 4. Agentic Ecosystem & AI Integration Architecture

The Agentic Ecosystem drives intelligent processing and automation. Agents are modular, event-driven, and user-controllable, registered in `sinex_schemas.agent_manifests` and run as systemd services. LLM integration is central, supporting local (Ollama) and remote models registered in `core_llm_models`. A Prompt Registry (`core_prompts`) manages versioned prompt templates (sourced from Git YAMLs), with frameworks for A/B testing and canary deployments. An LLM Router directs requests based on prompt needs, model capabilities, cost, and privacy. Complex agentic flows can be built using DSPy/LangGraph, with persistence for their states. Archetypal agents handle tasks like data processing, analysis, integration, and system maintenance.

- **Detailed Architecture:** `[AgenticEcosystem_Architecture.md](docs/arch_modules/AgenticEcosystem_Architecture.md)`
- **Key Implementation Details:** `TIM-AgentManifestManagement.md`, `TIM-LLMResourceOrchestration.md`.

## 5. User Interaction, Query & Feedback Architecture

This domain defines how users engage with the Exocortex. Primary interaction channels include a Neovim plugin (`sinnix-nvim` with LSP/RPC backend communication and Yjs for PKM - [ADR-004](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)), the `exo` CLI, and Grafana dashboards (future Web UI). An "Inbox Workflow" helps triage actionable items. Query capabilities are layered: direct SQL, a simplified `exo` syntax, and hybrid search (combining `pgvector` semantic search with PostgreSQL FTS, using Reciprocal Rank Fusion). Understanding is woven through explicit data relations (`event_relations`, `core_entity_relations`), agent-driven narratives (`meta.narrative_generated`), and flexible `event_annotations`. The system supports cognitive feedback loops and self-modeling by making actions and subjective states queryable. Derived semantic layers (activity segments, intents, composite actions) provide richer user context on top of lean `raw.events`.

- **Detailed Architecture:** `[UserInteraction_And_Query_Architecture.md](docs/arch_modules/UserInteraction_And_Query_Architecture.md)`
- **Key Implementation Details:** `TIM-NeovimPluginIntegration.md`, `TIM-HybridSearchPostgreSQL.md`.

## 6. System Operations, Integrity & Evolution Architecture

This domain ensures the Exocortex is robust, secure, and maintainable. Meta-Observability treats system operational data as first-class Exocortex events, monitored via Prometheus/Grafana. Security includes layered access control (PG roles, systemd users), encryption (at-rest LUKS/`git-annex`/`pgsodium`; in-transit TLS; secrets via `agenix` - [ADR-006](docs/adr/ADR-006-NixOSSecretsManagementTool.md)), user consent mechanisms, and process sandboxing (`seccomp-bpf`, AppArmor). Backup and Disaster Recovery rely on `pgBackRest` for PostgreSQL (PITR, automated test restores) and `git-annex` multi-remote strategies for blobs, with NixOS configuration versioned in Git. Data integrity is maintained via DB constraints, `pg_jsonschema`, and link/orphan checks. Performance, scalability, and schema evolution are managed through DB tuning, agent design, and versioned SQL migrations. Multi-device coherence (future) will use local-first principles with tools like LiteFS/Syncthing and CRDTs. Release Engineering uses Nix Flakes for reproducible builds and GitHub Actions for CI/CD (checks, tests, security scans, artifact publishing to Cachix).

- **Detailed Architecture:** `[SystemOperations_And_Integrity_Architecture.md](docs/arch_modules/SystemOperations_And_Integrity_Architecture.md)`
- **Key Implementation Details:** `TIM-ObservabilityStackSetup.md`, `TIM-SecretsManagementAgenix.md`, `TIM-PostgreSQLSecurityEncryption.md`, `TIM-ProcessSandboxing.md`, `TIM-PostgreSQLBackupDR_pgBackRest.md`, `TIM-ReleaseEngineeringCICD.md`, `TIM-MultiDeviceSyncArchitecture.md`.

## 7. Conclusion: Synthesized Technical Strategy

The Sinnix Exocortex architecture provides a modular, PostgreSQL-centric, local-first platform for comprehensive data capture and intelligent processing. It integrates a multi-modal ingestion layer, a robust data substrate with advanced knowledge representation, an AI-powered agentic ecosystem, and rich user interaction capabilities. Operationalized with NixOS, it emphasizes user agency, data integrity, and security, aiming to create a resilient and evolvable "sentient archive" for lifelong cognitive augmentation. Detailed specifications are found in the linked Architectural Modules, TIMs, and ADRs.
