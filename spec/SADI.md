# Sinex Exocortex: System Architecture & Document Interrelation (SADI) - v0.2

> **📊 IMPLEMENTATION STATUS**: Core database substrate ✅ **IMPLEMENTED**, Basic event processing ✅ **IMPLEMENTED**, Advanced features ❌ **PLANNED** - see detailed status in architectural modules.

**(Reflecting Modular Documentation Structure)**

**Preamble**

This System Architecture & Document Interrelation (SADI) guide serves as the central linking document and high-level overview for the Sinex Exocortex project. Its primary purpose is to:

1. Articulate the overall documentation strategy and the role of each canonical document.
2. Provide a concise summary of the core architectural pillars and key decisions (linking to detailed ADRs).
3. Act as an index to the main project documents, including the Vision, System Technical Architecture Document (STAD), Architectural Modules, Technical Implementation Modules (TIMs), and Architectural Decision Records (ADRs).

This document is intended for all contributors, including human maintainers who need to understand the system's structure and evolution, and AI development agents (like Claude) that require contextual guidance for implementation tasks. It helps navigate the comprehensive but modularized documentation set.

This is a living document. It will be updated as major architectural decisions are refined, new components are integrated, or the documentation structure itself evolves.

**Part I: Core Architectural Pillars & Key Decisions (Summary)**

*(This section provides a very brief summary of the chosen architecture, largely by referencing the SADI v0.1 content that summarized key decisions. Now, it primarily points to the standalone ADRs and the STAD for the current overview.)*

**1. Overall System Philosophy & Goals**

The Sinex Exocortex is conceived as a "sentient archive" – a comprehensive, user-owned cognitive habitat designed to combat digital amnesia and augment human intellect. It aims for universal capture, emergent structuring, powerful query and agentic assistance, prioritizing user agency and data sovereignty.

* **Primary Reference for Vision & Philosophy:** `VISION.md`

**2. Summary of Current Chosen Technical Stack & Key Architectural Decisions**

The Exocortex is built on a local-first, robust, and extensible technical foundation, primarily targeting a single-host Linux system managed with NixOS. Key technology choices and architectural decisions include:

* **Host Environment:** NixOS for reproducible, declarative management.
* **Primary Datastore:** PostgreSQL 15+ with extensions:
  * **TimescaleDB:** For `raw.events` hypertable partitioning.
  * **`pgx_ulid`:** For ULID primary keys ([ADR-001](docs/adr/ADR-001-PrimaryKeyStrategy.md)).
  * **`pgvector`:** For vector embeddings (HNSW index - [ADR-005](docs/adr/ADR-005-VectorIndexTypePgvector.md); CPU-first - [ADR-007](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)).
  * **`pg_jsonschema`:** For in-database event payload validation.
  * **`pgsodium`:** For field-level encryption.
* **Primary Development Language (Backend/Agents):** Rust.
* **Secrets Management:** `agenix` ([ADR-006](docs/adr/ADR-006-NixOSSecretsManagementTool.md)).
* **Large File (Blob) Management:** `git-annex` with metadata in `core_blobs`.
* **Core Data Flow:** Immutable `raw.events` -> PostgreSQL-based `sinex_schemas.promotion_queue` -> Agent-driven processing (Polling primary - [ADR-002](docs/adr/ADR-002-EventProcessingNotificationMechanism.md)).
* **Desktop Integration:** Hyprland IPC (primary), C++ Plugin (future) - [ADR-003](docs/adr/ADR-003-HyprlandCompositorIntegrationPath.md); Layered Terminal Capture (Atuin, Asciinema, Kitty RC) - [ADR-008](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md).
* **PKM Content Handling:** Database-native with Yjs CRDTs ([ADR-004](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)).

* **For a concise architectural overview and links to detailed specifications, see:** `STAD.md`
* **For detailed rationale on specific decisions, see the individual files in:** `docs/adr/`

**Part II: Canonical Document Mapping & Interrelation (v0.2 Structure)**

This section clarifies the role of each primary project document in the new modular structure.

1. **`VISION.md` (Refactored Vision Document - e.g., "Sinnix Exocortex: The Sentient Archive - A Vision for Cognitive Sovereignty v3.0")**
    * **Role:** The **foundational "Why" and high-level "What"** of the project. Articulates:
        * Core philosophy, manifesto, pledges, and human-centric design context.
        * Conceptual overview of key user-facing capabilities (Living Document, PKM, Universal Capture, Agentic Partnership, Query & Reflection concepts).
        * The project's sustaining ethos (meta-observability, security commitments, evolution) and long-term horizons.
        * Includes philosophical essays.
    * It answers "Why are we building this?" and "What are we aiming for conceptually?"
    * **Target Audience:** All stakeholders, new contributors, users seeking to understand the project's soul.
    * **Key Change:** Technical and deep architectural details are moved out to STAD, Architectural Modules, and TIMs.

2. **`STAD.md` (System Technical Architecture Document v1.x)**
    * **Role:** The **high-level architectural map** of the Exocortex. It replaces the detailed, monolithic "Unified Technical Implementation Guide."
        * Provides a top-level system overview and diagram.
        * Briefly introduces each major architectural domain/pillar (Data Substrate, Ingestion, Agentic Ecosystem, User Interaction, System Operations).
        * Serves as a primary index by linking directly to the more detailed Architectural Module documents for each domain.
        * Includes appendices indexing all ADRs and TIMs.
    * It answers "What are the major architectural components and how do they broadly fit together?"
    * **Target Audience:** Developers needing an architectural overview, system architects, AI agents needing a starting point for technical context.

3. **`docs/arch_modules/` (Architectural Module Documents - 5 key files)**
    * `DataSubstrate_Architecture.md`
    * `IngestionArchitecture_And_TelemetrySources.md`
    * `AgenticEcosystem_Architecture.md`
    * `UserInteraction_And_Query_Architecture.md`
    * `SystemOperations_And_Integrity_Architecture.md`
    * **Role:** Each document provides a **comprehensive architectural deep-dive into a specific domain** of the Exocortex. They explain the "what," "why," and "how components interrelate" *within that domain*.
        * They consolidate architectural descriptions previously in the old UG or Vision document.
        * They contain more detailed architectural diagrams and design rationale than the STAD overviews.
        * They link extensively to specific TIMs for implementation details and relevant ADRs for decisions.
    * They answer "What is the detailed architecture of this specific part of the system?"
    * **Target Audience:** Developers working within or integrating with a specific architectural domain, AI agents needing detailed context for a particular area.

4. **`docs/tims/` (Technical Implementation Modules - Numerous files)**
    * **Role:** Granular, self-contained documents providing **detailed technical specifications for implementing a single component, feature, or core concern.** This is where DDLs, code examples, specific configurations, API details, and step-by-step procedures reside.
    * They answer "Exactly how is this specific piece implemented or configured?"
    * **Target Audience:** Developers implementing or debugging specific components, AI agents generating code or tests for a specific module.

5. **`docs/adr/` (Architectural Decision Records - Standalone files, e.g., `ADR-001-PrimaryKeyStrategy.md`)**
    * **Role:** Each ADR documents a **single significant architectural decision**, including its context, discussed options, the decision itself, rationale, and consequences. They are the primary source for understanding *why* certain technical choices were made.
    * They answer "Why was this specific architectural path chosen over alternatives?"
    * **Target Audience:** All developers, architects, maintainers needing to understand design history and rationale.

6. **`SADI.md` (This Document)**
    * **Role:** As described in its preamble - the central map and high-level summary.

7. **`CDDG.md` (Claude-Driven Development Guide)**
    * **Role:** Outlines the **methodology and best practices for using Claude (or similar AI) as an autonomous agent for TDD-based development.** Details the TDD loop, context provision, test generation strategies.
    * It answers "How does Claude build the system defined by the Vision, STAD, Arch Modules, and TIMs?"
    * **Key Change:** Now references the new modular document structure for providing context to Claude.
    * **Target Audience:** Primarily the AI development agent (Claude) and human overseers of the CDD process.

8. **`GLOSSARY.md` (Project-Wide Glossary)**
    * **Role:** Centralized definitions for all key Exocortex terms (conceptual and technical).
    * **Target Audience:** All contributors.

9. **`docs/diagrams/` (Architectural Diagrams)**
    * **Role:** Contains visual representations (PlantUML, Mermaid, images) of system architecture, data flows, component interactions. Referenced from STAD and Architectural Modules.

**Part III: Index of Key Architectural Modules, ADRs, and TIMs**

*(This SADI acts as the master index. For brevity in this generated SADI, I will list the Architectural Modules and the ADRs. A full, detailed index of all TIMs would be very long here and is better maintained as an appendix within the STAD or as a separate TIM_INDEX.md file that this SADI could link to.)*

**3.1. Core Architectural Module Documents (`docs/arch_modules/`)**

* `DataSubstrate_Architecture.md`: Details storage, events, structuring, knowledge representation.
* `IngestionArchitecture_And_TelemetrySources.md`: Details the sensory network and data capture.
* `AgenticEcosystem_Architecture.md`: Details intelligent agents and LLM integration.
* `UserInteraction_And_Query_Architecture.md`: Details UIs, query mechanisms, feedback.
* `SystemOperations_And_Integrity_Architecture.md`: Details operations, security, resilience.

**3.2. Key Architectural Decision Records (`docs/adr/`)**

* `[ADR-001-PrimaryKeyStrategy.md](docs/adr/ADR-001-PrimaryKeyStrategy.md)`: ULIDs via `pgx_ulid`.
* `[ADR-002-EventProcessingNotificationMechanism.md](docs/adr/ADR-002-EventProcessingNotificationMechanism.md)`: PG Queue Table + Polling.
* `[ADR-003-HyprlandCompositorIntegrationPath.md](docs/adr/ADR-003-HyprlandCompositorIntegrationPath.md)`: Hyprland IPC first, C++ Plugin later.
* `[ADR-004-PKMNoteContentManagementAndSync.md](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)`: PKM DB-Native with Yjs CRDTs.
* `[ADR-005-VectorIndexTypePgvector.md](docs/adr/ADR-005-VectorIndexTypePgvector.md)`: HNSW index for `pgvector`.
* `[ADR-006-NixOSSecretsManagementTool.md](docs/adr/ADR-006-NixOSSecretsManagementTool.md)`: `agenix` for NixOS secrets.
* `[ADR-007-LargeScaleVectorSearchStrategy.md](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)`: `pgvector` CPU first, GPU DB later.
* `[ADR-008-TerminalActivityCaptureStrategy.md](docs/adr/ADR-008-TerminalActivityCaptureStrategy.md)`: Layered approach for terminal capture.

**3.3. Technical Implementation Modules (`docs/tims/`)**
A comprehensive suite of TIMs provides detailed implementation specifications for individual components and features. These are organized into subdirectories corresponding to the architectural modules (e.g., `data_substrate/`, `ingestors/`, `operations/`).

* **Refer to `STAD.md Appendix B` or a dedicated `docs/TIM_INDEX.md` for a full, categorized list of TIMs.**

This restructured SADI provides a clearer map to the now modularized documentation suite, guiding contributors to the appropriate level of detail they require.
