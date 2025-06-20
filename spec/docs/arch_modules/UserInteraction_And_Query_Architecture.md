# User Interaction & Query Architecture: The Bridge to Self

*   **Version:** 1.1
*   **Date:** 2025-01-19
*   **Implementation Status:** 🚧 **10% IMPLEMENTED** - Basic CLI exists, advanced interfaces and query systems not implemented
*   **Purpose:** This document describes the architectural principles and high-level design of how users interact with Sinex, query its vast data stores, and how the system facilitates understanding, narrative construction, and self-modeling. It outlines the architecture of user interfaces, query mechanisms, and feedback loops.
*   **Primary Sources:** Vision Document Part V; STAD (System Technical Architecture Document) Part IV (Retrieval sections like 17, 19).

## 1. Introduction & UI/UX Philosophy (Vision V.1)

### 1.1. Bridging the User and the Sentient Archive

Sinex's value is realized through effective user interaction. This layer translates the system's comprehensive data capture and agentic capabilities into an intuitive, empowering extension of the user's mind. The architecture aims to support a seamless flow between capturing thoughts, retrieving information, discovering connections, and reflecting on personal data.

### 1.2. Core UI/UX Principles Guiding Interaction Design

The design of all user-facing aspects is guided by principles outlined in the Vision Document (Part I.3 and V.1), including:
*   **Frictionless Capture, Always:** Minimizing barriers to logging thoughts, data, and meta-cognitive states.
*   **Context is King:** Presenting information relevant to the user's current task and focus.
*   **Discoverability & Learnability:** Making system capabilities accessible and understandable.
*   **User in Control:** Ensuring user agency over data, automation, and system behavior.
*   **Hackability & Extensibility:** Allowing users to customize and extend their Sinex.
*   **Performance as a Feature:** Ensuring responsive interfaces.
*   **Aesthetics of Clarity and Calm:** Designing interfaces that reduce cognitive load.
*   **Support for Neurodiversity and Varied Cognitive Styles:** Catering to different ways of thinking and working.

## 2. Primary Interaction Channels: Architectural Overview

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Basic CLI implemented, advanced interfaces not developed

Sinex offers multiple channels for interaction, each tailored to different needs and workflows.

### 2.1. Neovim Plugin (`sinex-nvim`) (Vision V.1.2)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Neovim plugin not developed

The Neovim plugin would serve as the primary power-user cockpit for deep, keyboard-driven interaction with Sinex.
*   **Architectural Role:** Would provide seamless integration within the Neovim environment for PKM, Living Document interaction, querying, and command execution.
*   **Key Architectural Features:** ❌ **NOT IMPLEMENTED**
    *   **Unified Search & Navigation (Telescope.nvim):** Would provide custom Telescope pickers for finding and opening PKM notes, web archives, raw events, Living Document nodes, blobs, tags, and entities.
    *   **Contextual Panels & Floating Windows:** Would dynamically display backlinks, outlinks, related raw events, semantically similar artifacts, or agent suggestions.
    *   **Living Document Interaction:** Would provide dedicated buffer type for stream-of-consciousness input and editing.
    *   **PKM Note Editing (Yjs-based, ADR-004):** Would manage fetching Yjs state/deltas from the backend and applying local edits.
    *   **Sinex Commands (`:Sinex...`):** Would provide comprehensive suite for manual event logging, agent triggering, PKM management, and system queries.
    *   **Visual Cues:** Would include status line integration, virtual text for annotations, highlighting for unresolved links.
*   **Communication with Sinex Backend:** ❌ **NOT IMPLEMENTED**
    *   **Custom Sinex Language Server (LSP):** Would be preferred method for rich semantic interactions.
    *   **Msgpack-RPC to Helper Process:** Would be alternative for specific tasks or interfacing with non-LSP backend components.
    *   **`exo` CLI Invocation:** Would be used for simpler, non-interactive tasks.
*   **Referenced TIMs:**
    *   `[TIM-NeovimPluginIntegration.md](docs/tims/ingestors/pkm_email_nvim/TIM-NeovimPluginIntegration.md)` for plugin architecture, LSP/RPC patterns, Yjs integration, Treesitter usage.
    *   `[ADR-004-PKMNoteContentManagementAndSync.md](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)` for PKM strategy.

### 2.2. `exo` Command-Line Interface (CLI) (Vision V.1.3)

> **✅ IMPLEMENTATION STATUS: BASIC WORKING** - Basic CLI implemented with limited functionality

The `exo` CLI is the scriptable and universally accessible backbone for all Sinex interactions.
*   **Architectural Role:** Provides comprehensive functionality for power users, automation scripts, and integration with other tools. It directly interacts with the Sinex backend (PostgreSQL database, agent control mechanisms).
*   **Architectural Design:**
    *   **Subcommand Structure:** ✅ **BASIC WORKING** - Basic subcommands implemented (e.g., `query`).
    *   **Output Formatting:** ❌ **NOT IMPLEMENTED** - Advanced output formatting options not implemented.
    *   **Shell Completions:** ❌ **NOT IMPLEMENTED** - Shell completions not implemented.
    *   **`fzf` Integration:** ❌ **NOT IMPLEMENTED** - Interactive selection not implemented.
*   **Referenced TIMs:**
    *   A future `TIM-ExoCLIReferenceAndDesign.md` (derived from UG App D and actual CLI implementation using libraries like `clap` for Rust) would detail all commands, options, and output formats.

### 2.3. Dashboards (Grafana; Future Web UI) (Vision V.1.4)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Dashboard infrastructure not implemented

Dashboards provide broader visual overviews of Sinex data and system health.
*   **Grafana Architecture:** ❌ **NOT IMPLEMENTED**
    *   **Role:** Would be primary tool for visualizing time-series metrics and logs.
    *   **Data Sources:** Would connect to Prometheus, Loki, and directly to PostgreSQL for querying Sinex tables.
    *   **Key Dashboards:** Would include personal analytics, knowledge graph metrics, system & agent health, LLM usage/cost.
    *   **Provisioning:** Would be provisioned declaratively via JSON models in NixOS configuration.
    *   *Referenced TIMs:* `[TIM-ObservabilityStackSetup.md](docs/tims/operations/TIM-ObservabilityStackSetup.md)` for Grafana setup and data source configuration.
*   **Future Web UI/Canvas Architecture (Conceptual):** ❌ **NOT IMPLEMENTED**
    *   **Role:** Would be a more interactive, read-write web interface for richer data exploration and interaction beyond Grafana's capabilities.
    *   **Potential Features:** Would include full graph visualization and navigation, interactive timelines, rich Living Document editing interface, mobile-friendly quick capture and query.
    *   **Technology Stack (Speculative):** Would use frontend frameworks with backend API and WebSocket for real-time updates.

### 2.4. Inbox Workflow Architecture (Vision V.1.5)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Inbox workflow not implemented

A core interaction pattern for managing new and attention-requiring items within Sinex.
*   **Architectural Role:** Would provide a centralized, dynamic, query-driven view for triaging and processing diverse items like newly captured/unprocessed PKM notes or web archives, agent-extracted task proposals, system errors/alerts, agent suggestions, and unresolved links.
*   **Architectural Implementation:** ❌ **NOT IMPLEMENTED**
    *   Would be a set of pre-defined (and user-customizable) queries against `core_artifacts`, `raw.events`, `agent_processing_dlq`, etc.
    *   Would be presented as a dedicated view in Neovim, `exo inbox` CLI command, or future Web UI.
    *   User actions would generate new Sinex events, ensuring all triage decisions are captured and feed back into the system.

## 3. The Architecture of Query (Vision V.2, STAD Part IV)

> **🚧 IMPLEMENTATION STATUS: 20% IMPLEMENTED** - Basic SQL queries working, advanced query interfaces not implemented

Unlocking the value of Sinex relies on powerful and flexible query capabilities.

### 3.1. Layered Query Capabilities

The system offers multiple layers for querying, catering to different needs and skill levels.
*   **Direct SQL on PostgreSQL:**
    *   **Architectural Role:** ✅ **WORKING** - Provides ultimate power and flexibility for complex queries, data analysis, and custom reporting. Users can directly query `raw.events` and basic tables.
    *   **Key Features Leveraged:** ✅ **BASIC** - JSONB operators working. ❌ **NOT IMPLEMENTED** - Full-Text Search, GIS, advanced window functions, recursive CTEs, and `pgvector` operators not configured.
*   **Simplified Query Syntax (`exo` CLI & Neovim Interface):**
    *   **Architectural Role:** 🚧 **BASIC** - Basic `exo query` command implemented with limited functionality.
    *   **Supported Filters:** 🚧 **PARTIAL** - Basic temporal and source filtering working. ❌ **NOT IMPLEMENTED** - Advanced payload content filtering, semantic similarity, tag-based search, and graph traversal not implemented.

### 3.2. Hybrid Search Architecture (Vector + Full-Text + RRF)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Hybrid search system not implemented

Combines semantic vector search with traditional keyword-based full-text search for enhanced retrieval relevance.
*   **Vector Search Component (`pgvector`):** ❌ **NOT IMPLEMENTED**
    *   Embeddings would be stored in tables like `artifact_embeddings`.
    *   HNSW indexes would be used for ANN search on `embedding_vector` columns.
    *   Would support metadata filtering in conjunction with ANN search.
*   **Full-Text Search (FTS) Component (PostgreSQL FTS):** ❌ **NOT IMPLEMENTED**
    *   Generated `tsvector` columns would be indexed with GIN.
    *   Queries would use `plainto_tsquery` or `websearch_to_tsquery`. Ranking via `ts_rank_cd`.
*   **Reciprocal Rank Fusion (RRF):** ❌ **NOT IMPLEMENTED**
    *   Results from vector search and FTS would be combined using the RRF algorithm.
    *   Would be implemented as a PostgreSQL SQL function that takes keyword query and query embedding as input.
*   **Referenced TIMs & ADRs:**
    *   `[TIM-HybridSearchPostgreSQL.md](docs/tims/processing_retrieval/TIM-HybridSearchPostgreSQL.md)` for `pgvector` HNSW setup, FTS setup, and the RRF SQL function implementation.
    *   `[TIM-EmbeddingGenerationModels.md](docs/tims/processing_retrieval/TIM-EmbeddingGenerationModels.md)` for embedding generation.
    *   `[ADR-005-VectorIndexTypePgvector.md](docs/adr/ADR-005-VectorIndexTypePgvector.md)` and `[ADR-007-LargeScaleVectorSearchStrategy.md](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)`.

### 3.3. Knowledge Graph Querying Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Knowledge graph querying not implemented

The Sinex Knowledge Graph is primarily stored relationally in `core_entities` and `core_entity_relations`.
*   **Querying Methods:** ❌ **NOT IMPLEMENTED**
    *   **Recursive Common Table Expressions (CTEs):** Would be SQL-standard method for traversing hierarchical or graph-like data.
    *   **(Optional) Apache AGE Extension:** Would provide OpenCypher query language support for complex graph pattern matching if installed.
*   **Referenced TIMs:**
    *   `[TIM-PostgreSQL-AdvancedFeatures.md](docs/tims/data_substrate/TIM-PostgreSQL-AdvancedFeatures.md)` (Graph section) for Recursive CTE examples and AGE setup.
    *   TIMs for `core_entities` and `core_entity_relations` DDLs (e.g., `TIM-KnowledgeGraphSchema.md`).

## 4. Weaving Understanding: Architecture of Relations & Narratives (Vision V.3)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Relations and narrative systems not implemented

Sinex helps users and agents understand connections and stories within the data.

### 4.1. Explicit Event & Artifact Relations Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Relation tables not implemented

Multiple tables model different types of relationships:
*   **`event_relations`:** For rich, typed links specifically *between `raw.events` entries* or between raw events and other core artifacts/entities. Captures semantic relationships like "derives_from," "explains_context_of."
*   **`core_entity_relations`:** For typed links *between canonical entities* in `core_entities` (Knowledge Graph edges).
*   **`core_artifact_links`:** Primarily for links *parsed from PKM notes or web archives*, connecting `core_artifacts` entries (e.g., Wikilinks). Includes `target_identifier_text` (raw link) and `resolved_target_artifact_id`.
*   **Creation:** ❌ **NOT IMPLEMENTED** - Links would be created manually by the user (via UI/CLI) or by agents inferring relationships.
*   **Referenced TIMs:** TIMs containing DDLs for these specific relation tables (e.g., `TIM-KnowledgeGraphSchema.md`, `TIM-EventRelationsSchema.md`).

### 4.2. Agent-Driven Narrativization Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Narrative generation agents not implemented

LLM agents play a key role in synthesizing human-readable narratives from complex Sinex data.
*   **Mechanism:** ❌ **NOT IMPLEMENTED** - A narrativization agent would be triggered by user request or periodically.
*   **LLM Processing:** ❌ **NOT IMPLEMENTED** - The agent would query relevant data and task an LLM to generate a narrative.
*   **Output (`meta.narrative_generated` event):** ❌ **NOT IMPLEMENTED** - The resulting narrative would be logged as events and stored as artifacts.
*   **Integration:** ❌ **NOT IMPLEMENTED** - Narratives would become valuable artifacts for retrospectives and planning.

### 4.3. Generic Event Annotations Architecture (`event_annotations`)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Event annotation system not implemented

Provides a flexible mechanism for layering user and agent insights directly onto individual `raw.events` entries without altering the immutable event itself.
*   **Architectural Role:** ❌ **NOT IMPLEMENTED** - Would enable users to comment on events, mark importance, or add fleeting thoughts.
*   **`event_annotations` Table:** ❌ **NOT IMPLEMENTED** - Would link annotations to events with actor, type, content, and timestamp.
*   **Interaction:** ❌ **NOT IMPLEMENTED** - UIs would provide means to view, add, and filter annotations.
*   **Referenced TIMs:** A TIM like `TIM-EventAnnotationsSchema.md` (or as part of `TIM-EventSubstrateDDL.md`) for the DDL.

## 5. Cognitive Feedback Loops & Self-Modeling Architecture (Vision V.4)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Self-modeling systems not implemented

The interaction layer aims to close the loop between data capture, analysis, and user action, enabling instrumented self-modeling.
*   **Surfacing Patterns & Anomalies:** ❌ **NOT IMPLEMENTED** - UIs that proactively display trends and agents that monitor for deviations not implemented.
*   **Intentional Tracking & Goal Alignment:** ❌ **NOT IMPLEMENTED** - Goal logging, correlation with activity, and progress visualization not implemented.
*   **Sinex as a Mirror for Self-Understanding:** ❌ **NOT IMPLEMENTED** - Data-driven introspection capabilities not implemented.

## 6. Derived Semantic Layers for User Context Architecture (Vision V.5)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Semantic layer derivation not implemented

High-level user contexts (sessions, intents, composite actions) are modeled as derived semantic layers, not as fields in every `raw.events` entry. This keeps `raw.events` lean and allows flexible contextual analysis.
*   **Activity Segments (Sessions):** ❌ **NOT IMPLEMENTED** - Would be modeled as `activity_segment.identified` derived events from agents or user commands.
*   **User Intents and Tasks:** ❌ **NOT IMPLEMENTED** - Would be modeled as distinct event types or entities with post-hoc linking by agents.
*   **Composite Actions:** ❌ **NOT IMPLEMENTED** - Complex user actions would be identified by agents analyzing clusters of low-level events.

This architecture would ensure that rich user context is built upon the foundational `raw.events` stream through intelligent agent-driven derivation and explicit user input.

