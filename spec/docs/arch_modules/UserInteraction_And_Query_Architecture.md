# User Interaction & Query Architecture: The Bridge to Self

*   **Version:** 1.0
*   **Date:** 2024-03-11
*   **Implementation Status:** ❌ **NOT IMPLEMENTED** - Basic CLI exists, Neovim plugin ❌ **MISSING**, Query interfaces ❌ **MISSING**, UI components ❌ **MISSING**
*   **Purpose:** This document describes the architectural principles and high-level design of how users interact with the Sinnix Exocortex, query its vast data stores, and how the system facilitates understanding, narrative construction, and self-modeling. It outlines the architecture of user interfaces, query mechanisms, and feedback loops.
*   **Primary Sources:** Vision Document Part V; STAD (System Technical Architecture Document) Part IV (Retrieval sections like 17, 19).

## 1. Introduction & UI/UX Philosophy (Vision V.1)

### 1.1. Bridging the User and the Sentient Archive

The Exocortex's value is realized through effective user interaction. This layer translates the system's comprehensive data capture and agentic capabilities into an intuitive, empowering extension of the user's mind. The architecture aims to support a seamless flow between capturing thoughts, retrieving information, discovering connections, and reflecting on personal data.

### 1.2. Core UI/UX Principles Guiding Interaction Design

The design of all user-facing aspects is guided by principles outlined in the Vision Document (Part I.3 and V.1), including:
*   **Frictionless Capture, Always:** Minimizing barriers to logging thoughts, data, and meta-cognitive states.
*   **Context is King:** Presenting information relevant to the user's current task and focus.
*   **Discoverability & Learnability:** Making system capabilities accessible and understandable.
*   **User in Control:** Ensuring user agency over data, automation, and system behavior.
*   **Hackability & Extensibility:** Allowing users to customize and extend their Exocortex.
*   **Performance as a Feature:** Ensuring responsive interfaces.
*   **Aesthetics of Clarity and Calm:** Designing interfaces that reduce cognitive load.
*   **Support for Neurodiversity and Varied Cognitive Styles:** Catering to different ways of thinking and working.

## 2. Primary Interaction Channels: Architectural Overview

The Exocortex offers multiple channels for interaction, each tailored to different needs and workflows.

### 2.1. Neovim Plugin (`sinnix-nvim`) (Vision V.1.2)

The Neovim plugin serves as the primary power-user cockpit for deep, keyboard-driven interaction with the Exocortex.
*   **Architectural Role:** Provides seamless integration within the Neovim environment for PKM, Living Document interaction, querying, and command execution.
*   **Key Architectural Features:**
    *   **Unified Search & Navigation (Telescope.nvim):** Custom Telescope pickers provide a consistent interface for finding and opening PKM notes, web archives, raw events, Living Document nodes, blobs, tags, and entities by querying `core_artifacts`, `raw.events`, `core_entities`, etc. Supports full-text, semantic, and tag-based search.
    *   **Contextual Panels & Floating Windows:** Dynamically display backlinks, outlinks, related raw events, semantically similar artifacts, or agent suggestions relevant to the current buffer or task.
    *   **Living Document Interaction:** Dedicated buffer type for stream-of-consciousness input, editing, and invoking LLM actions on Living Document content.
    *   **PKM Note Editing (Yjs-based, ADR-004):** Manages fetching Yjs state/deltas from the backend, applying local edits as Yjs operations, and sending Yjs update blobs on save for canonical storage in the database.
    *   **Exocortex Commands (`:Exo...`):** Comprehensive suite for manual event logging, agent triggering, PKM management, and system queries.
    *   **Visual Cues:** Status line integration, virtual text for annotations, highlighting for unresolved links.
*   **Communication with Exocortex Backend:**
    *   **Custom Exocortex Language Server (LSP):** Preferred method for rich semantic interactions (link resolution, backlink fetching, Yjs sync, agent suggestions). The plugin acts as an LSP client.
    *   **Msgpack-RPC to Helper Process:** Alternative/complementary for specific tasks or interfacing with non-LSP backend components (e.g., a Yjs helper process).
    *   **`exo` CLI Invocation:** For simpler, non-interactive tasks.
*   **Referenced TIMs:**
    *   `[TIM-NeovimPluginIntegration.md](docs/tims/ingestors/pkm_email_nvim/TIM-NeovimPluginIntegration.md)` for plugin architecture, LSP/RPC patterns, Yjs integration, Treesitter usage.
    *   `[ADR-004-PKMNoteContentManagementAndSync.md](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)` for PKM strategy.

### 2.2. `exo` Command-Line Interface (CLI) (Vision V.1.3)

The `exo` CLI is the scriptable and universally accessible backbone for all Exocortex interactions.
*   **Architectural Role:** Provides comprehensive functionality for power users, automation scripts, and integration with other tools. It directly interacts with the Exocortex backend (PostgreSQL database, agent control mechanisms).
*   **Architectural Design:**
    *   **Subcommand Structure:** Organized into logical groups (e.g., `log`, `query`, `find`, `pkm`, `web`, `agent`, `system`).
    *   **Output Formatting:** Supports JSON (default for scripts), YAML, CSV, and human-readable tables (`--output-format`).
    *   **Shell Completions:** Rich completions for Bash, Zsh, Fish.
    *   **`fzf` Integration:** Potential for interactive selection with `fzf` for commands like `exo pkm find`.
*   **Referenced TIMs:**
    *   A future `TIM-ExoCLIReferenceAndDesign.md` (derived from UG App D and actual CLI implementation using libraries like `clap` for Rust) would detail all commands, options, and output formats.

### 2.3. Dashboards (Grafana; Future Web UI) (Vision V.1.4)

Dashboards provide broader visual overviews of Exocortex data and system health.
*   **Grafana Architecture:**
    *   **Role:** Primary tool for visualizing time-series metrics and logs.
    *   **Data Sources:** Connects to Prometheus (for system/agent metrics, `sinex.*` event-derived metrics), Loki (for logs via Promtail), and directly to PostgreSQL (for querying Exocortex tables for custom dashboards).
    *   **Key Dashboards:** Personal analytics (focus time, task completion, mood correlations), knowledge graph metrics, system & agent health, LLM usage/cost.
    *   **Provisioning:** Dashboards can be provisioned declaratively via JSON models in NixOS configuration.
    *   *Referenced TIMs:* `[TIM-ObservabilityStackSetup.md](docs/tims/operations/TIM-ObservabilityStackSetup.md)` for Grafana setup and data source configuration.
*   **Future Web UI/Canvas Architecture (Conceptual):**
    *   **Role:** A more interactive, read-write web interface for richer data exploration and interaction beyond Grafana's capabilities.
    *   **Potential Features:** Full graph visualization and navigation (`core_entity_relations`, etc.), interactive timelines interleaving event streams, rich Living Document editing interface (potentially canvas-style), mobile-friendly quick capture and query.
    *   **Technology Stack (Speculative):** Frontend (e.g., React/Vue/Svelte with TypeScript), backend API (e.g., Rust Axum/Actix providing data to frontend), WebSocket for real-time updates.

### 2.4. Inbox Workflow Architecture (Vision V.1.5)

A core interaction pattern for managing new and attention-requiring items within the Exocortex.
*   **Architectural Role:** Provides a centralized, dynamic, query-driven view for triaging and processing diverse items like newly captured/unprocessed PKM notes or web archives, agent-extracted task proposals, system errors/alerts, agent suggestions, and unresolved links.
*   **Architectural Implementation:**
    *   Not a fixed table, but rather a set of pre-defined (and user-customizable) queries against `core_artifacts`, `raw.events` (e.g., `sinex.system.suggestion_created`), `agent_processing_dlq`, etc., filtered by status (e.g., `proposed`, `pending_review`).
    *   Presented as a dedicated view in Neovim, `exo inbox` CLI command, or future Web UI.
    *   User actions in the Inbox (triage, process, delegate, defer, dismiss) generate new Exocortex events (e.g., `core_artifacts.updated` with new tags, `sinex.task.status_changed`, `sinex.system.suggestion.actioned`), ensuring all triage decisions are captured and feed back into the system.

## 3. The Architecture of Query (Vision V.2, STAD Part IV)

Unlocking the value of the Exocortex relies on powerful and flexible query capabilities.

### 3.1. Layered Query Capabilities

The system offers multiple layers for querying, catering to different needs and skill levels.
*   **Direct SQL on PostgreSQL:**
    *   **Architectural Role:** Provides ultimate power and flexibility for complex queries, data analysis, and custom reporting. Users can directly query `raw.events`, domain tables, and knowledge graph tables.
    *   **Key Features Leveraged:** JSONB operators, Full-Text Search (`to_tsvector`), GIS (if location data is rich), window functions, recursive CTEs (for graph traversal), and `pgvector` operators for Approximate Nearest Neighbor (ANN) search.
*   **Simplified Query Syntax (`exo` CLI & Neovim Interface):**
    *   **Architectural Role:** Provides a user-friendly abstraction over SQL for common query patterns. The `exo find` and `exo query` commands (and corresponding Neovim functions) translate this syntax into underlying SQL queries.
    *   **Supported Filters:** Temporal (`since`, `between`), source/type/host, payload content (keyword, field-specific JSONPath/jq-like), semantic similarity (vector search), tag-based, and simplified graph traversal. Hybrid queries combining multiple filter types are supported.

### 3.2. Hybrid Search Architecture (Vector + Full-Text + RRF)

Combines semantic vector search with traditional keyword-based full-text search for enhanced retrieval relevance.
*   **Vector Search Component (`pgvector`):**
    *   Embeddings (from `TIM-EmbeddingGenerationModels.md`) are stored in tables like `artifact_embeddings`.
    *   HNSW indexes (ADR-005) are used for ANN search on `embedding_vector` columns.
    *   Supports metadata filtering in conjunction with ANN search.
*   **Full-Text Search (FTS) Component (PostgreSQL FTS):**
    *   Generated `tsvector` columns (e.g., on `core_artifact_contents.content_text`) are indexed with GIN.
    *   Queries use `plainto_tsquery` or `websearch_to_tsquery`. Ranking via `ts_rank_cd`.
*   **Reciprocal Rank Fusion (RRF):**
    *   Results from vector search and FTS (each producing a ranked list of document/artifact IDs) are combined using the RRF algorithm. This re-ranks items based on their presence and rank in both lists, typically improving overall relevance.
    *   Implemented as a PostgreSQL SQL function that takes keyword query and query embedding as input. Handles chunked embeddings by aggregating chunk scores to the document level before RRF.
*   **Referenced TIMs & ADRs:**
    *   `[TIM-HybridSearchPostgreSQL.md](docs/tims/processing_retrieval/TIM-HybridSearchPostgreSQL.md)` for `pgvector` HNSW setup, FTS setup, and the RRF SQL function implementation.
    *   `[TIM-EmbeddingGenerationModels.md](docs/tims/processing_retrieval/TIM-EmbeddingGenerationModels.md)` for embedding generation.
    *   `[ADR-005-VectorIndexTypePgvector.md](docs/adr/ADR-005-VectorIndexTypePgvector.md)` and `[ADR-007-LargeScaleVectorSearchStrategy.md](docs/adr/ADR-007-LargeScaleVectorSearchStrategy.md)`.

### 3.3. Knowledge Graph Querying Architecture

The Exocortex Knowledge Graph is primarily stored relationally in `core_entities` and `core_entity_relations`.
*   **Querying Methods:**
    *   **Recursive Common Table Expressions (CTEs):** SQL-standard method for traversing hierarchical or graph-like data (e.g., finding entities reachable within N hops, pathfinding).
    *   **(Optional) Apache AGE Extension:** If installed, provides OpenCypher query language support for more complex graph pattern matching, requiring synchronization between relational tables and the AGE graph.
*   **Referenced TIMs:**
    *   `[TIM-PostgreSQL-AdvancedFeatures.md](docs/tims/data_substrate/TIM-PostgreSQL-AdvancedFeatures.md)` (Graph section) for Recursive CTE examples and AGE setup.
    *   TIMs for `core_entities` and `core_entity_relations` DDLs (e.g., `TIM-KnowledgeGraphSchema.md`).

## 4. Weaving Understanding: Architecture of Relations & Narratives (Vision V.3)

The Exocortex helps users and agents understand connections and stories within the data.

### 4.1. Explicit Event & Artifact Relations Architecture

Multiple tables model different types of relationships:
*   **`event_relations`:** For rich, typed links specifically *between `raw.events` entries* or between raw events and other core artifacts/entities. Captures semantic relationships like "derives_from," "explains_context_of."
*   **`core_entity_relations`:** For typed links *between canonical entities* in `core_entities` (Knowledge Graph edges).
*   **`core_artifact_links`:** Primarily for links *parsed from PKM notes or web archives*, connecting `core_artifacts` entries (e.g., Wikilinks). Includes `target_identifier_text` (raw link) and `resolved_target_artifact_id`.
*   **Creation:** Links are created manually by the user (via UI/CLI) or by agents inferring relationships (e.g., from text analysis, temporal proximity).
*   **Referenced TIMs:** TIMs containing DDLs for these specific relation tables (e.g., `TIM-KnowledgeGraphSchema.md`, `TIM-EventRelationsSchema.md`).

### 4.2. Agent-Driven Narrativization Architecture

LLM agents play a key role in synthesizing human-readable narratives from complex Exocortex data.
*   **Mechanism:** A narrativization agent is triggered (e.g., by user request for a project summary, or periodically for weekly reviews). It receives a scope (e.g., all data related to a specific project entity or time range).
*   **LLM Processing:** The agent queries relevant data (events, artifacts, entities, relations), constructs a detailed context/prompt, and tasks an LLM to generate a narrative.
*   **Output (`meta.narrative_generated` event):** The resulting narrative (text), along with metadata (title, key objects referenced, themes identified, sentiment analysis, generation prompt/model used), is logged as a `meta.narrative_generated` event in `raw.events`. It can also be stored as a `core_artifacts` entry (type `narrative`) for easier retrieval and linking.
*   **Integration:** Narratives become valuable artifacts for retrospectives, planning, and providing context to other LLMs or the user.

### 4.3. Generic Event Annotations Architecture (`event_annotations`)

Provides a flexible mechanism for layering user and agent insights directly onto individual `raw.events` entries without altering the immutable event itself.
*   **Architectural Role:** Enables users to comment on events, mark importance, or add fleeting thoughts. Allows agents to flag events for review, store intermediate processing results, or propose links. Acts as a bridge between raw data and higher-level structures.
*   **`event_annotations` Table:** Links `annotation_id` to `target_event_id` (`raw.events.id`), stores `annotator_actor` (user/agent), `annotation_type`, `content_text` or `content_jsonb`, and timestamp. Can also store an embedding of the annotation content.
*   **Interaction:** UIs provide means to view, add, and filter annotations. Agents can read/write annotations.
*   **Referenced TIMs:** A TIM like `TIM-EventAnnotationsSchema.md` (or as part of `TIM-EventSubstrateDDL.md`) for the DDL.

## 5. Cognitive Feedback Loops & Self-Modeling Architecture (Vision V.4)

The interaction layer aims to close the loop between data capture, analysis, and user action, enabling instrumented self-modeling.
*   **Surfacing Patterns & Anomalies:** UIs (dashboards) proactively display trends (focus time, task velocity, friction clusters, mood/energy correlations). Agents monitor streams for significant deviations, generating alerts (`sinex.system.suggestion_created` or `sinex.analytics.pattern_alert`).
*   **Intentional Tracking & Goal Alignment:** Users log goals/intentions as `planning.goal.defined` or `meta.intention.created` events (which become `core_entities`). Agents correlate subsequent activity (from `raw.events`, `core_artifacts`) with these goals, visualizing progress or flagging deviations.
*   **Exocortex as a Mirror for Self-Understanding:** By making internal states (mood, friction, insights) and external actions (digital traces) equally queryable and linkable, the system facilitates data-driven introspection and allows users to formulate/test hypotheses about their own cognition and behavior (as explored in Vision Essay 1).

## 6. Derived Semantic Layers for User Context Architecture (Vision V.5)

High-level user contexts (sessions, intents, composite actions) are modeled as derived semantic layers, not as fields in every `raw.events` entry. This keeps `raw.events` lean and allows flexible contextual analysis.
*   **Activity Segments (Sessions):** Modeled as `activity_segment.identified` derived events (source: `sinex.agent.activity_segmenter` or user via `exo session mark_start/end`). Payload includes `segment_id`, `segment_type_user_defined`, `ts_start_orig`, `ts_end_orig`, description, linked entities. Raw events can be correlated by timestamp.
*   **User Intents and Tasks:** Modeled as `intent.declared`/`intent.concluded` distinct event types or as entities in `core_entities` (type `task` or `intent`). Raw activity events are linked to these post-hoc by agents (via `event_entity_links` or `event_relations`) or temporal correlation.
*   **Composite Actions:** Complex user actions (e.g., "save file and commit") are identified by an agent (`sinex.agent.action_correlator`) analyzing clusters of low-level events. Modeled either as new derived `composite_action.identified` events (payload lists constituent raw event IDs) or by creating `event_relations` between the constituent events.

This architecture ensures that rich user context is built upon the foundational `raw.events` stream through intelligent agent-driven derivation and explicit user input.

