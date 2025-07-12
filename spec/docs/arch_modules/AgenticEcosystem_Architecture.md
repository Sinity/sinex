# Automaton Ecosystem Architecture: Event Processing and Intelligence in the Satellite Constellation

*   **Version:** 1.2
*   **Date:** 2025-07-15
*   **Implementation Status:** 🚧 **30% IMPLEMENTED** - Satellite architecture operational, automaton framework working, Redis Streams active, LLM integration planned
*   **Purpose:** This document describes the Sinex automaton ecosystem within the satellite constellation architecture. It covers the distinction between deterministic automata and AI-powered agentic systems, Redis Streams processing, checkpoint management, and the planned LLM integration framework.
*   **Primary Sources:** STAD (System Technical Architecture Document) Part VI (Agent Manifests); Vision Document Part IV.

## 1. Introduction & Philosophy of Agentic Design (Vision IV.1.1)

### 1.1. Role of Automata in the Satellite Constellation

The Automaton Ecosystem provides the "active processing" layer of Sinex through the satellite constellation architecture. It comprises specialized event processors—automata—that operate on Redis Streams and the data substrate to perform deterministic event processing, data enrichment, and pattern detection. This layer transforms raw events into structured knowledge while maintaining clear separation between deterministic processing (automata) and AI-powered intelligence (agentic systems).

### 1.2. Core Design Tenets for Exocortex Agents

*   **Modularity and Specialization:** Automata are designed with clearly defined domain responsibilities (e.g., "canonical command synthesis," "PKM note linking," "health monitoring") following single-responsibility principles for maintainability and independent evolution.
*   **Stream-Driven Processing:** Automata consume events from Redis Streams using consumer groups, enabling horizontal scaling and reliable processing with exactly-once semantics through checkpoint management.
*   **Transparency and Auditability:** All automaton actions create events with full provenance tracking through `source_event_ids` fields, enabling complete replay and analysis of processing chains.
*   **User Control and Oversight:** Users control automaton behavior through NixOS configuration and can replay/reset processing from any checkpoint position. Critical decisions can be flagged for user review through the gateway API.
*   **Resource Management:** Automata run as independent systemd satellite services with resource quotas managed declaratively through NixOS.
*   **Robust Error Handling:** Automata use Redis Streams acknowledgment patterns with automatic retry and dead letter queue routing for failed events, ensuring no data loss.

## 2. The Agent Framework Architecture

This section details the core infrastructure supporting the agentic ecosystem.

### 2.1. Automaton Checkpoint System (`core.automaton_checkpoints`)

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Checkpoint system working with Redis Streams integration

The checkpoint system provides state management for all satellite processors, replacing the previous agent manifest approach with a unified state tracking mechanism.
*   **Architectural Role:** Provides unified state management for all satellite processors (ingestors and automata) enabling recovery, replay, and horizontal scaling through Redis Streams consumer groups.
*   **Key Schema Fields Overview:**
    *   `automaton_id TEXT PRIMARY KEY`: Unique processor identifier (e.g., `"sinex-terminal-satellite"`)
    *   `checkpoint_type TEXT`: Type of checkpoint (stream_position, file_offset, timestamp)
    *   `checkpoint_data JSONB`: Type-specific state data (Redis consumer group position, file offsets, etc.)
    *   `last_processed_event_id ULID`: For event stream processors, the last successfully processed event
    *   `created_at TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`: Checkpoint lifecycle tracking
    *   `metadata JSONB`: Additional processor-specific metadata for recovery and monitoring
*   **Referenced TIMs:**
    *   `[TIM-AgentManifestManagement.md](docs/tims/architecture_crosscutting/TIM-AgentManifestManagement.md)` (Section 2) for the full DDL and detailed field descriptions.

### 2.2. Redis Streams Integration and Consumer Groups

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Redis Streams consumer groups working with automatic state management

Automata integrate with Redis Streams for scalable, reliable event processing with automatic state management.
*   **Consumer Group Management:** ✅ **OPERATIONAL** - Automata automatically join Redis consumer groups enabling horizontal scaling and load balancing across multiple instances.
*   **Checkpoint Persistence:** ✅ **OPERATIONAL** - Processing state automatically saved to PostgreSQL checkpoints with Redis consumer group positions for durability.
*   **Automatic Recovery:** ✅ **OPERATIONAL** - Automata resume from last checkpoint after restart, with Redis handling unacknowledged message redelivery.
*   **Journald Heartbeat Pattern:** ✅ **OPERATIONAL** - Satellite services emit structured logs captured by journald and ingested as Sinex events for health monitoring.
*   **Referenced TIMs:**
    *   `[TIM-AgentManifestManagement.md](docs/tims/architecture_crosscutting/TIM-AgentManifestManagement.md)` (Sections 3 & 4) for JSON schema of static manifest and details on registration/heartbeat logic.

### 2.3. Systemd Integration & Lifecycle Management (Vision IV.1.3)

> **🚧 IMPLEMENTATION STATUS: PARTIAL** - Basic systemd services working, advanced management not implemented

Most agents run as dedicated systemd user services or timer units, managed by NixOS.
*   **Benefits:** Standardized lifecycle (start, stop, restart policies), resource quota enforcement (CPU, memory via cgroups), logging to `journald` (ingested for meta-observability).
*   ✅ **BASIC WORKING** - Basic NixOS modules for core services implemented.
*   ❌ **NOT IMPLEMENTED** - Advanced resource quota enforcement, comprehensive configuration generation, and full meta-observability integration.

### 2.4. Satellite Communication Patterns

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Satellite constellation communication patterns working through Redis Streams and gRPC

*   **Primary Data Flow:**
    *   **Event Ingestion:** ✅ **OPERATIONAL** - Satellites send events to `sinex-ingestd` via gRPC, which distributes to PostgreSQL and Redis Streams
    *   **Stream Processing:** ✅ **OPERATIONAL** - Automata consume from Redis Streams with consumer groups, process events, and emit results back through ingestd
    *   **Command/Response:** ✅ **OPERATIONAL** - API commands flow through Redis Streams with correlation IDs for request/response patterns
*   **Inter-Automaton Communication:** ✅ **OPERATIONAL** - Automata communicate through event provenance chains and Redis Streams, enabling complex processing pipelines
*   **Error Handling:** ✅ **OPERATIONAL** - Failed events routed to PostgreSQL dead letter queue with Redis consumer group acknowledgment patterns
    *   *Referenced TIMs:* `[TIM-DeadLetterQueueImplementation.md](docs/tims/data_substrate/TIM-DeadLetterQueueImplementation.md)`.

## 3. Agentic Systems: AI-Powered Intelligence Layer

> **🔨 IMPLEMENTATION STATUS: FRAMEWORK READY** - Schema and satellite infrastructure ready for LLM integration

Agentic systems represent the AI-powered intelligence layer that works alongside deterministic automata, providing semantic understanding, content generation, and complex reasoning capabilities through Large Language Models.

### 3.1. Diverse Roles of LLMs in Exocortex

LLMs are employed for a wide range of tasks, including but not limited to:
*   **Content Generation:** Summaries, narratives, drafts for PKM notes, email replies.
*   **Structuring & Parsing:** Interpreting Living Document segments, extracting tasks/claims from free-form text, parsing UI widget trees (from AT-SPI2), understanding natural language queries.
*   **Classification & Tagging:** Semantic tagging of artifacts, sentiment analysis, topic modeling.
*   **Code Assistance:** Potentially assisting in drafting new agent logic, data transformations, or queries (for developer use).
*   **Semantic Linking & Reasoning:** Proposing connections between disparate pieces of information, identifying contradictions, assisting in knowledge graph construction.

### 3.2. Model Management & Access Architecture

The Sinex would support both local LLMs (for privacy, cost-efficiency, offline use) and remote LLM APIs (for access to state-of-the-art models).
*   **Local LLM Management (Ollama):** ❌ **NOT IMPLEMENTED** - Ollama would be used to serve open-source models locally (e.g., Llama, Mistral). Would be managed as a NixOS service, with potential GPU acceleration.
*   **`core_llm_models` Registry (DB Table):** ❌ **NOT IMPLEMENTED** - This table would list all LLMs available to Sinex, whether local or remote. Would store model names, provider details, capabilities, cost information, and operational status.
*   **LLM Router Architecture:** ❌ **NOT IMPLEMENTED** - A conceptual centralized service would be responsible for routing LLM requests from agents to the most appropriate model instance with fallbacks and retries.
    *   *Referenced TIMs:* `[TIM-LLMResourceOrchestration.md](docs/tims/operations/TIM-LLMResourceOrchestration.md)` (Section 3) for LLM Router logic and `core_llm_models` DDL.

### 3.3. Prompt Engineering & Management Architecture (`core_prompts`) (Vision IV.2.3)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Prompt management system not developed

Effective LLM use relies on well-engineered prompts.
*   **`core_prompts` DB Table:** ❌ **NOT IMPLEMENTED** - Would store versioned prompt templates, their input variable schemas (JSON Schema), descriptions, target LLM families, default LLM parameters, and performance metrics.
*   **Git-Based Source for Prompts:** ❌ **NOT IMPLEMENTED** - Prompt templates would be authored as structured YAML files in a version-controlled repository. A CI/CD pipeline or sync agent would validate these and load them into the `core_prompts` table.
*   **A/B Testing & Canary Deployment:** ❌ **NOT IMPLEMENTED** - Frameworks for systematic testing of prompt versions and gradual rollout not implemented.
*   **Meta-Agents for Prompt Optimization:** ❌ **NOT IMPLEMENTED** - Future agents that would analyze `sinex.agent.llm_output_feedback` events for prompt refinement not implemented.

### 3.4. Cost Tracking and Budgeting Architecture (Vision IV.2.4)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - LLM cost tracking not implemented

Managing the cost of using remote LLM APIs is critical.
*   **Event Logging:** ❌ **NOT IMPLEMENTED** - LLM API calls would be logged as `sinex.agent.llm_api_call` events in `raw.events` with cost and performance data.
*   **Agent-Level Budgeting:** ❌ **NOT IMPLEMENTED** - Configurable cost budgets, spend monitoring, and automatic throttling not implemented.
*   **Monitoring:** ❌ **NOT IMPLEMENTED** - Grafana dashboards for LLM cost visualization not implemented.

### 3.5. DSPy/LangGraph Integration Architecture

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Advanced LLM frameworks not integrated

For building more complex, stateful, and adaptable LLM-powered agentic flows.
*   **Role:** DSPy would optimize (compile) few-shot prompts and LM weights. LangGraph would define LLM agent interactions as state graphs.
*   **Persistence:** ❌ **NOT IMPLEMENTED** - LangGraph checkpoints and DSPy programs would be persisted but integration not built.
*   **Debugging & Visualization:** ❌ **NOT IMPLEMENTED** - State history visualization and debugging tools not implemented.
*   **Operational Considerations:** ❌ **NOT IMPLEMENTED** - State management, retry strategies, cost tracking, and tracing not implemented.
*   **Referenced TIMs:**
    *   `[TIM-LLMResourceOrchestration.md](docs/tims/operations/TIM-LLMResourceOrchestration.md)` (Section 4) for persistence, debugging, and operational details.

## 4. Archetypal Agents and Their Architectural Roles (Vision IV.3)

> **❌ IMPLEMENTATION STATUS: NOT IMPLEMENTED** - Specific intelligent agents not developed

The Sinex agent ecosystem is diverse. This section provides an architectural overview of key agent archetypes and their general interactions, rather than exhaustive implementation details of each. *Specific agent implementations would typically not have their own TIM unless they are exceptionally complex or foundational; rather, their logic is implied by the TIMs for the data/services they interact with (e.g., a PKM agent's logic is related to TIMs for PKM content, embeddings, and knowledge graph).*

*   **4.1. Task-Oriented & Proactive Agents:**
    *   **Purpose:** Perform specific user-facing tasks or provide proactive assistance.
    *   **Examples & Interactions:**
        *   `DailyJournalPrompter`: Runs daily (systemd timer), queries recent Exocortex activity, uses LLM to generate a reflection prompt, creates a new PKM note artifact (`core_artifacts`, content in `core_artifact_contents`), emits `sinex.pkm.note_created` and `sinex.system.suggestion_created`.
        *   `WebPageArchiverAndMarkdownifier`: Consumes `sinex.web.capture_request`, uses tools from `TIM-WebArchivingTooling.md`, stores results in `core_blobs` / `core_artifact_contents`, emits `sinex.web.page_archived`.
        *   `TodoExtractorFromText`: Consumes text from `core_artifact_contents` (PKM, web), Living Document, etc. Uses LLM to identify tasks, proposes new `task_item` artifacts (surfaced via Inbox workflow), emits `sinex.task.created` on confirmation.

*   **4.2. Analytical & Retrospective Agents:**
    *   **Purpose:** Analyze historical data to uncover patterns, generate summaries, or reconstruct narratives.
    *   **Examples & Interactions:**
        *   `ActivityPatternMiner`: Periodically queries domain tables (e.g., `domain_desktop.focus_spans`), identifies routines, generates `sinex.analytics.pattern_report` events or updates dashboards.
        *   `ProjectTimelineConstructor`: On demand or milestone completion, traverses data linked to a project entity (`core_entities`), uses LLM to construct a narrative, emits `meta.narrative_generated`.
        *   `WeeklyNarrator`: Runs weekly, summarizes key activities/insights/friction using LLM, emits `meta.narrative_generated`.

*   **4.3. Integration & Synchronization Agents:**
    *   **Purpose:** Bridge Exocortex with external systems or manage internal data consistency.
    *   **Examples & Interactions:**
        *   `ExternalFeedImporter` (RSS, social media archives): Fetches items, emits `external.feed_item.ingested` events.
        *   `CalendarSyncAgent`: Syncs external calendars to a `domain_calendar.events` table.
        *   `PKMSyncAgent` (for Yjs/DB native content): Manages the *export* of canonical DB note content to a filesystem view (if enabled), and handles import/conflict resolution for files edited externally. Interacts with `core_artifacts`, `core_artifact_contents`, `core.pkm_note_yjs_deltas`.
        *   `GitAnnexDBReconciler`: Compares `core_blobs` with `git-annex` state, logs integrity issues.

*   **4.4. Meta-Reflective & System Maintenance Agents:**
    *   **Purpose:** Focus on the health, efficiency, and improvement of the Exocortex itself.
    *   **Examples & Interactions:**
        *   `SystemHealthMonitor` (`AgentMonitor`): Consumes `sinex.agent.heartbeat`/`error`, updates `agent_manifests`, generates alerts.
        *   `LLMCostAnalyzer`: Aggregates `sinex.agent.llm_api_call` events, reports costs, flags budget issues.
        *   `PromptOptimizerAgent` (Future): Consumes `meta.llm_output_feedback`, suggests prompt refinements.
        *   `OrphanedArtifactDetector`: Scans for unreferenced data, suggests cleanup via `sinex.data_cleanup.suggestion_created`.

