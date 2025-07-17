# TIM-LivingDocumentInternals: Living Document Backend Mechanics & Data Model

*   **Purpose:** Details the backend architecture, data models, event sourcing, processing logic, and persistence strategies for the Exocortex Living Document (LD).
*   **Source:** Derived from conceptual descriptions in original Vision Document Part II.1 (especially II.1.2, II.1.4) and related discussions.
*   **Dependencies:** `pgx_ulid`, `core.events`, `core.artifacts`, `core.artifact_contents`. Potentially Yjs (see `TIM-PKMContentCRDT_Yjs.md` for Yjs patterns if applied here). `TIM-CanonicalEventSchemas.md` for `livingdoc.*` event schemas.

## 1. Core Concept & Architectural Role

The Living Document (LD) serves as an externalized, persistent working memory for the user. It facilitates frictionless capture of stream-of-consciousness thoughts, iterative development of plans and drafts, and AI-augmented structuring of this information. Architecturally, the LD is treated as a primary, dynamic artifact within the Exocortex.

*   **Root Representation:** The entire Living Document (or distinct, large, user-defined sections of it if modularity is later desired) is represented by a root entry in `core.artifacts` with `artifact_type = 'living_document_main'`. This root artifact's `artifact_id` serves as the primary identifier for the LD.
*   **Content Management Decision (Yjs-centric for consistency with PKM):** To ensure robust versioning, conflict-free merging (especially for future multi-device/agent interaction), and consistency with PKM notes (ADR-004), the **primary textual content of the Living Document will be managed as a single, large Yjs document.**
    *   This Yjs document will contain the entire structured text of the LD.
    *   "Nodes" within the LD (paragraphs, list items, headings, embedded objects) will be represented as elements *within* this Yjs document structure, potentially using Yjs XML Elements/Fragments or custom Yjs types with attributes for node IDs, types, and metadata.
    *   This approach is preferred over managing potentially thousands of individual Yjs documents for each tiny LD node, or a complex relational schema (`core.living_document_nodes`) for granular node content if text is a primary component.

## 2. Event-Sourced Architecture for LD Changes

All modifications to the Living Document's Yjs document are driven by and result in events.
*   **Source of Changes:**
    *   `user.input.living_document_editor`: User typing, formatting, structuring via Neovim plugin or future UIs.
    *   `agent.living_document_manager`: Backend agents performing automated structuring, linking, or content modifications.
*   **`livingdoc.yjs_update_applied` Event:** This is the primary event type signifying a change to the LD.
    *   **`source`:** Actor initiating the change (e.g., "user.neovim_plugin.ld_editor", "agent.ld_auto_refactor_v1").
    *   **`event_type`:** `"yjs_update_applied"`
    *   **`payload` (see `TIM-CanonicalEventSchemas.md` for full schema):**
        *   `living_document_artifact_id_ulid`: ULID of the root LD artifact in `core.artifacts`.
        *   `yjs_delta_ids_ulid`: Array of ULIDs of the new delta(s) stored in `core.living_document_yjs_deltas` for this operation.
        *   `originator_actor`: More specific actor if different from `core.events.source`.
        *   `operation_summary_text`: Optional human-readable summary of the change (e.g., "Appended text to node X", "Restructured section Y").
        *   `affected_node_ids_internal`: Optional array of internal Yjs element IDs/paths affected.
*   These events are logged to `core.events` after the Yjs update has been successfully applied and persisted to `core.living_document_yjs_deltas`.

## 3. Yjs Content Representation & Persistence for the Living Document

The primary textual and structural content of the Living Document is stored and versioned using Yjs.

*   **`core.living_document_yjs_deltas` Table:** (Mirrors `core.pkm_note_yjs_deltas` but for the LD)
    ```sql
    CREATE TABLE IF NOT EXISTS core.living_document_yjs_deltas (
        delta_id                ULID PRIMARY KEY DEFAULT gen_ulid(),
        ld_artifact_id          ULID NOT NULL REFERENCES core.artifacts(artifact_id) ON DELETE CASCADE, -- FK to the root LD artifact
        yjs_update_blob         BYTEA NOT NULL,    -- The binary Yjs update data
        originator_actor        TEXT NOT NULL,     -- e.g., 'user_neovim_plugin_instance_X', 'agent_LDStructurer_v1'
        ts_created              TIMESTAMPTZ NOT NULL DEFAULT now(),
        -- Optional: yjs_prev_state_vector BYTEA NULLABLE,
        UNIQUE (ld_artifact_id, ts_created, delta_id)
    );
    CREATE INDEX IF NOT EXISTS idx_ld_yjs_deltas_artifact_ts ON core.living_document_yjs_deltas (ld_artifact_id, ts_created ASC, delta_id ASC);
    ```
*   **Markdown Snapshots in `core.artifact_contents`:**
    *   Periodically, or on significant structural changes, the current state of the LD's Yjs document is rendered into Markdown.
    *   This Markdown snapshot is stored as a new version in `core.artifact_contents`, linked to the LD's root `artifact_id`.
    *   The `core.artifacts.current_content_id` for the LD points to this latest Markdown snapshot.
    *   The `core.artifact_contents.metadata` for this snapshot should store information about the last `delta_id` from `core.living_document_yjs_deltas` that was incorporated into it.
*   **Loading/Editing Workflow:**
    1.  UI (e.g., Neovim plugin) requests to open the Living Document (identified by its root `artifact_id`).
    2.  Backend retrieves the latest Markdown snapshot from `core.artifact_contents`.
    3.  Backend retrieves all Yjs update blobs from `core.living_document_yjs_deltas` for that `ld_artifact_id` *since* the snapshot's basis delta.
    4.  Backend (or plugin, if Yjs logic is client-side) initializes a Yjs document by:
        a.  Parsing the Markdown snapshot into a Yjs structure (this is the complex part, requiring a robust Markdown -> Yjs converter that preserves block identity as much as possible, perhaps using the stable heading ID logic or other markers).
        b.  Applying the subsequent Yjs update blobs.
    5.  User edits in UI are translated to local Yjs ops.
    6.  On save, new Yjs update blobs are sent to backend, stored in `core.living_document_yjs_deltas`. Backend generates new `livingdoc.yjs_update_applied` event. Backend asynchronously updates Markdown snapshot.

## 4. Conceptual Processing Pipeline (LLM Node Graph as Agent Interactions)

The "LLM Node Graph" concept from the original Vision document is realized as a sequence or collaboration of specialized Exocortex agents interacting with the Living Document's Yjs state and `core.events`.

1.  **Input Ingestion & Segmentation Agent (or part of UI backend):**
    *   Handles raw text input, voice (via ASR using `TIM-ASR_WhisperCpp.md`), pasted content.
    *   Segments input into "thought units" if necessary (though continuous Yjs updates might handle this naturally).
    *   Identifies explicit commands (e.g., `/task Add X`, `/summarize last_hour`).
    *   Forwards processed input/commands to the `LivingDocumentManagerAgent`.
2.  **LivingDocumentManagerAgent (`agent.ld_manager`):**
    *   The primary agent responsible for applying changes to the canonical Yjs document for the LD.
    *   Receives user edits (as Yjs update blobs from UIs) or commands from the Input Ingestion Agent.
    *   Applies these changes to the Yjs document.
    *   Persists the new Yjs update blobs to `core.living_document_yjs_deltas`.
    *   Generates the `livingdoc.yjs_update_applied` event.
    *   May trigger asynchronous generation of Markdown snapshots.
    *   Can also perform automated structuring tasks based on LLM analysis (e.g., if a user types a paragraph and then "/make_list", this agent uses an LLM to convert the paragraph to list items within the Yjs doc).
3.  **ArtifactExtractionAgent (`agent.ld_artifact_extractor`):**
    *   Subscribes to `livingdoc.yjs_update_applied` events or monitors changes to LD Markdown snapshots.
    *   Scans new/modified LD content (either by interpreting Yjs structure or parsing Markdown snapshot).
    *   Uses LLMs (with prompts from `core.prompts`) or rule-based parsers to identify potential:
        *   Tasks: Creates a `core.artifacts` entry (type `task_item`), stores task details in `properties`. Emits `sinex.task.created_from_ld` event. Links task artifact to the source LD segment/node ID (stored as a property in the task artifact or via `core_artifact_links`).
        *   Claims, Hypotheses, Questions: Similar process, creating `core.artifacts` of appropriate types.
        *   Definitions of new concepts/entities: Proposes new entries for `core.entities`.
4.  **KnowledgeGraphIntegrationAgent (`agent.ld_kg_linker`):**
    *   Also subscribes to LD changes.
    *   Performs Named Entity Recognition (NER) on new/modified LD content (see `TIM-EntityResolutionTechniques.md`).
    *   For identified entities:
        *   Resolves against `core.entities`.
        *   Creates `core_entity_relations` linking the LD's root artifact (or specific LD "node" entities if the LD is further modeled in `core_entities`) to the recognized entities (e.g., relation_type `mentions_entity`).
    *   May also identify semantic relationships *within* the LD content and propose new `core_entity_relations` between concepts discussed.

## 5. Snapshotting, Versioning, and Rendered Views

*   **Versioning:** Primarily through the immutable sequence of Yjs update blobs in `core.living_document_yjs_deltas`. Each blob (or set from a single user save) represents a micro-version. Markdown snapshots in `core.artifact_contents` represent more significant, human-readable versions.
*   **Snapshotting Yjs to Markdown:** The `LivingDocumentManagerAgent` or a dedicated snapshotting agent is responsible for periodically rendering the Yjs document to Markdown and updating `core.artifact_contents`. This can be triggered by:
    *   Time interval (e.g., every N minutes if changes occurred).
    *   Number of new Yjs deltas (e.g., after every M deltas).
    *   Significant structural changes detected.
    *   On-demand request.
*   **Rendered Views (Conceptual):**
    *   **Markdown:** Primary queryable and exportable format from `core.artifact_contents`.
    *   **Interactive Outliner (UI feature):** UIs (Neovim, Web) parse the Yjs document structure (or derived Markdown with stable IDs) to present an outline view for navigation and manipulation. Changes in the outliner translate back to Yjs operations.
    *   **Canvas/Graph View (Future UI feature):** Visualizes "nodes" from the Yjs document and their explicit or inferred connections as a 2D canvas for spatial organization or graph exploration.

## 6. Stable Identifiers for LD "Nodes" (within Yjs)

If the LD is one large Yjs document, internal "nodes" (like paragraphs, list items, or user-defined blocks) need stable identifiers for linking, transclusion, and agent actions.
*   **Mechanism:**
    *   Use Yjs custom types or attach attributes to Yjs XML Elements representing these nodes.
    *   Assign a unique ULID to each significant structural element/block within the Yjs document when it's created. This ULID is stored as an attribute on the Yjs element.
    *   These internal ULIDs can then be referenced by artifact extraction agents, linking agents, or in user-created links within the LD itself (e.g., `[[#ld_node_ulid_xyz]]`).

This Yjs-centric approach for the Living Document, mirroring the PKM note strategy, provides a powerful and consistent foundation for its complex requirements.

