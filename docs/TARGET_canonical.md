
Of course. This is the logical culmination of our analysis. I will now synthesize everything—the refined doctrine from `TARGET_final.md`, the actionable refactoring steps, the identified gaps from the codebase analysis, and the full text of all provided specifications—into a single, comprehensive, canonical blueprint.

This document, `TARGET_CANONICAL_V6.md`, is designed to be the ultimate source of truth. It is structured to be read top-down, starting with the highest-level principles and progressively detailing the architectural components, data models, and the concrete implementation plan required to bring the current codebase into full alignment with this final vision. It explicitly calls out the current state vs. the target state for each major component.

---
--- START OF FILE TARGET_CANONICAL_V6.md ---

# The Sinex Exocortex: Canonical Architecture & Implementation Doctrine, v6.0

**Preamble:** This document is the definitive and single source of truth for the Sinex architecture. It supersedes all previous design documents and implementation plans. Its purpose is to guide a competent coding agent in the transformation of the existing codebase into its final, correct, and robust state, and to provide the blueprint for all future development.

## Invariants Quick Reference (The System's Ten Commandments)

These are the non-negotiable rules of the system. All components **MUST** be designed and implemented to uphold these invariants.

1.  **Single-Writer Ingest:** All canonical events (`core.events`) **MUST** be written by a single service (`ingestd`). Satellites **MUST NOT** write directly to the `core.events` table.
2.  **Post-Commit Publish:** An event **MUST** only be published to the real-time message bus (NATS) *after* it has been successfully committed to the PostgreSQL database.
3.  **Dual-Layer Provenance (XOR):** Every event in `core.events` **MUST** have either external provenance (`source_material_id`, `anchor_byte`) OR internal provenance (`source_event_ids`), but **NEVER** both, and **NEVER** neither. The database **MUST** enforce this with a `CHECK` constraint.
4.  **First-Order Idempotency:** An ingestor replaying the same Source Material **MUST** produce events that are idempotent. A `UNIQUE` constraint on `(material_id, anchor_byte)` **MUST** be enforced by the database.
5.  **Ledger Immutability:** The `raw.temporal_ledger` **MUST** be append-only. `UPDATE` and `DELETE` operations **MUST** be forbidden by a database trigger.
6.  **Archive-on-Delete:** `DELETE` operations on `core.events` **MUST** be forbidden for normal users. They can only be performed by a special role during an audited operation, which **MUST** be governed by a trigger that atomically moves the row to `audit.archived_events`.
7.  **No Dangling Pointers:** The `source_event_ids` array in a live `core.events` row **MUST NOT** reference any `event_id` that has been moved to `audit.archived_events`.
8.  **Replay Discipline:** All state-mutating replays **MUST** be initiated via the `exo replay` command, which **MUST** perform a `preview` and respect safety `gates` before execution.
9.  **Unified Processor Model:** All autonomous services (satellites) **MUST** implement the `StatefulStreamProcessor` trait and be managed by the SDK's `StreamProcessorRunner` and `processor_main!` macro.
10. **Environment Scoping:** All shared resources (database names, NATS subjects, sockets, file paths) **MUST** be programmatically namespaced by the `SINEX_ENVIRONMENT` variable to ensure strict isolation between development, testing, and production.

---

## 1. Purpose and Principles (The Sinex Doctrine)

*   **Purpose:** To build a local-first, user-sovereign exocortex that captures a user's digital life as a stream of immutable, provenanced events, and provides tools to transform this stream into structured, actionable knowledge. The system must be **explainable, auditable, reversible, and personally useful.**
*   **Core Principles:**
    *   **Source Material is Ground Truth:** The raw bytes captured from the external world are the immutable evidence. Events are interpretations of that evidence.
    *   **Rebuildability via Replay:** All derived states (the Knowledge Graph, canonical events, tags) are projections of the event history and must be rebuildable by replaying the processors that created them.
    *   **Provenance Everywhere:** Every piece of information is traceable to its physical origin (a specific slice of Source Material) or its logical origin (a set of parent events).
    *   **Human Agency:** The system is a tool for cognitive augmentation, not replacement. The user is the final arbiter of meaning and truth, facilitated by `proposal/judgment/finalizer` workflows.

## 2. Architecture at a Glance

The architecture is a set of specialized, decoupled services communicating through a central data substrate and a real-time message bus, coordinated by a unified user-facing tool.

**Component Roles:**

*   **`sensd` (The Senses):** A new, central daemon responsible for all low-level data acquisition from the external world (files, sockets, APIs). It produces immutable `Source Material` blobs and a high-precision `Temporal Ledger`. **This component does not yet exist and needs to be built.**
*   **Processors (Satellites):** Rust binaries built on the `sinex-satellite-sdk`.
    *   **Ingestor Role:** Consumes `Source Material` provided by `sensd` and interprets it into first-order `core.events` with external provenance.
    *   **Automaton Role:** Consumes events from the NATS bus and synthesizes higher-order events with internal provenance.
    *   **Actuator Role:** Consumes instructional events from the bus and acts upon the external world (e.g., controlling the desktop).
*   **`ingestd` (The Gatekeeper):** The single-writer service for the canonical event log. It receives event creation requests from all processors via gRPC, validates them, commits them atomically to PostgreSQL, and then publishes them to NATS.
*   **`exo` (The Coordinator):** The primary user-facing CLI. It orchestrates all high-level system operations like staging new data, initiating replays, and managing the curation workflow.
*   **Data Substrate:**
    *   **PostgreSQL:** The "System of Record." Stores the canonical event log, the source material registry, the temporal ledger, and all derived/materialized state.
    *   **NATS JetStream:** The "Speed Layer." A durable message bus for real-time distribution of committed events to automata and other subscribers.
    *   **Content-Addressed Store:** A `git-annex` repository for storing the raw bytes of all `Source Material` blobs.

**Data Flow:**
```
[External World] -> [sensd] -> [Source Material (Annex/Git) + Temporal Ledger (DB)]
                                      | (MaterialSliceStream)
                                      v
[Ingestor Satellites] --(gRPC events)--> [ingestd] --(commit)--> [PostgreSQL]
                                                            |
                                                            v (post-commit)
[Automata/Actuators] <--(NATS subscribe)-- [NATS JetStream] <--(publish)--+
        |
        +-----(gRPC derived events)-------------------------------------> [ingestd] ...
```

---

## 3. The Refactoring & Implementation Blueprint

This section details the specific, actionable steps required to bring the current codebase into alignment with this canonical architecture.

### **Phase 1: Architectural Consolidation (Critical Path)**

**Objective:** Stabilize the core architecture, eliminate inconsistencies, and enforce invariants.

1.  **Unify the Processor Runtime:**
    *   **Current State:** Two competing runtimes exist (`StatefulStreamProcessor` vs. `NatsStreamConsumer`).
    *   **Target State:** `StatefulStreamProcessor` is the *only* trait for satellites.
    *   **Action:**
        *   Deprecate and remove the `NatsEventBatchProcessor` trait.
        *   Refactor the `StreamProcessorRunner` in the SDK. Its `scan` method, when running an `Automaton` in `Continuous` mode, will now internally manage a `NatsStreamConsumer` loop.
        *   This loop will feed batches of events to a new required trait method: `async fn process_event_batch(&mut self, events: Vec<Event>) -> SatelliteResult<...>;`.
        *   Refactor every automaton in `/crate/satellites/` to use `processor_main!` and implement its logic within `process_event_batch`.

2.  **Harden `ingestd` and Enforce Single-Writer:**
    *   **Current State:** `ingestd` has an inefficient N+1 batch insert and does not guarantee post-commit publish. Some automata are configured to publish directly to NATS.
    *   **Target State:** `ingestd` is the sole, atomic, high-performance writer.
    *   **Action:**
        *   Rewrite `ingestd`'s `batch_write_to_db` to use a single `UNNEST`-based `INSERT` statement for true batching.
        *   Implement the Transactional Outbox pattern: `BEGIN -> INSERT events -> INSERT outbox -> COMMIT`, followed by an async task that reads the outbox, publishes to NATS, and then deletes from the outbox. A new migration will be needed for the `core.outbox` table.
        *   Remove all `NatsPublisher` logic from all automata. Their only output channel is to send events to `ingestd`'s gRPC endpoint.

3.  **Enforce Schema Contracts in the Database:**
    *   **Current State:** Critical invariants (provenance XOR, idempotency) are conventions, not constraints. The PKM/Artifacts schema is obsolete.
    *   **Target State:** The database physically prevents invalid data states.
    *   **Action:**
        *   Create a new migration file in `sinex-db-migration`.
        *   Add the `UNIQUE(material_id, anchor_byte)` index to `core.events`.
        *   Add the `CHECK` constraint for the provenance XOR rule to `core.events`.
        *   Implement the `core.fn_archive_before_delete()` trigger function and apply it to `core.events`, as specified in `TARGET_final.md`.
        *   Create another migration to `DROP` the now-obsolete `core.artifacts`, `core.artifact_contents`, `core.artifact_tags`, and related tables.
        *   Remove all corresponding models and repository methods from `sinex-db` and `sinex-services`.

4.  **Refactor and Simplify `sinex-test-utils`:**
    *   **Current State:** Contains a mix of valuable infrastructure and confusing API wrappers.
    *   **Target State:** A lean, powerful toolkit that supports production-style testing.
    *   **Action:**
        *   Audit `TestContext` and remove all methods that are simple wrappers around repository calls (e.g., `get_recent_events`).
        *   Refactor all existing tests to use the direct `ctx.pool.events().get_recent()` pattern.
        *   Strengthen the `database_pool` logic and the `#[sinex_test]` macro based on any lessons learned during the other refactoring phases.

### **Phase 2: Build the `sensd` Universal Acquisition Layer (High Priority)**

**Objective:** Abstract away I/O complexity from ingestors, centralize acquisition logic.

1.  **Create the `sensd` Crate and Binary:**
    *   Create a new service in `crate/core/sinex-sensd`.
    *   Implement the `raw.sensor_jobs` and `raw.temporal_ledger` tables via a new migration.
    *   Build the core `sensd` daemon logic: a job manager that reconciles running sensors against the `raw.sensor_jobs` table.

2.  **Develop Core Sensor Libraries/Modules:**
    *   Create internal `sensd` modules (or separate crates like `sinex-sensor-socket`) for the key acquisition patterns: `append_stream` (for sockets/logs) and `tree_watch` (for filesystems).

3.  **Refactor `sinex-fs-watcher` (First Target):**
    *   Remove all `notify`-related code and file I/O logic from `sinex-fs-watcher`.
    *   Its `scan` method will now consume a `MaterialSliceStream` provided by `sensd`. Its only job is to parse the file content/metadata from the slice and create `file.*` events.
    *   The `exo blob stage --watch` command will now create a `sensor_jobs` entry for `sensd` to pick up.

### **Phase 3: Implement the Replay Discipline and Curation Flows**

**Objective:** Make the system safely evolvable and put the human in the loop.

1.  **Build the Replay Planner in `exo`:**
    *   Implement the logic to perform a non-mutating dry-run. This will involve querying `core.events` and `audit.archived_events` to calculate the impact of a proposed replay (archive counts, cascade depth, etc.).
    *   Implement the safety gate checks against this preview.

2.  **Integrate `operation_id`:**
    *   The `exo replay` command will first create an entry in `core.operations_log`, get the `operation_id`, and then pass this ID to the satellite as part of the `ScanArgs`.
    *   The `StreamProcessorContext` in the SDK will be responsible for setting this ID as a session variable (`SET LOCAL sinex.operation_id = '...'`) on the database connections it uses.

3.  **Build the Curation UI (`exo explore curate`):**
    *   Create the TUI or interactive CLI flow for reviewing ambiguity events (e.g., `system.ambiguity.potential_duplicate_found`).
    *   The actions in this UI (`[P]refer`, `[M]erge`, etc.) will trigger the appropriate, audited `exo replay` or `exo event archive` commands.

---
*The full content of `TARGET_final.md` will be appended below this refactoring plan, serving as the detailed specification for all the components mentioned.*


Invariants Quick Reference (one-page)
- Single-writer ingest: Satellites → ingestd → Postgres (commit) → publish to NATS (post-commit); no direct satellite writes to DB/bus for canonical events.
- Dual-layer provenance: External (material_id, anchor/offsets) XOR Internal (source_event_ids) per event.
- Idempotency for first-order events: UNIQUE(material_id, anchor_byte) when material_id present.
- Ledger append-only: raw.temporal_ledger forbids UPDATE/DELETE; unique(material_id, offset_start).
- Archive-on-delete: core.events deletions require operation_id; BEFORE DELETE trigger archives to audit.archived_events; application-immutable semantics.
- No live→archived references: source_event_ids in live events must not reference archived IDs.
- Replay discipline: preview (counts, cascades, anchor churn, time-quality flips), gates, then execute; always cascade.
- NATS-only speed layer: post-commit publish by ingestd; consumers read from NATS with durable semantics.
- Rebuild via replay: derived structures (KG, tags) are projections rebuildable by replay; events remain source of truth.
- Namespacing: SINEX_ENVIRONMENT scopes DB/schema names, streams, sockets, and paths.

Contents
1) Purpose and principles
2) Architecture at a glance
3) Data model and provenance
4) Sensing (sensd) and stage‑as‑you‑go
5) Ingester SDK and unified stream
6) Replay discipline and evolution semantics
7) Tagging and relations as event‑native
8) Agents and proposal/judgment/finalizer
9) Browser and terminal reconciliation templates
10) Observability and telemetry (module-driven)
11) Privacy posture (TBD placeholder)
12) Inclusion Rule
13) Appendices
    A) Natural Keys Registry (consolidated)
    B) sensd material lifecycle and recovery
    C) Recommended indexes and invariants (readable form)
    D) Processor Contracts (succinct checklists)
    F) Event Families Canon (canonical names and minimal payload fields)
— End of snapshot.

1) Purpose and principles
Purpose: a local‑first exocortex that captures digital life as events, preserves original evidence (“Source Material”), and evolves beliefs via deterministic replay. The system is explainable, auditable, reversible, and personally useful.

Core principles:
- Capture‑first, structure‑later: Source Material is ground truth; events are interpretations.
- Rebuildability via replay: derived state is reconstructed by replaying processors; tables/views are projections of event history, not separate sources of truth.
- Provenance everywhere: dual‑layer provenance (external material/anchor and internal event lineage).
- Single‑writer discipline: ingestd validates/writes to DB and publishes to the bus after commit.
- Replay discipline: preview, safety gates, always cascade, archive‑on‑delete with operation_id.
- Human agency: proposal/judgment/finalizer flows; LD is mediated and attributable.
- Evolution‑aware ops: services are toggled/replaceable; post‑commit publish is a property, not tied to any one bus implementation.

2) Architecture at a glance
Roles:
- Sensing (sensd): acquires Source Material (files, sockets, APIs, DBs), manages rotation, writes temporal ledger.
- Ingestors: consume slices of Source Material; emit first‑order events with external provenance.
- Automata: deterministic synthesizers producing higher‑order events from event history.
- Agents: stochastic processors producing proposals/insights with strict provenance.
- Gateway + CLI (exo): command/response; replay/archival operations; curation flows; replay planner lives here and is non‑mutating.
- Explore (TUI/Web): timeline, provenance overlays, replay preview, source explorer, curation queue.

Data plane:
- Postgres (archive and serving store: core.events, raw.* registries, audit). Timescale/pgvector may be used; not required by doctrine.
- Speed layer: NATS JetStream (current). Invariant is “post‑commit durable publish”; implementation can change without altering this property.
- Content‑addressed store for large artifacts (e.g., git‑annex for blobs; git for text where applicable).

Ingest discipline:
- Satellite → ingestd → Postgres (commit) → publish to NATS → Automata/agents consume. Satellites never write canonical events directly to DB or bus.
- ingestd validation cache (fail‑closed): ingestd maintains an in‑memory cache of active schemas keyed by (source, event_type). Events with unknown/inactive schema or violated provenance XOR are rejected before insert (fail‑closed). Database JSON Schema CHECK/trigger is a safety net; app‑side validation is authoritative. Ingest path remains: validate → batch insert → commit → post‑commit publish to NATS.
- Gateway request/response durability: Gateway may fast‑path responses to the client for UX, but all api.response.* must be persisted as events via ingestd (post‑commit property preserved). Failures are emitted as explicit error events.
- Single‑writer enforcement (dev/CI): satellites must not link the canonical bus/DB write client for canonical events; integration tests assert canonical events only appear after DB commit (post‑commit publish).
- Active inference safety (minimal): actuations execute only from trusted sources (deny‑by‑default); all actuations are events; side effects logged and auditable.

3) Data model and provenance
Source Material (ground truth):
- Registry per capture material with status, rotation policy, timing model, metadata, storage path (annex or git for text).
- Temporal ledger per slice (append‑only) recording capture time, precision (exact|bounded), clock (monotonic|wall), and source_type (realtime_capture|intrinsic_content|inferred_mtime|inferred_ctime|inferred_user).

Events (core.events):
- External provenance: material_id, offset_kind (byte|line|rowid|logical), offset_start/offset_end, anchor_byte.
- Internal provenance: source_event_ids ULID[] (lineage for derived/synthesized events).
- Bitemporal fields: ts_orig (semantic event time; derived per precedence below), ts_ingest (derived from ULID).
- Archive‑on‑delete: BEFORE DELETE trigger moves rows to audit.archived_events; requires session operation_id; preserves superseded_by_event_id when applicable (application‑immutable: changes occur only via archive‑and‑replace).
- Tie‑breaks: when ts_orig is equal, order by event_id (ULID) deterministically for replay and projections.

ts_orig derivation precedence:
- temporal ledger (realtime_capture) > intrinsic content timestamp > inferred_mtime > inferred_ctime > inferred_user > staged_at. Record time_quality accordingly; consumers should display/source this in provenance narratives.

Constraints and invariants (readable form):
- UNIQUE(material_id, anchor_byte) WHERE material_id IS NOT NULL (idempotency for first‑order events).
- CHECK XOR: either external provenance present (material‑anchored) XOR internal provenance present (derived lineage).
- No live event may reference an archived ID in source_event_ids (enforced by CI “always‑cascade” check).
- Temporal ledger is append‑only; unique(material_id, offset_start).
- Recommended indexes: BTREE(material_id, anchor_byte), GIN(source_event_ids).

Projections:
- Use replay semantics to reconstruct derived state. Knowledge graph and tags are event‑native and materialized as projections when needed.

4) Sensing (sensd) and stage‑as‑you‑go
Concept:
- sensd centralizes acquisition. It creates in‑flight Source Material registry rows, writes bytes to canonical storage, updates temporal ledger per slice, rotates/finalizes materials with statuses (sensing → completed|recovered_partial|failed). Zero‑gap invariant: open the next before finalizing current for continuous streams; recovered_partial is used only for crash recovery.

Jobs and state (backing tables contract):
- raw.sensor_jobs (contract): job_id ULID PK, sensor_type TEXT, target_uri TEXT, source_identifier TEXT, acquisition_mode JSONB, parameters JSONB, owner TEXT, resource_limits JSONB, status TEXT, priority INT, created_at, updated_at.
- raw.sensor_states (contract): job_id ULID FK, current_position JSONB, last_successful_acquisition TIMESTAMPTZ, error_count INT, throughput JSONB, updated_at TIMESTAMPTZ.
- These records are the single source of truth for sensing configuration, ownership, and progress metrics. Jobs encode pattern/fetch/cursor; states track last positions and metrics.

Pattern catalog (declarative configs stored in raw.sensor_jobs.config):
- append_stream (logs, sockets, JSONL)
- batched_pull (API pagination; cursor/ETag)
- replace_snapshot (CSV/SQLite snapshotting)
- multi_file and tree_watch (filesystem drops and trees)
- db_snapshot and db_wal (DB backup API and WAL frames; WAL later with robust tests)
- rolling_window and changefeed (where the source supports it)

Operational outputs:
- raw.source_material_registry: identity, status, rotation policy, timing info, host/user, metadata.
- raw.temporal_ledger: per‑slice capture times and offsets (append‑only).

5) Ingester SDK and unified stream
Unified stream API:
- MaterialSliceStream yields Slice { material_id, offset_kind, offset_start/end, anchor_byte, bytes } and Control frames { RotationBoundary, EndOfMaterial, Gap{reason} }.
- Works identically for in‑flight and finalized materials; mode is advisory.

Helpers:
- SliceAssembler for record reassembly (e.g., line or JSON delimiter).
- LedgerReader + derive_ts_orig to compute ts_orig and time_quality.
- IdempotenceKey(material_id, anchor_byte, event_type) helpers and insert_or_ignore semantics.
- RowIdentitySpec + SnapshotDiff for snapshot sources (diff to inserts/updates/deletes).
- WindowedMatcher and normalization helpers for reconciliation (terminal/browser).
- Diagnostics emitter for anchor mismatches, backpressure, snapshot anomalies.
- Anchor rules: deterministic slicing; each ingestor registers anchor_rule_id and anchor_rule_version in its processor manifest; replay planner fetches these from processor_manifests to compute “anchor churn” and MUST emit ingestion.anchor_mismatch with expected/observed details when recomputed‑vs‑prior anchors diverge; subject to planner gates.

Ingester contract:
- Deterministic slicing; populate external provenance for first‑order events; archive‑and‑replace on replay with cascades.

6) Replay discipline and evolution semantics
CLI verb:
- exo replay --processor <name> [--blob <material_id> | --since/--until] [--dry-run]
  - Ingestor replay requires material_id; automaton replay requires a time window.
- Gateway/exo replay RPC envelope (minimal): { processor, mode: ingestor|automaton, scope: { blob_id | time_window }, dry_run: bool, operation_id }. This ensures operation_id and related session variables are wired correctly for archive triggers and audit.

Replay planner and gates:
- A non‑mutating replay planner (in gateway/exo) computes preview and enforces configurable gates. It does not mutate state; execution uses the same operation_id and scope established by the gateway. Defaults: anchor_churn_threshold_percent=5, time_quality_flip_threshold_percent=2, max_cascade_depth_warn=5, require_force_on_schema_mismatch=true. Reference values are also listed in Appendix E.6.
- Preview shows: archive/replace counts, cascade depth histogram, anchor churn %, time‑quality flips, storage fetch cost.

Execution:
- Always cascade: new rows inserted, old rows archived via trigger, derived rows updated/archived as needed, ensuring no live→archived references remain (CI checks enforce).
- operation_id required for any DELETE (archive), recorded in operations_log; execution reuses the planner’s scope and the same operation_id.
- Ordering and idempotency: automata MUST process inputs ordered by (ts_orig ASC, id ASC); outputs MUST be idempotent (insert‑if‑absent by exact source_event_ids). Use small‑batch checkpoints to bound rework. Replays MUST keep this ordering and idempotency to enable safe replays.
- Optional restore/rollback symmetry: when present, perform atomic subtree swap—archive replacements with linkage, reinstate originals from audit, then remove restored duplicates.

7) Tagging and relations as event‑native
Events:
- tag.definition.created/updated/deleted
- tag.assignment.proposed/confirmed/removed { tag_name|tag_id, taggable_type, taggable_id, confidence, evidence: source_event_ids, actor }
- relation.proposed/confirmed/removed { from_event_id, to_event_id, relation_type, confidence, detection_source }

Tables as projections:
- core.tags, core.tagged_items (with rebuild via replay)
- core.event_relations (+ clusters/members if used)
- Finalizers write tables on confirmed events; all are rebuildable from event history.

8) Agents and proposal/judgment/finalizer
Agents:
- Produce proposals/insights; include provenance minimums: agent_name, agent_version, model_name, model_version, prompt_hash | prompt_text, params_hash, run_id, tokens_in, tokens_out, cost_estimate (optional), and input_refs. These are carried in outputs and referenced by proposals/finalizers.
- Gateway durability: responses can be fast‑pathed to clients for UX, but agent api.response.* MUST be persisted via ingestd; failures become explicit error events.
- This document keeps roles generic to avoid locking old examples; provenance requirements remain strict.

Proposal/judgment/finalizer loop:
- Proposals: <domain>.proposal.* with targets, suggestion payload, confidence, evidence, rationale.
- Judgments: user.curation.judgment { proposal_id, verdict, corrected_payload?, comment? }.
- Finalizer: deterministic mapping to confirmed state; archives superseded syntheses; emits proposal.superseded.

LD (Living Document):
- MVP full‑text replacement with diff logging and provenance; future refinement (e.g., JSON Patch) is permissible under replay discipline.

9) Browser and terminal reconciliation templates
Browser:
- Sensing: WebExtension + native messaging (append_stream) for live navigation; DB snapshot/WAL for history (SQLite) as feasible.
- Reconciliation: windowed matching on URL + ts_orig, with source priority and time_quality marks.
- Output: browser.pageview.canonical with source_event_ids, confidence, provenance.

Terminal:
- Sensing: Atuin DB snapshots, IPC, histfiles, asciinema.
- Reconciliation: windowed matching (±2s), session/tty filters, field union rules (cwd, duration, exit_code) with priority order and normalization (argv canonicalization, secret masking).
- Output: terminal.command.executed.canonical with source_event_ids, time_quality, confidence.

10) Observability and telemetry (module-driven)
- Use the existing telemetry module as the primary mechanism.
- Capture, at minimum, examples (non‑exhaustive): ingestd commit‑to‑publish latency, NATS consumer lag, annex probe results, anchor churn %, coverage gaps/overlaps, replay preview vs execution latency.
- operations_log: Every replay/archive/restore writes core.operations_log { operation_id ULID, actor, scope (processor, window/blob filters), preview summary (counts, cascades, churn, flips), started_at, finished_at, outcome (success|error) }. Explore links here for provenance narratives. Detailed schema is in Appendix E.
- Presentation (e.g., Grafana) is an implementation detail; telemetry must make these measures queryable.

11) Privacy posture (minimal invariant; TBD details)
- Minimal invariant while detailed policy is TBD:
  - Private mode emits an event and MUST be enforced by all processors (deny‑by‑default while private).
  - sensd MUST NOT capture sources marked private while private mode is active.
  - All privacy toggles are auditable events.
  - Redaction/vaulting emits events; dependent syntheses are archived via replay using operation_id to preserve provenance integrity.
- Detailed masking rules (e.g., Vector/VRL allowlist/masking) will be integrated later without violating archive‑on‑delete invariants or rebuildability.

12) Inclusion Rule
- Unless explicitly superseded or contradicted later, earlier concepts from the discussion history are considered integrated and valid within this final.
- Appendices summarize integrated items that originated earlier for traceability; their presence here confirms inclusion.

13) Appendices

A) Natural Keys Registry (consolidated)
- browser.page_visit: (host, tab_id, url, event_ts) with transition_type or navigation sequence when available.
- focus.window: (host, pid, window_class, window_title, ts_bucket_short).
- screen.text_ocr: (host, region_hash, text_hash, ts_bucket_short).
- audio.segment_raw: (host, blob_sha256).
- audio.transcript: (origin_blob_sha256, model_name, model_version, language).
- terminal.session_cast: (host, blob_sha256).
- terminal.command: (host, ts_bucket_short, normalized_command_hash).
- webpage.snapshot_html: (source_url, blob_sha256).
- webpage.text_extracted: (source_blob_sha256, extractor_id/version).
- bookmark.raindrop: (raindrop_id).
- chat.message_import: (platform, conversation_id_platform, message_id_platform); fallback: (platform, conversation_id_platform, role, content_hash, ts_bucket_short).
- self.*: per category; (user_ts/bucket, category, optional context hash).
- ld.delta audit: (target_note_id, patch_hash, model_id/version, event_ts_bucket).
- screen_recording_manual: (host, blob_sha256).

B) sensd material lifecycle and recovery
- Statuses: sensing → completed | recovered_partial | failed.
- Zero‑gap invariant for continuous materials: next material staged before finalizing current.
- Recovery: orphaned in‑flight segments are finalized as recovered_partial; replay can close gaps using historical slices; ledger continuity remains append‑only.

C) Recommended indexes and invariants (readable form)
- core.events: BTREE(material_id, anchor_byte) WHERE material_id IS NOT NULL.
- core.events: GIN(source_event_ids).
- raw.temporal_ledger: UNIQUE(material_id, offset_start); BTREE(material_id, offset_start, offset_end); BTREE(timestamp_value, source_type).
- Archive trigger on core.events requires operation_id and writes to audit.archived_events; application‑immutable semantics (changes occur via archive‑and‑replace).
- Namespacing: SINEX_ENVIRONMENT scopes DB/schema names, streams, sockets, paths.
- Material store types: annex | git | fs. If using git for text materials, content diffs produce events; store commit SHA in the Source Material registry; events reference the source blob via commit+path to preserve replayability.

D) Processor Contracts (succinct checklists)

sensd (Sensing)
- [ ] Create in‑flight Source Material registry row before emission by dependents
- [ ] Append bytes to canonical storage; compute offsets deterministically
- [ ] Write temporal ledger per slice (ts_capture, precision, source_type, confidence? for inferred); append‑only
- [ ] Enforce zero‑gap for continuous materials (stage next before finalize current)
- [ ] Retry/backoff: maintain exponential backoff parameters and max_retries in sensor state; mark job failed after exhaustion and emit sensor.error/backoff_exhausted events
- [ ] Rotate/finalize with statuses: sensing → completed | recovered_partial | failed
- [ ] Emit diagnostics for backpressure/gaps; recovery finalization for orphaned segments

Ingestor
- [ ] Consume MaterialSliceStream; deterministic slicing; anchor rule id/version recorded (processor_manifests)
- [ ] Compute ts_orig from ledger/intrinsic; set time_quality
- [ ] Populate external provenance (material_id, offset_kind, offsets, anchor_byte)
- [ ] Insert with idempotency: UNIQUE(material_id, anchor_byte) for first‑order events
- [ ] On replay: insert new rows, archive old (always cascade)
- [ ] Emit diagnostics for anchor mismatch, malformed slices

Automaton
- [ ] Deterministic transforms over event history; no external mutable state for correctness
- [ ] Use internal provenance (source_event_ids) to reference inputs
- [ ] On replay: insert then archive/replace downstream deterministically; preserve lineage
- [ ] Respect invariants: no live→archived references; cascade updates as needed

Agent
- [ ] Produce proposals/insights with provenance: agent_name/version, model_name/version, params_hash, run_id, input_refs, optional costs
- [ ] Use proposal/judgment/finalizer; finalizer writes confirmed state; archive superseded syntheses
- [ ] Avoid hidden side effects; prefer idempotent writes via ingestd

Gateway/CLI (exo)
- [ ] Replay verb required; preview → gates → execute; requires operation_id for archival
- [ ] RPC envelope (minimal): { processor, mode: ingestor|automaton, scope: { blob_id | time_window }, dry_run: bool, operation_id }
- [ ] Blob/event archival commands always cascade; audit trail populated
- [ ] Namespaced operations via SINEX_ENVIRONMENT; telemetry spans for commit→publish and replay latency

F) Event Families Canon (canonical names and minimal payload fields)

Input
- input.key: { key, action (down|up|repeat), modifiers?, device?, ts_client? }
- input.mouse: { kind (move|click|scroll), button?, delta?, position?, device?, ts_client? }

Focus/Window
- focus.window: { window_class, window_title, pid?, workspace?, app_id?, ts_client? }

Browser
- browser.page_visit: { url, transition_type?, referrer?, tab_id?, window_id?, title?, ts_client? }
- browser.dom_event: { event_name, selector?, value_hash?, url?, tab_id?, ts_client? }
- browser.media_event: { media_kind (audio|video), action (play|pause|seek|end), url?, tab_id?, position_s?, ts_client? }
- browser.bookmark_added: { url, title?, folder_path?, source (manual|import), ts_client? }

Webpage processing
- webpage.snapshot_html: { source_url, blob_sha256, size_bytes?, mime?, charset?, title?, ts_client? }
- webpage.text_extracted: { source_blob_sha256, extractor_id, extractor_version, text_hash, length_chars, ts_client? }
- webpage.summary: { source_blob_sha256|source_text_hash, model_name, model_version, summary_hash, tokens_in?, tokens_out?, ts_client? }

Audio
- audio.segment_raw: { blob_sha256, mime?, duration_s?, sample_rate_hz?, channels?, ts_client? }
- audio.transcript: { origin_blob_sha256, model_name, model_version, language?, text_hash, duration_s?, ts_client? }

Screen/OCR
- screen.text_ocr: { region_hash, text_hash, bbox?, page?, confidence?, ts_client? }

Terminal
- terminal.session_cast: { blob_sha256, tty?, shell?, duration_s?, cmds_count?, ts_client? }
- terminal.command: { argv_norm_hash, argv?, cwd?, exit_code?, duration_ms?, tty?, session_id?, ts_client? }

Bookmarks/Reading
- bookmark.raindrop: { raindrop_id, url, title?, tags?, collection?, created_at?, ts_client? }

Chats
- chat.conversation_import: { platform, conversation_id_platform, title?, participants[], created_at? }
- chat.message_import: { platform, conversation_id_platform, message_id_platform, role (user|assistant|system|tool), content_hash, parent_id?, attachments?, ts_client? }

Self-tracking
- self.mood_event: { mood (scale), context?, note?, ts_client? }
- self.task_event: { task_id?, title?, status (created|started|done|blocked), project?, tags?, ts_client? }
- self.substance_event: { substance, dose?, unit?, route?, note?, ts_client? }

LD operations
- ld.input: { section?, intent?, text_hash, cursor?, ts_client? }
- ld.delta: { target_note_id, patch_hash|full_text_hash, model_name?, model_version?, rationale_hash?, ts_client? }

Metrics/Diagnostics (internal)
- system.heartbeat: { node, version?, uptime_s? }
- ingestion.anchor_mismatch: { processor, material_id, anchor_byte, rule_id, expected?, observed? }
- annex.probe: { sample_size, failures, bytes_missing, duration_ms }

Notes
- ts_client is optional client-side time if present; ts_orig is derived per doctrine from temporal ledger or intrinsic timing.
- Minimal payloads carry identities and hashes; large text is expected to live in blobs or be referenced by hashes when appropriate.
- These families name canonical event_type values and minimal payload keys; producers may add more fields, but canonical keys must exist when applicable.

Minimal diagram (text)
[Satellites] --(slices)--> [sensd: Source Material + Ledger]
[sensd] --(materials)--> [Ingestors: MaterialSliceStream] --(validated events)--> [ingestd]
[ingestd] --(commit)--> [Postgres] --(publish post‑commit)--> [NATS] --> [Automata/Agents]
[Automata/Agents] --(derived events via ingestd)--> [Postgres]
[Explore/Gateway/CLI] --(replay/curate)--> [ingestd] --(archive/replace)--> [Postgres + audit]

— End of snapshot.

J) Explore UX Canon (MVP panels, required data hooks, minimal query surfaces)

Purpose
- Define a minimal, consistent Explore experience to inspect history, understand provenance and changes, and safely run replays/curations. Keep it bus/DB‑agnostic; rely only on committed events and the existing telemetry module.

MVP Panels

1) Timeline
- Goal: Navigate events over time, filter by families/sources, and inspect provenance quickly.
- Required data hooks:
  - Event stream: SELECT id, event_type, ts_orig, ts_ingest, payload_summary, material_id?, anchor_byte?, source_event_ids?
  - Provenance overlays: material_id + anchor_byte; source_event_ids presence; time_quality flag (if computed)
  - Gaps overlay: derived from Source Material and ledger continuity (zero‑gap invariant; recovered_partial flags)
- Minimal queries:
  - Time‑bounded event list with filters (families, hosts, processors)
  - Per‑event fetch for envelope, provenance, and payload preview

2) Replay Preview
- Goal: Show what would change before executing replay; highlight risk.
- Required data hooks:
  - Replay planning service (or SQL estimator) to compute:
    - counts: insert, archive, replace by family
    - cascade depth histogram
    - anchor churn % (material‑anchored deltas)
    - time‑quality flips
  - Safety gates (thresholds) to require explicit confirmation
- Minimal queries:
  - Estimator output persisted or computed on‑demand
  - Operation scope summary (processor, window/blob, filters)

3) Source Explorer
- Goal: Monitor Source Material lifecycles and ledger integrity.
- Required data hooks:
  - raw.source_material_registry: id, source_identifier, status (sensing|completed|recovered_partial|failed), rotation_policy, staged_at/start_time/end_time, host/user
  - raw.temporal_ledger: material_id, offset_start/offset_end, ts_capture, precision, source_type
- Minimal queries:
  - Materials list with status filters and date ranges
  - Ledger continuity for a selected material
  - Recovered segments listing

4) Curation Queue
- Goal: Review proposals, make judgments, and produce confirmed state.
- Required data hooks:
  - Proposal events (<domain>.proposal.*) with evidence (source_event_ids), confidence, rationale
  - user.curation.judgment and finalizer outputs
- Minimal queries:
  - Proposals awaiting judgment by domain/time/priority
  - Evidence fetch (linked events + payload previews)
  - Judgment submission via gateway/ingestd

5) Provenance Narrative
- Goal: Explain “why this event has this time and content”.
- Required data hooks:
  - External provenance (material_id, anchor_byte, offsets)
  - Internal provenance (source_event_ids)
  - Time‑quality derivation source
  - Processor manifest excerpt (anchor_rule_id/version when applicable)
- Minimal queries:
  - Join by event_id to material/ledger and to parent source_event_ids
  - Manifest lookup by processor_id/version

Operator Workflows (MVP)

A) Safe Replay
- Select scope (processor + blob or time window) → Preview (counts, cascades, churn, flips) → If gates exceeded, require explicit confirm → Execute with operation_id → Show results and audit links.

B) Gap Inspection
- From Timeline or Source Explorer: highlight gaps/recovered_partial materials → Jump to relevant material/ledger slice → Optionally trigger historical replay to close gaps.

C) Proposal Review
- Open Curation Queue → Inspect proposal evidence and provenance narrative → Accept/Reject/Modify → Finalizer writes confirmed state → Link any superseded syntheses.

Data Model Notes for Explore
- Planner location: the replay planner runs in gateway/exo (non‑mutating) to compute previews; execution reuses the same scope+operation_id.
- No new tables required; Explore reads from:
  - core.events (and optional projections for KG/tags)
  - audit.archived_events
  - raw.source_material_registry and raw.temporal_ledger
  - operations_log (for operation_id narratives)
  - processor_manifests (for anchor_rule/version explainers)
- Heavy previews (replay planning) are performed by a service/command returning a summary payload; Explore renders the result.

Minimal Telemetry Hooks (examples, non‑prescriptive)
- commit→publish latency (ingestd)
- NATS consumer lag (per group)
- annex probe stats
- anchor churn; coverage gaps
- replay preview vs execution latency

Non‑goals (MVP)
- Full‑fidelity diffs for large payloads (use sample + narrative)
- Cross‑environment aggregation (respect SINEX_ENVIRONMENT scoping)
- UI framework choice (TUI vs Web is an implementation concern)


E) DDL and CI checks (executable invariants)

-- XOR: exactly one provenance mode (external XOR internal)
ALTER TABLE core.events
  ADD CONSTRAINT IF NOT EXISTS events_provenance_xor
  CHECK (
    (material_id IS NOT NULL AND source_event_ids IS NULL)
    OR
    (material_id IS NULL AND source_event_ids IS NOT NULL)
  );

-- Idempotency for first-order events (anchor uniqueness within material)
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_material_anchor
  ON core.events(material_id, anchor_byte)
  WHERE material_id IS NOT NULL;

-- GIN index for provenance traversal and cascade planning
CREATE INDEX IF NOT EXISTS ix_events_source_event_ids
  ON core.events USING GIN (source_event_ids);

-- Recommended serving indexes
CREATE INDEX IF NOT EXISTS ix_events_ts_orig ON core.events (ts_orig DESC);
CREATE INDEX IF NOT EXISTS ix_events_type_ts ON core.events (event_type, ts_orig DESC);

E.2 audit.archived_events and archive trigger (application‑immutable)
-- Archive table mirrors core.events + audit fields
CREATE TABLE IF NOT EXISTS audit.archived_events (LIKE core.events INCLUDING ALL);

ALTER TABLE audit.archived_events
  ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS archived_by TEXT,
  ADD COLUMN IF NOT EXISTS archive_reason TEXT,
  ADD COLUMN IF NOT EXISTS superseded_by_event_id ulid NULL;

-- Require operation_id for any delete; move OLD row into archive with context
CREATE OR REPLACE FUNCTION core.fn_archive_before_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  op_id TEXT := current_setting('sinex.operation_id', true);
  sup_id ulid := NULLIF(current_setting('sinex.superseded_by_id', true), '');
  who TEXT := current_setting('sinex.archived_by', true);
  why TEXT := current_setting('sinex.archive_reason', true);
BEGIN
  IF op_id IS NULL OR op_id = '' THEN
    RAISE EXCEPTION 'DELETE requires sinex.operation_id to be set in this session';
  END IF;

  INSERT INTO audit.archived_events (
    id, event_type, source, ts_orig, ts_ingest, host, payload,
    material_id, offset_kind, offset_start, offset_end, anchor_byte,
    source_event_ids, payload_schema_id, processor_manifest_id,
    archived_at, archived_by, archive_reason, superseded_by_event_id
  )
  VALUES (
    OLD.id, OLD.event_type, OLD.source, OLD.ts_orig, OLD.ts_ingest, OLD.host, OLD.payload,
    OLD.material_id, OLD.offset_kind, OLD.offset_start, OLD.offset_end, OLD.anchor_byte,
    OLD.source_event_ids, OLD.payload_schema_id, OLD.processor_manifest_id,
    now(), who, why, sup_id
  );

  RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS trg_events_archive_before_delete ON core.events;
CREATE TRIGGER trg_events_archive_before_delete
BEFORE DELETE ON core.events
FOR EACH ROW EXECUTE FUNCTION core.fn_archive_before_delete();

-- Temporal ledger (append‑only capture‑time records)
CREATE TABLE IF NOT EXISTS raw.temporal_ledger (
  entry_id ulid PRIMARY KEY,
  material_id ulid NOT NULL REFERENCES raw.source_material_registry(blob_id) ON DELETE CASCADE,
  offset_start BIGINT NOT NULL,
  offset_end BIGINT NOT NULL,
  offset_kind TEXT NOT NULL CHECK (offset_kind IN ('byte','line','rowid','logical')),
  ts_capture TIMESTAMPTZ NOT NULL,
  precision TEXT NOT NULL CHECK (precision IN ('exact','bounded')),
  clock TEXT NOT NULL CHECK (clock IN ('monotonic','wall')),
  source_type TEXT NOT NULL CHECK (source_type IN ('realtime_capture','intrinsic_content','inferred_mtime','inferred_ctime','inferred_user')),
  note TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(material_id, offset_start)
);

CREATE INDEX IF NOT EXISTS ix_tl_material_offsets ON raw.temporal_ledger (material_id, offset_start, offset_end);
CREATE INDEX IF NOT EXISTS ix_tl_ts ON raw.temporal_ledger (ts_capture, source_type);

-- Additional ledger rules
-- For realtime streams, precision is 'exact' with a bounded error specification (±5 ms default).
-- For inferred sources, set precision='bounded' and populate confidence; processors should document bounds.
-- Append‑only trigger (block UPDATE/DELETE)
CREATE OR REPLACE FUNCTION raw.fn_temporal_ledger_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'raw.temporal_ledger is append-only (no % allowed)', TG_OP;
END $$;

DROP TRIGGER IF EXISTS trg_tl_no_update ON raw.temporal_ledger;
CREATE TRIGGER trg_tl_no_update
BEFORE UPDATE OR DELETE ON raw.temporal_ledger
FOR EACH ROW EXECUTE FUNCTION raw.fn_temporal_ledger_append_only();

E.4 JSON Schema validation (safety net)
-- Example: require payload to match registered schema if payload_schema_id present.
-- This assumes a helper function json_matches_schema(schema_json JSONB, payload JSONB) exists.
CREATE OR REPLACE FUNCTION core.fn_validate_event_payload()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  schema_json JSONB;
BEGIN
  IF NEW.payload_schema_id IS NULL THEN
    RETURN NEW; -- schemaless allowed
  END IF;

  SELECT schema_content INTO schema_json
  FROM sinex_schemas.event_payload_schemas
  WHERE id = NEW.payload_schema_id;

  IF schema_json IS NULL THEN
    RAISE EXCEPTION 'Unknown payload_schema_id %', NEW.payload_schema_id;
  END IF;

  IF NOT json_matches_schema(schema_json, NEW.payload) THEN
    RAISE EXCEPTION 'Payload does not conform to schema %', NEW.payload_schema_id;
  END IF;

  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_events_validate_payload ON core.events;
CREATE TRIGGER trg_events_validate_payload
BEFORE INSERT OR UPDATE ON core.events
FOR EACH ROW EXECUTE FUNCTION core.fn_validate_event_payload();

E.5 CI checks (SQL snippets and expectations)
-- No live→archived references (provenance integrity)
WITH archived AS (SELECT id FROM audit.archived_events)
SELECT COUNT(*) AS live_refs_archived
FROM core.events e
WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived);
-- Expect 0 rows; fail CI if > 0.

-- XOR provenance check violations
SELECT COUNT(*) AS xor_violations
FROM core.events
WHERE (material_id IS NULL AND source_event_ids IS NULL)
   OR (material_id IS NOT NULL AND source_event_ids IS NOT NULL);
-- Expect 0.

-- Anchor uniqueness violations (should be impossible due to UNIQUE index)
SELECT material_id, anchor_byte, COUNT(*) c
FROM core.events
WHERE material_id IS NOT NULL
GROUP BY material_id, anchor_byte
HAVING COUNT(*) > 1;

-- Temporal ledger append-only (enforced by trigger)
-- CI can attempt an UPDATE/DELETE and expect failure.

-- Required indexes exist (assert presence in pg_indexes)
-- ix_events_source_event_ids, ux_events_material_anchor, ix_tl_material_offsets, ix_tl_ts, ix_events_ts_orig, ix_events_type_ts.

E.6 Replay gates (configuration defaults)
-- Defaults (tunable) enforced by replay planner:
-- anchor_churn_threshold_percent = 5
-- time_quality_flip_threshold_percent = 2
-- max_cascade_depth_warn = 5
-- require_force_if_any_schema_mismatch = true

Notes
- The JSON schema validation trigger is a safety net; primary validation occurs in ingestd against an active schema cache (Architecture: ingestd validates schema, enforces provenance XOR, batches to Postgres, then publishes to NATS after commit).
- The archive trigger relies on session settings:
  - sinex.operation_id (REQUIRED)
  - sinex.archived_by (optional, e.g., user@host)
  - sinex.archive_reason (optional)
  - sinex.superseded_by_id (optional when 1:1 mapping exists)
- For performance, consider partitioning audit.archived_events by archived_at if volume is high.

