> **Status:** Target architecture specification (aligned with JetStream-first ingestion)
> **Last Updated:** 2025-11-13
> This document describes the end-state architecture after the JetStream migration (phases 1–5) completes.
> **Purpose:** System-level target state reference (use it to evaluate future proposals; implementation details live in component docs).
> **Historical context:** Older sections mention retired pipelines for contrast. Those paths are retired; treat them as archival comparisons and follow `docs/current/architecture/Core_Architecture.md` for the live JetStream implementation.

Invariants Quick Reference (one-page)

- Single-writer storage: nodes publish raw slices/events to JetStream. `sinex-ingestd` is the exclusive writer of canonical Postgres rows and the sole publisher of confirmation events (`events.confirmations.*`). nodes never write to Postgres directly.
- Dual-layer provenance: External (material_id, anchor/offsets) XOR internal (source_event_ids) per event.
- Idempotency for first-order events: UNIQUE(material_id, anchor_byte) when material_id is present.
- Ledger append-only: `raw.temporal_ledger` forbids UPDATE/DELETE; unique(material_id, offset_start).
- Archive-on-delete: `core.events` deletions require `operation_id`; BEFORE DELETE trigger archives to `audit.archived_events`; rows are never mutated in place.
- No live→archived references: `source_event_ids` in live events must not reference archived IDs.
- Replay discipline: preview (counts, cascades, anchor churn, time-quality flips), enforce gates, then execute; always cascade.
- JetStream speed layer: ingestd consumes `events.raw.*`, validates, commits, then publishes confirmations (`events.confirmations.*`). Durable consumers guard downstream ordering.
- Rebuild via replay: derived structures (KG, tags, search indexes) are projections rebuildable by replay; events remain the source of truth.
- Namespacing: `SINEX_ENVIRONMENT` scopes DB/schema names, JetStream streams, sockets, and paths.

> **Cross-reference (2025-11-13)**
> Interface-level implementation notes for the gateway and CLI now live in `crate/core/sinex-gateway/docs/overview.md` and `docs/current/architecture/UserInteraction_And_Query_Architecture.md`. Treat this document as the system-level target state.

Contents

1) Purpose and principles
2) Architecture at a glance
3) Data model and provenance
4) Material acquisition (Stage-as-You-Go)
5) Ingester SDK and unified stream
6) Replay discipline and evolution semantics
7) Tagging and relations as event‑native
8) Agents and proposal/judgment/finalizer
9) Browser and terminal reconciliation templates
10) Observability and telemetry (module-driven)
11) Privacy posture (TBD)
12) Inclusion Rule
13) Appendices
    A) Natural Keys Registry (consolidated)
    B) Material lifecycle and recovery
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

- nodes: capture Source Material using AcquisitionManager (Stage-as-You-Go), publish slices/events directly to NATS JetStream.
- Ingestd: JetStream consumer that archives materials into git-annex, persists events to Postgres, publishes confirmations.
- Automata: deterministic synthesizers producing higher‑order events from confirmed event streams.
- Agents: stochastic processors producing proposals/insights with strict provenance.
- Gateway + CLI (exo): command/response; replay/archival operations; curation flows; replay planner lives here and is non‑mutating.
- Explore (TUI/Web): timeline, provenance overlays, replay preview, source explorer, curation queue.

Data plane:

- Postgres (archive and serving store: core.events, raw.* registries, audit). Timescale/pgvector may be used; not required by doctrine.
- Speed layer: NATS JetStream. Invariant is "post‑commit durable publish"; implementation can change without altering this property.
- Content‑addressed store for large artifacts (git‑annex for blobs; git for text where applicable).

Ingest discipline (JetStream-first):

- nodes → NATS JetStream → ingestd consumer → Postgres (commit) → confirmations published to NATS → Automata consume. nodes publish slices/events directly; ingestd is the single writer to canonical tables.
- ingestd validation cache (fail-soft): ingestd consumes `events.raw.*`, keeps the latest active JSON Schemas keyed by (source, event_type), and validates payloads before persistence. Invalid payloads are NAKed and routed to the DLQ; missing/inactive schemas emit warnings but the event is still accepted so that database CHECK constraints and the provenance XOR invariant remain the final guardrails. Flow: consume → validate (warn-on-miss) → batch insert → commit → emit confirmations (`events.confirmations.*`).
- Gateway request/response durability: Gateway may fast‑path responses to the client for UX, but all api.response.* must be persisted as events via ingestd (post‑commit property preserved). Failures are emitted as explicit error events.
- Single‑writer enforcement: production credentials isolate canonical writes to ingestd (nodes carry read-only roles). In dev/CI we rely on the enforced JetStream path and an integration test that asserts the post‑commit publish property (events stay invisible on a separate connection until commit); we have not yet hard-disabled ad‑hoc direct database clients inside test fixtures.
- Active inference safety (minimal): actuations execute only from trusted sources (deny‑by‑default); all actuations are events; side effects logged and auditable.

3) Data model and provenance
Source Material (ground truth):

- Registry per capture material with status, rotation policy, timing model, metadata, storage path (annex or git for text).
- Temporal ledger per slice (append‑only) recording capture time, precision (exact|bounded), clock (monotonic|wall), and source_type (realtime_capture|intrinsic_content|inferred_mtime|inferred_ctime|inferred_user).

Events (core.events):

- External provenance: material_id, offset_kind (byte|line|rowid|logical), offset_start/offset_end, anchor_byte.
- Internal provenance: source_event_ids ULID[] (lineage for derived/synthesized events).
- Bitemporal fields: ts_orig (semantic event time; derived per precedence below), ts_ingest (derived from ULID).

Processor control plane:

- `core.processor_manifests` is the manifest/catalog for every ingestor, node, and automaton. Each row tracks `{ node_name, version, node_type, anchor_rule_version, description, config_schema }`. We migrated manifests here during the JetStream refactor (2024‑Q4) and deleted the retired `raw.processor_registry`.
- Checkpoints live in the NATS KV bucket `sinex_checkpoints`, keyed by processor + consumer identifiers. Recent checkpoint work:
  - 2025‑01: Unified checkpoint payloads across ingestors and automata (now stored in KV) and retired offsets.
  - 2025‑02: Added checkpoint versioning + activity timestamps so consumers can detect rewinds and track liveness.
  - `processed_count` remains the monotonic counter used for telemetry; optimistic concurrency relies on `(node_name, consumer_group, consumer_name, checkpoint_version)`.
- These columns replaced the old `processor_state` and `processor_offsets` shims. Any new checkpoint fields must be added here; there is no secondary table.
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

4) Material Acquisition (Stage-as-You-Go)
Concept:

- nodes own material acquisition using AcquisitionManager from SDK. Each node creates Source Material registry rows, publishes slices to JetStream, computes hashes, and writes temporal ledger entries. Ingestd assembles slices into git-annex and finalizes materials. Zero‑gap invariant: open the next before finalizing current for continuous streams; recovered_partial is used only for crash recovery.

AcquisitionManager API (in nodes):

- `begin(MaterialKind, source_identifier)`: Creates in-flight registry row, publishes source_material.begin to JetStream, returns SourceMaterialHandle.
- `handle.append(bytes)`: Publishes slice to source_material.slices.<material_id>, updates ledger, computes incremental hash.
- `handle.finalize()`: Publishes source_material.end with final hash, closes material.

MaterialAssembler (in ingestd):

- Subscribes to source_material.* subjects
- Maintains per-material state (temp file, next offset, slice count)
- On source_material.end: verifies hash, moves to git-annex, updates registry status (sensing → completed), writes ledger
- On hash mismatch: routes to events.dlq, marks material as failed

Acquisition patterns (implemented by nodes):

- append_stream (logs, sockets, JSONL) - continuous streaming
- batched_pull (API pagination; cursor/ETag) - paginated fetch
- replace_snapshot (CSV/SQLite snapshotting) - full snapshots
- tree_watch (filesystem drops and trees) - directory monitoring
- db_snapshot (DB backup API) - database snapshots

Operational outputs:

- raw.source_material_registry: identity, status, rotation policy, timing info, host/user, metadata.
- raw.temporal_ledger: per‑slice capture times and offsets (append‑only).
- core.blobs: git-annex metadata (hash, size, path).

5) Ingester SDK and unified stream
Unified material stream contract:

- JetStream `source_material.*` subjects yield Slice { material_id, offset_kind, offset_start/end, anchor_byte, bytes } and Control frames { RotationBoundary, EndOfMaterial, Gap{reason} }.
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

- exo replay --node <name> [--blob <material_id> | --since/--until] [--dry-run]
  - Ingestor replay requires material_id; automaton replay requires a time window.
- Gateway/exo replay RPC envelope (minimal): { node, mode: ingestor|automaton, scope: { blob_id | time_window }, dry_run: bool, operation_id }. This ensures operation_id and related session variables are wired correctly for archive triggers and audit.

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
  - Private mode emits an event and MUST be enforced by all processors (deny-by-default while private).
  - nodes MUST NOT capture sources marked private while private mode is active.
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

B) Material lifecycle and recovery

- Statuses: sensing → completed | recovered_partial | failed.
- Zero‑gap invariant for continuous materials: node stages next material before finalizing current.
- Recovery: ingestd MaterialAssembler rebuilds state from JetStream on restart; orphaned in‑flight segments are finalized as recovered_partial; replay can close gaps using historical slices from JetStream; ledger continuity remains append‑only.

C) Recommended indexes and invariants (readable form)

- core.events: BTREE(material_id, anchor_byte) WHERE material_id IS NOT NULL.
- core.events: GIN(source_event_ids).
- raw.temporal_ledger: UNIQUE(material_id, offset_start); BTREE(material_id, offset_start, offset_end); BTREE(timestamp_value, source_type).
- Archive trigger on core.events requires operation_id and writes to audit.archived_events; application‑immutable semantics (changes occur via archive‑and‑replace).
- Namespacing: SINEX_ENVIRONMENT scopes DB/schema names, streams, sockets, paths.
- Material store types: annex | git | fs. If using git for text materials, content diffs produce events; store commit SHA in the Source Material registry; events reference the source blob via commit+path to preserve replayability.

D) Processor Contracts (succinct checklists)

node (Material Acquisition via AcquisitionManager)

- [ ] Use AcquisitionManager to begin material (creates registry row, publishes source_material.begin)
- [ ] Publish slices to source_material.slices.<material_id> with headers (Nats-Msg-Id, Slice-Index, Offset, Chunk-Hash)
- [ ] Write temporal ledger entries for each slice (ts_capture, precision, source_type); append‑only
- [ ] Enforce zero‑gap for continuous materials (stage next before finalize current)
- [ ] Finalize with source_material.end (includes final hash, total slices, total bytes)
- [ ] Emit diagnostics for acquisition errors, backpressure

Ingestd (MaterialAssembler)

- [ ] Subscribe to source_material.* subjects with durable consumer
- [ ] Maintain per-material state (temp file, offset tracking, slice count)
- [ ] Assemble slices in order (handle out-of-order delivery)
- [ ] On source_material.end: verify hash, move to git-annex, update registry (status=completed), write final ledger entries
- [ ] On hash mismatch: route to events.dlq, mark material failed
- [ ] Rebuild assembler state from JetStream on restart

Ingestor (Event Processing)

- [ ] Consume JetStream `source_material.*` streams; deterministic slicing; anchor rule id/version recorded (processor_manifests)
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
[nodes (AcquisitionManager)] --(source_material.* + events.raw.*)--> [JetStream Streams]
[JetStream Streams] --(materials + events)--> [ingestd (MaterialAssembler + Event Consumer)]
[ingestd] --(commit)--> [Postgres/git-annex] --(post-commit publish)--> [NATS JetStream] --> [Automata/Agents]
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
  - Manifest lookup by node_id/version

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

E.1 core.events extensions and constraints
-- External/internal provenance schema, XOR check, idempotency, and indexes
ALTER TABLE core.events
  ADD COLUMN IF NOT EXISTS material_id ulid NULL,
  ADD COLUMN IF NOT EXISTS offset_kind TEXT CHECK (offset_kind IN ('byte','line','rowid','logical')) NULL,
  ADD COLUMN IF NOT EXISTS offset_start BIGINT NULL,
  ADD COLUMN IF NOT EXISTS offset_end BIGINT NULL,
  ADD COLUMN IF NOT EXISTS anchor_byte BIGINT NULL,
  ADD COLUMN IF NOT EXISTS source_event_ids ulid[] NULL;

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

E.3 raw.source_material_registry and raw.temporal_ledger
-- Source Material registry (ground truth identity + timing model + rotation)
CREATE TABLE IF NOT EXISTS raw.source_material_registry (
  blob_id ulid PRIMARY KEY,
  checksum TEXT UNIQUE,
  source_identifier TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('sensing','completed','recovered_partial','failed')),
  timing_info_type TEXT NOT NULL CHECK (timing_info_type IN ('intrinsic','realtime','inferred','none')),
  source_material_format TEXT NOT NULL,
  rotation_policy JSONB,
  staged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  start_time TIMESTAMPTZ NULL,
  end_time TIMESTAMPTZ NULL,
  staged_by TEXT,
  staged_on_host TEXT,
  material_store TEXT NOT NULL CHECK (material_store IN ('annex','git','fs')),
  metadata JSONB
);

CREATE INDEX IF NOT EXISTS ix_sm_registry_srcid ON raw.source_material_registry (source_identifier, staged_at DESC);

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
