Of course. This is the correct next step. We will synthesize everything—the codebase's ergonomic strengths, the doctrinal principles from `TARGET_final.md`, and the mature operational contracts from our discussions—into a single, canonical blueprint.

This document, `TARGET_CANONICAL.md`, is designed to be the definitive guide for a competent coding agent. It is not an exhaustive list of every line of code but a set of precise constraints, contracts, and architectural mandates that will transform the current codebase into its final, correct form. It respects the existing implementation where it aligns with the doctrine and specifies the exact changes required where it diverges.

This will be a single, comprehensive output.

---
--- START OF FILE TARGET_CANONICAL.md ---

# The Sinex Exocortex: Canonical Architecture & Implementation Doctrine, v5.0

**Preamble:** This document is the definitive and single source of truth for the Sinex architecture. It synthesizes the implemented codebase with the project's refined doctrinal principles. Its purpose is to guide a competent coding agent in the transformation of the existing codebase into its final, correct, and robust state. It supersedes all previous design documents and implementation plans.

## Invariants Quick Reference (one-page)

*   **Single-Writer Ingest:** Satellites → `ingestd` (gRPC) → Postgres (commit) → publish to NATS (post-commit). Satellites **MUST NOT** publish canonical events directly to the bus or write to the database.
*   **Dual-Layer Provenance:** External (`material_id`, `anchor_byte`/offsets) XOR Internal (`source_event_ids`) per event. The database **MUST** enforce this with a `CHECK` constraint.
*   **Idempotency for First-Order Events:** `UNIQUE(material_id, anchor_byte)` **MUST** be enforced on `core.events` for material-anchored events.
*   **Ledger Append-Only:** `raw.temporal_ledger` **MUST** forbid `UPDATE`/`DELETE` via a trigger. `UNIQUE(material_id, offset_start)` **MUST** be enforced.
*   **Archive-on-Delete:** `DELETE` operations on `core.events` **MUST** require a session `operation_id`. A `BEFORE DELETE` trigger **MUST** atomically archive the row to `audit.archived_events`.
*   **No Live→Archived References:** The `source_event_ids` array in a live `core.events` row **MUST NOT** reference any `event_id` that exists in `audit.archived_events`. Replays **MUST** enforce this via cascading.
*   **Replay Discipline:** The `exo replay` command **MUST** first present a preview (counts, cascades, anchor churn, time-quality flips), respect safety gates, and only then execute. All replays **MUST** be cascading.
*   **Unified Processor Model:** All satellites **MUST** implement the `StatefulStreamProcessor` trait and be built using the `processor_main!` macro.
*   **Ergonomic Patterns:** Code **SHOULD** use `Id<T>`, `bon::Builder`, and the Repository Pattern (`DbPoolExt`) for type-safe, maintainable implementation.
*   **Namespacing:** The `SINEX_ENVIRONMENT` variable **MUST** be used to programmatically namespace all shared resources: database names, NATS stream/subject prefixes, consumer groups, sockets, and paths.

## 1. Purpose and Principles

*   **Purpose:** A local-first exocortex that captures digital life as events, preserves original evidence ("Source Material"), and evolves beliefs via deterministic, auditable, and reversible replay.
*   **Principles:**
    *   **Source Material is Ground Truth:** The raw bytes captured by `sensd` are immutable truth; events are interpretations.
    *   **Rebuildability via Replay:** All derived state (Knowledge Graph, canonical events, tags) is a projection of the event log and **MUST** be rebuildable.
    *   **Provenance Everywhere:** Every piece of information is traceable to its physical origin (a slice of Source Material) or its logical origin (a set of parent events).
    *   **Single-Writer Discipline:** Guarantees causal consistency and durability. An event seen on the bus is guaranteed to be in the database.
    *   **Replay as the Primary Verb:** All interpretation, correction, and evolution is expressed through the single, safe, user-driven `replay` operation.
    *   **Human-in-the-Loop:** The system assists, proposes, and automates, but the user is the final arbiter of meaning and truth via explicit curation flows.

## 2. Architecture at a Glance

The architecture is a tripartite data plane designed for clear separation of concerns.

**Components:**
*   **`sensd` (The Senses):** The universal acquisition daemon. Manages `raw.sensor_jobs`, captures raw byte streams from external sources, creates `Source Material` records, and writes to the `raw.temporal_ledger`. Its output is durably stored bytes and their temporal context.
*   **`ingestd` (The Gatekeeper):** The single-writer event service. Receives structured events from all processors (ingestors, automata) via gRPC. It validates, writes atomically to `core.events` in Postgres, and upon successful commit, publishes the event to the NATS "hotlog."
*   **Processors (Satellites):** Rust binaries built on the `sinex-satellite-sdk`. They implement the `StatefulStreamProcessor` trait and can play one or more roles:
    *   **Ingestor:** Interprets `Source Material` into first-order events.
    *   **Automaton:** Interprets events into higher-order synthesis events.
    *   **Actuator:** Interprets events as instructions to act upon the external world.
*   **`exo` (The Coordinator):** The user-facing CLI/TUI. Orchestrates all high-level operations (`stage`, `replay`, `archive`, `explore`) by communicating with the gateway and other components. It is the gatekeeper for setting the `operation_id`.

**Data Flow:**
```
[External World] -> [sensd] -> [Source Material (Annex/Git) + Temporal Ledger (DB)]
                                      |
                                      v
[Ingestor Satellites] --(read)---------+---(gRPC events)--> [ingestd] --(commit)--> [Postgres]
                                                                        |
                                                                        v (post-commit)
[Automata/Actuators] <--(NATS durable subscribe)-- [NATS JetStream] <--(publish)--+
        |
        +-----(gRPC derived events)---------------------------------------------> [ingestd] ...
```
**Action Item:** The current direct-to-NATS publishing model in the codebase **MUST** be reverted to this single-writer `ingestd` model. The `NATS_MIGRATION.md` is deprecated.

## 3. Data Model & Provenance

The database schema **MUST** be updated to reflect these contracts.

*   **`raw.source_material_registry`:** The universal manifest for all external data.
    *   **Key Fields:** `material_id` (ULID PK), `material_kind` ('annex'|'git'), `checksum` (for annex) or `git_commit_sha` (for git), `source_identifier`, `status` ('sensing'|'completed'|'recovered_partial'|'failed').
    *   **Lifecycle:** `sensd` creates rows with `status='sensing'`, writes data, and updates to `status='completed'` upon finalization.

*   **`raw.sensor_jobs`:** The declarative control plane for `sensd`.
    *   **Key Fields:** `job_id` (ULID PK), `sensor_type`, `target_uri`, `config` (JSONB for pattern-specific settings), `status` ('active'|'paused'|'retired').

*   **`raw.temporal_ledger`:** Append-only log of capture-time provenance.
    *   **Key Fields:** `material_id` (FK), `offset_start`, `ts`, `precision` ('realtime_capture'|'intrinsic_content'|'inferred_mtime'|'inferred_user').
    *   **Contract:** Ingestors **MUST** consult this ledger to derive `ts_orig`.

*   **`core.events`:** The single, unified event log.
    *   **External Provenance:** `material_id`, `anchor_byte`, `offset_start`, `offset_end`, `offset_kind`.
    *   **Internal Provenance:** `source_event_ids` (ULID[]).
    *   **Constraints:**
        *   `CHECK` constraint **MUST** enforce that either external or internal provenance is present, but not both (XOR).
        *   `UNIQUE` constraint **MUST** exist on `(material_id, anchor_byte)`.

*   **`audit.archived_events`:** Append-only archive for superseded events.
    *   **Contract:** A `BEFORE DELETE` trigger on `core.events` **MUST** populate this table. The trigger **MUST** fail if the `sinex.operation_id` session variable is not set.

## 4. Sensing (`sensd`) and Stage-as-you-go

`sensd` centralizes acquisition, making ingestors simpler and more robust.

*   **DB-Driven:** `sensd` **MUST** watch `raw.sensor_jobs` and reconcile its running sensor workers to match the 'active' jobs.
*   **Acquisition Pattern Catalog:** `sensd` **MUST** implement a core set of reusable acquisition patterns configured via `raw.sensor_jobs.config`. (See Appendix B).
*   **In-Flight Lifecycle (Stage-as-you-go):**
    1.  **Register:** Before capturing the first byte of a new chunk, `sensd` **MUST** create a `source_material_registry` row with `status='sensing'`.
    2.  **Capture & Log Time:** As bytes arrive, `sensd` appends them to a temporary file and writes corresponding entries to `raw.temporal_ledger` with `precision='realtime_capture'`.
    3.  **Finalize:** Upon rotation (based on time/size policy), `sensd` finalizes the chunk: moves the temp file to permanent storage (Annex/Git), computes the final checksum/commit, and updates the registry row to `status='completed'`.
    4.  **Handoff:** Before starting finalization, `sensd` **MUST** create the *next* in-flight record to ensure zero-gap capture (dual-writer handoff).
*   **Crash Recovery:** On startup, `sensd` **MUST** scan for orphaned in-flight records, finalize them with `status='recovered_partial'`, and emit diagnostic events.

## 5. Processor SDK & Unified Model (`StatefulStreamProcessor`)

*   **Canonical Trait:** The codebase's `StatefulStreamProcessor` trait is the canonical interface for all processors.
*   **Standard Entrypoint:** The `processor_main!` macro is the standard way to create a satellite binary. It **MUST** handle config loading (env-only), signal handling, and heartbeat emission.
*   **Three-Phase Startup (for Ingestors):**
    1.  **Snapshot:** Capture initial state.
    2.  **Gap-fill:** Process any data missed since the last checkpoint.
    3.  **Continuous:** Begin real-time processing.
*   **`MaterialSliceStream`:** The SDK **MUST** provide a unified stream abstraction that yields slices of `Source Material` and control messages (e.g., `RotationBoundary`). This stream can be backed by a historical blob or a live, in-flight file from `sensd`, making the ingestor's logic agnostic to the mode.
*   **Ingestor Contracts:** Ingestors built with the SDK **MUST** adhere to the contracts in Appendix A.

## 6. Replay Discipline and Evolution Semantics

*   **Unified `exo replay`:** The CLI **MUST** provide a single `replay` verb: `exo replay --processor <name> [--blob <id> | --since/--until]`. The system dispatches based on the processor's declared type.
*   **Mandatory Preview:** Replay operations **MUST** default to a dry-run that previews the impact:
    *   Counts of events to be archived and created.
    *   Cascade depth histogram for downstream synthesis changes.
    *   "Anchor churn" percentage (how many anchors moved).
    *   "Time-quality flip" count (how many events changed from inferred to exact time, or vice-versa).
*   **Safety Gates:** The replay planner **MUST** enforce configurable safety gates (e.g., `anchor_churn_threshold_percent=5`). Replays exceeding gates **MUST** require a `--force` flag.
*   **`operation_id`:** All replay and archive commands **MUST** be executed within a session where `sinex.operation_id` is set. The `exo` coordinator is responsible for creating an `operations_log` entry and setting this variable.
*   **Cascading Archives:** Replays **MUST** always cascade. When a root event is archived, all downstream synthesis events that depend on it **MUST** also be archived and recomputed. A CI check **MUST** enforce that no live event references an archived `event_id`.

---
## Appendices

### Appendix A: Processor Contracts (Succinct Checklists)

**`sensd` (Sensing Daemon)**
*   [ ] Creates in-flight `source_material_registry` row before dependents can reference it.
*   [ ] Appends raw bytes to canonical storage, computing offsets deterministically.
*   [ ] Writes to `raw.temporal_ledger` per slice; `append-only`.
*   [ ] Enforces zero-gap for continuous materials via dual-writer handoff.
*   [ ] Finalizes chunks with correct `status` (`completed`, `recovered_partial`, `failed`).
*   [ ] Emits diagnostics for backpressure, gaps, and recovery actions.

**Ingestor (Processor Role)**
*   [ ] Consumes `MaterialSliceStream`; logic is agnostic to real-time vs. historical.
*   [ ] Slicing logic is deterministic; registers its `anchor_rule_id` and `version` in its manifest.
*   [ ] Derives `ts_orig` by consulting `temporal_ledger` and applying fallback policy. Records time quality.
*   [ ] Populates full external provenance: `material_id`, `offset_kind`, `offsets`, `anchor_byte`.
*   [ ] Submits events via `ingestd` gRPC endpoint only.
*   [ ] On replay, archive-and-replace is handled by `ingestd` via natural key `(material_id, anchor_byte)`.

**Automaton (Processor Role)**
*   [ ] Consumes events from NATS post-DB-commit.
*   [ ] Transforms are deterministic; no external mutable state for correctness logic.
*   [ ] Populates internal provenance (`source_event_ids`).
*   [ ] Submits derived/synthesis events via `ingestd` gRPC endpoint only.
*   [ ] On replay, recomputes outputs; `ingestd` handles archive-and-replace of downstream products.
*   [ ] Respects `no live->archived references` invariant.

### Appendix B: `sensd` Acquisition Pattern Catalog

`sensd` **MUST** support these patterns, configurable via `raw.sensor_jobs`.

*   **`append_stream`**: For append-only sources (logs, sockets, JSONL).
*   **`batched_pull`**: For paginated APIs (uses cursor/ETag).
*   **`replace_snapshot`**: For sources that overwrite a single file (CSV/SQLite snapshots). `sensd` takes safe snapshots.
*   **`multi_file` / `tree_watch`**: For directory drops and filesystem trees.
*   **`db_snapshot` / `db_wal`**: For database sources (using backup APIs and WAL frames).
*   **`changefeed`**: For sources that support native change streams.

### Appendix C: Critical DDL Changes and Constraints

This is not a full DDL, but a checklist of required schema objects and constraints to be implemented or verified.

*   **Tables:**
    *   [ ] `raw.source_material_registry` (as specified)
    *   [ ] `raw.sensor_jobs` (as specified)
    *   [ ] `raw.temporal_ledger` (as specified, with append-only trigger)
    *   [ ] `audit.archived_events` (mirror of `core.events` + audit columns)
*   **Constraints on `core.events`:**
    *   [ ] `CHECK` constraint for external vs. internal provenance XOR.
    *   [ ] `UNIQUE` constraint on `(material_id, anchor_byte)`.
*   **Triggers:**
    *   [ ] `BEFORE DELETE ON core.events` trigger that archives to `audit.archived_events` and requires `sinex.operation_id`.
    *   [ ] `BEFORE UPDATE OR DELETE ON raw.temporal_ledger` trigger that `RAISE EXCEPTION`.

### Appendix D: `exo` CLI Command Matrix

| Command | Subcommand / Flags | Dispatch Logic & Developer Notes |
| :--- | :--- | :--- |
| `exo blob stage` | `<path> --source-identifier <id> --type [blob|git] --watch` | Creates `source_material_registry` entry directly (for files) or creates/updates a `sensor_jobs` row (for `--watch`). `sensd` picks up the job. |
| `exo replay` | `--processor <p> --blob <id>` | **Ingestor Replay.** Gateway initiates satellite `scan` on the specified processor. `ingestd` handles archive-and-replace. |
| | `--processor <p> --since/--until` | **Automaton Replay.** Gateway identifies processor's outputs in window, archives them via `ingestd`, then the automaton naturally recomputes from NATS. |
| | `--dry-run` / `--force` | Planner runs in gateway. Shows preview/gates. `execution` reuses planner's scope and `operation_id`. |
| `exo blob archive` | `<id> --since/--until` | **Negative Replay.** Archives events from a slice of a blob. |
| `exo restore` | `--operation <op_id>` | Uses `operations_log` and `audit.archived_events` to invert a replay. |
| `exo explore` | `curate` | UI for the Proposal/Judgment/Finalizer loop. |
| `exo system check`| | Runs a suite of diagnostic checks on all core components. |

---
--- END OF FILE TARGET_CANONICAL.md ---
