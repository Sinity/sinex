
### **To the Coding Agent: The Sinex Unified Architecture & Implementation Guide v3.1**

**Subject:** The Final, Canonical Architecture for the Sinex Exocortex

**Purpose:** This document is your definitive and single source of truth. It synthesizes all previous architectural discussions, plans, and corrections. All new development and refactoring must adhere strictly to the principles and schemas laid out herein. This document supersedes `comprehensive_implementation_plan.md`, `CLAUDE.md`, and all prior architectural notes.

---

### **Part I: The Core Philosophy - The Guiding Principles**

To build this system correctly, you must internalize the philosophy that drives every design decision. These are non-negotiable laws of the system.

1. **The Event is an Interpretation, Not the Raw Data.**
    This is the most critical concept. An "event" in our primary table (`core.events`) is not the ground truth. It is a **structured interpretation** of that truth. The absolute, immutable ground truth is the sequence of bytes from the outside world—a log line, a socket message, a database row. The system's primary mandate is to preserve this ground truth perfectly. The structured event is for querying and synthesis; the original byte slice is for permanence and replayability.

2. **Parse, Don't Normalize (at the Ingestion Layer).**
    The job of an **Ingestor** processor is **Mechanical Translation**, not intelligent normalization. If the source material contains logical duplicates, the ingestor **must** produce duplicate event interpretations. It must be a faithful, "dumb" translator. The complex work of normalization, deduplication, and resolving ambiguity is the exclusive jurisdiction of downstream **Automata**.

3. **The System Must be Intelligible, Auditable, and Reversible.**
    The system must record not just facts, but the history of its own understanding. It must answer:
    * *What* did the system know?
    * *Why* did it know it? (Provenance)
    * *Why* did it change its mind? (Operations Log)
    Every data modification must be a non-destructive, reversible, and fully-audited action.

4. **Deep Oneness: Dissolving Artificial Distinctions.**
    Previous distinctions (`raw` vs. `synthesis` tables, `scan` vs. `sense` operations) are dissolved.
    * There is **one event table**. Provenance distinguishes event types.
    * There is **one data processing primitive: `replay`**. "Sensing" a live stream is the act of capturing it into replayable material.
    * Ingestors and Automata are both **"Processors"**; they simply operate on different input streams (external material vs. internal events).

---

### **Part II: The Final Data Model - The Architecture of Truth and Provenance**

This is the canonical data model. It replaces all previous designs.

**1. `raw.source_material_registry`: The Data Inbox & Birth Certificate**

This table is the manifest of every external data source the system has ever been asked to be aware of. It is the system's long-term memory of its sources.

**Schema:**

```sql
CREATE TABLE raw.source_material_registry (
    -- Core Identity & Deduplication
    blob_id ULID PRIMARY KEY,
    checksum TEXT NOT NULL UNIQUE,      -- blake3 hash of the content to prevent duplicate staging
    stage_batch_id UUID NOT NULL,       -- Groups files staged in a single `exo` command invocation

    -- User-Provided Context (The "Human Story")
    source_identifier TEXT NOT NULL,  -- User-defined name, e.g., 'old-laptop-bash', 'live-kitty-stream'
    user_comment TEXT,                -- Free-text description from the user
    user_tags TEXT[],                 -- User-provided tags for grouping and filtering

    -- Ingestion Context (The "System Story")
    staged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    staged_by_user TEXT,              -- The system user who ran the staging command
    staged_on_host TEXT NOT NULL,     -- The hostname where staging occurred
    staged_via_command TEXT,          -- The exact 'exo blob stage ...' command used

    -- Original Source File Metadata
    source_path TEXT,                 -- Original absolute path of the file
    source_mtime TIMESTAMPTZ,         -- Original modification time (crucial for ts_orig inference)
    source_size BIGINT,               -- Original file size

    -- Content-Derived Metadata (The "Inferred Story")
    start_time TIMESTAMPTZ,            -- Earliest conceptual timestamp found *inside* the blob
    end_time TIMESTAMPTZ,              -- Latest conceptual timestamp found *inside* the blob
    timing_info_type TEXT NOT NULL CHECK (timing_info_type IN ('intrinsic', 'external_wrapper', 'inferred', 'none')),
    source_material_format TEXT NOT NULL DEFAULT 'raw',
    
    -- Processing State
    processing_status TEXT DEFAULT 'staged' CHECK (processing_status IN ('staged', 'processing', 'completed', 'failed', 'archived'))
);
```

**2. `core.events`: The Interpretation Layer (Using Normalized Pointers)**

This is the primary table for queries. It contains the system's *interpretation* of raw data. It does **not** store the raw byte slices themselves; it stores pointers to them.

**Schema:**

```sql
CREATE TABLE core.events (
    event_id ULID PRIMARY KEY,
    ts_ingest TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (event_id::timestamp) STORED,

    -- The Interpretation
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,           -- The processor (ingestor/automaton) that created this interpretation
    ts_orig TIMESTAMPTZ NOT NULL,   -- The conceptual timestamp, derived from the source material
    host TEXT NOT NULL,
    payload JSONB NOT NULL,         -- The "pleasant" structured JSON data
    correlation_id ULID,            -- For end-to-end distributed tracing

    -- Provenance Links (Dual-Layer)
    source_material_id ULID REFERENCES raw.source_material_registry(blob_id), -- External provenance (to the "whole blob")
    source_material_offset_start BIGINT, -- The "Anchor Byte" offset within the blob
    source_material_offset_end BIGINT,
    source_event_ids ULID[],         -- Internal provenance (to other events in this table)

    -- Convenience Link to Associated Data (e.g., a screenshot)
    associated_blob_ids ULID[],

    -- The Natural Key makes a raw event's identity deterministic
    CONSTRAINT unique_raw_event_origin UNIQUE (source_material_id, source_material_offset_start)
);
```

**Note on the "Anchor Byte":** The `source_material_offset_start` must point to a deterministically identifiable byte that marks the beginning of a logical entry. This anchor's position must *never* change, even if an updated ingestor's slicing logic captures more context (changing `source_material_offset_end`). This is what makes re-interpretation possible.

**3. `audit.archived_events`: The Immutable Past**

A complete, append-only log of every event interpretation that has been superseded or deleted, populated automatically by a `BEFORE DELETE` trigger on `core.events`.

**Schema:** (Identical to `core.events` plus these metadata columns)

```sql
CREATE TABLE audit.archived_events (
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_by TEXT,
    archive_reason TEXT,
    superseded_by_event_id ULID, -- The ULID of the new event that replaced this one
    -- ... all columns from core.events ...
);
```

**4. `core.operations_log`: The System's Diary**

This table provides **Intent-Level Auditability**, logging the high-level user and system actions that cause data to change.

**Schema:**

```sql
CREATE TABLE core.operations_log (
    operation_id ULID PRIMARY KEY,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('stage', 'replay', 'archive', 'restore', 'curate')),
    status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    invoked_by_user TEXT,
    parameters JSONB NOT NULL, -- The exact command and flags used
    summary JSONB              -- Summary of the outcome (events created/archived, etc.)
);
```

---

### **Part III: The Data Lifecycle & User Commands**

The user interacts with the system via a small, powerful set of commands.

1. **`exo blob stage` (Acquisition):**
    * **Intent:** "System, be aware of this external data."
    * **Action:** Computes a checksum. If the checksum is new, it adds the file to git-annex and creates a new, richly-contextualized record in `raw.source_material_registry`. Uses ingestor-assisted inference for time ranges where possible.

2. **`exo replay` (Interpretation):**
    * **Intent:** "System, process this specific data source using this specific logic for this specific time frame."
    * **Command:** `exo replay --ingestor <name> --blob <blob_id> [--since <start>] [--until <end>]`
    * **Action:** This is a **read-only operation on the source blob**. It streams slices from the blob to the coordinator. The coordinator filters them by the time range (derived from the slice content), then performs the "archive and replace" workflow against `core.events` for the matching slices.

3. **`exo blob archive` (Retraction - The Sledgehammer):**
    * **Intent:** "System, the data from this entire blob was a mistake. Remove it and all its consequences."
    * **Command:** `exo blob archive <blob_id>`
    * **Action:** Finds all raw events in `core.events` with the matching `source_material_id` and performs a cascading archive.

4. **`exo explore` (Curation - The Scalpel):**
    * **Intent:** "System, show me where my data is messy or ambiguous so I can fix it."
    * **Action:** Presents the user with logical duplicates or other anomalies and offers a menu of actions (`[P]refer Event A`, `[M]erge Provenance`, etc.). Internally, this command invokes the surgical `event archive` command.

5. **`exo event archive` (The Surgical Tool):**
    * **Intent:** (Usually called by `explore`) "System, archive this one specific event and everything that depends on it."
    * **Command:** `exo event archive <event_ulid> --reason "..."`
    * **Action:** Records the operation in `operations_log`, then `DELETE`s the single event from `core.events`, which triggers the audit log and cascading archives.

---

### **Part IV: Implementation Mandates and Patterns**

These are non-negotiable implementation requirements.

1. **ULID Handling:**
    * **Always** use `ulid_to_uuid()` before binding to a query.
    * **Always** use the `::uuid` cast and `as "id!"` alias when selecting.

2. **Error Handling:**
    * The `format!` macro is **forbidden** for creating error messages.
    * **All** fallible functions must use the `ErrorContext` builder to create rich, structured errors with source chaining.

3. **Test Reliability:**
    * `tokio::time::sleep` is **forbidden** in all test code.
    * **All** asynchronous tests must use the condition-based helpers from `wait_helpers.rs` to ensure determinism.

4. **Database Access:**
    * **No crate other than `sinex-db` shall contain raw `sqlx::query!` macros.** All database logic must be centralized in the `sinex-db` crate and exposed via type-safe functions.
    * A canonical Rust pattern for creating a raw event interpretation must be implemented in `sinex-db` and used everywhere. It must accept source material info (blob, offset) and interpreted data and correctly populate all provenance columns.

5. **Correlation ID Propagation:**
    * An ingestor **must** generate a new `correlation_id` ULID for each distinct external data capture.
    * This ID **must** be propagated through all gRPC calls, Redis messages, and subsequent synthesis events.
    * Every `tracing` log span **must** include the `correlation_id` and `event_id` as structured fields to enable log-based distributed tracing.

---

### **Part V: The Path Forward - Required Implementations**

This is the immediate work plan for the agent.

1. **Refactor Core Dependencies:** Execute the plan to break up `sinex-core` into `sinex-error`, `sinex-types`, and `sinex-config-schema` to eliminate circular dependencies.

2. **Complete the Deep Symmetry Migration:** This is the highest architectural priority. Refactor the `terminal`, `desktop`, and `system` satellites to implement the `StatefulStreamProcessor` trait. Remove the legacy `EventSource` trait from the codebase entirely.

3. **Implement the Curation/Exploration Stack:**
    * Create the `ExplorationService` in `sinex-services` to house the logic for `--find-duplicates` and other analytical queries.
    * Expose this service via the `sinex-gateway` with new RPC endpoints.
    * Implement the `explore` subcommand in `cli/exo.py`, including the interactive menu for resolving ambiguities.
    * Implement the surgical `exo event archive <event_id>` command.

4. **Implement Code Generation Macros:**
    * Create the `#[satellite_main]` and `#[automaton_main]` procedural macros in `sinex-macros`.
    * Refactor all satellite and automaton binaries to use these macros, drastically reducing their `main.rs` boilerplate.

5. **Update `CLAUDE.md`:** As a final step, the developer's personal reference document, `CLAUDE.md`, must be reviewed and updated to reflect this final, unified architecture, removing any obsolete information.
