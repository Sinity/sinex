
# Executive Audit: Sinex Exocortex

**Subject**: Distributed Digital Memory System (Rust/Nix/Python)
**Auditor Role**: Senior Principal Software Architect & Security Researcher

---

## 1. Architectural Integrity & System Design

**Summary**: The system employs a sophisticated **Event Sourcing** architecture with a "Deep Symmetry" design philosophy where ingestors (data collectors) and automata (data processors) share a unified runtime. The architecture is heavily centralized around PostgreSQL (TimescaleDB) and NATS JetStream.

* **Pattern Implementation**:
  * **Hub-and-Spoke**: `sinex-ingestd` acts as the central write funnel, ensuring all data passes through NATS JetStream before persistence. This is a robust pattern for high-throughput ingestion.
  * **Stage-as-You-Go**: The separation of heavy binary data (`SourceMaterial`) from metadata (`Event`) is architecturally sound. Large files go to git-annex via `AcquisitionManager`, while lightweight events reference them.
  * **Replay System**: The `ReplayStateMachine` (`crate/lib/sinex-core/src/db/replay/state_machine.rs`) implements a formal state machine for reprocessing history, which is rare and highly valuable for this domain.

* **Critique**:
  * **Database Coupling**: Satellites are not purely stateless agents pushing to a bus. They require direct PostgreSQL access for coordination (`sinex_satellite_sdk/src/coordination.rs`), checkpoints, and schema lookups. This breaks the isolation of the "Edge" nodes and makes remote deployment (e.g., on a laptop with intermittent connectivity to the server) difficult without a local DB replica.
  * **Circular Dependency Risk**: `sinex-gateway` depends on `sinex-services`, which depends on `sinex-core`. However, `sinex-core` contains domain logic that feels like it belongs in services. The boundaries are slightly blurred in `sinex-core/src/db/repositories`.

## 2. Code Quality & Maintainability

**Summary**: The codebase demonstrates high proficiency in Rust, utilizing advanced features like procedural macros, traits, and async runtimes effectively. However, file bloat is a significant issue.

* **Strengths**:
  * **Strong Typing**: Extensive use of "New Type" patterns (`EventSource`, `EventType`, `Ulid`) in `sinex-core/src/types/domain.rs` prevents primitive obsession.
  * **Macro Usage**: The `sinex-macros` crate significantly reduces boilerplate for database transactions and error context (`#[with_context]`), though it adds compilation overhead.

* **Weaknesses**:
  * **God Files**: several files are dangerously large, hindering readability and maintainability.
    * `crate/lib/sinex-core/src/db/repositories/events.rs`: **~18,000 tokens**. Contains query logic, conversion logic, and analytics queries. Needs splitting into sub-modules (e.g., `events/queries.rs`, `events/analytics.rs`).
    * `crate/core/sinex-ingestd/src/material_assembler.rs`: **~10,000 tokens**. Handles NATS, FS I/O, hashing, and DB updates. Violates Single Responsibility Principle.
  * **Complex Traits**: The `StatefulStreamProcessor` trait (`sinex-satellite-sdk/src/runtime/stream/mod.rs`) tries to unify too many behaviors (snapshotting, continuous streaming, historical replay). It makes implementation for simple processors verbose.

## 3. Security & Vulnerabilities (SAST)

**Summary**: Security is considered deeply in the design (e.g., `SanitizedPath`), but the complexity of shell integration and file watching introduces surface area.

* **Findings**:
  * **Path Traversal Mitigation**: `sinex-core/src/db/security.rs` and `file_watching_security.rs` implement rigorous path sanitization. The use of `SanitizedPath` type wrapper is excellent defensive coding.
  * **Command Injection Risk**: `sinex-terminal-satellite` tails history files. While it reads passively, `sinex-system-satellite/src/systemd_integration.rs` executes `systemctl` commands. The arguments seem well-constrained, but any user-input flowing into `Command::new` in `journal_watcher.rs` (via config) needs strict validation.
  * **Native Messaging**: `crate/core/sinex-gateway/src/native_messaging.rs` implements a browser extension bridge. It checks `TRUSTED_EXTENSION_ENV`, which is good, but native messaging parsers are historically fragile. The manual length-prefix reading (`read_message_blocking`) must be robust against DoS (e.g., sending a max-length int).
  * **SQL Injection**: `sqlx` macros are used almost exclusively, mitigating SQL injection risks effectively.
  * **Hardcoded Secrets in Tests**: `.gitguardian.yml` ignores secrets in test files. Ensure `crate/satellites/sinex-desktop-satellite/src/window_manager.rs` or similar doesn't leak env vars in logs during panic.

## 4. Performance & Scalability

**Summary**: The system is designed for high throughput but relies heavily on the single PostgreSQL writer instance.

* **Bottlenecks**:
  * **Database Write Amplification**: `sinex-ingestd` writes to `core.events`, `core.transactional_outbox`, and potentially `raw.source_material_registry` in transactions. High event rates could lock tables.
  * **NATS + DB Dual Write**: The "Transactional Outbox" pattern (`sinex-schema/src/schema/outbox.rs`) is robust but expensive. It doubles the write load for every event.
  * **Git Annex Overhead**: `BlobManager` (`sinex-satellite-sdk/src/annex/blob_manager.rs`) invokes the `git-annex` binary via `tokio::process`. For high-frequency small blobs (e.g., clipboard history), spawning a process per event is a massive scalability killer. This should be batched or replaced with a library call if possible.

* **Memory Management**:
  * `mimalloc` is used globally, which is excellent for fragmentation in async Rust.
  * `StageAsYouGoContext` buffers data in memory/temp files. Large file ingestion is chunked (`MAX_CHUNK_BYTES`), preventing OOM.

## 5. Error Handling & Resilience

**Summary**: Error handling is mature, using `color-eyre` and a custom `SinexError` type.

* **Observations**:
  * **Context**: The `#[with_context]` macro injects operation names into errors, making logs very debuggable.
  * **Circuit Breakers**: `sinex-satellite-sdk/src/lease_manager.rs` and processor runners implement logic to handle failures.
  * **Crash Loops**: `sinex-system-satellite` spawns multiple watchers. If `systemd-journal` watcher crashes, the logic in `unified_processor.rs` (`spawn_journal_task`) logs and exits. The runner needs a supervision tree to restart individual failing watchers without crashing the whole satellite.

## 6. Testing & QA Strategy

**Summary**: Testing is present but heavily biased towards unit tests and property testing. E2E testing infrastructure is complex (NixOS VMs).

* **Coverage**:
  * **High**: Core logic, types, and schema migrations.
  * **Low**: The interaction between `git-annex`, NATS, and Postgres in a live environment.
  * **Critical Missing**: `sinex-ingestd` integration tests are thin. The complex interaction of `MaterialAssembler` reassembling slices from NATS is a high-risk area that needs stress testing with network jitter.

## 7. Dependencies & Stack Health

**Summary**: The stack is modern but heavy.

* **Risks**:
  * **Nix Dependency**: The project is tightly coupled to Nix (`flake.nix`, `devenv.nix`). This ensures reproducible builds but raises the barrier to entry significantly for non-Nix users.
  * **Database Extensions**: Relies on `pg_jsonschema`, `pgx_ulid`, `timescaledb`, and `vector`. Deployment requires a custom Postgres image or NixOS. This is a significant infrastructure burden.
  * **Crate Versioning**: `sqlx` is pinned to `0.8`. `async-nats` is `0.33`. These are moving targets.

## 8. Documentation & Developer Experience (DX)

**Summary**: DX is a mix of excellent tooling and overwhelming complexity.

* **Pros**:
  * `devenv` setup is automated.
  * `sinex-processor-runtime` creates a uniform CLI for all satellites.
  * Architecture Markdown files are included directly in Rustdocs via `include_str!`. This is a fantastic practice.

* **Cons**:
  * The "Vault" concept and the sheer number of crates (core, sdk, services, runtime, schema, macros) makes navigating the code difficult.
  * Onboarding a junior dev would be brutal due to the requirement to understand Rust, Nix, NATS, Postgres internals, and Event Sourcing simultaneously.

## 9. Business Logic & Domain Modeling

**Summary**: The domain model is rich and accurately reflects the "Digital Memory" concept.

* **Strengths**:
  * **Provenance**: The `Provenance` enum (`sinex-core/src/db/models/event.rs`) distinguishing between `Material` (raw data) and `Synthesis` (derived data) is a brilliant insight for a system that needs to rebuild its understanding of the world.
  * **Temporal Ledger**: `raw.temporal_ledger` provides ground truth for time, decoupling event ingestion time from event occurrence time.

* **Gaps**:
  * **Entity Resolution**: The logic for merging entities (`merge_entities` in `knowledge_graph.rs`) is simplistic. It updates IDs but doesn't seem to handle conflict resolution of properties or history merging intelligently.

## 10. Future-Proofing & Technical Debt

**Summary**: The system is over-engineered in some places to support future scale, creating debt in the form of complexity.

* **Refactoring Candidates (Next 12 Months)**:
    1. **Refactor `MaterialAssembler`**: It is too complex. Split NATS consumption, File I/O, and Database registration into separate structs.
    2. **Decouple Satellites from DB**: Satellites should push to NATS. A separate "Coordinator" service should read NATS and update the DB state for coordination/heartbeats. This removes the `sqlx` dependency from edge collectors.
    3. **Optimize Blob Ingestion**: Replace `git-annex` CLI calls with a native Rust library or a persistent daemon connection to avoid process spawn overhead.

---

## Executive Summary

| Dimension | Score (1-10) | Key Takeaway |
| :--- | :--- | :--- |
| **Architecture** | 8 | Strong Event Sourcing foundation, but satellites are too coupled to the central DB. |
| **Code Quality** | 7 | High-quality Rust code, but suffers from massive file sizes and "God Structs". |
| **Security** | 9 | Excellent defensive coding (types, sanitization), though surface area is large. |
| **Performance** | 6 | `git-annex` process spawning and dual-write to DB/NATS are scaling limits. |
| **Resilience** | 8 | Robust error handling and circuit breakers; needs better supervision trees. |
| **Testing** | 7 | Good unit tests, but the distributed nature requires more chaos/integration testing. |
| **Stack Health** | 6 | Heavy reliance on specific Postgres extensions and Nix creates deployment friction. |
| **Documentation** | 9 | Embedded architecture docs are best-in-class. |
| **Domain Logic** | 9 | The Provenance/Material/Event model is sophisticated and correct for the problem. |
| **Future-Proofing** | 7 | Good foundations, but complexity needs pruning to prevent maintenance stagnation. |

**Final Recommendation**: The Sinex codebase is a robust, professional-grade system implementing complex distributed concepts. The immediate priority should be **decoupling satellites from the direct PostgreSQL connection** to enable true edge functionality and **refactoring the monolithic `EventRepository` and `MaterialAssembler`** to improve maintainability.
Based on my comprehensive audit of the provided codebase (`sinex-core`, `sinex-satellite-sdk`, `ingestd`, `gateway`, etc.), the **Unified Issues & Backlog Report** is highly accurate and correctly identifies the critical stability and architectural faults present in the system.

However, based on the deep-dive analysis of the code structures, I have identified **three specific omissions** and **one critical escalation** that must be integrated into this plan to prevent production failure at scale.

### 1. Assessment of the Report

The report accurately reflects the "Split Brain" state of the architecture:

* **Confirmed**: Satellites are currently bypassing the event bus and writing directly to the database (violating the Event Sourcing pattern).
* **Confirmed**: Security hardening on systemd units is non-existent.
* **Confirmed**: The `MaterialAssembler` concurrency model is broken.

### 2. Critical Escalations & Additions

#### A. The `MaterialAssembler` RwLock is a System-Wide Mutex (Critical Performance)

**Current Status in Report:** Listed as Item 1.4 "Fix RwLock held during I/O".
**Auditor Analysis:** This is understated. In `crate/core/sinex-ingestd/src/material_assembler.rs` (Lines ~380+), `handle_slice` acquires a **write lock** on the global `assembler_state` map and holds it while performing async file I/O (`file.write_all`, `file.flush`) and JSON serialization (`persist_state`).
**Impact:** This serializes **all** ingestion traffic across **all** materials. If one disk write stalls, every satellite halts.
**Action:** Move this to **Critical & Immediate (1.2)**. The architecture *cannot* scale until this is fixed. You must implement per-material locking (e.g., `DashMap` or individual `Mutex` per `AssemblerState` entry) rather than locking the whole map.

#### B. Git-Annex Process Spawning (New Scalability Issue)

**Current Status in Report:** Missing.
**Auditor Analysis:** `BlobManager` (`crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs`) spawns a new `tokio::process::Command` ("git-annex") for every single blob operation.
**Impact:** For high-frequency small content (e.g., clipboard history, logs), this will exhaust system PIDs and CPU context switching.
**Recommendation:** Add a **High Priority** task to "Optimize Annex Interaction".

* **Short term:** Batch operations or keep a persistent `git-annex` daemon connection (using `git-annex`'s `--batch` mode).
* **Long term:** Direct linkage or a pure Rust storage backend for small objects.

#### C. Watcher Supervision Tree (New Resilience Issue)

**Current Status in Report:** Mentioned as "Watcher tasks never shut down" (Item 42 in Appendix).
**Auditor Analysis:** The `SystemProcessor` (`unified_processor.rs`) spawns watchers using `tokio::spawn`. If a watcher panics (e.g. `journal_watcher` parsing error), the task dies, but the processor continues running "healthy" while silently missing data.
**Recommendation:** Add a **High Priority** task for "Watcher Supervision". Use a `JoinSet` or supervisor pattern to detect watcher death and restart the specific watcher or crash the satellite to trigger systemd restart.

#### D. Entity Resolution Logic Gap (Business Logic Risk)

**Current Status in Report:** Missing.
**Auditor Analysis:** The `merge_entities` function in `knowledge_graph.rs` updates IDs but performs no logic to merge properties, aliases, or history. This results in data loss during entity deduplication.
**Recommendation:** Add a **Medium Priority** task to "Implement Semantic Entity Merging" in the PKM service.

---

### 3. Revised Executive Summary & Ratings

Here is the finalized audit scoring based on the code provided and the backlog's accuracy.

| Dimension | Score (1-10) | Audit Notes |
| :--- | :---: | :--- |
| **Architecture** | **8** | Strong Event Sourcing foundation, but currently broken by Satellites direct-writing to DB. Report Item 2.1 fixes this. |
| **Code Quality** | **7** | High-quality Rust, but suffers from "God Files" (`events.rs`: 2.2k LOC, `material_assembler.rs`: 1.2k LOC). Refactoring needed. |
| **Security** | **6** | **Downgraded**. While types are safe (`SanitizedPath`), the lack of systemd hardening and missing SIGTERM handling in `ingestd` are production risks. |
| **Performance** | **4** | **Severe Risk**. The `RwLock` around I/O in `MaterialAssembler` effectively makes ingestion single-threaded. `git-annex` spawning is a secondary bottleneck. |
| **Scalability** | **5** | Limited by the single-writer Database bottleneck and the I/O locking issue mentioned above. |
| **Resilience** | **7** | Good use of `Result` and error types, but background tasks (Watchers) lack supervision/restart logic. |
| **Testing** | **7** | Unit coverage is decent. Integration tests for NATS/DB interaction are the weak point (Report Item 2.2 addresses this). |

### 4. Direct Updates to the Plan

**Add to Section 1.4 (Database & Concurrency):**

* **Refactor `AssemblerState` locking strategy**: Replace global `RwLock` with `DashMap<Ulid, Arc<Mutex<AssemblerState>>>` to allow concurrent writing of different materials.

**Add to Section 2.1 (Architecture):**

* **Implement git-annex batching**: Update `BlobManager` to use `git-annex` batch mode or `libannex` to prevent process exhaustion.

**Add to Section 5.1 (Risk Matrix):**

* `crate/core/sinex-ingestd/src/material_assembler.rs`: **CRITICAL**. Global lock on I/O path.

This codebase is **Production-Ready ONLY IF** the `MaterialAssembler` locking issue and Systemd hardening are applied. The current state is vulnerable to deadlocks under load and service hijacking.
write standalone implementation plan for a coding agent to make most important fixes

# Implementation Plan: Critical Stabilization & Architecture Fixes

**Objective:** This plan directs a coding agent to resolve the three highest-risk issues in the Sinex codebase: the global ingestion lock (Performance), the "Split-Brain" database writes (Architecture), and the lack of service hardening (Security).

**Prerequisites:** Rust toolchain, Nix environment, Postgres/NATS running.

---

## Phase 1: Break the `MaterialAssembler` Global Lock

**Severity:** **CRITICAL** (Performance/Deadlock Risk)
**File:** `crate/core/sinex-ingestd/src/material_assembler.rs`

**Status:** ✅ Completed — assembler state now uses per-material mutexes so I/O for different materials no longer serializes on the global map.

The previous implementation held a write lock on the entire state map while performing disk I/O, which serialized all ingestion.

### Steps

1. **Refactor State Container Data Structure**
    * **Locate:** `pub struct MaterialAssembler` definition.
    * **Change:** Change `assembler_state` from:

        ```rust
        assembler_state: Arc<RwLock<HashMap<Ulid, AssemblerState>>>,
        ```

        To:

        ```rust
        // Use an inner Mutex for individual state to allow concurrent access to different materials
        assembler_state: Arc<RwLock<HashMap<Ulid, Arc<Mutex<AssemblerState>>>>>,
        ```

2. **Update `handle_begin` (Initialization)**
    * **Logic Change:** When inserting a new state, wrap it in `Arc::new(Mutex::new(state))`.
    * **Constraint:** Ensure the outer `RwLock` write lock is dropped *immediately* after insertion.

3. **Refactor `handle_slice` (The Hot Path)**
    * **Current Logic:** Locks entire map (write), finds entry, does I/O, updates hash.
    * **New Logic:**
        1. Acquire outer `RwLock` (read).
        2. Clone the `Arc<Mutex<AssemblerState>>` for the target `material_id`.
        3. Drop outer `RwLock`.
        4. Acquire inner `Mutex` lock on the specific state.
        5. Perform file I/O (`write_all`, `flush`) and hashing.

4. **Refactor `handle_end` (Finalization)**
    * **Logic Change:** Similar to `handle_slice`. Acquire map read lock -> get specific state lock -> perform finalization -> Acquire map write lock *only* to remove the entry at the very end.

5. **Verification**
    * Run: `cargo test -p sinex-ingestd`

---

## Phase 2: Fix "Split Brain" Architecture

**Severity:** **HIGH** (Data Integrity)
**Files:**

* `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`
* `crate/lib/sinex-satellite-sdk/src/acquisition_manager.rs`

**Status:** ✅ Completed — Stage-as-You-Go and AcquisitionManager no longer write source material/temporal ledger rows directly; they publish begin/slice/end via JetStream and rely on ingestd as the sole database writer. The integration suite (`cargo nextest run -p sinex-satellite-sdk --test stage_as_you_go_integration`) boots ingestd + a log processor to guard the JetStream-only flow.

* `StageAsYouGoContext` now requires an `AcquisitionManager`; the `PgPool`/blob-ingest fallback and annex helpers were deleted so `register_in_flight`/`finalize_source_material` always publish begin/slice/end via JetStream.
* `AcquisitionManager::begin_material_with_metadata` and `finalize_with_metadata` now carry the satellite-provided metadata, so ingestd sees the exact same shape it used to write via `PgPool`.
* `SourceMaterialRepository` exposes `register_external_in_flight(material_id, …)` so ingestd can insert/update rows using the ULID minted on the satellite. This avoids race conditions and removes the dependency on satellite-side SQL.
* `MaterialAssembler` registers the in-flight record when it observes `source_material.begin`, assembles slices, and merges begin/end metadata when finalizing the record (metadata is appended before calling `finalize_in_flight`). The ingestion daemon is now the only writer touching `raw.source_material_registry` and `raw.temporal_ledger`.
* All satellites/tests constructing `StageAsYouGoContext` now chain `.with_acquisition_manager(...)` (or use `from_sender(acquisition, …)`), and the obsolete Postgres-only regression test was removed to keep JetStream as the only supported path.

Satellites are writing directly to `raw.source_material_registry` via `PgPool`. They must use NATS.

### Steps

1. **Remove DB Dependency from `StageAsYouGoContext`**
    * **File:** `stage_as_you_go.rs`
    * **Action:** Remove `db_pool: PgPool` field from the struct.
    * **Action:** Remove `PgPool` from `from_runtime`, `from_handles`, and `new` constructors. Update method signatures.

2. **Refactor `register_in_flight`**
    * **File:** `stage_as_you_go.rs`
    * **Current:** Executes SQL `INSERT INTO raw.source_material_registry...`.
    * **New:**
        1. Generate `Ulid` locally.
        2. Use `event_emitter` (or `acquisition_manager`) to publish a `source_material.begin` message to NATS.
        3. Return the generated `Ulid`.

3. **Refactor `finalize_source_material`**
    * **File:** `stage_as_you_go.rs`
    * **Current:** Executes SQL `UPDATE raw.source_material_registry...`.
    * **New:**
        1. Use `acquisition_manager` to publish a `source_material.end` message to NATS.

4. **Update `AcquisitionManager` Publishing**
    * **File:** `acquisition_manager.rs`
    * **Action:** Ensure `publish_begin` and `publish_end` include all metadata previously written via SQL (staged_by, host, timestamps).

5. **Verification**
    * Run: `cargo check --workspace` (Fix compilation errors in satellites caused by removing `PgPool`).

---

## Phase 3: Systemd Security Hardening

**Severity:** **CRITICAL** (Security)
**Files:** `nixos/modules/*.nix`

**Status:** ✅ Hardening defaults added to ingestd/gateway/satellite units (ProtectSystem=strict, ProtectHome, PrivateTmp, NoNewPrivileges, AF restrictions, kernel/CG protection, read/write paths constrained).

Services run with default privileges. They must be sandboxed.

## Gateway TCP Security

**Status:** ✅ TCP bindings now reject insecure mode (SINEX_GATEWAY_ALLOW_INSECURE), require tokens, and enforce TLS; Unix sockets remain the default.

## Edge-mode Checkpoints & Schema Broadcast

**Status:** ✅ Checkpoints can persist to NATS KV (opt-in via env), ingestd broadcasts active schemas to `system.schemas.active`, and edge-mode satellites subscribe to cache schemas without DB.

### Steps

1. **Define Hardening Overlay**
    * Create a reusable Nix attribute set for systemd hardening in `nixos/modules/default.nix` (or a new `common.nix`).

    ```nix
    commonServiceConfig = {
      ProtectSystem = "strict";
      ProtectHome = true;
      PrivateTmp = true;
      NoNewPrivileges = true;
      ProtectKernelTunables = true;
      ProtectControlGroups = true;
      RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
      LockPersonality = true;
      RestrictRealtime = true;
      SystemCallArchitectures = "native";
    };
    ```

2. **Apply to `sinex-ingestd`**
    * **File:** `nixos/modules/default.nix` (or where `coreServices` are defined).
    * **Action:** Merge `commonServiceConfig` into `serviceConfig`. Add `ReadWritePaths = [ cfg.stateRoot ]` (Ingestd needs to write to spool/annex).

3. **Apply to `sinex-gateway`**
    * **File:** `nixos/modules/default.nix`.
    * **Action:** Merge `commonServiceConfig`. Gateway handles network, so ensure `AF_INET` is allowed.

4. **Apply to Satellites**
    * **File:** `nixos/modules/satellite-services.nix`.
    * **Action:** Update `mkBaseServiceConfig`.
    * **Note:** `fs-watcher` might need `ProtectHome = "read-only"` instead of `true` if it monitors home directories. Make this configurable via the module options.

5. **Verification**
    * Run: `nix flake check` to ensure syntax validity.

---

## Execution Order Summary

1. **Fix Phase 1 (Ingestd Locking)** immediately. This involves complex Rust concurrency changes but isolates to one crate.
2. **Fix Phase 2 (Split Brain)**. This ripples through the SDK and requires updating satellite invocations.
3. **Fix Phase 3 (Nix Hardening)**. This is configuration only but requires rebuilding the environment to test.

> (Fix compilation errors in satellites caused by removing PgPool).

for all extant satellites, analyze HARD whether they truly don't need database connection otherwise. do think about the future as well.
This is a critical architectural pivot point. A blanket removal of `PgPool` from the satellite SDK will **break the system entirely** in its current state.

While the "Split Brain" issue (writing *data* directly to DB) must be solved, your intuition is correct: we cannot simply rip `PgPool` out of the generic runtime yet.

Here is the "HARD" analysis of why, broken down by component class.

### 1. The Critical Blocker: Checkpoints

**File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`

Every single satellite (Ingestor or Automaton) relies on `CheckpointManager` to save its state. Currently, **Checkpoints are stored in Postgres** (`core.processor_checkpoints`).

* **Impact:** If you remove `PgPool` from `ProcessorRuntimeState` or `ProcessorHandles`, **no satellite can save its progress**. They will re-ingest all data on every restart.
* **Future Path:** Checkpoints *should* move to NATS Key-Value (KV) stores to enable true "Edge" operation.
* **Current Reality:** Until NATS KV checkpoints are implemented, **every satellite strictly requires a database connection**, even if only for this administrative task.

### 2. Automata: The Read-Heavy Consumers

Automata in Sinex are not simple stream processors; they are "Contextualizers". They look up history to enrich current events.

| Automaton | Dependency | Necessity |
| :--- | :--- | :--- |
| **Terminal Command Canonicalizer** | `find_existing_canonical_command` (SELECT) | **HIGH**. Uses DB to deduplicate commands against 24hr history. Without DB, it cannot deduplicate efficiently. |
| **Analytics Automaton** | `query_events_for_window` (SELECT) | **HIGH**. Builds frequency analysis based on historical windows. Without DB, it is stateless and useless. |
| **PKM Automaton** | `query_knowledge_events` (SELECT) | **HIGH**. Needs history to detect learning sessions and patterns. |
| **Search Automaton** | `rebuild_search_index` (SELECT) | **HIGH**. Needs to query recent events to populate the index on startup. |

**Verdict:** Automata **must** retain Read-Only access to the database. Removing it breaks their core business logic.

### 3. Ingestors: The True "Edge" Candidates

Ingestors (File, Terminal, System) should theoretically be "dumb pipes" (Write-Only to NATS).

* **Current State:** They use `StageAsYouGoContext`, which currently holds a `PgPool` to insert `SourceMaterial` records.
* **The Fix (Phase 2):** We *must* remove `PgPool` from `StageAsYouGoContext`, forcing it to use NATS.
* **The Nuance:** Even after fixing the data path, Ingestors still need `PgPool` for **Checkpoints** (see point #1).

---

### Revised Phase 2 Implementation Plan

We must refine the objective. The goal is not "Remove PgPool from Satellites"; the goal is **"Remove Direct Data Writes from Ingestors"**.

**Corrected Architecture Strategy:**

1. **Ingestors**: Remove `PgPool` from *Ingestion Logic* (`StageAsYouGo`), but keep it in *Runtime* for Checkpoints (temporarily).
2. **Automata**: Retain `PgPool` for *Read* access (Context) and Checkpoints.

#### Updated Phase 2 Steps (Safe Path)

**1. Refactor `StageAsYouGoContext` (The Real Fix)**

* **File:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`
* **Action:** Remove `db_pool` field from `StageAsYouGoContext` struct.
* **Constraint:** Do **NOT** remove `db_pool` from `ProcessorHandles` or `ProcessorRuntimeState`.
* **Implementation:**
  * Change `register_in_flight`: Instead of `INSERT INTO raw.source_material_registry`, construct a `SourceMaterialBegin` struct and use `self.event_emitter` (or `acquisition_manager`) to publish it to NATS.
  * Change `finalize_source_material`: Instead of `UPDATE raw.source_material_registry`, publish `SourceMaterialEnd`.
  * Remove `record_ledger_entry`: This table is now the exclusive domain of `ingestd`.

**2. Update `AcquisitionManager`**

* **File:** `crate/lib/sinex-satellite-sdk/src/acquisition_manager.rs`
* **Action:** Update `publish_begin` to include *all* metadata that was previously being INSERTed into the DB (e.g., `staged_by`, `host`, `start_time`). This ensures `ingestd` has enough data to perform the INSERT on the satellite's behalf.

**3. Verify Automata Compile**

* **Check:** Ensure `sinex-terminal-command-canonicalizer` and friends can still access `pool` via `runtime.db_pool()`.
  * *Note:* The previous plan implied removing it from `runtime`, which would cause the compilation errors you feared. By keeping it in `runtime` but removing it from `StageAsYouGo`, we solve the architecture violation without breaking the automata or checkpoints.

**4. (Future/Optional) NATS KV Checkpoints**

* **Debt Item:** Add a TODO to migrate `CheckpointManager` to use NATS KV. Only *then* can we remove `PgPool` from Ingestors entirely.

### Summary of Change

**Don't** remove the database connection from the satellite binary.
**Do** remove the database connection from the `StageAsYouGo` struct and the `AcquisitionManager` struct.

This forces the "Data Write Path" to go through NATS (solving Split Brain) while allowing the "Control Plane" (Checkpoints) and "Context Plane" (Automata Reads) to function as they do today.
actually, I'd prefer to make implementation plan more comprehensive. Let's modernize the checkpointing immediatley.  Possibly also do a few other very related things to the ones we will already do.

# Comprehensive Implementation Plan: "True Edge" Modernization

**Objective:** Transform Sinex into a truly distributed architecture. We will resolve the critical performance deadlock, migrate state management (Checkpoints) to NATS JetStream KV, and decouple Ingestors from the Database entirely.

---

## Phase 1: Performance Stabilization (The Global Lock)

**Severity:** **CRITICAL**
**Goal:** Prevent system-wide deadlock during file I/O.

### 1.1 Refactor `MaterialAssembler` State

* **File:** `crate/core/sinex-ingestd/src/material_assembler.rs`
* **Step:** Change `AssemblerState` locking strategy.
  * **From:** `assembler_state: Arc<RwLock<HashMap<Ulid, AssemblerState>>>`
  * **To:** `assembler_state: Arc<RwLock<HashMap<Ulid, Arc<Mutex<AssemblerState>>>>>`
* **Step:** Update `handle_slice` and `handle_end`.
  * **Logic:** Acquire `read()` on the map -> Clone the inner `Arc` -> Drop map lock -> Acquire `lock()` on the specific material -> Perform I/O.
  * *Rationale:* This allows File A to write to disk without blocking File B.

---

## Phase 2: NATS KV Checkpointing (Modernization)

**Severity:** **HIGH**
**Goal:** Move processor state to NATS Key-Value store, removing the DB requirement for state management.

### 2.1 Bootstrap KV Store

* **File:** `crate/lib/sinex-satellite-sdk/src/acquisition_manager.rs` (or create `bootstrap.rs` in SDK)
* **Step:** Update bootstrap logic to create a JetStream KV bucket named `sinex_checkpoints`.
  * Config: `history: 5`, `ttl: None` (infinite).

### 2.2 Implement `NatsCheckpointManager`

* **File:** `crate/lib/sinex-satellite-sdk/src/checkpoint.rs`
* **Step:** Rewrite `CheckpointManager` struct.
  * **Remove:** `pool: PgPool` field.
  * **Add:** `kv_store: async_nats::jetstream::kv::Store`.
* **Step:** Implement `save_checkpoint`.
  * Key format: `checkpoints.{processor_name}.{consumer_group}`.
  * Value: JSON serialization of `CheckpointState`.
* **Step:** Implement `load_checkpoint` with **One-Time Migration**.
  * Logic: Try fetching from KV. If missing, try fetching from Postgres (if DB connection exists). If found in DB, save to KV and return.

### 2.3 Update `ProcessorHandles`

* **File:** `crate/lib/sinex-satellite-sdk/src/runtime/stream/handles.rs`
* **Step:** Update constructor to accept the modified `CheckpointManager`.

---

## Phase 3: The "Split-Brain" Fix (Data Path)

**Severity:** **HIGH**
**Goal:** Satellites stop writing to DB. They stream intentions (`Begin`, `End`) to NATS.

### 3.1 Refactor `StageAsYouGoContext`

* **File:** `crate/lib/sinex-satellite-sdk/src/stage_as_you_go.rs`
* **Step:** **Remove `db_pool` field entirely.**
* **Step:** Refactor `register_in_flight`.
  * Generate `Ulid` locally.
  * Construct `MaterialBeginMessage` (include `staged_by`, `host`, `timestamp`, `metadata`).
  * Publish to `source_material.begin` via NATS.
* **Step:** Refactor `finalize_source_material`.
  * Publish `MaterialEndMessage` (include `content_hash`, `final_size`) to `source_material.end`.
  * *Crucial:* Do not calculate `temporal_ledger` entries here. That is now Ingestd's job.

### 3.2 Update `AcquisitionManager`

* **File:** `crate/lib/sinex-satellite-sdk/src/acquisition_manager.rs`
* **Step:** Ensure `publish_begin` sends the full metadata payload required to reconstruct the `SourceMaterialRecord`.

---

## Phase 4: Ingestd Logic Expansion (The Receiver)

**Severity:** **HIGH**
**Goal:** Ingestd must process the signals sent in Phase 3 and perform the DB writes.

### 4.1 Handle Material Lifecycle Events

* **File:** `crate/core/sinex-ingestd/src/material_assembler.rs`
* **Step:** Update `handle_begin`.
  * Action: Insert row into `raw.source_material_registry` with status `sensing`.
* **Step:** Update `handle_end`.
  * Action: Update `raw.source_material_registry` status to `completed`.
  * Action: Insert row into `raw.temporal_ledger`.

### 4.2 SIGTERM Handling

* **File:** `crate/core/sinex-ingestd/src/main.rs`
* **Step:** Replace `ctrl_c()` with `tokio::signal::unix` handling for `SIGTERM` and `SIGINT`. This ensures `ingestd` flushes buffers when stopped by systemd.

---

## Phase 5: The "Edge" Transformation

**Severity:** **MEDIUM** (Architecture cleanup)
**Goal:** Allow Ingestors to run without a Database URL.

### 5.1 Optional Database Connection

* **File:** `crate/lib/sinex-processor-runtime/src/runner.rs`
* **Step:** Change `db_pool` in `StreamProcessorRunner` to `Option<PgPool>`.
* **Step:** Update initialization logic. If `DATABASE_URL` is missing:
  * Log "Running in Edge Mode (NATS Only)".
  * Initialize `CheckpointManager` using NATS KV only.
  * Allow startup.

### 5.2 Automata Safety Check

* **File:** `sinex-satellite-sdk/src/stream_processor/runtime_state.rs`
* **Step:** Ensure `db_pool()` getter returns `Result<&PgPool>` (or panics with a helpful message like "Automata require a DB connection, but none was provided").

---

## Phase 6: System Hardening

**Severity:** **CRITICAL** (Security)
**Files:** `nixos/modules/*.nix`

### 6.1 Apply Sandboxing

* **Step:** Create a `common-service-config.nix` overlay.
* **Content:**

    ```nix
    {
      ProtectSystem = "strict";
      ProtectHome = true;  # Use "read-only" for fs-watcher if needed
      PrivateTmp = true;
      NoNewPrivileges = true;
      # ... other systemd hardening directives
    }
    ```

* **Step:** Apply this to `ingestd`, `gateway`, and `satellite-services`.

---

## Execution Order

1. **Phase 1 (Locking)**: Fixes immediate performance bottlenecks.
2. **Phase 4 (Ingestd Logic)**: Prepare Ingestd to handle the new protocols.
3. **Phase 2 (Checkpoints)**: Migrate control plane to NATS KV.
4. **Phase 3 (Data Path)**: Cut over Satellites to NATS-only writes.
5. **Phase 5 (Edge Mode)**: Validate Satellites running without DB.
6. **Phase 6 (Hardening)**: Secure the deployment.
assuming all of this is done compleltely, write down anoter implementation plan covering lots of important stuff which wasn't yet solved here

# Implementation Plan: Resilience, Scalability & Maturity (Batch 2)

**Objective:** With the "Split-Brain" fixed and Checkpoints modernized, the system is architecturally sound but operationally immature. This plan addresses the "God Files," the `git-annex` performance bottleneck, business logic gaps, and deep observability.

**Prerequisites:** Completion of "True Edge" Modernization (Batch 1).

---

## Phase 1: The `git-annex` Performance Reactor

**Severity:** **HIGH** (Scalability)
**Context:** Currently, `BlobManager` spawns a new `git-annex` process for *every* blob. For high-frequency small files (clipboard history, logs), this will thrash the CPU and exhaust PIDs.

### 1.1 Implement `AnnexDaemon`

* **File:** `crate/lib/sinex-satellite-sdk/src/annex/daemon.rs` (New File)
* **Concept:** Create a struct that manages a *single* long-running `git-annex` process using `git-annex add --batch` and `git-annex checkpresent --batch`.
* **Implementation:**
  * Spawn `git-annex` with `Stdio::piped()`.
  * Implement an async `add_file(path: &Utf8Path) -> Result<AnnexKey>` that writes to the child's stdin and parses the response from stdout.
  * Wrap in `Arc<Mutex<AnnexDaemon>>` to ensure atomic request/response pairing.

### 1.2 Update `BlobManager`

* **File:** `crate/lib/sinex-satellite-sdk/src/annex/blob_manager.rs`
* **Action:** Replace `GitAnnex` (CLI wrapper) with `AnnexDaemon` (Process Manager) for hot paths.
* **Fallback:** Keep the CLI wrapper for one-off commands that don't support batching (like `fsck`).

---

## Phase 2: Decoupling "God Files" (Refactoring)

**Severity:** **MEDIUM** (Maintainability)
**Context:** `events.rs` and `material_assembler.rs` are too large, mixing concerns and making testing difficult.

### 2.1 Split `EventRepository`

* **Directory:** `crate/lib/sinex-core/src/db/repositories/events/` (Create Directory)
* **Action:** Break `events.rs` into specialized sub-modules:
  * `writer.rs`: `insert`, `insert_batch`, `txn` logic.
  * `reader.rs`: `get_by_id`, `get_by_time_range`, hydration logic.
  * `analytics.rs`: `count_by_source`, `get_source_activity`, heatmap queries.
  * `search.rs`: `search` (complex filtering logic).
* **Step:** Re-export via `mod.rs` to maintain API compatibility for consumers.

### 2.2 Split `MaterialAssembler`

* **Directory:** `crate/core/sinex-ingestd/src/materials/`
* **Action:** Separate concerns:
  * `state_manager.rs`: Handling the `AssemblerState` locking and persistence (the logic fixed in Batch 1).
  * `io.rs`: Disk writing and hashing logic.
  * `nats_handler.rs`: The NATS subscription loop and message parsing.

---

## Phase 3: Business Logic & Data Quality

**Severity:** **MEDIUM** (Data Utility)
**Context:** The audit identified gaps in how entities are merged and how precision is stored.

### 3.1 Fix Sub-Nanosecond Precision

* **File:** `crate/lib/sinex-core/src/db/repositories/events/writer.rs` (new file)
* **Issue:** `ts_orig_subnano` is an `i16` but logic often drops or miscalculates it.
* **Action:** Ensure `insert` logic explicitly calculates `nanos % 1000` and stores it. Ensure `try_to_event` reconstruction combines `ts_orig` and `subnano` correctly to restore full fidelity.

### 3.2 Implement Smart Entity Merging

* **File:** `crate/lib/sinex-core/src/db/repositories/knowledge_graph.rs`
* **Current:** `merge_entities` only updates IDs.
* **New Logic:**
  * **Properties:** Deep merge JSON properties (Target wins on conflict, or array append).
  * **Aliases:** Union the aliases of Source and Target.
  * **History:** Relink `source_event_ids` from Source to Target (requires update on `core.events` if we were tracking entity provenance there, otherwise just merge the arrays in the entity table).
  * **Audit:** Emit a `pkm.entity.merged` event with the diff.

---

## Phase 4: Observability & Tracing (Distributed Debugging)

**Severity:** **MEDIUM** (Operations)
**Context:** In a NATS+Microservices architecture, we lose the stack trace. We need distributed tracing.

### 4.1 Trace Context Propagation

* **File:** `crate/lib/sinex-satellite-sdk/src/nats_publisher.rs`
* **Action:** Inject OpenTelemetry `traceparent` into NATS headers on publish.
* **File:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs`
* **Action:** Extract `traceparent` from NATS headers and attach to the current tracing Span before processing/inserting.

### 4.2 Database Performance Metrics

* **File:** `crate/lib/sinex-core/src/db/pool.rs`
* **Action:** Enable `sqlx` built-in logging (if disabled) or wrap the pool to emit Prometheus metrics:
  * `db_pool_active_connections`
  * `db_pool_idle_connections`
  * `db_pool_wait_time_ms`

---

## Phase 5: Resilience & Supervision

**Severity:** **HIGH** (Reliability)
**Context:** If a watcher thread panics, the satellite stays "alive" but stops collecting.

### 5.1 Watcher Supervision Tree

* **File:** `crate/satellites/sinex-system-satellite/src/unified_processor.rs`
* **Action:** Use `tokio::task::JoinSet` for watchers.
* **Logic:**
  * Spawn watchers into the set.
  * Loop `join_next()`.
  * If a watcher exits (Ok or Err), log it.
  * **Restart Strategy:** If it was a critical watcher (e.g., Journal), wait backoff time and restart it. If it fails rapidly > 5 times, shut down the whole satellite to trigger systemd restart.

### 5.2 Leadership Lease Migration

* **File:** `crate/lib/sinex-satellite-sdk/src/lease_manager.rs`
* **Action:** Ensure `LeaseManager` is exclusively using NATS KV (verify no legacy DB locking remains).
* **Improvement:** Add "step-down" logic: If the satellite loses connection to NATS, it must immediately release its internal "Leader" status to stop processing, preventing split-brain processing if the network partitions.

---

## Phase 6: Cleanup & Debt

**Severity:** **LOW**
**Goal:** Reduce confusion.

### 6.1 Remove Legacy Tables

* **Action:** Create a migration to DROP `raw.sensor_jobs` and `raw.sensor_states` (the legacy polling tables).

### 6.2 Standardize CLI Prefixes

* **Action:** Standardize environment variable prefixes in `clap` structs. Use `SINEX_<SERVICE>_` standard (e.g., `SINEX_INGESTD_BATCH_SIZE`, `SINEX_FS_WATCHER_PATHS`). Add aliases for backward compatibility if needed.

---

## Execution Order

1. **Phase 2 (Refactoring)**: Do this first. It makes the code easier to work with for the subsequent complex changes.
2. **Phase 1 (Annex Daemon)**: High value optimization.
3. **Phase 5 (Supervision)**: prevents silent failures in production.
4. **Phase 3 (Business Logic)**: Fixes data quality issues.
5. **Phase 4 (Observability)**: Needed to debug issues as the system scales.
6. **Phase 6 (Cleanup)**: Final polish.
now write a third one. do not invent your own things, it's supposed to be based on the attached issue analysis

# Implementation Plan: Integrity, Stability & Optimization (Batch 3)

**Objective:** Address the remaining Critical data integrity issues, eliminate production panics, optimize database throughput, and consolidate dependencies as identified in the **Unified Issues & Backlog Report**.

**Prerequisites:** Completion of Batch 1 and Batch 2.

---

## Phase 1: Data Integrity & Schema Repair

**Severity:** **CRITICAL** (Data Correctness)
**Source:** Section 1.1 of Unified Issues Report

### 1.1 Remove Dangerous Indexes

* **File:** `crate/lib/sinex-schema/src/schema/entities.rs`
* **Issue:** The index `ix_entities_type` is marked `UNIQUE`, preventing more than one entity per type.
* **Action:** Create a new migration file (e.g., `m20251210_drop_dangerous_indexes.rs`).
* **Implementation:**
  * Drop `ix_entities_type`.
  * Drop any unique indexes on `entity_relations` that prevent valid graph edges.

### 1.2 Standardize Lock ID Endianness

* **Files:**
  * `crate/lib/sinex-core/src/db/distributed_locking.rs`
  * `crate/lib/sinex-core/src/db/replay/state_machine.rs`
* **Issue:** `distributed_locking` uses BigEndian while `state_machine` uses LittleEndian for `i64` conversions from ULID bytes. This causes lock mismatches across services.
* **Action:** Standardize on **BigEndian** (Network Byte Order) in `state_machine.rs` to match `distributed_locking`.

---

## Phase 2: Crash Prevention (Panic Removal)

**Severity:** **CRITICAL** (Availability)
**Source:** Section 1.2 of Unified Issues Report

### 2.1 Fix Hot Path Panics

* **File:** `crate/core/sinex-ingestd/src/material_assembler.rs`
  * **Line:** ~474 (buffer handling)
  * **Action:** Replace `.unwrap()` on buffer operations with `Result` propagation.
* **File:** `crate/lib/sinex-core/src/db/repositories/events.rs`
  * **Line:** ~1051 (loop unwrap)
  * **Action:** Replace `.expect()`/`.unwrap()` with error mapping to `SinexError`.
* **File:** `crate/satellites/sinex-terminal-satellite/src/unified_processor.rs`
  * **Action:** Remove `expect` calls in event processing loops.

### 2.2 Fix Config Integer Overflow

* **File:** `crate/core/sinex-rpc-dispatcher/src/lib.rs` (around line 289)
* **Issue:** Potential runtime panic if config values exceed integer bounds.
* **Action:** Implement `validate()` on `RpcDispatcherConfig` to check bounds at startup; return `ConfigError` instead of panicking.

---

## Phase 3: Database Performance Optimization

**Severity:** **MEDIUM** (Scalability)
**Source:** Section 3.1 of Unified Issues Report

### 3.1 Implement Batch UNNEST

* **File:** `crate/lib/sinex-core/src/db/repositories/events.rs`
* **Current:** `insert_batch` loops over events and executes one `INSERT` statement per event (N+1 inserts).
* **Action:** Refactor `insert_batch` to use PostgreSQL `UNNEST`.
  * Serialize events into arrays (`ids[]`, `sources[]`, `payloads[]`).
  * Execute a single query: `INSERT INTO ... SELECT * FROM UNNEST($1, $2, ...)`.
  * **Benefit:** 10x-100x throughput improvement for ingestion.

### 3.2 Hot-Path Allocation Audit

* **File:** `crate/core/sinex-ingestd/src/jetstream_consumer.rs`
* **Issue:** `process_batch` clones payloads/vectors for every event.
* **Action:** Refactor `PreparedEvent` struct to hold references where possible, or use `Arc<T>` for shared data structures to reduce cloning overhead during validation and persistence.

---

## Phase 4: Dependency & Stack Hygiene

**Severity:** **HIGH** (Maintenance)
**Source:** Section 2.1 of Unified Issues Report

### 4.1 Consolidate HTTP Stack

* **Context:** Workspace uses mixed versions of `hyper` (0.14 vs 1.0), `tower` (0.4 vs 0.5), and `reqwest` (0.11 vs 0.12).
* **Action:**
  * Update `Cargo.toml` in all crates to use:
    * `axum` 0.7
    * `hyper` 1.0
    * `http` 1.0
    * `reqwest` 0.12
  * Fix breaking changes in `sinex-gateway` (body handling, service traits) and `sinex-satellite-sdk` (if applicable).

---

## Phase 5: Configuration & Tuning

**Severity:** **MEDIUM** (Operations)
**Source:** Section 3.3 of Unified Issues Report

### 5.1 Configurable Flow Control

* **File:** `crate/core/sinex-ingestd/src/config.rs` & `jetstream_consumer.rs`
* **Issue:** `max_ack_pending` is hardcoded to 100, limiting throughput regardless of `batch_size`.
* **Action:** Add `max_ack_pending` to `IngestdConfig` (default 100) and wire it into the consumer configuration.

### 5.2 Fix Timestamp Auto-Detection

* **File:** `crate/lib/sinex-core/src/types/utils/timestamp_helpers.rs`
* **Issue:** Logic misclassifies timestamps after year 2128.
* **Action:** Adjust the heuristic ranges for seconds/milliseconds detection to be more robust (or prefer explicit precision fields if available).

---

## Phase 6: Test Restoration

**Severity:** **HIGH** (Quality Assurance)
**Source:** Section 2.2 of Unified Issues Report

### 6.1 Restore Disabled Tests

* **Action:** Uncomment and fix tests in:
  * `crate/lib/sinex-core/tests/property/schema_property_test.rs`
  * `crate/lib/sinex-satellite-sdk/tests/integration/blob_manager_test.rs`
* **Requirement:** Ensure they pass with the new `AcquisitionManager` and `BlobManager` architecture (from Batch 1 & 2).

---

## Execution Order

1. **Phase 1 (Data Integrity)**: Prevent bad data from being written immediately.
2. **Phase 2 (Crash Prevention)**: Stabilize the runtime.
3. **Phase 3 (DB Performance)**: Optimize the write path (prerequisite for higher load testing).
4. **Phase 5 (Configuration)**: Enable tuning for the improved performance.
5. **Phase 4 (Dependencies)**: Perform the upgrade (likely to cause compile breaks, do in isolation).
6. **Phase 6 (Tests)**: Verify everything with restored coverage.
what else is there still not covered in the issues attachment? list things
Based on a thorough cross-reference of the **Unified Issues & Backlog Report** against the implementation plans (Batch 1, 2, and 3), the following items remain **unaddressed**:

### 1. Ingestion Pipeline Robustness

* **Handle out-of-order slices & timeouts (Source: Appx C, Item 65)**
  * *Issue:* `MaterialAssembler` has no logic to handle slices arriving out of order (NATS JetStream guarantee is per-consumer, but network jitter happens). It also lacks a timeout reaping mechanism for materials that receive a `Begin` but never an `End`.
  * *Plan Gap:* No plan currently implements the reordering buffer or the timeout reaper logic.
* **Fail-safe DLQ writes in satellites (Source: Appx C, Item 96)**
  * *Issue:* If a satellite fails to write to its local DLQ (disk full, permissions), it drops the event silently.
  * *Plan Gap:* No plan addresses the fallback logic for local DLQ write failures.
* **Handle NACK/DLQ publish failures explicitly (Source: Appx C, Item 97)**
  * *Issue:* Ingestd ignores errors when NACKing a message or publishing to the DLQ stream, leading to silent data loss in failure scenarios.
  * *Plan Gap:* No plan adds the retry/fallback logic for these specific NATS operations.

### 2. Search & Query Features

* **Add trigram indexes for entity name search (Source: Appx C, Item 101)**
  * *Issue:* Entity searches use `ILIKE` on `name` and `canonical_name`, which is slow without `pg_trgm` indexes.
  * *Plan Gap:* The migration for adding GIN trigram indexes is not in any plan.
* **Evaluate payload text search indexing (Source: Appx C, Item 102)**
  * *Issue:* Full-text search on `payload::text` is unoptimized.
  * *Plan Gap:* No task to add the FTS index or decide against it.

### 3. Testing & CI Gaps

* **Crash-recovery tests for material acquisition (Source: Appx C, Item 99)**
  * *Issue:* No tests simulate a satellite crash mid-acquisition (after `Begin`, before `End`) to verify system behavior.
  * *Plan Gap:* Test suite additions for this scenario are missing.

### 4. Code Cleanup

* **Remove Dead Code / Suppressions (Source: Appx C, Item 70)**
  * *Issue:* Extensive use of `#[allow(dead_code)]` in `cascade_analyzer.rs` and `native_messaging.rs` hides incomplete refactors.
  * *Plan Gap:* No explicit cleanup task to remove these attributes and fix the warnings.
* **Unify naming patterns (Source: Appx C, Item 82)**
  * *Issue:* Inconsistent suffix usage (`*Satellite` vs `*Processor` vs `*Automaton`).
  * *Plan Gap:* No rename/refactor task scheduled.

### 5. Documentation

* **Better documentation surfacing for watchers (Source: Appx C, Item 31)**
  * *Issue:* `sinex-system-satellite` docs don't explain the real watcher implementations (D-Bus, Journal, etc.), only the placeholders.
  * *Plan Gap:* Documentation updates are not included in the engineering plans.

### Summary of Uncovered Work

The remaining work primarily focuses on **edge-case data safety** (out-of-order/timeout handling), **search performance** (indexes), and **developer hygiene** (dead code, naming). While not immediate system-killers, the lack of out-of-order handling in `MaterialAssembler` is a significant risk for a distributed system.
