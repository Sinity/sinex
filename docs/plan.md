Of course. Here is the final, canonical architectural directive. It is written to be a clear, elegant, and unambiguous blueprint for the coding agent. It synthesizes all our refined insights into a single, coherent vision, providing both the "what" and the "why" for the required refactoring.

---
### **Architectural Directive: The Canonical Sinex Ingestion & Processing Model**

**TO:** Sinex Coding Agent
**FROM:** System Architect
**SUBJECT:** Final Mandate for Refactoring the Sinex Architecture. This document represents the definitive design and supersedes all prior specifications and discussions.

#### **Preamble: The Principle of Universal Provenance**

Our architectural journey has led us to a profound and simplifying principle: **every piece of information in Sinex must be traceable to a physical, replayable artifact.** There are no "logical" or "virtual" sources. All data originates from a `Source Material` blob, a concrete sequence of bytes representing the immutable evidence of an observation.

This principle resolves all previous architectural ambiguities. It dictates a single, unified pipeline for all data, regardless of its origin. The following directives will refactor the codebase to flawlessly implement this vision.

#### **1. The Core Components: Redefined Roles**

The component model is now clarified. The `sensd` daemon concept is **abolished**. Its valuable ideas are absorbed into the SDK and the Ingestor Satellites.

*   **Ingestor Satellite (formerly also `sensd`):**
    *   **Role:** The system's "senses." A standalone daemon (`sinex-fs-watcher`, `sinex-terminal-satellite`, etc.) that is the **sole master of a specific data domain.**
    *   **Responsibility:** To perform the complete acquisition and interpretation pipeline: observe the external world, persist the raw observation as versioned and replayable `Source Material`, interpret that material into canonical Sinex `Event`s with immutable provenance, and submit them to `ingestd`.

*   **`ingestd` (The Archiver & Broadcaster):**
    *   **Role:** The system's single, hardened gateway to the permanent record.
    *   **Responsibility:** To consume provisional events and material slices from NATS JetStream, validate and commit them atomically to PostgreSQL (`core.events`, `raw.source_material_registry`) and publish confirmations for downstream consumers.

*   **Automaton:**
    *   **Role:** The system's "cognitive reflexes." A standalone daemon that synthesizes new knowledge.
    *   **Responsibility:** To consume the stream of durable, validated events from NATS, perform deterministic transformations, and submit new, derived `Event`s (with internal `source_event_ids` provenance) back to `ingestd`.

#### **2. The Universal Ingestor Workflow: The "Serialized Evidence" Pattern**

Every Ingestor Satellite, without exception, **MUST** adhere to the following workflow for every observation. This ensures universal replayability.

1.  **Acquire:** Interact with the source (e.g., call a library like `sysinfo`, read a `journald` entry, receive a D-Bus message). The result is an in-memory, structured object or raw byte buffer.

2.  **Serialize Evidence:** Immediately serialize this raw data into a deterministic byte representation (e.g., a JSONL string). **This serialized byte slice is the `Source Material`.**

3.  **Persist Evidence:** Use the SDK's `AcquisitionContext` to atomically append this byte slice to the current `Source Material` blob and log its metadata (capture time, byte offsets) in the `raw.temporal_ledger`.

4.  **Interpret & Add Provenance:** Parse the in-memory object from Step 1 into a canonical Sinex `Event` payload. Populate the event's external provenance with the `material_id` and `anchor_byte` returned by the `AcquisitionContext` in Step 3.

5.  **Emit:** Send the final, fully-provenanced `Event` to `ingestd`.

#### **3. The Refactoring Blueprint: Actionable Steps**

You are to refactor the codebase to implement this canonical model.

**I. Empower the SDK (`sinex-satellite-sdk`):**

*   **Implement the `AcquisitionContext`:**
    *   Create a new module, `sdk::acquisition`.
    *   Implement the `AcquisitionContext` struct, which will be injected into the `StreamProcessorContext`.
    *   It must provide a high-level, safe API for the "Serialized Evidence" pattern:
        *   `register_in_flight(...) -> SourceMaterialHandle`: Creates the `sensing` record.
        *   `append_slice(...) -> (material_id, anchor_byte)`: Persists evidence and returns its coordinates.
        *   `finalize_material(...)`: Finalizes the blob.
        *   It must encapsulate all direct `sqlx` interaction with the `raw.*` tables and all `git-annex` logic.

**II. Refine the Ingestor Satellites (e.g., `sinex-system-satellite`):**

*   **Unify the Logic:** The core `scan` loop of every Ingestor must be rewritten to follow the Universal Ingestor Workflow.
*   **Remove Direct I/O:** All manual file writing or direct database inserts related to `Source Material` must be replaced by calls to the `AcquisitionContext`.
*   **Implement Serializers:** Each Ingestor must contain the logic to serialize the raw data from its specific source (e.g., a `sysinfo::System` struct) into the byte slice that will be persisted as evidence.
*   **Implement Resiliency for Schema Drift:** For sources that are external libraries (like `sysinfo`), the Ingestor's parsing logic must use the `#[serde(untagged)]` enum pattern to gracefully handle and migrate multiple historical versions of the serialized `Source Material` evidence.

**III. Harden `ingestd` and the Database:**

*   **Enforce Invariants:** Create a new database migration in `sinex-db-migration` to:
    *   Add a `UNIQUE` index on `(material_id, anchor_byte)` in `core.events`. This is the universal idempotency key.
    *   Add a `CHECK` constraint to `core.events` to enforce the `provenance XOR` invariant.
*   **Optimize Performance:** Rewrite `ingestd`'s `batch_write_to_db` to use a true `UNNEST`-based batch insert for high-throughput performance.
*   **Guarantee Atomicity:** Implement the **Transactional Outbox Pattern** in `ingestd` to guarantee the "post-commit publish" invariant.

**IV. Unify the Automata:**

*   **Consolidate the Runtime:** Refactor all Automata to be driven by the `StatefulStreamProcessor` trait and the `processor_main!` macro, as detailed in our previous analyses.
*   **Deprecate `NatsStreamConsumer`:** The `NatsStreamConsumer` and `EventBatchProcessor` pattern is now an internal implementation detail of the SDK's runner, not a public-facing interface for automata. Remove its usage from all `main.rs` files in the automata crates.

---

**Conclusion:**

This refactoring brings the entire system into architectural alignment. It establishes a single, powerful, and universally applicable pattern for data acquisition that guarantees the core principles of provenance and replayability. By moving the right complexity into the SDK and enforcing the core invariants at the database level, we will create a foundation that is simple to build upon, yet robust enough to support the full, ambitious vision of the Sinex project. Execute this blueprint.
