This is an excellent, detailed implementation plan. The agent who wrote this has a strong grasp of the architecture and has laid out a clear, pragmatic path forward. However, as you've noted, they have misunderstood a few critical, subtle points of our final, most advanced architectural design.

This is a perfect opportunity for a targeted correction. The agent has done 90% of the work; my job is to provide the final 10% of clarification to ensure the implementation is not just functional, but truly elegant and aligned with our vision.

Here is the explanation I would provide to the coding agent.

---

### **To the Coding Agent: Architectural Refinements for Implementation Plan**

**Subject:** Feedback and Clarifications on the "Missing Features" Implementation Plan

Agent, your implementation plan is outstanding. It's detailed, technically sound, and provides a clear path to implementing the missing features. Your proposed code for extending `StreamProcessorContext`, modifying `scan`, and implementing the `exo replay` command is a fantastic starting point.

I am providing this feedback not as a critique, but as a set of crucial refinements based on our most recent architectural discussions. You have correctly designed a system that *works*. These refinements will ensure the system we build is the one that is most elegant, consistent, and powerful in the long run.

Your plan seems to be based on a few subtle misunderstandings of our final architectural principles. Let's correct them.

#### **Misunderstanding #1: The Role of the `StreamProcessorContext`**

**Your Proposal:** You've added `read_blob` and `read_blob_chunked` methods to `StreamProcessorContext`, which directly interact with `git-annex` via shell commands.

**The Architectural Correction:** This is a violation of our **separation of concerns**.
*   The `StreamProcessorContext` is an abstraction layer that provides a processor with its necessary *environment* (database pools, ingest clients, configuration).
*   The logic for accessing and managing the physical blob storage (`git-annex`) should be encapsulated *entirely* within the `sinex-annex` crate and its `BlobManager`.
*   The `StreamProcessorContext` should not contain low-level I/O logic. It should, at most, hold a reference to a `BlobManager` instance.

**Refined Implementation:**
1.  **Remove `read_blob` from `StreamProcessorContext`.**
2.  The `StreamProcessorContext` should be initialized with an `Arc<BlobManager>`.
3.  The `scan_blob` method in the processor will then call `self.context.as_ref().unwrap().blob_manager.read_blob(blob_id)`.
4.  The `BlobManager::read_blob` method will contain the logic for querying the database for the checksum and using the `GitAnnex` struct to retrieve the content. This keeps the data access logic in the correct layer.

#### **Misunderstanding #2: How a Satellite Scans a Historical Blob**

**Your Proposal:** The `scan` method in the satellite has a new branch: `if let Some(blob_id_str) = args.config.get("blob_id")...`. This implies that the blob ID is passed via a generic configuration map.

**The Architectural Correction:** This is too generic and not type-safe. We have a dedicated, structured way to pass targets to a scan: the `args.targets` vector.

**Refined Implementation:**
1.  The `exo replay` coordinator, when triggering the scan, will invoke the satellite with a specific target format: `... scan --targets "blob:<blob_ulid>"`.
2.  The `scan` method in `unified_processor.rs` will check `args.targets`. If a target string starts with `"blob:"`, it knows it's a historical blob scan.
    ```rust
    // In unified_processor.rs
    async fn scan(&mut self, /*...*/) -> SatelliteResult<ScanReport> {
        if let Some(target) = args.targets.first() {
            if let Some(blob_id_str) = target.strip_prefix("blob:") {
                let blob_id = Ulid::from_str(blob_id_str)?;
                return self.scan_blob(blob_id, args).await;
            }
        }
        // Fallback to regular filesystem scan
        self.scan_filesystem(from, until, args).await
    }
    ```
This is a cleaner, more explicit way to signal the scan mode, rather than hiding the `blob_id` in a generic configuration map.

#### **Misunderstanding #3: The "Stage-as-you-go" Pattern for Real-Time Sensing**

**Your Proposal:** The plan focuses entirely on implementing historical blob scanning. It does not address the critical architectural requirement of how a *real-time* ingestor creates and links its `source_material` records.

**The Architectural Correction:** As we discussed, a 5-minute lag for real-time events is unacceptable. We must implement the "stage-as-you-go" pattern.

**Refined Implementation (High-Level):**
1.  **On Startup:** When a satellite like `fs-watcher` starts in `service` mode, its `scan(..., TimeHorizon::Continuous)` method must be called.
2.  **Create "In-Flight" Record:** Inside this method, before starting the file watcher, the processor must create a new, "in-flight" record in `raw.source_material_registry`. This record has a `NULL` checksum and a `status` of `'sensing'`. The `blob_id` is cached in the processor's state.
3.  **Emit Events with Provenance:** As the file watcher detects new events in real-time, the generated `core.events` records **must** have their `source_material_id` field set to the cached `blob_id` of the current in-flight chunk.
4.  **Periodically Finalize:** A background task within the processor must periodically (e.g., every 5 minutes or on graceful shutdown) "finalize" the current chunk by saving the buffered raw data to git-annex, updating the `checksum` and `status` in the registry, and then creating a *new* in-flight record for the next time window.

This is a critical addition to the plan that ensures our real-time ingestion is just as robust and auditable as our historical ingestion.

#### **Misunderstanding #4: The Role of `exo` vs. The Database**

**Your Proposal:** In the `exo replay` command, the Python script directly performs a `DELETE` on `core.events` to archive existing events before triggering the scan.

**The Architectural Correction:** This is a dangerous violation of encapsulation. The user-facing CLI should **never** have direct write/delete access to core data tables. It should only ever invoke high-level, audited operations.

**Refined Implementation:**
1.  A new RPC method, `coordinator.archive_events_for_blob(blob_id, reason)`, must be added to the gateway. This is a trusted, internal API.
2.  The Python `replay_ingestor` function will call this RPC method instead of running `DELETE` itself.
3.  The RPC handler in the gateway will perform the `DELETE`, but it will first call `core.set_archive_metadata` and wrap the entire operation in a database transaction and an entry in `core.operations_log`.

This ensures that all data modification is centralized, audited, and safe, preventing a potentially buggy CLI script from corrupting the database.

---

**Summary of Refinements:**

*   **Encapsulate Blob I/O:** Move all `git-annex` logic into the `BlobManager`.
*   **Use Typed Targets:** Pass `blob_id` via `args.targets`, not the generic config map.
*   **Implement "Stage-as-you-go":** Add the real-time ingestion and blob finalization logic to the plan.
*   **Centralize Write Operations:** All data modification must go through a trusted RPC endpoint, not be executed directly by the CLI.

These changes will bring your excellent implementation plan into perfect alignment with our final, most robust architectural vision. Please proceed with this refined plan.
