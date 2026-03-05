# Material Assembly Subsystem

The Material Assembly subsystem in \`sinex-ingestd\` is responsible for reconstructing source materials (files, streams, blobs) from fragmented data slices arriving over NATS `JetStream`.

## State Machine & Assembly Workflow

Assembly is managed by a per-material state machine that handles out-of-order delivery and provides transactional guarantees.

1. **Initialization (\`MaterialBegin\`)**: An assembly begins when a \`MaterialBegin\` message is received, providing the \``material_id`\` and initial metadata.
2. **Slice Accumulation (\``MaterialSlice`\`)**: Data slices are written to a temporary assembly file.
   - **Sequential Delivery**: Slices are appended directly to the temporary file if they match the expected byte offset.
   - **Out-of-Order Handling**: Slices arriving out of sequence are buffered in temporary slice files and tracked in a \``BTreeMap`\`. When the missing gap is filled, the buffered chain is automatically flushed to the main assembly file.
3. **Finalization (\`MaterialEnd\`)**: Upon receiving the \`MaterialEnd\` message, the system verifies the total size and BLAKE3 hash of the assembled content.
4. **Blob Storage**: Verified content is imported into **git-annex**. The resulting annex key is registered in the \`core.blobs\` table.
5. **Registry Update**: The original source material record is updated with the \`blob_id\` and marked as \`completed\`.

## Crash Recovery (WAL)

To ensure data integrity across service restarts or crashes, the assembler utilizes a **Write-Ahead Log (WAL)**.

- **Durable State**: Every state transition (\`Begin\`, \`Slice\`, \`End\`) is recorded in the WAL before the operation is acknowledged to NATS.
- **Recovery Process**: On startup, the assembler replays the WAL files to reconstruct the state of all in-flight assemblies.
- **Incremental Hashing**: During recovery, the BLAKE3 hash is recalculated by reading the temporary assembly file, ensuring that the in-memory hasher state matches the disk content.

## Concurrency & Isolation

- **Per-Material Locking**: Assembly state is protected by granular, per-material mutexes (\``DashMap`<Uuid, Mutex<State>>\`). This ensures that multiple materials can be assembled in parallel without lock contention.
- **Semaphore Limits**: The system limits the number of concurrent in-flight assemblies (default 50) to prevent resource exhaustion (memory, disk space, file handles).

## Error Handling & Dead Letter Queue (DLQ)

If an assembly fails due to corruption, timeout, or storage errors:
- **DLQ Routing**: The material ID and failure context are routed to a Dead Letter Queue for manual investigation.
- **Cleanup**: Temporary files and WAL entries are purged to reclaim disk space.
- **State Update**: Source material is marked failed and metrics/logs capture failure context.
