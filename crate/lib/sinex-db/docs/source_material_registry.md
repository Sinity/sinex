# Source Material Registry

The Source Material Registry is the "birth certificate" system for all external data entering Sinex. It implements the **Stage-as-you-go** pattern to maintain event provenance during long-running data captures.

## Stage-as-you-go Pattern

This pattern allows ingestors to register a stable ID for events before the entire data source (file or stream) has been fully captured or moved to permanent storage.

1. **Registration (`SENSING`)**: At the start of a capture, an entry is created with a `sensing` status. This provides a ULID that can be used immediately by event generators.
2. **Streaming**: Data is captured and streamed to NATS. All resulting events reference the Material ID.
3. **Finalization (`COMPLETED`)**: Once the capture is done, the data is moved to blob storage (git-annex), and the registry entry is updated with the `blob_id` and `completed` status.
4. **Failure (`FAILED`)**: If the capture is interrupted, the entry is marked as `failed` with a recorded reason, preserving the audit trail for any partial events that were generated.

## Temporal Ledger

The `temporal_ledger` is a specialized table that provides high-precision ground truth for event timing.

- **Capture Mapping**: It records exactly when specific byte ranges or record offsets were acquired from the source.
- **Timestamp Derivation**: When events are processed later, the system queries the ledger to derive the original `ts_orig` based on the event's offset in the source material.
- **Immutability**: The ledger is append-only, ensuring that the historical timeline of data acquisition remains tamper-proof.

## Implementation Details

- **Uniqueness**: The `source_identifier` (usually a URI or path) must be unique across the entire registry.
- **Storage**: Most materials are backed by the `annex` storage backend, linking the registry metadata to actual binary content in git-annex.
- **Idempotency**: The registration methods use `ON CONFLICT DO UPDATE` patterns to handle distributed retries safely.
