# Large File Management (Annex Subsystem)

The Annex subsystem provides content-addressed storage for large binary files using `git-annex`. It ensures data deduplication and integrity across the entire Sinex ecosystem.

## 🛡️ Data Integrity: Dual-Hash Verification

Sinex uses a defense-in-depth approach to data integrity:

1.  **BLAKE3 (Sinex-Native)**: Fast hashing used for pre-ingestion deduplication and local verification.
2.  **SHA256 (Git-Annex Native)**: The canonical hash stored within the git-annex backend.

Every file retrieval triggers a **Dual-Hash Verification**. If the content fails to match *either* hash, the blob is marked as `corrupted` in the database, and the retrieval fails.

## 🧱 The `BlobManager`

The `BlobManager` is the primary interface for large file operations. It orchestrates:
- **Deduplication**: BLAKE3 hashes are checked before ingestion to prevent redundant storage.
- **Metadata Registry**: Metadata is stored in the `core.blobs` table.
- **Lifecycle Events**: Emits `blob.ingested`, `blob.retrieved`, and `blob.verified` events for auditability.

### Ingestion Workflow

1.  **Detect**: Node detects a large file (>100KB) or raw bytes.
2.  **Hash**: BLAKE3 hash is computed locally.
3.  **Check**: `BlobManager` queries the database for an existing BLAKE3 match.
4.  **Store**: If new, the file is added via `git-annex add`.
5.  **Register**: Metadata (MIME type, size, hashes) is persisted to `core.blobs`.

## 📂 Path Security

To prevent path traversal and symlink attacks, the Annex subsystem enforces strict rules:
- **`VerifiedPath`**: All ingestion paths must pass through the `VerifiedPath` type, which rejects `../` patterns.
- **Secure Temp Files**: Byte-based ingestion uses `create_secure_temp_path` with unpredictable names to prevent symlink-following vulnerabilities.

## 🚦 Verification Status

The system tracks the integrity status of every blob:
- `sensing`: In-flight ingestion.
- `verified`: Passed hash check on last retrieval.
- `corrupted`: Failed hash check (triggering alerts).

## 🛠️ Operational Tasks

### Cleanup
Stale temporary buffers are automatically cleaned up after 5 minutes of inactivity via the `reconcile_inflight` mechanism.

### In-place vs. Byte Ingestion
- **`ingest_file`**: Adds an existing file on disk to the annex (moves it to the object store).
- **`ingest_from_bytes`**: buffers in-memory data (e.g., clipboard) to a secure temp file before annexing.