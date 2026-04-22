# Material Content Store

The content store is the SDK-owned storage boundary for source materials and
blob payloads. It exposes backend-neutral concepts (`ContentStoreKey`,
`ContentBackend`, `MaterialContentStore`, `ContentStoreManager`) while hiding the
mechanics of the concrete storage backend.

Current storage policy is hybrid:

- Small finalized materials are copied into a local BLAKE3 CAS under
  `SINEXBLAKE3-s<size>--<digest>`.
- Larger materials are handed to the large-object backend, currently a bounded
  short-lived `git-annex add --json` invocation.

Callers should not choose or invoke the backend directly. They hand a file or
byte buffer to the content-store API and receive a `ContentStoreKey`.

## Data Integrity

Sinex uses backend-aware integrity verification:

1. **BLAKE3 (Sinex-native)**: Fast hashing used for pre-ingestion
   deduplication, the local CAS key, and local-CAS retrieval verification.
2. **Backend digest**: The digest fragment embedded in backend-generated keys
   for large objects.

Every retrieval verifies against the strongest digest that belongs to the
stored backend. Local-CAS content verifies through BLAKE3. Large-object content
verifies through its backend digest when available and falls back to the stored
BLAKE3 checksum.

## The `ContentStoreManager`

The `ContentStoreManager` is the primary interface for large file operations. It orchestrates:
- **Deduplication**: BLAKE3 hashes are checked before ingestion to prevent redundant storage.
- **Metadata Registry**: Metadata is stored in the `core.blobs` table.
- **Lifecycle Events**: Emits `blob.ingested`, `blob.retrieved`, and `blob.verified` events for auditability.

### Ingestion Workflow

1. **Detect**: Node detects a file or raw bytes.
2. **Hash**: BLAKE3 hash is computed locally.
3. **Check**: `ContentStoreManager` queries the database for an existing BLAKE3 match.
4. **Store**: Files up to 16 MiB are copied into the SDK local CAS under a
   `SINEXBLAKE3-s<size>--<hash>` key. Larger files are added through a
   short-lived bounded large-object backend process.
5. **Register**: Metadata (MIME type, size, hashes) is persisted to
   `core.blobs`.

High-rate logical observations must be coalesced before this layer using
source-material streams such as `AppendStreamAcquirer` or
`BufferedAppendStreamWriter`. The storage boundary should see low-cardinality
completed materials. It must not keep resident `git-annex add --batch`
processes or spawn git-annex for every tiny source material.

## Path Security

To prevent path traversal and symlink attacks, the content store enforces strict rules:
- **`VerifiedPath`**: All ingestion paths must pass through the `VerifiedPath` type, which rejects `../` patterns.
- **Secure Temp Files**: Byte-based ingestion uses `create_secure_temp_path` with unpredictable names to prevent symlink-following vulnerabilities.

## Verification Status

The system tracks the integrity status of every blob:
- `sensing`: In-flight ingestion.
- `verified`: Passed hash check on last retrieval.
- `corrupted`: Failed hash check (triggering alerts).

## Operational Tasks

### Cleanup
Stale temporary buffers are automatically cleaned up after 5 minutes of inactivity via the `reconcile_inflight` mechanism.

### In-place vs. Byte Ingestion
- **`ingest_file`**: Adds an existing file on disk to the hybrid content
  store.
- **`ingest_from_bytes`**: buffers in-memory data (for example clipboard
  content) to a secure temp file before content-store ingestion.
