# Blob Storage & Content Deduplication

Sinex uses the SDK content store for source-material and blob bytes. Metadata is
stored in PostgreSQL, while bytes are stored in a hybrid content-addressed
backend: small finalized materials use a local BLAKE3 CAS, and larger objects
use the large-object backend currently implemented with git-annex.

## Content-Addressable Design

Every blob is uniquely identified by a combination of three properties:
1. **Storage Backend**: The backend identifier used in the content-store key (for example `SINEXBLAKE3` or `SHA256E`).
2. **Content Hash**: The backend digest fragment.
3. **Size**: The total bytes of the object.

This triple forms a natural key that maps directly to content-store keys such as
`SINEXBLAKE3-s123--hash` or `SHA256E-s123--hash`.

## Deduplication Logic

The system automatically deduplicates identical content across different files and sources.

- **Unique Constraints**: The database enforces a unique constraint on the stored backend/digest identity. The column is still named `annex_backend` for schema compatibility, but the Rust model exposes it as `storage_backend`.
- **Insert or Return**: When a new blob is registered, the system attempts an insert. If a unique violation occurs, it retrieves and returns the existing blob record instead.
- **Race Condition Handling**: To handle concurrent inserts of the same content, the system implements a retry loop to wait for in-flight transactions to commit before fetching the existing record.

## BLAKE3 Checksums

In addition to any backend digest, Sinex computes a **BLAKE3** checksum for every blob.

- **Secondary Deduplication**: BLAKE3 provides a backend-independent identifier. If two blobs have different storage backends but identical BLAKE3 hashes, the system can identify them as the same content.
- **Integrity Verification**: The BLAKE3 hash is used for independent data integrity checks, augmenting backend-native checksums.

## Provenance Tracking

Even when content is deduplicated, the system preserves the provenance of every original file.

- **Metadata Array**: The `original_filenames` array in the blob metadata tracks all filenames that have referenced this specific content.
- **Event Linking**: Events link to blobs via `associated_blob_ids`, enabling a path from an event back to its raw binary source.

## Backend Integration

While PostgreSQL manages metadata, the SDK content store owns byte placement:
- **Local CAS**: Small materials are retrieved directly from the local BLAKE3 CAS path.
- **Large-object backend**: Larger objects are retrieved and verified through the backend implementation.
- **Verification**: The `verification_status` and `last_verified_at` fields are updated after content-store verification.
- **Retrieval**: Retrieval is performed by resolving a Blob ID to a content-store key, then asking the content store to ensure the content is local.
