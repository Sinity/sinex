# Blob Storage & Content Deduplication

Sinex stores source-material metadata and provenance in PostgreSQL and exact
encoded bytes in the Sinex-owned local BLAKE3 CAS. The canonical authority is
the pair of an immutable `MaterialManifestV1` and its exact-byte CAS object.
Third-party stores, including git-annex, are subordinate compatibility,
migration, replication, or backup adapters. They do not define replay identity.

The architecture decision and its proof requirements are recorded in
[`CAS architecture decision`](../../sinexd/docs/content_store/cas-architecture-decision.md).

## Content-Addressable Design

Every canonical source-material object is identified by its BLAKE3 digest and
encoded byte count. Backend-specific keys may exist in adapters, but they are
not authoritative identities and must retain the canonical digest and manifest
alongside the copied bytes.

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
- **Local CAS**: Finalized materials are retrieved from the canonical local BLAKE3 CAS path.
- **Adapters**: A third-party backend may provide packing, chunking, compression, replication, or backup, provided the exact-byte and manifest round trip is verified.
- **Verification**: The `verification_status` and `last_verified_at` fields are updated after content-store verification.
- **Retrieval**: Retrieval is performed by resolving a Blob ID to a content-store key, then asking the content store to ensure the content is local.
