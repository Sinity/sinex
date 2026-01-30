# Blob Storage & Content Deduplication

Sinex uses a content-addressable blob storage system designed for efficient handling of large binary objects. Metadata is stored in PostgreSQL, while the actual binary content is managed by **git-annex**.

## Content-Addressable Design

Every blob is uniquely identified by a combination of three properties:
1. **Annex Backend**: The hashing algorithm used (e.g., `SHA256E`).
2. **Content Hash**: The digest of the file content.
3. **Size**: The total bytes of the object.

This triple forms a natural key that maps directly to git-annex keys (e.g., `SHA256E-s123--hash`).

## Deduplication Logic

The system automatically deduplicates identical content across different files and sources.

- **Unique Constraints**: The database enforces a unique constraint on `(annex_backend, content_hash, size_bytes)`.
- **Insert or Return**: When a new blob is registered, the system attempts an insert. If a unique violation occurs, it retrieves and returns the existing blob record instead.
- **Race Condition Handling**: To handle concurrent inserts of the same content, the system implements a retry loop to wait for in-flight transactions to commit before fetching the existing record.

## BLAKE3 Checksums

In addition to the annex backend, Sinex computes a **BLAKE3** checksum for every blob.

- **Secondary Deduplication**: BLAKE3 provides a backend-independent identifier. If two blobs have different annex backends but identical BLAKE3 hashes, the system can identify them as the same content.
- **Integrity Verification**: The BLAKE3 hash is used for independent data integrity checks, augmenting git-annex's internal checksums.

## Provenance Tracking

Even when content is deduplicated, the system preserves the provenance of every original file.

- **Metadata Array**: The `original_filenames` array in the blob metadata tracks all filenames that have referenced this specific content.
- **Event Linking**: Events link to blobs via `associated_blob_ids`, enabling a path from an event back to its raw binary source.

## git-annex Integration

While PostgreSQL manages the metadata, git-annex handles the heavy lifting of storage:
- **Distributed Storage**: Content can live in multiple remotes (local disk, S3, external drives).
- **Verification**: The `verification_status` and `last_verified_at` fields are updated following `git-annex fsck` operations to track storage health.
- **Retrieval**: Retrieval is performed by resolving a Blob ID to an annex key, which is then used to fetch the content from the annex.
