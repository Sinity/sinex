# TIM-GitAnnexLargeFileMgmt: Large File Management with Git & git-annex

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 85% (Core git-annex integration and blob metadata working, advanced features pending)
**Dependencies**: git-annex binary, PostgreSQL, core.blobs table, BLAKE3 hashing
**Blocks**: Large file storage, content deduplication, blob metadata management

## MVP Specification
- Git-annex content-addressed storage
- core.blobs metadata registry table
- BLAKE3 hash-based deduplication
- Automatic symlink and annex key management
- Integration with filesystem ingestion

## Enhanced Features
- Multi-location backup and sync
- Automated git-annex repository management
- Advanced metadata extraction pipelines
- Performance-optimized batch operations
- Distributed annex coordination

## Implementation Checklist
- [x] core.blobs table schema with ULID keys
- [x] Git-annex integration workflow
- [x] BLAKE3 content hashing
- [x] Deduplication logic
- [x] Annex key management
- [x] Symlink handling
- [ ] Multi-location sync
- [ ] Automated repository management
- [ ] Advanced metadata extraction
- [ ] Batch operation optimization

*   **Relevant ADR:** (N/A directly, core infrastructure decision from Vision Doc)
*   **Original UG Context:** Section 20
*   **Vision Document Reference:** Part III.4

This TIM details the use of `git-annex` for managing large binary files (blobs) within the Exocortex, integrated with the `core_blobs` PostgreSQL table for metadata.

## 1. Rationale Summary

`git-annex` provides content-addressed storage, deduplication, integrity checking, and location independence for large files, keeping the primary PostgreSQL database and Git repositories (for config/metadata) lean. `core_blobs` acts as the Exocortex's metadata registry for these annexed files.

## 2. Core Workflow and `core_blobs` Integration [UG Sec 20.1, OR2]

### 2.1. `git-annex` Mechanism

*   Content stored by hash (e.g., SHA256E key) in `.git/annex/objects/`.
*   Working tree file replaced by a symlink (or pointer file) to annexed content.
*   Git tracks only these symlinks/pointers.

### 2.2. `core_blobs` Table (Metadata Store)

*   **DDL (from Primary Document Appendix A, refined):**
    ```sql
    CREATE TABLE IF NOT EXISTS core.blobs (
        blob_id                 ULID PRIMARY KEY DEFAULT gen_ulid(), -- Using pgx_ulid
        content_annex_key       TEXT UNIQUE NOT NULL, -- Key used by git-annex (e.g., "SHA256E-s12345--hash.dat")
        content_blake3_hash     TEXT UNIQUE NULLABLE, -- Exocortex-computed BLAKE3 hash for verification/lookup
        mime_type               TEXT,
        size_bytes              BIGINT NOT NULL,
        original_filenames      TEXT[], -- Array of original filenames this blob content has been associated with
        user_description        TEXT,
        extracted_media_metadata JSONB, -- e.g., EXIF for images, ID3 for audio
        -- schema_id ULID REFERENCES sinex_schemas.event_payload_schemas(id) NULLABLE, -- If blob itself has a schema (e.g. structured JSON blob)
        created_at_ts_orig      TIMESTAMPTZ NULLABLE, -- If original file had a meaningful creation/mod time
        ingested_at_ts          TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    COMMENT ON TABLE core.blobs IS 'Metadata registry for content-addressed blobs, typically managed by git-annex.';
    CREATE INDEX IF NOT EXISTS idx_core_blobs_blake3_hash ON core.blobs (content_blake3_hash) WHERE content_blake3_hash IS NOT NULL;
    ```

### 2.3. Integration Workflow

1.  **Ingestion by Exocortex Agent (e.g., Filesystem Ingestor, Web Archiver):**
    a.  File received/generated.
    b.  Agent computes BLAKE3 hash of file content.
    c.  **Deduplication Check:** Agent queries `core_blobs` for existing entry with same `content_blake3_hash`.
        *   If exists: Reuse existing `blob_id` and `content_annex_key`. Add current filename to `original_filenames` array if new. Ensure symlink at current path points to this existing annex object (may involve `git annex add --force-small /path/to/new_symlink_target` if creating a new symlink to existing content, or careful symlink management).
        *   If not exists (new content):
            i.  Agent runs `git annex add /path/to/file`. This moves content to annex, creates symlink, returns `annex_key`.
            ii. Agent inserts new row into `core_blobs` with `blob_id`, `annex_key`, `content_blake3_hash`, mime, size, etc.
    d.  Agent commits the symlink (and `.gitattributes` if needed) to the Git repository backing the annex.
    e.  Agent logs `sinex.blob.ingested` event (payload: `blob_id`, `annex_key`, `blake3_hash`, source info).
2.  **Linking:** Other Exocortex entities (`core_artifacts`, `raw.events.payload`) reference blobs via `blob_id` or `annex_key`.
3.  **Accessing Content [OR2]:**
    a.  Query `core_blobs` for `annex_key` (or path to symlink).
    b.  Run `git annex get <path_to_symlink_or_annex_key>` in context of annex repo to ensure content is local.
    c.  Access via symlink path.

## 3. Scalability: Hierarchical Annex Repositories [UG Sec 20.2, SR1]

*   **Problem:** Single `git-annex` repo with millions of files experiences performance degradation for annex commands.
*   **Recommendation [SR1]:** For very large scale, use multiple, smaller `git-annex` repositories (sharded by time, content type, or project).
    *   `core_blobs` would need `annex_repo_identifier TEXT` column to indicate which shard holds the content.
    *   Keep individual annex repos < 50k-100k files.

## 4. Integrity Checks and Redundancy [UG Sec 20.3, OR3]

*   **`git annex fsck [--fast] [--incremental] [--from=<remote>]`:**
    *   Verifies integrity of annexed content (checksums, availability).
    *   Run regularly via systemd timer. Log results as `sinex.data_integrity.annex_fsck_result` events.
*   **`annex.numcopies = N` (e.g., `git config annex.numcopies 2`):**
    *   `git-annex` tries to ensure N copies exist across available remotes.
    *   `git annex drop` respects this (unless `--force`).
    *   `git annex sync --content` or `git annex copy` helps satisfy.
*   **`annex.largefiles`:** Config for `git add` to auto-annex large files (less relevant for agent-driven annexing).

