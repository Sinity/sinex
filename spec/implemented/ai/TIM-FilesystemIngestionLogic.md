# TIM-FilesystemIngestionLogic: Content Hashing, Dedupe, Rename/Move Detection

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 80% (BLAKE3 hashing and git-annex integration working, rename detection partial)
**Dependencies**: git-annex, BLAKE3 library, filesystem watchers, core.blobs schema
**Blocks**: Advanced file tracking, PKM document versioning, content-addressable search

## MVP Specification
- BLAKE3 content hashing for all file changes
- Git-annex integration for blob storage
- Basic deduplication via content addressing
- File metadata tracking (mtime, size, hash)
- Core event generation for file operations

## Enhanced Features
- Advanced rename/move detection with inotify cookies
- Cross-filesystem move handling
- Intelligent content change detection heuristics
- Path normalization and case-folding
- Performance optimization for large file operations

## Implementation Checklist
- [x] BLAKE3 streaming hash implementation
- [x] Git-annex integration
- [x] Core deduplication logic
- [x] File metadata tracking
- [x] Basic event generation
- [x] Content-addressable storage
- [ ] Complete rename detection (inotify cookies)
- [ ] Cross-filesystem move handling
- [ ] Path normalization system
- [ ] Performance optimization

*   **Relevant ADR:** (N/A directly, core for filesystem ingestor)
*   **Original UG Context:** Section 12.2, 12.3

This TIM details the logic within the Exocortex Filesystem Ingestor for processing detected file changes, including content hashing, deduplication against existing blobs, and rename/move detection.

## 1. Rationale Summary

When the filesystem watcher (see `TIM-FilesystemMonitoringWatchers.md`) detects a file creation or modification, the ingestor must process the file's content to ensure integrity, avoid redundant storage, and correctly track file identity across operations like renames.

## 2. Mandatory Content Hashing (BLAKE3) [UG Sec 12.2.1]

*   **Algorithm:** **BLAKE3** is used for hashing all new or modified file content within watched directories. Chosen for speed and cryptographic strength.
*   **When to Hash [OR3]:**
    1.  **New File (`IN_CREATE` / `notify::EventKind::Create`):** Hash the new file's content.
    2.  **File Modified (`IN_CLOSE_WRITE` preferred, or heuristic for `notify::EventKind::Modify(ModifyKind::Data(_))`):**
        a.  `stat` the file to get current `mtime` and `size`.
        b.  Compare with last known `mtime`/`size` for this path (from ingestor's local cache or Exocortex DB `core_artifacts`/`core_blobs` metadata).
        c.  If `mtime` or `size` has *not* changed: Likely metadata-only update (e.g., `chmod`) or spurious event. Optionally, re-hash only if `mtime` changed but `size` is same (catches in-place edits not changing size). If both same, consider skipping re-hash.
        d.  If `mtime` or `size` *has* changed: Re-compute full BLAKE3 content hash.
*   **Streaming Hash Computation [OR3]:** For large files, use a streaming BLAKE3 implementation (read file in chunks, update hash state) to avoid loading entire file into memory.
    ```rust
    // use std::fs::File;
    // use std::io::{BufReader, Read};
    // use blake3::Hasher; // From 'blake3' crate

    // fn calculate_blake3_hash(file_path: &std::path::Path) -> Result<String, std::io::Error> {
    //     let input = File::open(file_path)?;
    //     let mut reader = BufReader::new(input);
    //     let mut hasher = Hasher::new();
    //     let mut buffer = [0; 65536]; // 64KB buffer

    //     loop {
    //         let n = reader.read(&mut buffer)?;
    //         if n == 0 {
    //             break;
    //         }
    //         hasher.update(&buffer[..n]);
    //     }
    //     Ok(hasher.finalize().to_hex().to_string())
    // }
    ```

## 3. Integration with `git-annex` and `core_blobs`

If a file's content (based on its BLAKE3 hash) is new or represents a new version:
1.  **Add to `git-annex`:** The Filesystem Ingestor invokes `git annex add /path/to/file`. This command:
    *   Moves the file content into the annex (`.git/annex/objects/`).
    *   Replaces the original file with a symlink (or pointer file) to the annexed content.
    *   Returns the `annex_key` (e.g., "SHA256E-s<size>--<hash>.<suffix>").
2.  **Update `core_blobs`:**
    *   A row is inserted/updated in `core_blobs` with the `annex_key`, the computed `content_blake3_hash`, `mime_type`, `size_bytes`, `original_filenames` (including current path), etc. This provides the link between Exocortex metadata and the physical annexed blob.
3.  **Event Generation:** An event like `filesystem.file.content_ingested_to_annex` is logged to `core.events`, containing the `core_blobs.blob_id`, `annex_key`, `content_blake3_hash`, original file path, and `mtime`/`size`.

## 4. Deduplication [UG Sec 12.2]

*   **Content-Addressed Storage:** `git-annex` inherently deduplicates identical file content because it keys content by hash.
*   **Exocortex DB Deduplication:**
    *   Before adding a file to `git-annex` and creating a new `core_blobs` entry, the ingestor checks if a blob with the same `content_blake3_hash` already exists in `core_blobs`.
    *   If yes:
        *   The existing `core_blobs.blob_id` and `annex_key` are reused.
        *   The current file path is added to `core_blobs.original_filenames` array for the existing blob if not already present.
        *   A new symlink is created at the current file path pointing to the existing annexed content (if `git annex add` wasn't already run and handled this).
        *   An event `filesystem.file.identified_as_duplicate_of_blob` is logged, linking current path to existing `blob_id`.
    *   If no: Proceed with `git annex add` and new `core_blobs` entry as above.

## 5. Metadata Tracking for Change Detection [UG Sec 12.2.2, OR3]

The Filesystem Ingestor (or a central Exocortex metadata store for files) needs to track:
*   `file_path` (canonical)
*   `device_id`, `inode_number` (platform-specific, for rename heuristics)
*   `last_known_mtime`, `last_known_size_bytes`
*   `last_known_content_blake3_hash`
*   `exocortex_blob_id` (if content ingested) or `exocortex_artifact_id` (if this file represents a PKM note artifact).
This state is used to determine if a file event signifies a content change (requiring re-hash) or just metadata change.

## 6. Rename/Move Detection Algorithm [UG Sec 12.2.3]

### 6.1. Using `inotify` Events (Linux - Preferred) [CR3]

*   `IN_MOVED_FROM` event (path `old_path`, `cookie=X`).
*   `IN_MOVED_TO` event (path `new_path`, `cookie=X`).
*   Matching `cookie` links these as an atomic rename/move.
*   Ingestor updates `file_path` in its metadata store for the corresponding `inode_number` (if same filesystem) or for the `content_blake3_hash`/`blob_id`.
*   Logs `filesystem.file.renamed` event: `{ "old_path": "...", "new_path": "...", "blob_id": "...", "inode": "..." }`.

### 6.2. Inferring from Hash Correlation (Fallback/Cross-Platform) [OR3, CR3]

If `inotify` cookies are unavailable/unreliable (e.g., non-Linux, missed events, cross-filesystem move appearing as delete+create):
1.  File at `old_path` detected as deleted (`IN_DELETE` or disappears from scan).
2.  File at `new_path` detected as created (`IN_CREATE` or appears in scan) shortly after.
3.  If `new_path`'s `content_blake3_hash` matches `old_path`'s last known hash:
    *   Strongly suggests a rename/move.
    *   Heuristics [CR3]: Use inode (if same FS), size, mtime (if preserved), and partial content hashes (e.g., first/last 1KB) for initial candidate matching before full hash comparison to improve performance. CR3 suggests "inode+size+mtime+partial_hash correlation with 95% confidence".
    *   Temporal window for matching delete and create events (e.g., within 1-5 seconds [CR3]).
4.  Performance [CR3]: Correlation check ~15,000 files/sec, sub-ms per comparison.

## 7. Case-Folding Behavior and Path Normalization [UG Sec 12.3, CR3]

*   **Problem:** Filesystems differ in case-sensitivity (e.g., ext4 default sensitive, APFS default insensitive but preserving, ZFS configurable). If Exocortex ingests from multiple FS types, paths need consistent handling.
*   **Exocortex Path Normalization Strategy:**
    1.  **Canonical Path Storage:** Paths stored in Exocortex DB (e.g., as `core_artifacts.canonical_identifier` for PKM notes, or in file metadata associated with `core_blobs`) should be normalized.
        *   **Recommendation:** Normalize to a consistent Unicode Normalization Form (e.g., **NFC** is common for Linux/web).
        *   **Case:** Store paths with their **original casing** as observed on the filesystem. Perform case-insensitive comparisons during lookups if the underlying filesystem for a *query context* is known to be case-insensitive, or if global case-insensitivity for identification is desired. PostgreSQL collations can support case-insensitive and accent-insensitive searches.
    2.  **Ingestor Awareness:** The Filesystem Ingestor should, if possible, detect the case-sensitivity of the filesystem it's monitoring (e.g., by attempting to create `TestFile.txt` and `testfile.txt` in a temp dir). This knowledge can inform its local path matching logic.
    3.  **Querying:** Queries for files by path should:
        *   Apply the same Unicode normalization as used for storage.
        *   Use case-insensitive collations or operators in SQL if matching across filesystems with different case sensitivities (e.g., `WHERE lower(path_column) = lower(query_path)` or use a case-insensitive collation).

