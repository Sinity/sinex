# Annex Subsystem

Git-annex integration for large file management

This crate provides integration with git-annex for managing large binary files
within Sinex, using content-addressed storage with deduplication.

## Architecture Overview

- **Git-annex**: Provides content-addressed storage, deduplication, and integrity checking
- **core.blobs table**: PostgreSQL metadata registry for annexed files
- **BLAKE3 hashing**: Fast content hashing for deduplication
- **Symlink management**: Working tree files replaced by symlinks to annexed content

## Workflow

1. Large files detected during ingestion (e.g., >100KB)
2. File added to git-annex repository via `git annex add`
3. Annex key extracted and metadata stored in core.blobs
4. Original file location tracked via symlink
5. Content retrievable via annex key or blob_id

## Git-annex Key Format

Keys follow the pattern: `BACKEND-sSIZE--HASH.ext`
- Example: `SHA256E-s12345--abc123def456.dat`
- Backend: Hash algorithm (SHA256E, BLAKE3, etc.)
- Size: File size in bytes
- Hash: Content hash

## Deduplication

Files with identical content share the same annex object:
- Multiple symlinks can point to same annexed content
- Reduces storage for duplicate files
- BLAKE3 hash used for fast content comparison

## Future Enhancements (Not Yet Implemented)

### Health Monitoring and Verification
- Automated fsck scheduling via systemd timer
- Log results as `sinex.data_integrity.annex_fsck_result` events
- Track verification status per blob
- Monitor available space and annex.numcopies compliance
- Alert on missing/corrupted content

### Advanced Metadata Extraction
- File-type specific extractors:
- Images: EXIF data, dimensions, color profile
- Documents: Page count, author, creation date
- Media: Duration, codec, bitrate
- Archives: Contents listing, compression ratio
- Automatic MIME type detection
- Text extraction for searchability
- Thumbnail generation for preview

### Performance Optimizations
- Parallel file processing with worker pools
- Multi-threaded BLAKE3 checksum computation
- Bulk database insertions
- In-memory cache for frequently accessed blob metadata
- Filesystem cache for small annexed files
