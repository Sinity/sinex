# TIM-GitAnnexLargeFileMgmt: Unimplemented Operational Features

## Health Monitoring and Verification (Not Implemented)

### Automated fsck scheduling
- Run `git annex fsck [--fast] [--incremental]` via systemd timer
- Log results as `sinex.data_integrity.annex_fsck_result` events
- Track verification status per blob

### Repository health monitoring
- Track available space in annex repository
- Monitor number of copies (annex.numcopies compliance)
- Alert on missing/corrupted content

## Advanced Metadata Extraction (Not Implemented)

### File-type specific extractors
- Image files: EXIF data, dimensions, color profile
- Documents: Page count, author, creation date
- Media files: Duration, codec, bitrate
- Archives: Contents listing, compression ratio

### Content analysis pipelines
- Automatic MIME type detection
- Text extraction for searchability
- Thumbnail generation for preview

## Performance Optimizations (Not Implemented)

### Batch operation improvements
- Parallel file processing with worker pools
- Optimized checksum computation (multi-threaded BLAKE3)
- Bulk database insertions
- Connection pooling optimization

### Caching layer
- In-memory cache for frequently accessed blob metadata
- Filesystem cache for small annexed files
- Pre-computed checksums cache

## Multi-location sync features (Removed from scope)
Original design included syncing across multiple git-annex remotes, but this was removed for architectural simplicity in the single-host MVP.