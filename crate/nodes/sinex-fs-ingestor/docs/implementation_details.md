# Filesystem Ingestor Implementation Details

The Filesystem Ingestor monitors directory trees for changes and captures file content as source material. it is designed for high-volume environments with a focus on security and data integrity.

## Event Capture & Backpressure

- **inotify-based Watching**: Uses the `notify` crate to receive real-time events from the Linux kernel. Monitoring is recursive by default.
- **Burst Tolerance**: Employs a large (10,000 event) bounded channel buffer to handle rapid filesystem activity (e.g., source code builds or large file extractions).
- **Dropped Event Tracking**: An atomic counter tracks events dropped due to channel saturation, providing visibility into backpressure issues.

## Security Controls

- **Path Validation**: All watch roots are validated against a security policy before monitoring begins, preventing access to sensitive system directories (e.g., `/proc`, `/sys`).
- **Symlink Protection**: Configurable symlink following (default: off) prevents escape attacks.
- **Traversal Limits**: The `max_depth` configuration prevents resource exhaustion from deeply nested directory structures.

## Data Integrity

- **TOCTOU Mitigation**: To prevent Time-of-Check to Time-of-Use races, the ingestor opens files first and then retrieves metadata from the open file descriptor. This ensures that the size checks and reads are performed on the same file.
- **Cumulative Size Tracking**: During content streaming, the system maintains a cumulative byte counter. If a file grows beyond the configured limit during the read operation, the capture is aborted to prevent resource exhaustion.
- **Transient Error Retry**: File read operations implement exponential backoff to handle transient issues like temporary file locks or concurrent writes.

## Provenance

Every event (created, modified, deleted, moved) is linked to a source material entry.
- **Content-Rich Events**: Non-empty created and modified events capture the actual file content.
- **Metadata-Only Events**: Deleted, moved, and empty-file created/modified events append a JSONL observation record to the filesystem observation stream. This preserves byte-range provenance without creating a fresh zero-byte material for every transient filesystem event.
