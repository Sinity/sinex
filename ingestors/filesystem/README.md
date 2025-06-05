# Filesystem Activity Ingestor

The filesystem ingestor monitors file system activity and captures file operations using inotify on Linux.

## Features

- Real-time file system event monitoring
- Debouncing to handle rapid file changes
- Batch processing for efficient database writes
- File content hashing (BLAKE3) for integrity tracking
- Configurable include/exclude patterns
- Recursive directory monitoring

## Configuration

Create a configuration file at `~/.config/sinex/filesystem-ingestor.toml`:

```toml
[database]
url = "postgresql://localhost/sinex"
max_connections = 5

[logging]
level = "info"
format = "pretty"

[filesystem]
# Directories to watch (~ expansion supported)
watch_directories = [
    "~/Documents",
    "~/Projects",
    "/etc/important"
]

# Patterns to exclude (glob syntax)
exclude_patterns = [
    "*.tmp",
    "*.log",
    "*.cache",
    ".git/**",
    "node_modules/**",
    "__pycache__/**",
    "*.swp",
    "*.swo"
]

# Patterns to include (overrides excludes)
include_patterns = [
    "important.log"  # Include this even though *.log is excluded
]

# Event processing settings
debounce_ms = 500         # Wait time before processing
batch_size_events = 50    # Events per batch
batch_timeout_ms = 5000   # Max wait time for batch

# File hashing
hash_files = true
max_hash_size_bytes = 10485760  # 10MB

# Agent settings
heartbeat_interval_secs = 60
max_retries = 3
retry_delay_secs = 5
```

## Running

```bash
# Check database connection
filesystem-ingestor check

# Run the ingestor
filesystem-ingestor run

# Generate example config
filesystem-ingestor generate-config
```

## Events Produced

### `filesystem.file_created`

```json
{
  "path": "/home/user/documents/report.pdf",
  "object_type": "file",
  "blake3_hash": "af1349b9f5f9a1a6..."
}
```

### `filesystem.file_modified`

```json
{
  "path": "/home/user/documents/report.pdf",
  "object_type": "file",
  "blake3_hash": "bf2450c9f6f0b2b7..."
}
```

### `filesystem.file_deleted`

```json
{
  "path": "/home/user/documents/old_report.pdf",
  "object_type": "file"
}
```

### `filesystem.file_renamed`

```json
{
  "path": "/home/user/documents/old_name.pdf",
  "new_path": "/home/user/documents/new_name.pdf",
  "object_type": "file",
  "blake3_hash": "cf3451d0g7g1c3c8..."
}
```

## Performance Considerations

1. **Debouncing**: Prevents duplicate events during rapid file changes (e.g., saving in editors)
2. **Batching**: Groups events for efficient database writes
3. **Hashing**: Only hashes files under the size limit to avoid performance issues
4. **Patterns**: Use exclude patterns to ignore high-churn directories

## Limitations

- Rename detection depends on inotify capabilities
- Some file systems may not support all event types
- Network file systems may have limited event support
- Very large directories may hit inotify watch limits

## System Requirements

### Increase inotify limits if needed:

```bash
# Check current limits
cat /proc/sys/fs/inotify/max_user_watches

# Increase temporarily
sudo sysctl fs.inotify.max_user_watches=524288

# Increase permanently
echo "fs.inotify.max_user_watches=524288" | sudo tee -a /etc/sysctl.conf
```