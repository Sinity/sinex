# Filesystem Ingestor

Monitors file system activity and captures file operations as events.

## Features

- Real-time file system monitoring using notify
- Configurable include/exclude patterns
- File content hashing (BLAKE3) for integrity
- Event debouncing for rapid changes
- Batch processing for efficiency
- Recursive directory watching

## Usage

```bash
# Run with default config
cargo run --bin filesystem-ingestor

# Dry run (logs events to console)
cargo run --bin filesystem-ingestor -- --dry-run

# Output to file instead of database
cargo run --bin filesystem-ingestor -- --output-file events.json

# Use custom config
cargo run --bin filesystem-ingestor -- --config config/filesystem/production.toml

# Show current configuration
cargo run --bin filesystem-ingestor -- config

# Check database connection
cargo run --bin filesystem-ingestor -- check
```

## Configuration

Configuration uses TOML format. See `config/filesystem/` for examples.

Key settings:
- `watch_directories` - Paths to monitor (supports ~ expansion)
- `exclude_patterns` - Glob patterns to ignore
- `include_patterns` - Override excludes for specific patterns
- `debounce_ms` - Delay before processing events
- `hash_files` - Enable file content hashing
- `max_hash_size_bytes` - Skip hashing large files

## Events Captured

### file.created
```json
{
  "path": "/home/user/document.txt",
  "object_type": "file",
  "blake3_hash": "af1349b9f5f9a1a6..."
}
```

### file.modified
```json
{
  "path": "/home/user/document.txt",
  "object_type": "file",
  "blake3_hash": "bf2450c9f6f0b2b7..."
}
```

### file.deleted
```json
{
  "path": "/home/user/document.txt",
  "object_type": "file"
}
```

## System Requirements

On Linux, you may need to increase inotify limits:

```bash
# Check current limit
cat /proc/sys/fs/inotify/max_user_watches

# Increase temporarily
sudo sysctl fs.inotify.max_user_watches=524288

# Increase permanently
echo "fs.inotify.max_user_watches=524288" | sudo tee -a /etc/sysctl.conf
```

## Architecture

Uses the SimpleIngestor pattern - the ingestor only captures events while IngestorRuntime handles:
- Heartbeats
- Error recovery and retries
- Dead letter queue
- Graceful shutdown
- Event batching