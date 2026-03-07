# Unified Journal Watcher

## Overview

The unified journal watcher uses one `journalctl` subprocess for journal and systemd event extraction.

## Architecture

The watcher uses a single `journalctl -f -o json` process and:

1. Parses each journal entry once
2. Emits a `JournalEntryWritten` event to the journal channel
3. Checks if the entry has a `_SYSTEMD_UNIT` field
4. If yes, parses systemd-specific events and emits to systemd channel

## Benefits

- **50% reduction in subprocess count** (2 processes → 1 process)
- **Single I/O stream** instead of duplicate reads
- **Unified cursor tracking** ensures no events are missed
- **Simpler lifecycle management** with one process to supervise

## Usage

```rust
use sinex_system_node::UnifiedJournalWatcher;

let mut watcher = UnifiedJournalWatcher::new(
    journal_config,
    systemd_enabled,
).await?;

// Optional: track specific systemd units
watcher.track_systemd_units(vec!["nginx.service".to_string()]);

// Start streaming to both channels
watcher.start_streaming(
    journal_tx,
    Some(systemd_tx),
    material,
).await?;
```

## Event Filtering

The watcher filters systemd events by:

1. Checking for `_SYSTEMD_UNIT` field presence
2. Matching message patterns (`Started `, `Stopped `, `Failed `, etc.)
3. Optionally filtering by tracked unit names
4. Emitting typed systemd events based on message content

## Historical Import

The watcher supports historical import with:

- Time-based filtering (`--since=-Xh`)
- Cursor-based resumption (`--after-cursor=...`)
- Unit filtering
- Priority filtering
- Batch processing for efficiency

## Cursor Tracking

The unified watcher maintains a single cursor position that:

- Tracks the last processed journal entry
- Persists to disk for crash recovery
- Ensures exactly-once processing semantics
- Works for both journal and systemd events
