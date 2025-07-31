# Understanding Provenance in Sinex

## Core Principle

Every raw event in Sinex MUST have provenance - an unbreakable link to the exact source material (raw byte stream) that created it. This is non-negotiable for the architecture's integrity.

## What Source Materials Are

Source materials are **raw byte streams** that enter ingestors, NOT interpretations:
- For terminal: The raw socket output from Kitty/tmux (e.g., `\x1b[0mcommand\n\x1b[32m$\x1b[0m`)
- For fs-watcher: The inotify event stream (e.g., `CREATE /home/user/foo.txt\n`)
- For clipboard: The X11/Wayland event stream
- For log files: The actual log file contents

## Current State (Broken)

1. **No automatic provenance**: The SDK's `context.emit_event()` just passes events through
2. **No raw data capture**: Ingestors parse data and emit events but never store the raw input
3. **Events lack source_material_id**: Violating the core principle
4. **Stage-as-you-go exists but unused**: It's in the SDK but not integrated into the main flow

## How It Should Work

### The Stage-as-You-Go Pattern

For real-time streams with unknown end boundaries:

1. **Register "in-flight" source material** when starting to capture
2. **Buffer raw bytes locally** as they arrive
3. **Emit events immediately** with references to the in-flight material
4. **Periodically finalize chunks** (e.g., every 5 minutes):
   - Write buffer to git-annex
   - Update source material record with checksum
   - Start new in-flight chunk

### Unified Stream Processing

The key insight from Gemini: **Both historical and continuous modes process identical streams**.

The SDK should provide a `StreamingIngestorFramework` where:
- Ingestors implement a simple `StreamParser` trait
- The framework handles all provenance, buffering, and finalization
- Historical mode: Reads from git-annex blobs
- Continuous mode: Reads from live sources
- **Same parsing logic for both**

### Example Flow

```rust
// Ingestor only implements:
trait StreamParser {
    type Stream: AsyncBufRead;
    
    // Connect to source (socket for live, file for historical)
    async fn connect(&mut self) -> Result<Self::Stream>;
    
    // Get next chunk of bytes
    async fn next_slice(&mut self, stream: &mut Self::Stream) -> Result<Option<Vec<u8>>>;
    
    // Parse bytes into events (pure function)
    fn interpret_slice(&self, slice: &[u8], metadata: &SliceMetadata) -> Result<Vec<Event>>;
}

// Framework handles:
- Creating/managing source material records
- Buffering raw data
- Adding provenance to events
- Periodic chunk finalization
- Crash recovery
```

## What Needs to Change

1. **Database**: Add `status` column to source_material_registry for in-flight records
2. **Repository**: Add methods for in-flight lifecycle (register, finalize)
3. **SDK**: Implement StreamingIngestorFramework
4. **Ingestors**: Refactor to implement StreamParser instead of StatefulStreamProcessor

## Important Misconceptions to Avoid

- **Files are NOT source material** for fs-watcher - the inotify stream is
- **Stage-as-you-go is NOT for all events** - only for streaming data
- **Provenance is NOT optional** - every raw event must have it
- **The SDK SHOULD handle this** - not individual ingestors

## Current Workarounds

Until the proper framework is built:
- Ingestors emit events without provenance (architectural violation)
- Stage-as-you-go is only used for specific log processing
- The `_process_file_with_staging` method in fs-watcher is completely wrong and should be deleted