# Clipboard Module

Clipboard watcher with Stage-as-you-go source material capture

Monitors clipboard changes and text selection events, capturing them as source material
for later event creation with proper provenance tracking.

## Architecture

This module follows the Stage-as-you-go pattern:
1. **Source Material Capture**: Clipboard content → `raw.source_material_registry`
2. **Temporal Ledger**: Precise timing → `raw.temporal_ledger`
3. **Event Generation**: Material processing → events with `Provenance::Material`

## Features

- BLAKE3 content hashing for deduplication
- Source application detection via window manager integration
- File path extraction and URL detection
- Support for both clipboard and primary selection
- Comprehensive metadata capture

## Configuration Constants

The following hardcoded values control behavior:

- `DEFAULT_MAX_PREVIEW_LENGTH`: 100 chars - Length of text preview in events
- `DEFAULT_MAX_CONTENT_SIZE`: 10MB - Maximum clipboard content size (warning threshold)
- `DEFAULT_MAX_HISTORY_ENTRIES`: 1000 - Maximum entries in deduplication history
- `CLIPBOARD_COMMAND_TIMEOUT`: 5s - Timeout for window manager queries
- Poll interval: 100ms (fixed for native clipboard API)

These values are not currently configurable but may become so in future versions.
