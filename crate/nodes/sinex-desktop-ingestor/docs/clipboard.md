# Clipboard Module

Clipboard watcher with Stage-as-you-go source material capture

Monitors clipboard changes and text selection events, capturing them as source material
for later event creation with proper provenance tracking.

## Architecture

This module follows the Stage-as-you-go pattern:
1. **Source Material Capture**: Clipboard content → raw.source_material_registry
2. **Temporal Ledger**: Precise timing → raw.temporal_ledger
3. **Event Generation**: Material processing → events with Provenance::Material

## Features

- BLAKE3 content hashing for deduplication
- Source application detection via window manager integration
- File path extraction and URL detection
- Support for both clipboard and primary selection
- Comprehensive metadata capture
