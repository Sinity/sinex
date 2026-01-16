# Event Sources Coverage Analysis and Roadmap

This document tracks the current coverage of digital activity capture and outlines the roadmap to comprehensive system monitoring.

## Current Coverage: 35% of Digital Activity

### Operational Event Sources

#### Filesystem Monitor (5% coverage)
- File creation, modification, deletion events
- Implemented in `sinex-fs-watcher` node
- Captures file system changes but not file content

#### Clipboard Monitor (2% coverage)
- Copy/paste events with git-annex storage
- Part of `sinex-desktop-node`
- Stores clipboard content for text and images

#### Terminal Sources (8% coverage)
- Kitty terminal integration
- Asciinema recording playback
- Shell history import (bash, zsh, fish)
- Implemented in `sinex-terminal-node`

#### Window Manager (5% coverage)
- Hyprland IPC integration
- Basic X11 window tracking
- Part of `sinex-desktop-node`
- Captures workspace switches, window focus

#### System Sources (15% coverage)
- Git repository events
- Downloads directory monitoring
- SQLite history extraction
- Implemented in `sinex-system-node`

## Critical Gap: 65% of Digital Activity Not Captured

### High-Priority Missing Sources

#### Browser Activity Monitor (40-60% of knowledge work)
**Impact**: Largest gap in coverage — most knowledge work happens in browsers
- WebExtension captures page visits, interactions (forms, media), downloads, tab/window context
- Native messaging host sends events to the gateway; node normalizes and submits via ingestd
- Event types: `browser.page_visit`, `browser.dom_event`, `browser.media_event`, `browser.download`
- Privacy: opt‑in capture by domain/category; redact PII at source when configured

#### Process Execution Tracker (All non-terminal programs)
**Impact**: Missing visibility into GUI application usage
- Application launches and exits
- Process resource usage
- Window-to-process association
- Command-line arguments

#### Network Activity Monitor (External interactions)
**Impact**: No visibility into API calls, network services
- HTTP/HTTPS request patterns
- API endpoint usage
- Network service connections
- Bandwidth utilization

#### Screen Capture with OCR (Visual context)
**Impact**: Enables visual context and OCR
- Wayland capture via `xdg-desktop-portal` (user consent) and PipeWire screencast
- Streams frames to encoder or OCR pipeline; store full frames as annex blobs with hashes
- Event types: `screen.frame_captured`, `screen.text_ocr`
- Performance: DMA‑BUF zero‑copy paths; prefer H.264 HW encode, configurable FPS

#### Input Pattern Monitor (Activity detection)
**Impact**: Cannot detect user presence/absence
- Keyboard activity patterns
- Mouse movement heatmaps
- Idle time detection
- Input velocity metrics

## Roadmap to 80%+ Coverage

### Phase 1: Browser Integration (Q1)
1. Develop WebExtension for Firefox/Chrome
2. Implement native messaging host
3. Create browser event node
4. Target: +45% coverage (80% total)

### Phase 2: Process & Network Monitoring (Q2)
1. Implement eBPF-based process tracking
2. Add network activity monitoring
3. Create system monitoring node
4. Target: +10% coverage (90% total)

### Phase 3: Visual & Input Capture (Q3)
1. Implement Wayland screen capture
2. Add OCR processing pipeline
3. Create input monitoring node
4. Target: +5% coverage (95% total)

## Implementation Priority

1. **Browser Extension** - Highest impact, clearest path
2. **Process Tracking** - Fills major visibility gap
3. **Screen Capture** - Enables new use cases
4. **Network Monitor** - Advanced correlation
5. **Input Patterns** - Presence detection

## Technical Considerations

- Privacy-preserving design for all sources
- Configurable capture policies
- Efficient storage strategies
- Real-time vs batch processing tradeoffs
- Cross-platform compatibility where possible

## Success Metrics

- Coverage percentage of daily digital activity
- Event capture latency
- Storage efficiency
- Query performance across sources
- User-controlled privacy settings adoption
