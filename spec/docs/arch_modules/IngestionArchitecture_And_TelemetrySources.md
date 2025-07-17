# Ingestion Architecture & Telemetry Sources: Domain-Specific Event Capture

*   **Version:** 2.1
*   **Date:** 2025-07-17
*   **Implementation Status:** ✅ **OPERATIONAL** - Multiple ingestor satellites capturing diverse telemetry sources
*   **Purpose:** This document describes the domain-specific telemetry sources and event capture patterns within the Sinex satellite constellation. It focuses on the unique architectural approaches for each telemetry domain rather than the general satellite architecture (covered in DataSubstrate_Architecture.md).
*   **Scope:** Covers filesystem, terminal, desktop, system, and application-specific ingestion patterns with their implementation details and data schemas.

## 1. Telemetry Source Overview

### 1.1. Ingestion Principles

*   **Layered Fidelity:** Capture data from the most direct and semantically rich source available
*   **Ambient Capture:** Continuous, unobtrusive background data collection
*   **Minimal Processing:** Ingestors focus on reliable capture, complex analysis handled by automata
*   **Strategic Redundancy:** Multiple layers provide error checking and context fusion
*   **Idempotency:** Avoid duplicate events from the same source data

### 1.2. Operational Ingestor Services

**Active Ingestors:**
- **sinex-fs-watcher:** Filesystem monitoring and change detection
- **sinex-terminal-satellite:** Terminal session and command capture
- **sinex-desktop-satellite:** Desktop environment interaction capture
- **sinex-system-satellite:** System logs and service monitoring

**Event Flow:** All ingestors follow the unified satellite architecture pattern described in DataSubstrate_Architecture.md.

### 1.3. Common Event Structure

All ingestors emit events with this unified structure:
```json
{
  "id": "01HK...",
  "source": "sinex-fs-watcher",
  "event_type": "file.created",
  "ts_orig": "2025-07-17T10:30:00Z",
  "host": "sinnix-prime",
  "payload": {
    "path": "/home/user/document.txt",
    "size": 1024,
    "mime_type": "text/plain"
  }
}
```

## 2. Filesystem Telemetry (sinex-fs-watcher)

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Filesystem monitoring with inotify integration

### 2.1. Filesystem Monitoring Architecture

**Platform-Specific Watchers:**
- **Linux:** inotify with recursive directory watching
- **Cross-Platform:** notify-rust crate for abstraction
- **Overflow Recovery:** Handles inotify queue overflow gracefully

**Event Processing:**
```rust
use notify::{Watcher, RecursiveMode, Event, EventKind};

fn handle_filesystem_event(event: Event) -> Result<RawEvent, Error> {
    match event.kind {
        EventKind::Create(_) => create_file_event(event),
        EventKind::Modify(_) => modify_file_event(event),
        EventKind::Remove(_) => delete_file_event(event),
        EventKind::Move(_) => move_file_event(event),
        _ => Ok(None),
    }
}
```

### 2.2. File Change Detection

**Monitored Events:**
- **File Creation:** `file.created` with metadata
- **File Modification:** `file.modified` with size/mtime changes
- **File Deletion:** `file.deleted` with last-known metadata
- **File Moves:** `file.moved` with source and destination paths
- **Directory Changes:** `dir.created`, `dir.deleted`

**Event Payloads:**
```json
{
  "event_type": "file.created",
  "payload": {
    "path": "/home/user/document.txt",
    "size": 1024,
    "mime_type": "text/plain",
    "blake3_hash": "abc123...",
    "permissions": "0644",
    "created_at": "2025-07-17T10:30:00Z",
    "parent_dir": "/home/user"
  }
}
```

### 2.3. Git-Annex Integration

**Content Management:**
1. Detect file creation/modification
2. Compute BLAKE3 hash for content addressing
3. Check for existing content (deduplication)
4. `git annex add` for new content
5. Update `core_blobs` metadata table
6. Emit `sinex.blob.ingested` event

**Deduplication Logic:**
```rust
if let Some(existing_blob) = check_existing_content(&blake3_hash).await? {
    // File already exists, just update metadata
    update_blob_metadata(existing_blob.id, &file_info).await?;
} else {
    // New content, add to git-annex
    let annex_key = git_annex_add(&file_path).await?;
    create_blob_record(annex_key, &file_info).await?;
}
```

## 3. Desktop Environment Integration & Telemetry

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Desktop environment interaction capture via sinex-desktop-satellite

### 3.1. Hyprland Compositor Integration (Wayland)

**IPC Socket Integration:**
- **socket1:** Command/query interface for compositor state
- **socket2:** Real-time event stream for window/workspace changes
- **Event Enrichment:** socket2 events enhanced with socket1 state queries

**Captured Events:**
- **Window Events:** `window.opened`, `window.closed`, `window.focused`, `window.moved`, `window.resized`
- **Workspace Events:** `workspace.switched`, `workspace.created`, `workspace.destroyed`
- **Monitor Events:** `monitor.focused`, `monitor.connected`, `monitor.disconnected`
- **Input Events:** `keyboard.pressed`, `mouse.moved`, `mouse.clicked`

**Event Payload Example:**
```json
{
  "event_type": "window.focused",
  "payload": {
    "window_id": "0x1234567",
    "title": "terminal - vim",
    "class": "kitty",
    "pid": 12345,
    "workspace": "1",
    "geometry": {
      "x": 100, "y": 100,
      "width": 800, "height": 600
    },
    "fullscreen": false,
    "floating": false
  }
}
```

**Implementation:**
```rust
use hyprland::shared::HyprDataActive;
use tokio::net::UnixStream;

async fn monitor_hyprland_events() -> Result<(), Error> {
    let mut socket = UnixStream::connect("/tmp/hypr/socket2").await?;
    
    loop {
        let event = read_hyprland_event(&mut socket).await?;
        let enriched_event = enrich_with_state(&event).await?;
        
        emit_desktop_event(enriched_event).await?;
    }
}
```

### 3.2. GUI Accessibility Framework (AT-SPI2)

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Widget-level UI interaction capture

**Accessibility Bus Integration:**
- **D-Bus Connection:** Monitor AT-SPI2 accessibility events
- **Widget Tracking:** Capture focus changes, text input, UI state changes
- **Application Context:** Identify active applications and UI components

**Event Types:**
- **Focus Events:** `ui.widget.focused`, `ui.widget.unfocused`
- **Text Events:** `ui.text.changed`, `ui.text.selected`
- **State Events:** `ui.widget.state_changed`, `ui.widget.value_changed`
- **Navigation Events:** `ui.navigation.menu_opened`, `ui.navigation.dialog_opened`

**Implementation:**
```python
import pyatspi2

class AccessibilityMonitor:
    def __init__(self):
        pyatspi2.Registry.registerEventListener(
            self.on_focus_changed, "focus:")
        pyatspi2.Registry.registerEventListener(
            self.on_text_changed, "object:text-changed")
    
    def on_focus_changed(self, event):
        widget_info = {
            "name": event.source.name,
            "role": event.source.getRole(),
            "application": event.source.getApplication().name,
            "path": get_widget_path(event.source)
        }
        emit_ui_event("ui.widget.focused", widget_info)
```

### 3.3. Low-Level Input Capture (evdev)

> **⚠️ IMPLEMENTATION STATUS: OPTIONAL** - Raw input capture with security considerations

**Security-First Architecture:**
- **Minimal Privileged Component:** Sandboxed evdev reader
- **Journald Bridge:** Structured JSON output to journald
- **Clear User Consent:** Explicit opt-in with UI notifications
- **Privilege Separation:** Unprivileged processing component

**Captured Data:**
- **Keyboard:** Raw scancodes, keysyms, timing
- **Mouse:** Movement deltas, button states, wheel events
- **Touchpad:** Gesture recognition, pressure sensitivity

**Implementation (Privileged Component):**
```rust
use evdev::{Device, InputEventKind};

fn main() -> Result<(), Error> {
    let device = Device::open("/dev/input/event0")?;
    
    for event in device.fetch_events()? {
        match event.kind() {
            InputEventKind::Key(key) => {
                let key_event = json!({
                    "type": "key",
                    "code": key.code(),
                    "value": event.value(),
                    "timestamp": event.timestamp()
                });
                println!("{}", key_event);
            },
            InputEventKind::RelAxis(axis) => {
                // Handle mouse movement
            },
            _ => {}
        }
    }
}
```

**Security Measures:**
- Process sandboxing with minimal capabilities
- Automatic UI notification when active
- Encrypted storage for sensitive data
- User-configurable filtering rules

### 3.4. Clipboard Monitoring

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Cross-platform clipboard event capture

**Wayland Implementation:**
```bash
# Monitor clipboard changes
wl-paste --watch sinex-clipboard-handler
```

**X11 Implementation:**
```rust
use x11rb::protocol::xfixes::*;
use x11rb::connection::Connection;

fn monitor_clipboard_x11() -> Result<(), Error> {
    let (conn, screen) = x11rb::connect(None)?;
    
    // Register for selection notifications
    select_selection_input(&conn, screen.root, SelectionEventMask::SET_SELECTION_OWNER)?;
    
    loop {
        let event = conn.wait_for_event()?;
        if let Event::SelectionNotify(notify) = event {
            let content = get_selection_content(&conn, notify.selection)?;
            emit_clipboard_event(content).await?;
        }
    }
}
```

**Event Types:**
- **Text Content:** `clipboard.text.copied` with plain text
- **Image Content:** `clipboard.image.copied` with image metadata
- **Rich Content:** `clipboard.rich.copied` with HTML/RTF content
- **File Lists:** `clipboard.files.copied` with file paths

**Privacy Considerations:**
- Optional content filtering for sensitive data
- Configurable retention policies
- User-controlled enable/disable
- Content hashing for deduplication

## 4. Terminal Activity Capture (sinex-terminal-satellite)

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - Multi-layered terminal activity logging

### 4.1. Atuin Command History Integration

**Structured Command History:**
- **SQLite Database:** Read from Atuin's local history database
- **Rich Metadata:** Command, timestamp, CWD, exit status, duration
- **Incremental Sync:** Checkpoint-based processing of new commands

**Event Schema:**
```json
{
  "event_type": "command.executed",
  "payload": {
    "command": "git commit -m 'fix: update schema'",
    "cwd": "/home/user/project",
    "exit_status": 0,
    "duration_ms": 1250,
    "session_id": "abc123",
    "hostname": "sinnix-prime",
    "user": "sinity"
  }
}
```

**Implementation:**
```rust
use sqlx::SqlitePool;

struct AtuinHistoryReader {
    pool: SqlitePool,
    last_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl AtuinHistoryReader {
    async fn sync_new_commands(&mut self) -> Result<Vec<Command>, Error> {
        let query = "
            SELECT command, timestamp, cwd, exit, duration 
            FROM history 
            WHERE timestamp > ? 
            ORDER BY timestamp ASC
        ";
        
        let commands = sqlx::query_as::<_, Command>(query)
            .bind(self.last_timestamp)
            .fetch_all(&self.pool)
            .await?;
        
        if let Some(last_cmd) = commands.last() {
            self.last_timestamp = Some(last_cmd.timestamp);
        }
        
        Ok(commands)
    }
}
```

### 4.2. PTY Session Recording

**Asciinema Integration:**
- **Session Capture:** Full terminal I/O with timing information
- **Replay Capability:** Store `.cast` files for session replay
- **Metadata Extraction:** Session duration, command count, exit codes

**Recording Workflow:**
1. Detect new terminal session
2. Start asciinema recording
3. Monitor session for completion
4. Process recording file
5. Extract metadata and store in git-annex
6. Emit session events

### 4.3. Kitty Terminal Integration

**RC Protocol Usage:**
- **Window/Tab State:** Track terminal window organization
- **Scrollback Capture:** Extract terminal scrollback buffer
- **CWD Tracking:** Monitor current working directory changes

**Event Types:**
- **Session Events:** `session.started`, `session.ended`
- **Navigation Events:** `terminal.cwd_changed`, `terminal.tab_switched`
- **Content Events:** `terminal.output_captured`, `terminal.scrollback_saved`

### 4.4. Unified Terminal Processing

**sinex-terminal-satellite Service:**
- Combines all terminal telemetry sources
- Correlates events across different layers
- Provides unified terminal session context
- Handles session boundaries and state management

### 4.5. Screen and Audio Capture

> **✅ IMPLEMENTATION STATUS: OPERATIONAL** - User-consented multimedia capture

**Wayland Screen Capture:**
- **PipeWire Integration:** Standard screen capture via PipeWire streams
- **User Consent:** xdg-desktop-portal permission system
- **Efficient Capture:** DMA-BUF support for zero-copy operations
- **Flexible Sources:** Full screen, window, or region capture

**Audio Capture:**
- **Microphone Input:** Capture user audio for speech recognition
- **System Audio:** Monitor system audio output
- **ASR Optimization:** 16kHz mono 16-bit PCM format
- **Privacy Controls:** User-configurable audio filtering

**Implementation:**
```rust
use pipewire::stream::Stream;
use pipewire::spa::param::ParamType;

struct ScreenCapture {
    stream: Stream,
    frame_buffer: Vec<u8>,
}

impl ScreenCapture {
    async fn start_capture(&mut self) -> Result<(), Error> {
        self.stream.connect(
            pipewire::stream::StreamDirection::Input,
            Some("screen-capture"),
            pipewire::stream::StreamFlags::AUTOCONNECT,
        )?;
        
        // Process frames in callback
        Ok(())
    }
    
    fn on_frame(&mut self, frame: &[u8]) {
        // Process captured frame
        self.emit_frame_event(frame);
    }
}
```

## 4. Application & Content Ingestion (STAD Part III)

This section details ingestors for data originating from specific applications or content types beyond general desktop telemetry.

### 4.1. Browser Integration

Captures web browsing activity.
*   **Architectural Approach:** A Manifest V3 compliant browser extension communicates with a local native messaging host (`sinex_browser_native_host`).
*   **Extension Capabilities (WebExtension APIs):** Uses `webNavigation` (navigation lifecycle), `storage.local` (cache/config), `tabs` (tab lifecycle, state), `history`, `bookmarks`, `scripting`/Content Scripts (page content extraction, in-page interaction capture).
*   **Native Messaging:** Protocol uses JSON messages length-prefixed over stdin/stdout. Native host (Rust/Python) processes messages and ingests to `raw.events`. NixOS manages host manifest.
*   **Key Data Captured:** Visited URLs, page titles, navigation transitions, tab states, bookmarks, form submissions (redacted), downloaded files metadata, extracted page text/main content.
*   **Referenced TIMs:**
    *   `[TIM-BrowserExtensionAPIs.md](docs/tims/ingestors/application/TIM-BrowserExtensionAPIs.md)`
    *   `[TIM-BrowserNativeMessaging.md](docs/tims/ingestors/application/TIM-BrowserNativeMessaging.md)`

### 4.2. Web Archiving

Creates durable, high-fidelity archives of web pages. Orchestrated by a `WebArchivingAgent`.
*   **Architectural Approach (Hybrid Workflow):**
    1.  **Trafilatura:** Lightweight first pass for main text/metadata extraction.
    2.  **SingleFile CLI:** For high-fidelity self-contained HTML snapshots (good for JS-heavy, authenticated single pages).
    3.  **Browsertrix Crawler:** Primary tool for deep, dynamic, authenticated WARC/WACZ archival (uses headless Chrome/Brave). Manages browser profiles/cookies. Typically run via Docker.
    4.  **(Optional) ArchiveBox:** Can orchestrate some of these and produce multiple output formats.
*   **Advanced Techniques:** Chrome DevTools Protocol (CDP) for fine-grained authenticated session capture. DOM Diffing (`diff-dom`) for efficient change tracking on re-crawled pages.
*   **Storage:** Archives (WARC, WACZ, HTML) stored as `core_blobs` (git-annexed). Extracted Markdown in `core_artifact_contents`. Metadata in `core_artifacts`.
*   **Referenced TIMs:**
    *   `[TIM-WebArchivingTooling.md](docs/tims/ingestors/application/TIM-WebArchivingTooling.md)`
    *   `[TIM-WebArchivingCDP_DOMDiff.md](docs/tims/ingestors/application/TIM-WebArchivingCDP_DOMDiff.md)`

### 4.3. Filesystem Monitoring

Ingests new or updated user files from configured directories.
*   **Architectural Approach:**
    *   **Platform-Specific Watchers:** `inotify` on Linux, FSEvents on macOS. `notify-rust` crate as cross-platform abstraction. Handles recursive watching and overflow recovery (for `inotify`).
    *   **Processing Logic:** On file creation/modification (detecting completed writes, e.g., `IN_CLOSE_WRITE`), compute BLAKE3 hash. Integrate with `git-annex` (store content, get `annex_key`). Update `core_blobs` (deduplicating via BLAKE3 hash). Detect renames/moves (using `inotify` cookies or hash/inode correlation). Normalize paths.
*   **Key Data Captured:** File creation, modification, deletion events. File content (via `git-annex`), metadata (path, size, mtime, hashes).
*   **Referenced TIMs:**
    *   `[TIM-FilesystemMonitoringWatchers.md](docs/tims/ingestors/filesystem/TIM-FilesystemMonitoringWatchers.md)`
    *   `[TIM-FilesystemIngestionLogic.md](docs/tims/ingestors/filesystem/TIM-FilesystemIngestionLogic.md)`

### 4.4. Personal Knowledge Management (PKM) (ADR-004)

Manages PKM note content, primarily Markdown.
*   **Architectural Approach (DB-Native with Yjs CRDTs):** As per `[ADR-004-PKMNoteContentManagementAndSync.md](docs/adr/ADR-004-PKMNoteContentManagementAndSync.md)`.
    *   PostgreSQL (`core_artifacts`, `core_artifact_contents`, `core.pkm_note_yjs_deltas`) is canonical store.
    *   Yjs for textual content. Editing workflow via Neovim plugin (`sinnix-nvim`) involves fetching Yjs state/deltas, local Yjs ops, sending update blobs on save. Backend persists deltas, generates Markdown snapshots for `core_artifact_contents`.
    *   Stable heading IDs for persistent linking.
*   **Key Data Captured:** Versioned PKM note content (Yjs deltas, Markdown snapshots), metadata, links.
*   **Referenced TIMs:**
    *   `[TIM-PKMContentCRDT_Yjs.md](docs/tims/ingestors/pkm_email_nvim/TIM-PKMContentCRDT_Yjs.md)`

### 4.5. Email Access

Ingests email content and metadata.
*   **Architectural Approach:**
    *   **Gmail API (Preferred for Gmail):** Uses OAuth2. `gmail.readonly` scope. Fetches message structure, headers, body, attachments. Uses `history.list` for incremental sync.
    *   **IMAP (Broader Compatibility):** Uses standard IMAP commands (`LOGIN`, `SELECT`, `SEARCH`, `FETCH`). Requires MIME parsing.
*   **Storage:** Email text/Markdown in `core_artifact_contents`, attachments as `core_blobs`. Metadata in `core_artifacts`.
*   **Referenced TIMs:**
    *   `[TIM-EmailAccessProtocols.md](docs/tims/ingestors/pkm_email_nvim/TIM-EmailAccessProtocols.md)`

### 4.6. Neovim Plugin Integration

Provides deep Exocortex integration within Neovim.
*   **Architectural Approach (`sinnix-nvim` Lua plugin):**
    *   **Communication with Backend:** Custom Exocortex Language Server (LSP) preferred for rich semantics (link resolution, Yjs sync for PKM). Msgpack-RPC to helper processes or `exo` CLI calls as alternatives.
    *   **Treesitter Integration:** Custom queries for semantic extraction from buffers (links, tags, headings).
*   **Key Data Captured/Interactions:** Editor actions, PKM edits via Yjs, context for Exocortex commands, logs meta-events.
*   **Referenced TIMs:**
    *   `[TIM-NeovimPluginIntegration.md](docs/tims/ingestors/pkm_email_nvim/TIM-NeovimPluginIntegration.md)`


