# Deep Analysis: Desktop Satellite (Clipboard & Window Manager)

**Analysis Date:** 2025-11-17
**Focus:** Clipboard monitoring, window manager integration, desktop event capture
**Lines Analyzed:** 2,345 (clipboard: 790, window_manager: 648, unified_processor: 907)

---

## 📋 Desktop Satellite Architecture

### Design Philosophy: Source Material First

**Core Concept:**
```
Desktop Activity (clipboard/windows)
      ↓
Direct SQL to source_material_registry
      ↓
No immediate event emission
      ↓
Automata process source materials later
```

**Benefits:**
- ✅ Separation of capture from processing
- ✅ Complete audit trail
- ✅ Reprocessable source materials
- ✅ No event loss if processing fails

**Components:**
1. **ClipboardWatcher** - Polling-based clipboard monitoring
2. **WindowManagerWatcher** - Event-driven window manager integration (Hyprland only)
3. **DesktopProcessor** - Unified processor (mostly stub)

---

## 📋 Clipboard Monitoring Deep Dive

### Architecture Overview

```rust
pub struct ClipboardWatcher {
    poll_interval: Duration,                           // Default: 2 seconds
    last_content: Option<ClipboardContent>,            // Main clipboard state
    last_primary_content: Option<ClipboardContent>,    // Primary selection (Linux)
    clipboard_history: VecDeque<ClipboardHistoryEntry>, // Max 1000 entries
    max_preview_length: usize,                         // 100 chars
    max_content_size: usize,                           // 10MB
    blob_manager: Option<Arc<BlobManager>>,            // For large content
    db_pool: Option<PgPool>,                           // Direct DB access
}
```

### Clipboard Capture Strategy

**Multi-Tier Access:**

```rust
async fn get_clipboard_content(&self) -> Option<ClipboardContent> {
    // 1. Try external tools first (best compatibility)
    self.get_clipboard_content_external("clipboard")
        .await
    // 2. Fallback to copypasta library
        .or_else(|| self.get_clipboard_content_fallback())
}
```

**External Tool Chain:**

```rust
// Wayland (preferred)
wl-paste --no-newline

// X11 (fallback)
xclip -o -selection clipboard

// Library (last resort)
copypasta::ClipboardContext
```

**Analysis:**
- ✅ **EXCELLENT:** Multi-tier fallback ensures compatibility
- ✅ Tries native tools first (wl-paste, xclip) before library
- ✅ Handles both Wayland and X11
- ⚠️ **ISSUE:** No timeout on external commands (could hang)
- ⚠️ **ISSUE:** Command output not sanitized (potential injection)

### Content Type Detection

**Algorithm:**

```rust
fn analyze_content(&self, content: &str) -> (String, Option<String>, Option<Vec<String>>) {
    // 1. File paths/URIs
    if content.starts_with("file://")
        || (content.lines().all(|l| l.starts_with('/') || l.is_empty())) {
        ("files", None, extract_file_paths(content))
    }
    // 2. Images (heuristic: long + all ASCII graphic chars)
    else if content.len() > 100 && content.chars().all(|c| c.is_ascii_graphic()) {
        ("image", None, None)
    }
    // 3. URLs
    else if content.starts_with("http://") || content.starts_with("https://") {
        let preview = content.chars().take(max_preview_length).collect();
        ("url", Some(preview), None)
    }
    // 4. Default: text
    else {
        let preview = content.chars().take(max_preview_length).collect();
        ("text", Some(preview), None)
    }
}
```

**Analysis:**
- ✅ Simple, clear heuristics
- ✅ Preview generation for large text
- ⚠️ **ISSUE:** Image detection is primitive (only ASCII check)
- ⚠️ **ISSUE:** No binary data detection (could try to store binary as text)
- ⚠️ **ISSUE:** Base64-encoded images not detected
- 💡 **INSIGHT:** File path detection handles multi-line file lists

### BLAKE3 Deduplication

**Implementation:**

```rust
fn calculate_hash(&self, content: &str) -> String {
    blake3::hash(content.as_bytes()).to_hex().to_string()
}

fn update_history(&mut self, content_hash: String, content_type: String) {
    if let Some(entry) = self.clipboard_history.iter_mut().find(|e| e.content_hash == content_hash) {
        // Already seen - update timestamp and count
        entry.last_seen = now;
        entry.copy_count += 1;
    } else {
        // New content - add to history
        self.clipboard_history.push_back(ClipboardHistoryEntry { ... });

        // Trim if exceeded max
        if self.clipboard_history.len() > self.max_history_entries {
            self.clipboard_history.pop_front();
        }
    }
}
```

**Analysis:**
- ✅ **EXCELLENT:** BLAKE3 is 10-15× faster than SHA256
- ✅ Tracks copy count (usage frequency)
- ✅ Bounded history (max 1000 entries prevents memory bloat)
- ✅ `find_original_hash` enables provenance tracking
- ⚠️ **ISSUE:** History is in-memory only (lost on restart)
- ⚠️ **ISSUE:** No cross-session deduplication

### Large Content Handling

**Strategy:**

```rust
async fn store_clipboard_source_material(&self, content: &ClipboardContent, selection_type: &str) -> Result<Option<Ulid>> {
    let storage = if data_bytes.len() <= self.max_content_size {
        ClipboardStorage::Inline(content.size_bytes)  // ≤10MB: store inline
    } else if let Some(reference) = self.ingest_large_clipboard_content(content).await? {
        ClipboardStorage::Annex(reference)  // >10MB: git-annex
    } else {
        warn!("Large clipboard content ({:?} bytes) skipped", content.size_bytes);
        return Ok(None);  // No blob manager available
    };
}

async fn ingest_large_clipboard_content(&self, content: &ClipboardContent) -> Result<Option<AnnexBlobReference>> {
    let Some(manager) = &self.blob_manager else {
        return Ok(None);  // Graceful degradation
    };

    let filename = format!("clipboard-{}-{}.txt", timestamp, &hash[..8]);
    let blob = manager.ingest_from_bytes(content.text.as_bytes(), &filename, mime_type).await?;

    Ok(Some(AnnexBlobReference { blob_id, annex_key, ... }))
}
```

**Analysis:**
- ✅ **EXCELLENT:** 10MB threshold is reasonable
- ✅ Graceful degradation if blob manager unavailable
- ✅ Content-addressed storage via git-annex
- ✅ BLAKE3 deduplication at blob level
- ⚠️ **ISSUE:** All clipboard content stored as `.txt` (even images!)
- ⚠️ **ISSUE:** No chunking for >10MB clipboard (single blob)
- 💡 **INSIGHT:** Filename includes timestamp + hash prefix for uniqueness

### Window Context Capture

**Implementation:**

```rust
async fn get_active_window_app(&self) -> Option<String> {
    // Try Hyprland first
    if let Ok(output) = Command::new("hyprctl").args(["activewindow", "-j"]).output().await {
        if let Ok(json) = serde_json::from_slice::<Value>(&output.stdout) {
            return json.get("class").and_then(|v| v.as_str()).map(String::from);
        }
    }

    // Try xdotool for X11
    if let Ok(output) = Command::new("xdotool").args(["getactivewindow", "getwindowclassname"]).output().await {
        return Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }

    None
}

async fn get_active_window_title(&self) -> Option<String> {
    // Similar implementation for window title
}
```

**Analysis:**
- ✅ **EXCELLENT:** Captures context (which app user copied from)
- ✅ Supports both Wayland (Hyprland) and X11 (xdotool)
- ✅ Graceful fallback (returns None if unavailable)
- ✅ Enriches clipboard events with provenance
- ⚠️ **ISSUE:** No timeout on external commands
- ⚠️ **ISSUE:** Commands run on every clipboard poll (overhead)
- 💡 **RECOMMENDATION:** Cache window info, only update on focus change

### Primary Selection Support (Linux)

**Implementation:**

```rust
async fn get_primary_selection_content(&self) -> Option<ClipboardContent> {
    if !self.enable_primary_selection {
        return None;
    }

    let text = self.get_clipboard_content_external("primary").await?;
    // ... same processing as main clipboard
}

async fn check_clipboard_changes(&mut self) -> Result<()> {
    self.check_main_clipboard().await?;
    if self.enable_primary_selection {
        self.check_primary_selection().await?;
    }
    Ok(())
}
```

**Analysis:**
- ✅ **EXCELLENT:** Linux primary selection support (text selection = copy)
- ✅ Separate tracking for clipboard vs primary
- ✅ Configurable (can disable if unwanted)
- ⚠️ **ISSUE:** Doubles polling overhead
- 💡 **INSIGHT:** Primary selection captures highlighted text automatically

### Polling Loop

**Implementation:**

```rust
pub async fn start_monitoring(&mut self) -> Result<()> {
    info!("Starting clipboard monitoring (sensd mode)");

    let mut poll_interval = interval(self.poll_interval);  // Default: 2 seconds

    loop {
        poll_interval.tick().await;

        if let Err(e) = self.check_clipboard_changes().await {
            error!("Error checking clipboard changes: {}", e);
            // Continue polling even if there's an error
        }
    }
}
```

**Analysis:**
- ✅ Simple, reliable polling loop
- ✅ Continues polling on errors (resilient)
- ✅ Uses `tokio::time::interval` for consistent timing
- ⚠️ **ISSUE:** 2-second default = up to 2s capture latency
- ⚠️ **ISSUE:** No exponential backoff on repeated errors
- ⚠️ **ISSUE:** No metrics (can't monitor polling health)
- 💡 **RECOMMENDATION:** Reduce to 500ms-1s for better responsiveness

---

## 🪟 Window Manager Integration Deep Dive

### Architecture Overview

```rust
pub struct WindowManagerWatcher {
    wm_type: WindowManagerType,              // Only Hyprland currently
    socket_path: Option<String>,             // Event socket (.socket2.sock)
    command_socket_path: Option<String>,     // Command socket (.socket.sock)
    windows: HashMap<String, WindowInfo>,    // All tracked windows
    workspaces: HashMap<String, WorkspaceInfo>, // All workspaces
    current_focused_window: Option<String>,
    current_workspace: Option<String>,
    current_monitor: Option<String>,
    db_pool: Option<PgPool>,                // Direct DB access
}
```

### Socket Discovery

**Hyprland Socket Discovery:**

```rust
async fn discover_hyprland_sockets(&mut self) -> Result<()> {
    let hyprland_instance_sig = std::env::var("HYPRLAND_INSTANCE_SIGNATURE")
        .map_err(|_| Error::Processing("HYPRLAND_INSTANCE_SIGNATURE not set"))?;

    let xdg_runtime = std::env::var("XDG_RUNTIME_DIR")
        .map_err(|_| Error::Processing("XDG_RUNTIME_DIR not set"))?;

    let base_path = format!("{}/hypr/{}", xdg_runtime, hyprland_instance_sig);
    let event_socket = format!("{}.socket2.sock", base_path);
    let command_socket = format!("{}.socket.sock", base_path);

    // Test event socket connection
    if UnixStream::connect(&event_socket).await.is_ok() {
        self.socket_path = Some(event_socket.clone());
    } else {
        return Err(Error::Processing(format!("Cannot connect to event socket: {}", event_socket)));
    }

    // Test command socket (optional)
    if UnixStream::connect(&command_socket).await.is_ok() {
        self.command_socket_path = Some(command_socket);
    }

    Ok(())
}
```

**Analysis:**
- ✅ **EXCELLENT:** Environment variable-based discovery (no hardcoded paths)
- ✅ Tests socket connectivity before proceeding
- ✅ Command socket is optional (degrades gracefully)
- ✅ Clear error messages if Hyprland not running
- ⚠️ **ISSUE:** Hyprland-specific (no X11/GNOME/KDE support)
- 💡 **INSIGHT:** Uses `.socket2.sock` for events, `.socket.sock` for commands

### Event-Driven Monitoring

**Event Stream Protocol:**

```rust
async fn stream_hyprland_events(&mut self) -> Result<()> {
    let mut consecutive_failures = 0;
    let mut reconnect_backoff = Self::hyprland_backoff();

    loop {
        match self.connect_to_hyprland_events().await {
            Ok(stream) => {
                consecutive_failures = 0;  // Reset on success
                reconnect_backoff = Self::hyprland_backoff();

                let reader = BufReader::new(stream);
                let mut lines = reader.lines();

                loop {
                    tokio::select! {
                        // Read events from socket
                        line_result = lines.next_line() => {
                            match line_result {
                                Ok(Some(line)) => {
                                    self.process_hyprland_event(&line).await?;
                                }
                                Ok(None) => {
                                    warn!("Event stream ended");
                                    break;
                                }
                                Err(e) => {
                                    error!("Error reading socket: {}", e);
                                    break;
                                }
                            }
                        }

                        // Periodic state snapshot
                        _ = sleep(Duration::from_secs(300)) => {
                            self.capture_state_snapshot().await?;
                        }
                    }
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                error!("Failed to connect (attempt {}): {}", consecutive_failures, e);

                let delay = Self::next_backoff(&mut reconnect_backoff);
                warn!("Reconnecting in {:?}...", delay);
                sleep(delay).await;
            }
        }

        consecutive_failures += 1;
        let delay = Self::next_backoff(&mut reconnect_backoff);
        sleep(delay).await;
    }
}
```

**Analysis:**
- ✅ **EXCELLENT:** Event-driven (real-time, no polling)
- ✅ **EXCELLENT:** Exponential backoff reconnection
- ✅ **EXCELLENT:** Automatic reconnection on connection loss
- ✅ Periodic state snapshots every 5 minutes
- ✅ `tokio::select!` for concurrent event handling + snapshots
- ⚠️ **ISSUE:** No timeout on `lines.next_line()` (could hang indefinitely)
- ⚠️ **ISSUE:** State snapshots every 300s could be expensive
- 💡 **INSIGHT:** Resets backoff on successful connection

### Exponential Backoff Strategy

**Implementation:**

```rust
const HYPRLAND_INITIAL_BACKOFF_MS: u64 = 500;
const HYPRLAND_MAX_BACKOFF: Duration = Duration::from_secs(60);

fn hyprland_backoff() -> BackoffStrategy {
    Box::new(
        ExponentialBackoff::from_millis(HYPRLAND_INITIAL_BACKOFF_MS)
            .factor(2)
            .max_delay(HYPRLAND_MAX_BACKOFF)
            .map(jitter)  // Only in non-test builds
    )
}

fn next_backoff(backoff: &mut BackoffStrategy) -> Duration {
    backoff.next().unwrap_or(HYPRLAND_MAX_BACKOFF)
}
```

**Backoff Sequence:**
```
500ms → 1s → 2s → 4s → 8s → 16s → 32s → 60s (cap)
+ jitter for thundering herd prevention
```

**Analysis:**
- ✅ **EXCELLENT:** Well-tuned backoff parameters
- ✅ Jitter prevents thundering herd on mass restarts
- ✅ 60-second cap prevents excessive delays
- ✅ Comprehensive unit tests for backoff behavior
- 💡 **INSIGHT:** Disables jitter in tests for deterministic behavior

### Event Parsing & Handling

**Event Format:**

```
Hyprland event format: "EVENT_TYPE>>DATA"

Examples:
focusedwindow>>firefox,Mozilla Firefox
openwindow>>0x12345,1,firefox,Mozilla Firefox - Home
closewindow>>0x12345
movewindow>>0x12345,2
workspace>>2
focusedmon>>DP-1,2
```

**Parsing Logic:**

```rust
async fn process_hyprland_event(&mut self, line: &str) -> Result<()> {
    if line.is_empty() {
        return Ok(());
    }

    if let Some((event_type, event_data)) = line.split_once(">>") {
        match event_type {
            "focusedwindow" => self.handle_window_focused(event_data).await?,
            "openwindow" => self.handle_window_opened(event_data).await?,
            "closewindow" => self.handle_window_closed(event_data).await?,
            "movewindow" => self.handle_window_moved(event_data).await?,
            "workspace" => self.handle_workspace_changed(event_data).await?,
            "focusedmon" => self.handle_monitor_focused(event_data).await?,
            _ => {
                debug!("Unhandled event: {}", event_type);
                self.store_window_manager_source_material(event_type, event_data, json!({"unhandled": true})).await?;
            }
        }
    }

    Ok(())
}
```

**Analysis:**
- ✅ Simple, clear parsing logic
- ✅ Handles 6 major event types
- ✅ **EXCELLENT:** Unhandled events still stored (no data loss!)
- ⚠️ **ISSUE:** No validation of event_data format
- ⚠️ **ISSUE:** Malformed events cause silent failures
- 💡 **RECOMMENDATION:** Add event validation and metrics

### Window Focused Event

**Implementation:**

```rust
async fn handle_window_focused(&mut self, data: &str) -> Result<()> {
    // Format: "class,title"
    if let Some((class, title)) = data.split_once(',') {
        let window_address = format!("0x{:x}", data.len());  // ❌ PLACEHOLDER!

        let metadata = json!({
            "window_class": class,
            "window_title": title,
            "window_id": window_address,
            "previous_window_id": self.current_focused_window,
        });

        self.store_window_manager_source_material("focusedwindow", data, metadata).await?;
        self.current_focused_window = Some(window_address);
    }

    Ok(())
}
```

**Analysis:**
- ✅ Captures window class and title
- ✅ Tracks previous focused window (provenance)
- ❌ **CRITICAL ISSUE:** `format!("0x{:x}", data.len())` is completely wrong!
  - Uses data length as window address
  - Same-length class+title = same "address"
  - Not a real window address at all
- 💡 **RECOMMENDATION:** Remove window_address or parse from actual Hyprland data

### Window Opened Event

**Implementation:**

```rust
async fn handle_window_opened(&mut self, data: &str) -> Result<()> {
    // Format: "address,workspace,class,title"
    let parts: Vec<&str> = data.split(',').collect();
    if parts.len() >= 4 {
        let window_address = parts[0].to_string();
        let workspace_id = parts[1].to_string();
        let window_class = parts[2].to_string();
        let window_title = parts[3..].join(",");  // Title might contain commas

        // Store in HashMap for tracking
        self.windows.insert(window_address.clone(), WindowInfo {
            address: window_address,
            class: window_class,
            title: window_title,
            workspace_id,
            last_seen: SystemTime::now(),
            floating: false,
            fullscreen: false,
        });
    }

    Ok(())
}
```

**Analysis:**
- ✅ **EXCELLENT:** Handles commas in window title
- ✅ Tracks window state in HashMap
- ✅ Records last_seen timestamp
- ⚠️ **ISSUE:** `floating` and `fullscreen` hardcoded to false (not from event)
- ⚠️ **ISSUE:** No window limit (unbounded HashMap growth)
- 💡 **RECOMMENDATION:** Prune windows not seen in 24+ hours

### Periodic State Snapshots

**Implementation:**

```rust
async fn capture_state_snapshot(&mut self) -> Result<()> {
    debug!("Capturing window manager state snapshot");

    let snapshot_data = json!({
        "windows": self.windows.values().collect::<Vec<_>>(),
        "workspaces": self.workspaces.values().collect::<Vec<_>>(),
        "current_workspace": self.current_workspace,
        "current_monitor": self.current_monitor,
        "current_focused_window": self.current_focused_window,
    });

    self.store_window_manager_source_material(
        "state_snapshot",
        &snapshot_data.to_string(),
        json!({"snapshot": true}),
    ).await?;

    Ok(())
}
```

**Analysis:**
- ✅ Periodic snapshots for state reconstruction
- ✅ Captures all tracked windows and workspaces
- ⚠️ **ISSUE:** 300-second interval is expensive
- ⚠️ **ISSUE:** Full state dump on every snapshot (not incremental)
- ⚠️ **ISSUE:** No state persistence (lost on restart)
- 💡 **RECOMMENDATION:** Incremental state updates + initial snapshot only

---

## 🔍 Critical Issues Found

### Issue 1: Clipboard Polling Latency (MEDIUM)

**File:** `crate/satellites/sinex-desktop-satellite/src/clipboard.rs:116`

**Issue:**
```rust
self.poll_interval = Duration::from_secs(poll_interval_secs);  // Default: 2 seconds
```

**Problem:**
- 2-second polling interval = up to 2s capture latency
- User copies text → waits 0-2s before captured
- Poor UX for fast workflows

**Impact:**
- Clipboard changes not captured immediately
- Time-sensitive copies might be overwritten before captured
- Users perceive system as "laggy"

**Recommendation:**
```rust
const DEFAULT_CLIPBOARD_POLL_INTERVAL_MS: u64 = 500;  // 500ms = better responsiveness

// Or: Event-driven clipboard monitoring
// Linux: Monitor X11 clipboard selection events
// Wayland: wl-clipboard-history integration
```

### Issue 2: No Timeout on External Commands (HIGH)

**File:** `crate/satellites/sinex-desktop-satellite/src/clipboard.rs:510-518`

**Issue:**
```rust
let wl_result = Command::new("wl-paste")
    .arg("--no-newline")
    .output()
    .await;  // No timeout!
```

**Problem:**
- `wl-paste` or `xclip` could hang indefinitely
- No timeout on command execution
- Clipboard polling blocked until command completes

**Scenario:**
```
1. Clipboard contains large binary data
2. wl-paste tries to read it, hangs
3. Entire clipboard monitoring blocked
4. No further clipboard events captured
```

**Impact:**
- Single hang blocks all clipboard monitoring
- No recovery mechanism
- Manual restart required

**Recommendation:**
```rust
use tokio::time::timeout;

let wl_result = timeout(Duration::from_secs(5),
    Command::new("wl-paste")
        .arg("--no-newline")
        .output()
).await;

match wl_result {
    Ok(Ok(output)) => { /* process */ }
    Ok(Err(e)) => warn!("Command failed: {}", e),
    Err(_) => warn!("Command timed out after 5s"),
}
```

### Issue 3: Window Address Placeholder Bug (CRITICAL)

**File:** `crate/satellites/sinex-desktop-satellite/src/window_manager.rs:350`

**Issue:**
```rust
let window_address = format!("0x{:x}", data.len());  // ❌ COMPLETELY WRONG!
```

**Problem:**
- Uses string length as window address
- Not a real window address
- Collisions: Same-length strings → same "address"
- Breaks window tracking

**Example:**
```
"firefox,Home" (12 chars) → 0xc
"terminal,Vim" (12 chars) → 0xc  ← COLLISION!
```

**Impact:**
- Window identity lost
- Cannot correlate focus changes with window opens/closes
- Provenance tracking broken

**Recommendation:**
```rust
// Option 1: Remove window_address (not in focusedwindow event)
let metadata = json!({
    "window_class": class,
    "window_title": title,
    "previous_window": self.current_focused_window,
});

// Option 2: Query actual window address via command socket
// hyprctl activewindow -j → parse actual address
```

### Issue 4: No Clipboard History Persistence (MEDIUM)

**File:** `crate/satellites/sinex-desktop-satellite/src/clipboard.rs:48`

**Issue:**
```rust
clipboard_history: VecDeque<ClipboardHistoryEntry>,  // In-memory only!
```

**Problem:**
- History lost on restart
- No cross-session deduplication
- Cannot track "first seen" across restarts

**Impact:**
- Same content copied after restart = treated as new
- Loses "original_hash" provenance
- Cannot answer "when did I first copy this?"

**Recommendation:**
```rust
// Persist history to SQLite or source_material_registry
async fn load_clipboard_history(&mut self) -> Result<()> {
    let entries = sqlx::query!(
        "SELECT content_hash, first_seen, last_seen, content_type, copy_count
         FROM clipboard_history
         ORDER BY last_seen DESC
         LIMIT 1000"
    )
    .fetch_all(self.db_pool?)
    .await?;

    for entry in entries {
        self.clipboard_history.push_back(entry.into());
    }

    Ok(())
}
```

### Issue 5: No Clipboard Content Validation (MEDIUM)

**File:** `crate/satellites/sinex-desktop-satellite/src/clipboard.rs:466-498`

**Issue:**
```rust
let text = self.get_clipboard_content_external("clipboard").await
    .or_else(|| self.get_clipboard_content_fallback());

if let Some(text) = text {
    // No validation of text content!
    let hash = self.calculate_hash(&text);
}
```

**Problem:**
- No UTF-8 validation
- Binary data processed as text
- Null bytes could corrupt database
- No size check before reading

**Impact:**
- Database insertion failures on binary data
- Invalid UTF-8 sequences
- Potential data corruption

**Recommendation:**
```rust
fn validate_clipboard_content(text: &str) -> Result<(), ClipboardError> {
    // 1. Check size before processing
    if text.len() > MAX_CLIPBOARD_SIZE {
        return Err(ClipboardError::TooLarge(text.len()));
    }

    // 2. Validate UTF-8 (already done by String, but check for null bytes)
    if text.contains('\0') {
        return Err(ClipboardError::ContainsNullBytes);
    }

    // 3. Check for binary data
    if text.chars().filter(|&c| c.is_control() && c != '\n' && c != '\t').count() > 10 {
        return Err(ClipboardError::LikelyBinary);
    }

    Ok(())
}
```

### Issue 6: Single Window Manager Support (MEDIUM)

**File:** `crate/satellites/sinex-desktop-satellite/src/window_manager.rs:16-19`

**Issue:**
```rust
pub enum WindowManagerType {
    Hyprland,  // Only one variant!
}
```

**Problem:**
- Only Hyprland supported
- No X11/GNOME/KDE/i3/Sway support
- Unusable for most Linux users
- Desktop satellite non-functional without Hyprland

**Impact:**
- Limited user base
- Cannot capture window events on non-Hyprland systems
- Users on GNOME/KDE get no window context in clipboard events

**Recommendation:**
```rust
pub enum WindowManagerType {
    Hyprland,   // Wayland compositor
    Sway,       // Wayland compositor (i3 for Wayland)
    I3,         // X11 window manager
    Gnome,      // Via D-Bus org.gnome.Shell
    Kde,        // Via D-Bus org.kde.KWin
    Xorg,       // Generic X11 via _NET_WM properties
}

// Implement adapters for each WM type
trait WindowManagerAdapter {
    async fn connect(&self) -> Result<Connection>;
    async fn stream_events(&self) -> Result<EventStream>;
}
```

### Issue 7: No Unix Socket Read Timeout (HIGH)

**File:** `crate/satellites/sinex-desktop-satellite/src/window_manager.rs:524`

**Issue:**
```rust
line_result = lines.next_line() => {
    // No timeout! Could block indefinitely
    match line_result { ... }
}
```

**Problem:**
- `next_line()` can block indefinitely
- If Hyprland hangs, watcher hangs
- No heartbeat mechanism
- No deadlock detection

**Impact:**
- Window manager monitoring silently stops
- No events captured
- No error indication
- Manual restart required

**Recommendation:**
```rust
tokio::select! {
    line_result = lines.next_line() => { ... }

    // Add timeout/heartbeat
    _ = sleep(Duration::from_secs(30)) => {
        warn!("No events received in 30s, reconnecting");
        break;  // Trigger reconnection
    }
}
```

### Issue 8: Unbounded Window State Growth (MEDIUM)

**File:** `crate/satellites/sinex-desktop-satellite/src/window_manager.rs:81-82`

**Issue:**
```rust
windows: HashMap<String, WindowInfo>,      // No size limit!
workspaces: HashMap<String, WorkspaceInfo>, // No size limit!
```

**Problem:**
- Windows added on `openwindow` events
- Windows removed on `closewindow` events
- But if closewindow missed: window tracked forever
- Long-running sessions accumulate thousands of entries

**Scenario:**
```
System runs for 30 days
User opens/closes 1000 windows per day
Missed 1% of closewindow events
= 300 leaked window entries
= ~50KB memory + JSON serialization overhead
```

**Impact:**
- Memory leak (slow)
- Periodic snapshot serialization gets expensive
- HashMap lookup performance degrades

**Recommendation:**
```rust
const MAX_TRACKED_WINDOWS: usize = 100;
const WINDOW_TTL: Duration = Duration::from_secs(24 * 3600);  // 24 hours

fn prune_stale_windows(&mut self) {
    let cutoff = SystemTime::now() - WINDOW_TTL;
    self.windows.retain(|_addr, window| window.last_seen >= cutoff);

    // Also enforce hard cap
    if self.windows.len() > MAX_TRACKED_WINDOWS {
        // Remove oldest windows
        let mut windows: Vec<_> = self.windows.iter().collect();
        windows.sort_by_key(|(_, w)| w.last_seen);
        for (addr, _) in windows.iter().take(self.windows.len() - MAX_TRACKED_WINDOWS) {
            self.windows.remove(*addr);
        }
    }
}
```

### Issue 9: Expensive Periodic State Snapshots (LOW)

**File:** `crate/satellites/sinex-desktop-satellite/src/window_manager.rs:543`

**Issue:**
```rust
_ = sleep(Duration::from_secs(300)) => {
    // Full state dump every 5 minutes!
    self.capture_state_snapshot().await?;
}
```

**Problem:**
- Full state serialization every 300 seconds
- All windows + workspaces serialized to JSON
- Database insertion overhead
- Not incremental (duplicates data)

**Impact:**
- Unnecessary database writes
- JSON serialization CPU cost
- Storage bloat (90% duplicate data)

**Recommendation:**
```rust
// Option 1: Longer interval (30 minutes)
_ = sleep(Duration::from_secs(1800)) => { ... }

// Option 2: Incremental snapshots
// Only snapshot on significant state changes:
// - 10+ windows opened/closed since last snapshot
// - 5+ workspace changes since last snapshot

// Option 3: Remove entirely
// Individual events already provide full state
// Snapshot only needed on startup for initial state
```

### Issue 10: Missing Metrics (LOW)

**Severity:** LOW
**Impact:** No observability into desktop satellite health

**Missing Metrics:**

**Clipboard:**
- `desktop_clipboard_polls_total` - Poll attempts
- `desktop_clipboard_changes_total{selection="clipboard|primary"}` - Content changes
- `desktop_clipboard_size_bytes{content_type}` - Content sizes
- `desktop_clipboard_dedup_hits_total` - Deduplication hits
- `desktop_clipboard_tool_failures_total{tool="wl-paste|xclip|copypasta"}` - Tool failures
- `desktop_clipboard_annex_ingestions_total` - Large content ingestions

**Window Manager:**
- `desktop_wm_events_total{event_type}` - Event counts by type
- `desktop_wm_connection_failures_total` - Connection failures
- `desktop_wm_reconnect_duration_seconds` - Reconnection backoff duration
- `desktop_wm_tracked_windows` - Current window count
- `desktop_wm_snapshot_duration_seconds` - State snapshot duration

**Recommendation:**
Add comprehensive metrics using `metrics` crate.

---

## ✅ Architectural Strengths

### 1. BLAKE3 Clipboard Deduplication (⭐⭐⭐⭐⭐)
- 10-15× faster than SHA256
- Copy count tracking
- Provenance via original_hash
- Bounded history (1000 entries)

### 2. Multi-Tier Clipboard Access (⭐⭐⭐⭐⭐)
- External tools first (best compatibility)
- Wayland (wl-paste) + X11 (xclip)
- Library fallback (copypasta)
- Graceful degradation

### 3. Exponential Backoff Reconnection (⭐⭐⭐⭐⭐)
- Well-tuned parameters (500ms → 60s)
- Jitter prevents thundering herd
- Resets on successful connection
- Comprehensive unit tests

### 4. Event-Driven Window Monitoring (⭐⭐⭐⭐⭐)
- Real-time (no polling!)
- Automatic reconnection
- Handles connection loss gracefully
- Uses Unix domain sockets

### 5. Window Context Enrichment (⭐⭐⭐⭐)
- Captures active window app + title
- Clipboard events include provenance
- Supports Wayland (hyprctl) + X11 (xdotool)
- Graceful fallback if unavailable

### 6. Primary Selection Support (⭐⭐⭐⭐)
- Linux primary selection (text selection = copy)
- Separate tracking from main clipboard
- Configurable (can disable)
- Valuable for Linux workflows

### 7. Large Content via git-annex (⭐⭐⭐⭐)
- 10MB threshold
- BLAKE3 deduplication at blob level
- Content-addressed storage
- Graceful degradation if unavailable

### 8. Source Material First (⭐⭐⭐⭐⭐)
- Direct SQL to source_material_registry
- Complete audit trail
- Reprocessable
- Separation of capture from processing

---

## ⚠️ Weaknesses & Recommendations

### Immediate (High Priority)

1. **Add timeout to external commands** (< 1 day)
   - wl-paste, xclip, hyprctl, xdotool
   - 5-second timeout sufficient

2. **Fix window address placeholder bug** (< 2 hours)
   - Remove fake window_address
   - Use class+title for identity

3. **Add Unix socket read timeout** (< 1 day)
   - 30-second heartbeat
   - Automatic reconnection on timeout

### Short Term (Medium Priority)

4. **Reduce clipboard polling interval** (< 2 hours)
   - 2s → 500ms for better responsiveness
   - Or implement event-driven monitoring

5. **Add clipboard history persistence** (< 4 hours)
   - SQLite or source_material_registry
   - Cross-session deduplication

6. **Add content validation** (< 2 hours)
   - UTF-8 validation
   - Binary data detection
   - Size limits enforced

7. **Prune window state periodically** (< 2 hours)
   - 24-hour TTL
   - Max 100 tracked windows

### Long Term (Nice to Have)

8. **Extend window manager support** (< 2 weeks)
   - Sway, i3, GNOME, KDE
   - Adapter pattern
   - Auto-detection

9. **Add comprehensive metrics** (< 4 hours)
   - Clipboard + window manager observability
   - Error rates, latencies, counts

10. **Optimize state snapshots** (< 4 hours)
    - Incremental updates
    - Longer interval (30 minutes)
    - Or remove entirely

---

## 📊 Desktop Satellite Summary

**Code Quality:** ⭐⭐⭐⭐ (4/5)
- Well-structured, clear code
- Good separation of concerns
- Some critical bugs (window address)

**Architecture:** ⭐⭐⭐⭐ (4/5)
- Source material first pattern
- Event-driven window monitoring
- Polling-based clipboard (acceptable)

**Security:** ⭐⭐⭐ (3/5)
- External command execution needs sanitization
- No timeouts on external commands
- Content validation missing

**Performance:** ⭐⭐⭐⭐ (4/5)
- BLAKE3 deduplication
- Efficient event-driven window monitoring
- Clipboard polling could be more responsive

**Reliability:** ⭐⭐⭐⭐ (4/5)
- Excellent reconnection logic
- Multi-tier fallbacks
- Needs timeout handling

**Platform Support:** ⭐⭐ (2/5)
- Hyprland only (window manager)
- Good Wayland/X11 clipboard support
- Limited user base

**Overall:** ⭐⭐⭐⭐ (4/5)

**Desktop satellite is well-implemented** with excellent BLAKE3 deduplication, event-driven window monitoring, and multi-tier clipboard access. Critical issues: window address placeholder bug, lack of timeouts, and limited window manager support (Hyprland only).

---

**Analysis Completeness:** Comprehensive
**Files Analyzed:** 3 (clipboard: 790 lines, window_manager: 648 lines, unified_processor: 907 lines)
**Issues Found:** 10 critical issues cataloged
**Next:** System satellite analysis (systemd, journald, D-Bus, udev subsystems)
