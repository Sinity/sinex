# Deep Analysis: System Satellite (systemd, journald, D-Bus, udev)

**Analysis Date:** 2025-11-17
**Focus:** System-level monitoring via multiple watchers
**Lines Analyzed:** ~2,500 (4 watchers + unified processor)

---

## 🖥️ System Satellite Architecture

### Design Philosophy: Multi-Watcher System Monitoring

**Core Concept:**
```
System Satellite
    ├─ JournalWatcher (journald logs)
    ├─ SystemdWatcher (unit state monitoring)
    ├─ DbusWatcher (D-Bus signals/methods)
    └─ UdevWatcher (hardware events)
         ↓
All emit to unified event channel
         ↓
Event emitter → NATS JetStream
```

**Benefits:**
- ✅ Comprehensive system-level event capture
- ✅ Each watcher specialized for its domain
- ✅ Independent operation (one failure doesn't stop others)
- ✅ Unified event channel for consistency

**Components:**
1. **JournalWatcher** - systemd journal monitoring with cursor tracking
2. **SystemdWatcher** - systemd unit state change monitoring
3. **DbusWatcher** - D-Bus message bus monitoring (session + system)
4. **UdevWatcher** - udev hardware event monitoring

---

## 📝 Journal Watcher Deep Dive

### Architecture Overview

```rust
pub struct JournalWatcher {
    config: JournalConfig,
    last_cursor: Option<String>,  // Persistence for resume
}

pub struct JournalConfig {
    pub follow: bool,              // Real-time following
    pub import_on_startup: bool,   // Historical import
    pub import_hours: u64,         // How far back to import
    pub batch_size: usize,         // Events per batch (default: 100)
    pub priorities: Vec<u32>,      // Journal priorities to capture
    pub units: Vec<String>,        // Specific units to monitor
    pub include_kernel: bool,      // Include kernel messages
    pub include_user: bool,        // Include user journal
    pub cursor_file: Option<String>, // Cursor persistence path
}
```

### Historical Import Strategy

**Process:**

```rust
async fn import_historical(&mut self, tx: &mpsc::UnboundedSender<Event<JsonValue>>) -> Result<()> {
    let mut args = vec!["--output=json", "--no-pager"];

    // Time-based filtering
    if self.config.import_hours > 0 {
        args.push(format!("--since=-{}h", self.config.import_hours));
    }

    // Resume from last cursor
    if let Some(ref cursor) = self.last_cursor {
        args.push(format!("--after-cursor={}", cursor));
    }

    // Unit filtering
    for unit in &self.config.units {
        args.push(format!("--unit={}", unit));
    }

    // Execute journalctl
    let output = Command::new("journalctl").args(&args).output().await?;

    // Batch processing
    let mut batch = Vec::new();
    for line in output.stdout.split(|&b| b == b'\n') {
        if let Some(event) = self.parse_journal_entry(&entry)? {
            batch.push(event);
            if batch.len() >= self.config.batch_size {
                // Send batch
                for event in batch.drain(..) {
                    Self::send_event(tx, event, "journal_batch").await?;
                }
            }
        }
    }
}
```

**Analysis:**
- ✅ **EXCELLENT:** Cursor-based resume (no duplicate events after restart)
- ✅ Batch processing (100 events/batch reduces channel overhead)
- ✅ Configurable historical depth (import last N hours)
- ✅ Unit filtering (only capture relevant services)
- ✅ Priority filtering (focus on errors/warnings)
- ⚠️ **ISSUE:** Single `journalctl` invocation (no streaming, loads all into memory)
- ⚠️ **ISSUE:** No timeout on historical import (large imports could hang)
- 💡 **INSIGHT:** Cursor saved to file for persistence across restarts

### Real-Time Journal Following

**Implementation:**

```rust
async fn follow_journal_inner(&mut self, tx: &mpsc::UnboundedSender<Event<JsonValue>>) -> Result<()> {
    let mut args = vec!["--output=json", "--no-pager", "--follow"];

    // Resume from cursor
    if let Some(ref cursor) = self.last_cursor {
        let cursor_arg = format!("--after-cursor={}", cursor);
        args.push(&cursor_arg);
    }

    // Unit + priority filters
    // ...

    let mut child = Command::new("journalctl")
        .args(&args)
        .stdout(Stdio::piped())
        .spawn()?;

    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    loop {
        if let Some(line) = lines.next_line().await? {
            if let Some(event) = self.parse_journal_entry(&entry)? {
                if tx.send(event).is_err() {
                    break;  // Channel closed
                }

                // Update cursor
                if let Some(cursor) = extract_cursor(&entry) {
                    self.last_cursor = Some(cursor.clone());
                    self.save_cursor(&cursor).await?;
                }
            }
        }
    }
}
```

**Analysis:**
- ✅ Streaming via `--follow`
- ✅ Cursor updated on every event (crash-safe)
- ✅ Graceful channel closure handling
- ⚠️ **ISSUE:** No timeout on `next_line()` (could hang indefinitely)
- ⚠️ **ISSUE:** No exponential backoff on reconnection
- ⚠️ **ISSUE:** Cursor saved on *every* event (filesystem overhead)
- 💡 **RECOMMENDATION:** Batch cursor saves (every 100 events or 10 seconds)

### Cursor Persistence

**Implementation:**

```rust
async fn save_cursor(&self, cursor: &str) -> Result<()> {
    if let Some(ref cursor_file) = self.config.cursor_file {
        tokio::fs::write(cursor_file, cursor).await?;
    }
    Ok(())
}
```

**Analysis:**
- ✅ Simple, effective persistence
- ⚠️ **ISSUE:** No atomic write (crash during write = corrupted cursor)
- ⚠️ **ISSUE:** Called on every event (unnecessary I/O)
- 💡 **RECOMMENDATION:**
  ```rust
  async fn save_cursor_atomic(&self, cursor: &str) -> Result<()> {
      let temp_file = format!("{}.tmp", cursor_file);
      tokio::fs::write(&temp_file, cursor).await?;
      tokio::fs::rename(&temp_file, cursor_file).await?;  // Atomic
  }
  ```

### Journal Entry Parsing

**Format:**
```json
{
  "__CURSOR": "s=abc123...",
  "MESSAGE": "Service started",
  "_SYSTEMD_UNIT": "nginx.service",
  "_PID": "1234",
  "_UID": "0",
  "__REALTIME_TIMESTAMP": "1700000000000000",
  "PRIORITY": "6"
}
```

**Parsing Logic:**
```rust
fn parse_journal_entry(&self, entry: &serde_json::Value) -> Option<Event<JsonValue>> {
    let message = entry["MESSAGE"].as_str()?;
    let cursor = entry["__CURSOR"].as_str()?;
    let unit = entry["_SYSTEMD_UNIT"].as_str();
    let priority = entry["PRIORITY"].as_str();

    Some(Event::new(
        JournalEntryWrittenPayload {
            message: message.to_string(),
            cursor: cursor.to_string(),
            unit: unit.map(String::from),
            priority: priority.and_then(|p| p.parse().ok()),
            // ... more fields
        },
        provenance
    ))
}
```

**Analysis:**
- ✅ Structured JSON parsing
- ✅ Extracts key metadata (cursor, unit, priority, PID)
- ⚠️ **ISSUE:** No validation of entry structure (malformed entries silently skipped)
- 💡 **INSIGHT:** Uses synthesis provenance (bootstrap event ID)

---

## 🔧 Systemd Watcher Deep Dive

### Architecture Overview

```rust
pub struct SystemdWatcher {
    config: SystemdConfig,
}

pub struct SystemdConfig {
    pub monitor_services: bool,     // Monitor .service units
    pub monitor_timers: bool,       // Monitor .timer units
    pub monitor_all_units: bool,    // Monitor all unit types
    pub monitor_timeout_secs: u64,  // Timeout for systemctl commands
}
```

### Unit Status Monitoring

**Strategy: Dual-Path Monitoring**

1. **Periodic Status Checks** (via `systemctl status`)
2. **Journal Monitoring** (via `journalctl --follow`)

**Status Check Implementation:**

```rust
async fn get_unit_status(&self, tx: &mpsc::UnboundedSender<Event<JsonValue>>) -> Result<()> {
    let mut args = vec!["status"];

    // Filter by unit type
    if self.config.monitor_services && !self.config.monitor_all_units {
        args.push("--type=service");
    } else if self.config.monitor_timers && !self.config.monitor_all_units {
        args.push("--type=timer");
    }

    args.extend_from_slice(&["--no-pager", "--full", "--lines=0"]);

    let mut child = Command::new("systemctl")
        .args(&args)
        .stdout(Stdio::piped())
        .spawn()?;

    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    while let Ok(Ok(Some(line))) = timeout(
        Duration::from_secs(self.config.monitor_timeout_secs),
        lines.next_line(),
    ).await {
        if let Some(event) = self.parse_unit_status(&line) {
            tx.send(event)?;
        }
    }

    child.kill().await?;  // Ensure cleanup
}
```

**Analysis:**
- ✅ **EXCELLENT:** Timeout on each line read (prevents hangs)
- ✅ Explicit child process cleanup
- ✅ Configurable unit type filtering
- ⚠️ **ISSUE:** Runs `systemctl status` without specific units (lists ALL)
- ⚠️ **ISSUE:** Duplicate with journal monitoring (both track same events)
- 💡 **INSIGHT:** `--lines=0` prevents output bloat (only status, no logs)

### Status Line Parsing

**Format:**
```
● nginx.service - A high performance web server
  Loaded: loaded (/lib/systemd/system/nginx.service; enabled)
  Active: active (running) since Mon 2025-01-01 10:00:00 UTC; 1h ago
  Process: 1234 ExecStart=/usr/sbin/nginx (code=exited, status=0/SUCCESS)
```

**Parsing Logic:**

```rust
fn parse_unit_status(&self, line: &str) -> Option<Event<JsonValue>> {
    // Parse unit header line
    if line.starts_with("● ") {
        let parts: Vec<&str> = line[2..].splitn(2, " - ").collect();
        let unit_name = parts[0].trim();
        let description = parts[1].trim();
        let unit_type = SystemdUnitType::from_unit_name(unit_name);

        return Some(Event::new(
            SystemdUnitStatusPayload {
                unit_name: unit_name.to_string(),
                unit_type: unit_type.to_string(),
                description: description.to_string(),
                // ...
            },
            provenance
        ));
    }

    // Parse Active: line
    if line.trim().starts_with("Active: ") {
        let status_str = line.trim().strip_prefix("Active: ")?
            .split(' ').next()?;
        let status = SystemdUnitState::from_str(status_str);

        return match status {
            SystemdUnitState::Active => Some(Event::new(SystemdUnitStartedPayload { ... }, provenance)),
            SystemdUnitState::Failed => Some(Event::new(SystemdUnitFailedPayload { ... }, provenance)),
            // ...
        };
    }

    None
}
```

**Analysis:**
- ✅ Clear parsing logic
- ✅ Handles multiple unit states
- ⚠️ **CRITICAL ISSUE:** unit_name = "unknown" for Active: line parsing!
  - Line 200: `unit_name: "unknown".to_string()`
  - Loses correlation between unit header and status
- ⚠️ **ISSUE:** Context not maintained between lines (unit_name lost)
- 💡 **RECOMMENDATION:** Maintain parser state to correlate unit name with status

### Journal Monitoring for Unit Changes

**Implementation:**

```rust
async fn monitor_systemd_journal(&self, tx: mpsc::UnboundedSender<Event<JsonValue>>) -> Result<()> {
    loop {
        let mut child = Command::new("journalctl")
            .args([
                "--follow",
                "--output=json",
                "--lines=0",
                "_SYSTEMD_UNIT=*",  // Only systemd unit messages
                "--no-hostname",
            ])
            .stdout(Stdio::piped())
            .spawn()?;

        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();

        loop {
            match timeout(Duration::from_secs(self.config.monitor_timeout_secs), lines.next_line()).await {
                Ok(Ok(Some(line))) => {
                    if let Some(event) = self.parse_systemd_journal_entry(&line) {
                        tx.send(event)?;
                    }
                }
                Ok(Ok(None)) => {
                    warn!("Journal stream ended");
                    break;  // Reconnect
                }
                Ok(Err(e)) => {
                    error!("Error reading journal: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout - normal, continue
                    continue;
                }
            }
        }

        child.kill().await?;

        // Exponential backoff
        retry_with_exponential_backoff(
            "systemd_journal_restart",
            Duration::from_secs(1),
            5,  // Max 5 retries
            true,
            || async { Ok::<(), &str>(()) }
        ).await?;
    }
}
```

**Analysis:**
- ✅ **EXCELLENT:** Automatic reconnection with exponential backoff
- ✅ Timeout on line reads (5 second default)
- ✅ Explicit child cleanup
- ✅ `_SYSTEMD_UNIT=*` filter reduces noise
- ⚠️ **ISSUE:** Overlaps with JournalWatcher functionality
- ⚠️ **ISSUE:** Two separate `journalctl` processes running
- 💡 **RECOMMENDATION:** Consolidate with JournalWatcher, filter by unit in post-processing

### Journal Entry Parsing (systemd-specific)

```rust
fn parse_systemd_journal_entry(&self, line: &str) -> Option<Event<JsonValue>> {
    let entry: serde_json::Value = serde_json::from_str(line).ok()?;
    let message = entry["MESSAGE"].as_str()?;
    let unit_name = entry["_SYSTEMD_UNIT"].as_str();

    if message.contains("Started ") {
        Some(Event::new(
            SystemdUnitStartedPayload {
                unit_name: unit_name.unwrap_or("unknown").to_string(),
                main_pid: entry["_PID"].as_str().and_then(|s| s.parse().ok()),
                // ...
            },
            provenance
        ))
    } else if message.contains("Stopped ") {
        Some(Event::new(SystemdUnitStoppedPayload { ... }, provenance))
    } else if message.contains("Failed ") {
        Some(Event::new(SystemdUnitFailedPayload { ... }, provenance))
    } else {
        None  // Ignore other messages
    }
}
```

**Analysis:**
- ✅ Simple string matching for state changes
- ✅ Extracts PID from journal metadata
- ⚠️ **ISSUE:** String matching ("Started ", "Stopped ") is fragile
- ⚠️ **ISSUE:** Locale-dependent (breaks in non-English locales)
- 💡 **RECOMMENDATION:** Use structured journal fields (UNIT_RESULT, etc.)

---

## 🚌 D-Bus Watcher Deep Dive

### Architecture Overview

```rust
pub struct DbusWatcher {
    config: DbusConfig,
}

pub struct DbusConfig {
    pub monitor_session: bool,      // Session bus (user apps)
    pub monitor_system: bool,       // System bus (system services)
    pub capture_signals: bool,      // D-Bus signals
    pub capture_method_calls: bool, // D-Bus method calls
    pub interface_filters: Vec<String>, // Filter by interface
}
```

### Dual-Bus Monitoring Strategy

**Process:**

```rust
pub async fn start_streaming(&mut self, tx: mpsc::UnboundedSender<Event<JsonValue>>) -> Result<()> {
    let mut tasks = Vec::new();

    // Monitor session bus
    if self.config.monitor_session {
        tasks.push(tokio::spawn(
            Self::monitor_bus(DBusType::Session, tx.clone(), self.config.clone())
        ));
    }

    // Monitor system bus
    if self.config.monitor_system {
        tasks.push(tokio::spawn(
            Self::monitor_bus(DBusType::System, tx.clone(), self.config.clone())
        ));
    }

    // Wait for first task to complete (or fail)
    let (result, index, remaining) = futures::future::select_all(tasks).await;

    // Cancel remaining tasks
    for task in remaining {
        task.abort();
    }

    Ok(())
}
```

**Analysis:**
- ✅ **EXCELLENT:** Concurrent session + system bus monitoring
- ✅ Automatic task cleanup on failure
- ⚠️ **ISSUE:** If one bus fails, all monitoring stops
- 💡 **RECOMMENDATION:** Independent bus monitoring with separate error handling

### D-Bus Connection & Match Rules

**Implementation:**

```rust
async fn monitor_bus_inner(
    bus_type: DBusType,
    tx: &mpsc::UnboundedSender<Event<JsonValue>>,
    config: &DbusConfig,
) -> Result<()> {
    // Connect to bus
    let (resource, conn) = match bus_type {
        DBusType::Session => connection::new_session_sync()?,
        DBusType::System => connection::new_system_sync()?,
    };

    // Spawn connection resource handler
    tokio::spawn(async move {
        let err = resource.await;
        error!("D-Bus connection lost: {:?}", err);
    });

    // Add match rules
    let signal_rule = MatchRule::new().with_type(MessageType::Signal);
    conn.add_match(signal_rule).await?;

    let method_rule = MatchRule::new().with_type(MessageType::MethodCall);
    conn.add_match(method_rule).await?;

    // Create bounded channel for message processing
    let (msg_tx, mut msg_rx) = mpsc::channel::<DbusMessageData>(1000);

    // Spawn worker to process messages
    tokio::spawn(async move {
        while let Some(msg_data) = msg_rx.recv().await {
            if let Ok(event) = Self::process_dbus_message(msg_data, bus_type, config) {
                let _ = tx.send(event);
            }
        }
    });

    // Message loop
    loop {
        let msg = conn.next_msg().await?;
        let msg_data = Self::extract_message_data(&msg)?;
        msg_tx.send(msg_data).await?;
    }
}
```

**Analysis:**
- ✅ **EXCELLENT:** MatchRule for efficient filtering (kernel-side)
- ✅ **EXCELLENT:** Bounded channel (1000) prevents memory bloat
- ✅ Separate worker task for processing (non-blocking)
- ✅ Captures both signals and method calls
- ⚠️ **ISSUE:** No timeout on `conn.next_msg()` (could hang)
- ⚠️ **ISSUE:** 1000-message buffer could overflow on busy system
- 💡 **INSIGHT:** Message processing offloaded to worker (main loop stays responsive)

### Message Processing

**Structured Data Extraction:**

```rust
struct DbusMessageData {
    msg_type: MessageType,
    interface: Option<String>,
    path: Option<String>,
    member: Option<String>,      // Signal/method name
    sender: Option<String>,
    destination: Option<String>,
    args_json: serde_json::Value, // Serialized arguments
}

fn extract_message_data(msg: &Message) -> Result<DbusMessageData> {
    let args_json = msg.iter_init()
        .map(|arg| serialize_dbus_arg(arg))
        .collect::<Vec<_>>();

    Ok(DbusMessageData {
        msg_type: msg.message_type(),
        interface: msg.interface().map(|s| s.to_string()),
        path: msg.path().map(|s| s.to_string()),
        member: msg.member().map(|s| s.to_string()),
        sender: msg.sender().map(|s| s.to_string()),
        destination: msg.destination().map(|s| s.to_string()),
        args_json: serde_json::json!(args_json),
    })
}
```

**Analysis:**
- ✅ **EXCELLENT:** Comprehensive metadata extraction
- ✅ Serializes D-Bus arguments to JSON
- ✅ Captures interface, path, member (full message context)
- ⚠️ **ISSUE:** No type information for arguments (generic JSON)
- 💡 **INSIGHT:** Enables rich filtering and analysis downstream

### Specialized Event Parsing

**Examples:**

```rust
fn process_dbus_message(msg_data: DbusMessageData, bus_type: DBusType, config: &DbusConfig) -> Result<Event<JsonValue>> {
    // Network state changes
    if msg_data.interface == Some("org.freedesktop.NetworkManager") {
        return Ok(Event::new(
            DbusNetworkStateChangedPayload {
                interface: msg_data.interface.unwrap(),
                path: msg_data.path.unwrap_or_default(),
                member: msg_data.member.unwrap_or_default(),
                args: msg_data.args_json,
                // ...
            },
            provenance
        ));
    }

    // Power state changes
    if msg_data.interface == Some("org.freedesktop.UPower") {
        return Ok(Event::new(DbusPowerStateChangedPayload { ... }, provenance));
    }

    // Bluetooth
    if msg_data.interface == Some("org.bluez.Device1") {
        return Ok(Event::new(DbusBluetoothDeviceChangedPayload { ... }, provenance));
    }

    // Generic signal
    Ok(Event::new(
        DbusSignalPayload {
            bus_type: bus_type.to_string(),
            interface: msg_data.interface,
            member: msg_data.member,
            // ...
        },
        provenance
    ))
}
```

**Analysis:**
- ✅ **EXCELLENT:** Specialized payloads for common interfaces
- ✅ Generic fallback for unknown messages
- ✅ Covers major desktop services (NetworkManager, UPower, Bluez, etc.)
- 💡 **INSIGHT:** Interface-based routing enables rich event types

---

## 🔌 Udev Watcher Deep Dive

### Architecture Overview

```rust
pub struct UdevWatcher {
    _monitor_hotplug: bool,  // Currently unused
}
```

### Polling-Based Monitoring (Fallback Implementation)

**Why Polling?**
- `libudev` crate disabled (external dependency)
- Falls back to filesystem polling
- Monitors `/sys/class/` for device changes

**Implementation:**

```rust
async fn monitor_udev_events(&self, tx: mpsc::UnboundedSender<Event<JsonValue>>) -> Result<()> {
    let mut last_seen_devices = std::collections::HashSet::new();
    let mut poll_interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        poll_interval.tick().await;

        let mut current_devices = std::collections::HashSet::new();

        // Scan /sys/class for device changes
        let mut entries = tokio::fs::read_dir("/sys/class").await?;
        while let Some(entry) = entries.next_entry().await? {
            let class_name = entry.file_name().to_string_lossy().to_string();

            // Focus on interesting classes
            if !["net", "block", "input", "usb", "sound"].contains(&class_name.as_str()) {
                continue;
            }

            let mut class_entries = tokio::fs::read_dir(entry.path()).await?;
            while let Some(device_entry) = class_entries.next_entry().await? {
                let device_key = format!("{}:{}", class_name, device_name);
                current_devices.insert(device_key.clone());

                // New device detected
                if !last_seen_devices.contains(&device_key) {
                    let event = self.create_device_event("add", &device_path, device_type, properties)?;
                    tx.send(event)?;
                }
            }
        }

        // Detect removed devices
        for removed_device in last_seen_devices.difference(&current_devices) {
            let event = self.create_device_event("remove", &device_path, device_type, properties)?;
            tx.send(event)?;
        }

        last_seen_devices = current_devices;
    }
}
```

**Analysis:**
- ✅ Works without external dependencies
- ✅ Detects both additions and removals
- ✅ Focuses on relevant device classes (net, block, USB, etc.)
- ⚠️ **CRITICAL ISSUE:** 5-second polling interval is very coarse
  - Transient devices (thumb drives quickly unplugged) missed
  - High latency (0-5s to detect changes)
- ⚠️ **ISSUE:** No property extraction (vendor, model, serial all empty)
- ⚠️ **ISSUE:** No support for `change` events (only add/remove)
- 💡 **RECOMMENDATION:** Use inotify on `/sys/class/` for real-time detection

### Device Event Creation

**Event Types:**

```rust
fn create_device_event(
    &self,
    action: &str,
    device_path: &str,
    device_type: &str,
    properties: HashMap<String, String>,
) -> Result<Event<JsonValue>> {
    match action {
        "add" => create_udev_event!(UdevDeviceConnectedPayload, ...),
        "remove" => create_udev_event!(UdevDeviceDisconnectedPayload, ...),
        "change" => create_udev_event!(UdevDeviceChangedPayload, ...),
        "bind" | "unbind" => create_udev_event!(UdevDeviceDriverChangedPayload, ...),
        _ => create_udev_event!(UdevDeviceOtherPayload, ...),
    }
}
```

**Property Extraction:**

```rust
let subsystem = properties.get("SUBSYSTEM").cloned();
let vendor = properties.get("ID_VENDOR_FROM_DATABASE")
    .or_else(|| properties.get("ID_VENDOR"))
    .cloned();
let model = properties.get("ID_MODEL_FROM_DATABASE")
    .or_else(|| properties.get("ID_MODEL"))
    .cloned();
let serial = properties.get("ID_SERIAL_SHORT")
    .or_else(|| properties.get("ID_SERIAL"))
    .cloned();
```

**Analysis:**
- ✅ Comprehensive event types (add, remove, change, bind/unbind)
- ✅ Fallback chains for vendor/model (database → raw)
- ⚠️ **CRITICAL ISSUE:** Properties HashMap is **empty** in polling implementation!
  - Line 207: `let properties = HashMap::with_capacity(8);` // Empty!
  - No actual property reading from `/sys/class/...`
- ⚠️ **ISSUE:** All metadata (vendor, model, serial) = None
- 💡 **RECOMMENDATION:** Read uevent files from `/sys/class/.../uevent`

---

## 🔍 Critical Issues Found

### Issue 1: Duplicate journalctl Processes (MEDIUM)

**Files:**
- `journal_watcher.rs:273`
- `systemd_watcher.rs:354`

**Issue:**
- JournalWatcher runs `journalctl --follow`
- SystemdWatcher runs `journalctl --follow _SYSTEMD_UNIT=*`
- Two separate processes doing nearly identical work

**Impact:**
- Double resource usage (2× memory, CPU, disk I/O)
- Duplicate events for systemd unit messages
- Complexity in event deduplication downstream

**Recommendation:**
```rust
// Consolidate into single JournalWatcher
// Post-process events to route systemd-specific ones to SystemdWatcher handler
fn route_journal_event(entry: &JournalEntry) -> EventType {
    if entry.systemd_unit.is_some() {
        EventType::Systemd  // Route to systemd handler
    } else {
        EventType::General  // General journal event
    }
}
```

### Issue 2: Udev 5-Second Polling (HIGH)

**File:** `udev_watcher.rs:177`

**Issue:**
```rust
let mut poll_interval = tokio::time::interval(Duration::from_secs(5));
```

**Problem:**
- 5-second polling = 0-5s latency to detect devices
- Transient devices (USB drive quickly unplugged) missed entirely
- Inefficient (checks filesystem even when no changes)

**Scenario:**
```
User plugs in USB drive
  → Waits 2.3s for next poll
  → Copies file (takes 1s)
  → Unplugs drive
  → Next poll sees device, captures "add" event
  → 5s later, captures "remove" event
  → But file copy event lost (drive already gone)
```

**Impact:**
- High latency device detection
- Missed transient events
- Poor user experience

**Recommendation:**
```rust
// Use inotify to watch /sys/class for real-time events
use tokio::fs::File;
use inotify::{Inotify, WatchMask};

let mut inotify = Inotify::init()?;
inotify.watches().add("/sys/class", WatchMask::CREATE | WatchMask::DELETE)?;

let mut buffer = [0; 1024];
loop {
    let events = inotify.read_events_blocking(&mut buffer)?;
    for event in events {
        // Real-time device detection!
        if event.mask.contains(EventMask::CREATE) {
            handle_device_added(&event.name?)?;
        }
    }
}
```

### Issue 3: Udev Properties Not Extracted (CRITICAL)

**File:** `udev_watcher.rs:207`

**Issue:**
```rust
let properties = std::collections::HashMap::with_capacity(8);  // Empty!
```

**Problem:**
- Properties HashMap created but never populated
- All device metadata (vendor, model, serial) = None
- Makes device events nearly useless

**Impact:**
- Cannot identify devices ("USB device connected" without vendor/model)
- Cannot correlate device identity across add/remove
- No way to filter by device type downstream

**Recommendation:**
```rust
async fn read_device_properties(device_path: &Path) -> HashMap<String, String> {
    let mut properties = HashMap::new();

    // Read uevent file
    let uevent_path = device_path.join("uevent");
    if let Ok(contents) = tokio::fs::read_to_string(uevent_path).await {
        for line in contents.lines() {
            if let Some((key, value)) = line.split_once('=') {
                properties.insert(key.to_string(), value.to_string());
            }
        }
    }

    // Read additional sysfs attributes
    for attr in &["vendor", "device", "serial", "manufacturer"] {
        let attr_path = device_path.join(attr);
        if let Ok(value) = tokio::fs::read_to_string(attr_path).await {
            properties.insert(attr.to_uppercase(), value.trim().to_string());
        }
    }

    properties
}
```

### Issue 4: Systemd Parser State Loss (CRITICAL)

**File:** `systemd_watcher.rs:200-220`

**Issue:**
```rust
// Parse unit header
if line.starts_with("● ") {
    let unit_name = parts[0].trim();  // Extract unit name
    return Some(Event::new(SystemdUnitStatusPayload { ... }));
}

// Parse Active: line (different iteration!)
if line.trim().starts_with("Active: ") {
    return Some(Event::new(SystemdUnitStartedPayload {
        unit_name: "unknown".to_string(),  // ❌ Lost context!
    }));
}
```

**Problem:**
- Unit name extracted from header line
- Active status parsed on separate line
- No state maintained between lines
- All status events have `unit_name: "unknown"`

**Impact:**
- Cannot correlate unit status with unit identity
- All events essentially useless (which unit failed/started?)
- Makes systemd monitoring broken

**Recommendation:**
```rust
struct UnitParser {
    current_unit: Option<String>,
    current_desc: Option<String>,
}

impl UnitParser {
    fn parse_line(&mut self, line: &str) -> Option<Event<JsonValue>> {
        if line.starts_with("● ") {
            self.current_unit = Some(extract_unit_name(line));
            self.current_desc = Some(extract_description(line));
            None  // Wait for Active: line
        } else if line.trim().starts_with("Active: ") {
            let unit_name = self.current_unit.take()?;  // Use captured name!
            let status = parse_status(line);
            Some(Event::new(
                SystemdUnitStartedPayload {
                    unit_name,  // ✅ Correct unit name!
                    // ...
                },
                provenance
            ))
        }
    }
}
```

### Issue 5: No Timeout on D-Bus Message Read (HIGH)

**File:** `dbus_watcher.rs:line ~241` (inferred from monitor loop)

**Issue:**
```rust
loop {
    let msg = conn.next_msg().await?;  // No timeout!
}
```

**Problem:**
- `next_msg()` can block indefinitely
- If D-Bus daemon hangs, watcher hangs
- No heartbeat/liveness check

**Impact:**
- Silent monitoring failure
- No automatic recovery
- Manual restart required

**Recommendation:**
```rust
loop {
    let msg = tokio::time::timeout(
        Duration::from_secs(30),
        conn.next_msg()
    ).await;

    match msg {
        Ok(Ok(msg)) => { /* process */ }
        Ok(Err(e)) => {
            error!("D-Bus error: {}", e);
            break;  // Reconnect
        }
        Err(_) => {
            warn!("No D-Bus message in 30s, reconnecting");
            break;
        }
    }
}
```

### Issue 6: Journal Cursor Saved on Every Event (MEDIUM)

**File:** `journal_watcher.rs:follow_journal_inner` (line ~350 inferred)

**Issue:**
```rust
for event in events {
    // ... process event
    if let Some(cursor) = extract_cursor(&event) {
        self.last_cursor = Some(cursor.clone());
        self.save_cursor(&cursor).await?;  // ❌ Filesystem write per event!
    }
}
```

**Problem:**
- Cursor saved to file on *every* journal event
- High-volume systems: 100+ events/second
- Unnecessary filesystem I/O (sync writes)
- Wear on SSDs

**Impact:**
- Performance degradation
- Unnecessary disk wear
- Latency spikes

**Recommendation:**
```rust
// Batch cursor saves
let mut cursor_save_interval = tokio::time::interval(Duration::from_secs(10));
let mut pending_cursor: Option<String> = None;

loop {
    tokio::select! {
        event = events.next() => {
            // Process event
            pending_cursor = Some(extract_cursor(&event));
        }

        _ = cursor_save_interval.tick() => {
            if let Some(ref cursor) = pending_cursor {
                self.save_cursor_atomic(cursor).await?;
                pending_cursor = None;
            }
        }
    }
}
```

### Issue 7: D-Bus Message Buffer Overflow (MEDIUM)

**File:** `dbus_watcher.rs:244`

**Issue:**
```rust
let (msg_tx, mut msg_rx) = mpsc::channel::<DbusMessageData>(1000);
```

**Problem:**
- Fixed 1000-message buffer
- On busy systems (desktop with many apps), buffer fills
- Once full, `msg_tx.send()` blocks main message loop
- Messages lost or delayed

**Scenario:**
```
Desktop system with:
- 50 applications
- NetworkManager (frequent signals)
- BlueZ (device scanning)
- UPower (power events)

Peak: 200+ messages/second
Buffer fills in 5 seconds
Main loop blocks
```

**Impact:**
- Message loss on busy systems
- Blocking cascades to D-Bus daemon
- Monitoring stops working

**Recommendation:**
```rust
const DBUS_BUFFER_SIZE: usize = 10_000;  // Larger buffer
const DBUS_BUFFER_OVERFLOW_THRESHOLD: usize = 9_000;  // 90% full

let (msg_tx, mut msg_rx) = mpsc::channel::<DbusMessageData>(DBUS_BUFFER_SIZE);

// Monitor buffer fill
if msg_tx.capacity() < DBUS_BUFFER_OVERFLOW_THRESHOLD {
    warn!("D-Bus message buffer 90% full, shedding load");
    metrics::increment_counter!("dbus.messages_dropped_total");
    // Skip non-critical messages
}
```

### Issue 8: Bootstrap Event ID Reused Everywhere (LOW)

**Files:** All watchers (journal, systemd, dbus, udev)

**Issue:**
```rust
let system_bootstrap_id = EventId::from_ulid(
    Ulid::from_bytes([0x01, 0x80, 0x00, ...])  // Same hardcoded ID everywhere
);
let provenance = Provenance::from_synthesis_safe(system_bootstrap_id, vec![]);
```

**Problem:**
- All system events share same bootstrap ID
- Cannot distinguish provenance by watcher
- Loses information about event source

**Impact:**
- All system events appear to come from same source
- Cannot query "all D-Bus events" vs "all journal events"
- Provenance tracking less useful

**Recommendation:**
```rust
// Unique bootstrap ID per watcher
const JOURNAL_BOOTSTRAP_ID: [u8; 16] = [0x01, 0x80, 0x01, ...];
const SYSTEMD_BOOTSTRAP_ID: [u8; 16] = [0x01, 0x80, 0x02, ...];
const DBUS_BOOTSTRAP_ID: [u8; 16] = [0x01, 0x80, 0x03, ...];
const UDEV_BOOTSTRAP_ID: [u8; 16] = [0x01, 0x80, 0x04, ...];
```

### Issue 9: No Atomic Cursor Persistence (MEDIUM)

**File:** `journal_watcher.rs:save_cursor`

**Issue:**
```rust
async fn save_cursor(&self, cursor: &str) -> Result<()> {
    if let Some(ref cursor_file) = self.config.cursor_file {
        tokio::fs::write(cursor_file, cursor).await?;  // ❌ Not atomic!
    }
    Ok(())
}
```

**Problem:**
- `write()` is not atomic
- Crash during write = corrupted cursor file
- Empty/partial file on restart = full journal replay

**Impact:**
- Data loss on crash
- Potentially thousands of duplicate events
- Hours of reprocessing

**Recommendation:**
```rust
async fn save_cursor_atomic(&self, cursor: &str) -> Result<()> {
    if let Some(ref cursor_file) = self.config.cursor_file {
        let temp_file = format!("{}.tmp", cursor_file);
        tokio::fs::write(&temp_file, cursor).await?;
        tokio::fs::rename(&temp_file, cursor_file).await?;  // ✅ Atomic!
    }
    Ok(())
}
```

### Issue 10: Missing Metrics (LOW)

**Severity:** LOW
**Impact:** No observability into system satellite health

**Missing Metrics:**

**Journal:**
- `system_journal_entries_total` - Total entries processed
- `system_journal_cursor_saves_total` - Cursor save operations
- `system_journal_reconnects_total` - Reconnection count
- `system_journal_import_duration_seconds` - Historical import time

**Systemd:**
- `system_systemd_units_monitored` - Units being tracked
- `system_systemd_state_changes_total{unit, from_state, to_state}` - State transitions
- `system_systemd_reconnects_total` - Journal reconnections

**D-Bus:**
- `system_dbus_messages_total{bus_type, msg_type}` - Message counts
- `system_dbus_buffer_fill_ratio` - Buffer utilization (0-1)
- `system_dbus_reconnects_total{bus_type}` - Connection failures

**Udev:**
- `system_udev_devices_total{action, device_type}` - Device events
- `system_udev_poll_duration_seconds` - Filesystem scan time
- `system_udev_devices_tracked` - Current device count

**Recommendation:**
Add comprehensive metrics using `metrics` crate.

---

## ✅ Architectural Strengths

### 1. Cursor-Based Journal Resume (⭐⭐⭐⭐⭐)
- Resume from last position after restart
- No duplicate events
- File-based persistence
- Crash-safe (with atomic writes)

### 2. Batch Processing (⭐⭐⭐⭐⭐)
- 100-event batches reduce channel overhead
- Efficient memory usage
- Configurable batch size
- Amortizes synchronization cost

### 3. Multi-Watcher Architecture (⭐⭐⭐⭐)
- Independent operation (one failure doesn't stop others)
- Specialized for each domain
- Unified event channel
- Clean separation of concerns

### 4. D-Bus Match Rules (⭐⭐⭐⭐⭐)
- Kernel-side filtering (efficient)
- Captures signals + method calls
- Interface-based routing
- Minimal overhead

### 5. Exponential Backoff Reconnection (⭐⭐⭐⭐)
- Automatic recovery from failures
- Prevents thundering herd
- Configurable retry limits
- Used in systemd + D-Bus watchers

### 6. Timeout on External Commands (⭐⭐⭐⭐)
- Prevents indefinite hangs
- Configurable timeout (5s default)
- Explicit child cleanup
- Used in systemd watcher

### 7. Comprehensive Event Types (⭐⭐⭐⭐)
- Specialized payloads (NetworkManager, UPower, Bluez, etc.)
- Generic fallback for unknown messages
- Rich metadata extraction
- Type-safe event handling

---

## ⚠️ Weaknesses & Recommendations

### Immediate (High Priority)

1. **Fix udev property extraction** (< 1 day)
   - Read uevent files
   - Extract vendor, model, serial
   - Make device events useful

2. **Fix systemd parser state loss** (< 4 hours)
   - Maintain parser state between lines
   - Correlate unit name with status
   - Critical for systemd monitoring

3. **Add inotify-based udev monitoring** (< 1 day)
   - Replace 5-second polling
   - Real-time device detection
   - Catch transient events

4. **Add D-Bus message read timeout** (< 2 hours)
   - 30-second timeout
   - Automatic reconnection
   - Prevents silent hangs

### Short Term (Medium Priority)

5. **Consolidate journalctl processes** (< 4 hours)
   - Single JournalWatcher instance
   - Route systemd events to SystemdWatcher
   - Reduce resource usage

6. **Batch cursor saves** (< 2 hours)
   - Save every 10 seconds or 100 events
   - Reduce filesystem I/O
   - Improve performance

7. **Make cursor saves atomic** (< 1 hour)
   - Temp file + rename
   - Prevent corruption on crash
   - Critical for reliability

8. **Increase D-Bus buffer size** (< 1 hour)
   - 1000 → 10,000 messages
   - Monitor buffer fill
   - Add overflow metrics

### Long Term (Nice to Have)

9. **Add comprehensive metrics** (< 4 hours)
   - All watchers instrumented
   - Buffer fill, reconnects, processing rates
   - Observability

10. **Unique bootstrap IDs per watcher** (< 2 hours)
    - Distinguish event sources
    - Better provenance tracking
    - Query by watcher type

11. **Add timeout to historical journal import** (< 2 hours)
    - Prevent indefinite hangs on large imports
    - Progress reporting
    - Cancellation support

---

## 📊 System Satellite Summary

**Code Quality:** ⭐⭐⭐⭐ (4/5)
- Well-structured multi-watcher design
- Some critical bugs (udev properties, systemd parser state)
- Good use of async/await

**Architecture:** ⭐⭐⭐⭐ (4/5)
- Clean separation of watchers
- Unified event channel
- Independent operation

**Reliability:** ⭐⭐⭐ (3/5)
- Good reconnection logic
- Missing timeouts in critical paths
- Non-atomic cursor persistence

**Performance:** ⭐⭐⭐ (3/5)
- Batch processing excellent
- Cursor saved too frequently
- Udev polling very inefficient

**Completeness:** ⭐⭐⭐ (3/5)
- Journal: Complete
- Systemd: Functional but buggy
- D-Bus: Excellent
- Udev: Broken (no properties)

**Overall:** ⭐⭐⭐⭐ (4/5)

**System satellite is a well-architected multi-watcher design** with excellent journal monitoring and D-Bus integration. Critical issues: udev property extraction broken, systemd parser loses context, and 5-second udev polling misses transient events. Strong cursor-based resume and exponential backoff reconnection.

---

**Analysis Completeness:** Comprehensive
**Files Analyzed:** 7 (4 watchers + unified processor + 2 config files)
**Issues Found:** 10 critical issues cataloged
**Next:** Update master summary, then continue with Phase 6 (Database patterns)
