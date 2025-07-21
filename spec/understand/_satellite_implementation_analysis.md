# Satellite Implementation Analysis

## Overview

This analysis examines the concrete satellite implementations in Sinex to understand how they capture data from external sources using the unified `StatefulStreamProcessor` trait. The analysis covers architecture patterns, the three-phase startup sequence, event creation, checkpoint management, and source-specific implementations.

## Core Architecture: Deep Symmetry

All satellites implement the `StatefulStreamProcessor` trait from `sinex-satellite-sdk`, achieving "deep symmetry" between ingestors and automata:

```rust
pub trait StatefulStreamProcessor: Send + Sync {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()>;
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport>;
    
    fn processor_name(&self) -> &str;
    fn processor_type(&self) -> ProcessorType;
    fn capabilities(&self) -> ProcessorCapabilities;
    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint>;
    // ... other methods
}
```

This unified interface replaces the old `EventSource::start_streaming()` approach and enables consistent behavior across all data capture mechanisms.

## Three-Phase Startup Pattern

All satellites follow a consistent three-phase startup sequence implemented in `StreamProcessorRunner::run_service()`:

### Phase 1: Snapshot
- **Purpose**: Capture current state of the external system
- **TimeHorizon**: `TimeHorizon::Snapshot`
- **Behavior**: Takes an instantaneous snapshot of available data
- **Example**: Filesystem satellite scans existing files, terminal satellite captures current shell history

### Phase 2: Gap-filling (Historical)
- **Purpose**: Process events that occurred while the satellite was offline
- **TimeHorizon**: `TimeHorizon::Historical { end_time }`
- **Behavior**: Bounded scan from last checkpoint to current time
- **Conditional**: Only runs if processor supports historical scanning and has a previous checkpoint

### Phase 3: Continuous Processing
- **Purpose**: Real-time event monitoring and streaming
- **TimeHorizon**: `TimeHorizon::Continuous`
- **Behavior**: Unbounded scan that continues indefinitely
- **Implementation**: Runs until shutdown signal received

```rust
// Service startup sequence from StreamProcessorRunner
pub async fn run_service(&mut self) -> SatelliteResult<()> {
    // Phase 1: Snapshot
    if self.processor.capabilities().supports_snapshot {
        let snapshot_report = self.processor
            .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
            .await?;
    }
    
    // Phase 2: Gap-filling
    if self.processor.capabilities().supports_historical {
        let current_checkpoint = self.processor.current_checkpoint().await?;
        if !matches!(current_checkpoint, Checkpoint::None) {
            let gap_fill_report = self.processor
                .scan(current_checkpoint, TimeHorizon::Historical { end_time: Utc::now() }, ScanArgs::default())
                .await?;
        }
    }
    
    // Phase 3: Continuous processing
    if self.processor.capabilities().supports_continuous {
        let current_checkpoint = self.processor.current_checkpoint().await?;
        let _continuous_report = self.processor
            .scan(current_checkpoint, TimeHorizon::Continuous, ScanArgs::default())
            .await?;
    }
}
```

## Satellite Implementations

### 1. Filesystem Satellite (`sinex-fs-watcher`)

**Status**: Production-ready with full implementation

**Key Features**:
- **Debounced monitoring** using `notify_debouncer_full` with configurable delay (default 100ms)
- **Pattern-based filtering** with glob patterns for inclusion/exclusion
- **Recursive directory watching** with optional depth limits
- **Rename operation tracking** to detect file moves vs. copy+delete operations
- **Enhanced move detection** using filesystem cookies

**Configuration**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    pub watch_patterns: Vec<String>,      // Default: ["**/*"]
    pub ignore_patterns: Vec<String>,     // Default: ["target/**", "**/.git/**", "**/node_modules/**"]
    pub debounce_ms: u64,                 // Default: 100
    pub max_depth: Option<usize>,         // Default: None (unlimited)
}
```

**Event Types**:
- `file.discovered` - File found during snapshot/historical scan
- `dir.discovered` - Directory found during snapshot/historical scan
- Various filesystem events from notify (create, modify, delete, move) in continuous mode

**Capabilities**:
```rust
ProcessorCapabilities {
    supports_continuous: true,
    supports_historical: true,
    supports_snapshot: true,
    supports_interactive: false,
    max_scan_size: Some(100000),
    supports_concurrent: false,
}
```

**Implementation Highlights**:
- Uses `WalkDir` for directory traversal during snapshot/historical scans
- Implements sophisticated pattern matching for path filtering
- Maintains rename operation tracking with automatic cleanup
- Provides file metadata (size, permissions, timestamps) in events

### 2. Terminal Satellite (`sinex-terminal-satellite`)

**Status**: Architecture complete, watchers await full implementation

**Key Features**:
- **Multi-source support**: Atuin database, shell history files, Kitty protocol, recordings, scrollback
- **Configurable sources** with individual enable/disable flags
- **Batch processing** with configurable batch sizes
- **Database integration** for Atuin SQLite database querying

**Configuration**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalConfig {
    pub enabled_sources: HashMap<String, bool>,
    pub atuin_db_path: Option<PathBuf>,           // Default: ~/.local/share/atuin/history.db
    pub history_files: Vec<PathBuf>,              // Default: .bash_history, .zsh_history, fish_history
    pub kitty_socket_path: Option<PathBuf>,       // Auto-detected
    pub recording_output_dir: Option<PathBuf>,    // Default: ~/.local/share/sinex/recordings
    pub scrollback_capture_enabled: bool,
    pub polling_interval_secs: u64,               // Default: 5
    pub batch_size: usize,                        // Default: 100
}
```

**Planned Watchers**:
- `AtuinWatcher` - SQLite database monitoring
- `HistoryWatcher` - Shell history file monitoring
- `KittyWatcher` - Kitty terminal protocol integration
- `RecordingWatcher` - Terminal session recording management
- `ScrollbackWatcher` - Terminal scrollback capture

**Event Types**:
- `terminal.snapshot` - Terminal state snapshot
- `terminal.monitoring_started` - Continuous monitoring initiated
- `shell.command_historical` - Historical command from Atuin
- `shell.history_historical` - Historical command from shell history files

**Capabilities**:
```rust
ProcessorCapabilities {
    supports_continuous: true,
    supports_historical: true,
    supports_snapshot: true,
    supports_interactive: false,
    max_scan_size: Some(10000),
    supports_concurrent: false,
}
```

### 3. Desktop Satellite (`sinex-desktop-satellite`)

**Status**: Architecture complete, focusing on clipboard and window manager events

**Key Features**:
- **Clipboard monitoring** with configurable polling intervals
- **Window manager integration** (currently Hyprland-focused)
- **State tracking** for active windows, workspaces, and clipboard content
- **Privacy controls** with content hashing instead of raw clipboard data

**Configuration**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesktopConfig {
    pub clipboard_enabled: bool,                  // Default: true
    pub window_manager_enabled: bool,             // Default: true
    pub window_manager_type: String,              // Default: "hyprland"
    pub clipboard_poll_interval_secs: u64,       // Default: 2
}
```

**Planned Watchers**:
- `ClipboardWatcher` - System clipboard monitoring
- `WindowManagerWatcher` - Window manager event integration

**Event Types**:
- `desktop.snapshot` - Desktop state snapshot
- `desktop.monitoring_started` - Continuous monitoring initiated
- `clipboard.historical` - Limited historical clipboard data
- `wm.historical` - Limited historical window manager data

**Capabilities**: Same as terminal satellite with focus on real-time desktop events.

### 4. System Satellite (`sinex-system-satellite`)

**Status**: Architecture complete, handling D-Bus, journal, udev, and systemd

**Key Features**:
- **D-Bus monitoring** with configurable bus selection (session, system)
- **Journal following** for systemd journal integration
- **udev monitoring** for hardware device events
- **systemd integration** for service state tracking
- **Multi-source coordination** with independent enable/disable per source

**Configuration**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    pub dbus_enabled: bool,                       // Default: true
    pub journal_enabled: bool,                    // Default: true
    pub udev_enabled: bool,                       // Default: true
    pub systemd_enabled: bool,                    // Default: true
    pub dbus_buses: String,                       // Default: "session,system"
    pub journal_follow_mode: String,              // Default: "tail"
    pub udev_monitor_subsystems: Vec<String>,     // Default: all subsystems
    pub systemd_unit_patterns: Vec<String>,       // Default: all units
}
```

**Planned Watchers**:
- `EnhancedDbusWatcher` - D-Bus signal monitoring
- `EnhancedJournalWatcher` - Journal entry following
- `UdevWatcher` - Device event monitoring
- `SystemdWatcher` - Service state monitoring

**Event Types**:
- `system.snapshot` - System state snapshot
- `system.monitoring_started` - Continuous monitoring initiated
- `journal.historical` - Historical journal entries
- `systemd.historical` - Historical unit state changes

**Historical Capabilities**: System satellite has better historical scanning potential due to journal persistence and systemd state history.

## Event Creation Patterns

All satellites use the unified `EventFactory` for consistent event creation:

### 1. EventFactory Initialization
```rust
let factory = EventFactory::new(sources::FS);  // Using constants for source identification
```

### 2. Generic Event Creation
```rust
let event = factory.create_event("file.discovered", json!({
    "path": file_path,
    "size": metadata.len(),
    "discovered_at": Utc::now(),
}));
```

### 3. Fluent Builder APIs
```rust
// Filesystem events
let event = factory.filesystem()
    .path("/path/to/file")
    .created()
    .size(1024)
    .permissions(0o644)
    .build();

// Terminal events
let event = factory.terminal()
    .command("git status")
    .exit_code(0)
    .duration_ms(150)
    .build();

// Clipboard events
let event = factory.clipboard()
    .content_type(ClipboardContentType::Text)
    .content_hash("sha256:...")
    .build();
```

### 4. Event Emission
```rust
// Via StreamProcessorContext
context.emit_event(event).await?;

// Batch emission
context.emit_events(vec![event1, event2, event3]).await?;
```

### 5. Privacy and Content Handling
- File content is not stored in events by default
- Clipboard content is hashed rather than stored raw
- Size limits prevent excessive payload sizes
- Sensitive patterns can be filtered at creation time

## Checkpoint Management

All satellites use the unified checkpoint system with automatic migration:

### 1. Checkpoint Types
```rust
pub enum Checkpoint {
    None,                                    // Start from beginning
    External { position, description },      // Ingestor external state
    Internal { event_id, message_count },    // Automaton event processing
    Stream { message_id, event_id },         // Redis Stream processing
    Timestamp { timestamp, metadata },       // Time-based processing
}
```

### 2. Checkpoint Storage
- **Unified Storage**: All checkpoints in `core.automaton_checkpoints` table
- **Automatic Migration**: V1 (Redis-only) → V2 (unified) format migration
- **Atomic Updates**: Using `ON CONFLICT` upserts for consistency
- **Instance Isolation**: Separate checkpoints per hostname+PID

### 3. Checkpoint Usage Patterns

**Filesystem Satellite**:
```rust
async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
    // Timestamp-based checkpoints for modification time filtering
    Ok(Checkpoint::timestamp(Utc::now(), None))
}
```

**Terminal Satellite**:
```rust
// External checkpoints for database positions
Checkpoint::external(
    json!({"db_path": atuin_path, "last_id": 12345}),
    "Last processed Atuin entry ID: 12345"
)
```

**System Satellite**:
```rust
// Stream checkpoints for journal cursors
Checkpoint::stream("journal_cursor_123", Some(event_ulid))
```

### 4. Checkpoint Recovery
- Satellites automatically resume from last checkpoint on restart
- Failed checkpoints fall back to `Checkpoint::None` (full rescan)
- Historical scans use checkpoints to determine start positions
- Gap-filling relies on checkpoint timestamps for range determination

## Common Patterns Across Satellites

### 1. Configuration Management
- Environment-only configuration (no config files)
- JSON parsing from `StreamProcessorContext.config` map
- Fallback to sensible defaults on parsing errors
- Individual config value overrides supported

### 2. State Snapshots
- All satellites implement `take_snapshot()` for diagnostics
- Snapshots capture current external system state
- Used by `ExplorationProvider` for CLI diagnostics
- Enable coverage analysis and health monitoring

### 3. Watcher Initialization
- Lazy initialization of individual watchers
- Configuration-driven watcher selection
- Graceful degradation when sources unavailable
- Stub implementations during development phase

### 4. Error Handling
- Consistent error types via `SatelliteError` enum
- Graceful degradation on individual source failures
- Warning collection in scan reports
- Retry logic with exponential backoff

### 5. Metrics and Observability
- Automatic metrics generation via `#[sinex_macros::auto_satellite_metrics]`
- Processor-specific labels for metrics differentiation
- Event emission tracking and performance monitoring
- Health check implementations for service monitoring

### 6. CLI Integration
- All satellites implement `ExplorationProvider` trait
- Automatic CLI generation via `sinex-satellite-sdk`
- Consistent command patterns across satellites:
  - `scan` - Run snapshot/historical/continuous scans
  - `status` - Show current processor state
  - `export` - Export state data in various formats

### 7. Testing Patterns
- `#[sinex_test]` macro for database-backed tests
- `TestContext` for consistent test environments
- Mock implementations for external dependencies
- Property-based testing for checkpoint serialization

## Development Status Summary

| Satellite | Status | Core Features | Watchers Status |
|-----------|--------|---------------|-----------------|
| **Filesystem** | ✅ Production Ready | Debounced monitoring, pattern filtering, move detection | ✅ Complete |
| **Terminal** | 🔄 Architecture Complete | Multi-source support, batch processing | ⏳ Stub implementations |
| **Desktop** | 🔄 Architecture Complete | Clipboard monitoring, WM integration | ⏳ Stub implementations |
| **System** | 🔄 Architecture Complete | D-Bus, journal, udev, systemd | ⏳ Stub implementations |

## Key Architectural Achievements

1. **Deep Symmetry**: Unified interface between ingestors and automata eliminates cognitive overhead
2. **Resumable Processing**: Comprehensive checkpoint system enables reliable restarts
3. **Configuration Consistency**: Environment-only config eliminates file-based complexity
4. **Privacy by Design**: Content hashing and size limits built into event creation
5. **Operational Excellence**: Comprehensive metrics, health checks, and diagnostics
6. **Testability**: Consistent testing patterns and mock infrastructure
7. **Developer Experience**: Automatic CLI generation and consistent command patterns

The satellite implementations successfully demonstrate the unified architecture's effectiveness, with the filesystem satellite serving as a production reference and others following the established patterns for consistency and maintainability.