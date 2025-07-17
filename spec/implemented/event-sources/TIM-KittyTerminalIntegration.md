# TIM-KittyTerminalIntegration: Kitty Terminal Specific Integration

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 95% (Comprehensive implementation using StatefulStreamProcessor)
**Dependencies**: kitty terminal, unix sockets, kitty @ remote control, StatefulStreamProcessor trait
**Blocks**: Rich terminal context, command completion tracking, pane-level monitoring

## ✅ IMPLEMENTATION STATUS
**Status**: FULLY IMPLEMENTED - Uses unified StatefulStreamProcessor architecture
**Implementation**: Terminal satellite with snapshot, historical, and continuous scanning modes
**Sources**: Atuin history, shell history files, kitty remote control, scrollback capture
**Architecture**: Terminal satellite processor with checkpoint management and exploration support

## MVP Specification
- Kitty socket discovery and connection
- Window and tab enumeration
- Basic command polling
- Remote control integration
- Terminal state querying

## Enhanced Features
- Real-time command execution tracking
- Scrollback content analysis
- OSC escape sequence monitoring
- Advanced pane management
- Terminal performance metrics

## Implementation Checklist
- [x] Socket discovery mechanism
- [x] Remote control connection
- [x] Window/tab listing
- [x] Polling infrastructure with StatefulStreamProcessor
- [x] Command execution detection
- [x] Scrollback content access
- [x] OSC sequence monitoring
- [x] Real-time event streaming

## Current Architecture

### StatefulStreamProcessor Implementation
```rust
// Terminal processor with unified scan interface
async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<ScanReport> {
    match until {
        TimeHorizon::Snapshot => self.capture_current_state().await,
        TimeHorizon::Historical { end_time } => self.scan_historical(from, end_time).await,
        TimeHorizon::Continuous => self.start_continuous_monitoring(from).await,
    }
}
```

### Key Features
- **Multi-Source Terminal Monitoring**: Atuin command history, shell history files, kitty remote control, scrollback capture
- **Checkpoint Management**: Tracks processing state across different terminal data sources
- **Exploration Support**: Provides coverage analysis and source state diagnostics
- **Event Types**: Command execution, session changes, window state, scrollback content

### API Signatures
```rust
// Core terminal processor operations
async fn capture_current_state(&mut self) -> SatelliteResult<ScanReport>;
async fn scan_historical(&mut self, from: Checkpoint, end_time: DateTime<Utc>) -> SatelliteResult<ScanReport>;
async fn start_continuous_monitoring(&mut self, from: Checkpoint) -> SatelliteResult<ScanReport>;

// Terminal-specific operations
async fn get_kitty_state(&self) -> Result<KittyState>;
async fn capture_scrollback(&self) -> Result<String>;
async fn parse_command_history(&self, source: &str) -> Result<Vec<CommandEvent>>;
```

## Kitty Remote Control Protocol

### Communication Methods
- **UNIX Domain Socket**: Primary method for reliable communication
- **Socket Path**: Configurable, defaults to `/tmp/kitty-*` pattern
- **Authentication**: Optional password protection supported

### Core Commands
- `{"cmd": "ls"}` - List all windows, tabs, and panes
- `{"cmd": "get-text", "match": "focused:true", "extent": "scrollback"}` - Get scrollback content
- `{"cmd": "get-window-state", "match": "focused:true"}` - Get window state (title, PID, CWD)

### Performance Characteristics
- Simple commands: ~1-2ms latency
- Large data retrieval: ~100ms per MB
- Socket method: ~2x faster than PTY escapes

## Security Considerations
- **Socket-Only Mode**: Disable PTY remote control, use socket only
- **Restrictive Permissions**: Socket file permissions 0600 or group-restricted
- **Password Protection**: Optional remote control password
- **Input Sanitization**: Validated JSON command structure

## Event Types Generated
- `terminal.session.started/ended` - Terminal session lifecycle
- `terminal.command.executed` - Command execution events
- `terminal.window.focused/created/closed` - Window state changes
- `terminal.scrollback.captured` - Scrollback content snapshots
- `terminal.process.changed` - Foreground process changes

## Configuration
```rust
pub struct TerminalConfig {
    pub enabled_sources: HashMap<String, bool>,
    pub atuin_db_path: Option<PathBuf>,
    pub history_files: Vec<PathBuf>,
    pub kitty_socket_path: Option<PathBuf>,
    pub polling_interval_secs: u64,
    pub batch_size: usize,
}
```

## References
- **Relevant ADR**: ADR-008-TerminalActivityCaptureStrategy.md
- **Original Context**: Terminal activity capture strategy
- **Architecture**: Unified StatefulStreamProcessor with terminal-specific implementations