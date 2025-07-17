# TIM-ComprehensiveEventSources.md

**Status**: Planned  
**Priority**: High  
**Effort**: 3-6 months  
**Dependencies**: Plugin architecture, privacy framework  

## Overview

Sinex currently captures ~35% of system activity across 11 event sources. To achieve the vision of comprehensive digital life capture, we need to implement 10 additional high-priority event sources that will bring coverage to 80%+ of meaningful system activity.

## Current Coverage Analysis

### Implemented Sources (35% coverage)
- **Filesystem Monitor**: File operations (5% coverage)
- **Clipboard Monitor**: Copy/paste events (2% coverage)  
- **Terminal Sources**: Kitty, Asciinema, generic (8% coverage)
- **Window Manager**: Hyprland, X11 (5% coverage)
- **System Sources**: Shell, Git, SQLite, Downloads (15% coverage)

### Coverage Gaps
The missing 65% represents the majority of knowledge work and system interaction. Critical gaps include web browsing (40-60% of activity), process execution, network activity, and visual/input context.

## Top 10 Missing Event Sources

### 1. Browser Activity Monitor
**Impact**: Critical (40-60% of knowledge work)  
**Complexity**: High  
**Privacy Impact**: Very High  
**Implementation Time**: 2-3 weeks  

**Captures**:
- Page visits with full URL and metadata
- Tab switches and session management
- Form interactions and input events
- Download events and file handling
- Bookmark and history changes
- Extension-triggered events

**Implementation Strategy**:
```rust
// Browser extension + native messaging architecture
pub struct BrowserMonitor {
    native_host: NativeMessagingHost,
    extension_connections: HashMap<String, ExtensionConnection>,
    privacy_filter: BrowserPrivacyFilter,
}

// Extension manifest.json
{
    "name": "Sinex Browser Monitor",
    "permissions": ["tabs", "history", "downloads", "bookmarks"],
    "native_messaging_hosts": ["com.sinex.browser_monitor"]
}
```

**Privacy Controls**:
- Configurable URL filtering (block banking, medical, etc.)
- Content sanitization for form data
- Incognito mode exclusion
- Per-domain privacy levels

### 2. Process Execution Tracker
**Impact**: High (all non-terminal programs)  
**Complexity**: Medium  
**Privacy Impact**: Medium  
**Implementation Time**: 1 week  

**Captures**:
- Program launches with full command line
- Process lifecycle (start, stop, crash)
- Resource usage patterns
- Parent-child process relationships
- Exit codes and error conditions

**Implementation Strategy**:
```rust
// eBPF-based process monitoring
pub struct ProcessTracker {
    ebpf_program: Option<EbpfHandler>,
    procfs_monitor: ProcfsWatcher,
    process_tree: ProcessTree,
}

// Alternative: procfs polling for systems without eBPF
impl StatefulStreamProcessor for ProcessTracker {
    async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<()> {
        if let Some(ebpf) = &mut self.ebpf_program {
            self.scan_ebpf_events(from, until, args).await
        } else {
            self.scan_procfs_events(from, until, args).await
        }
    }
}
```

**Features**:
- Efficient eBPF monitoring on supported kernels
- Fallback to procfs polling
- Process tree reconstruction
- Resource usage correlation

### 3. Network Activity Monitor  
**Impact**: High (external interactions)  
**Complexity**: High  
**Privacy Impact**: High  
**Implementation Time**: 2 weeks  

**Captures**:
- Connection establishment/teardown
- DNS queries and responses
- Traffic volume and patterns
- Protocol analysis (HTTP/HTTPS metadata)
- Network interface changes

**Implementation Strategy**:
```rust
// Netlink socket monitoring + optional packet capture
pub struct NetworkMonitor {
    netlink_socket: NetlinkSocket,
    dns_interceptor: Option<DnsInterceptor>,
    connection_tracker: ConnectionTracker,
    privacy_config: NetworkPrivacyConfig,
}

// DNS monitoring via /etc/systemd/resolved.conf.d/
impl NetworkMonitor {
    async fn monitor_dns_queries(&self) -> Result<()> {
        // Monitor systemd-resolved logs or use netlink
        // Extract domain names while respecting privacy
    }
}
```

**Privacy Considerations**:
- No packet content capture by default
- Domain-level filtering for sensitive sites
- IP address anonymization options
- Optional VPN/Tor traffic exclusion

### 4. Screen Capture with OCR
**Impact**: Medium (visual context)  
**Complexity**: Medium  
**Privacy Impact**: Very High  
**Implementation Time**: 1-2 weeks  

**Captures**:
- Periodic screenshots at configurable intervals
- OCR text extraction from screen content
- Application window contents
- Screen region changes and updates

**Implementation Strategy**:
```rust
// Wayland screencopy + OCR pipeline
pub struct ScreenCaptureMonitor {
    wayland_recorder: WaylandScreencopy,
    ocr_engine: TesseractOCR,
    screenshot_scheduler: ScreenshotScheduler,
    privacy_zones: Vec<PrivacyZone>,
}

// Privacy-aware screenshot processing
impl ScreenCaptureMonitor {
    async fn capture_and_process(&self) -> Result<ScreenCaptureEvent> {
        let screenshot = self.wayland_recorder.capture().await?;
        let filtered = self.apply_privacy_filters(screenshot)?;
        let text = self.ocr_engine.extract_text(&filtered).await?;
        
        Ok(ScreenCaptureEvent {
            timestamp: Utc::now(),
            text_content: text,
            screen_regions: self.analyze_regions(&filtered),
            privacy_level: self.assess_privacy_level(&text),
        })
    }
}
```

**Privacy Features**:
- Configurable privacy zones (password fields, etc.)
- Content-based filtering (credit cards, SSNs)
- Blur/redact sensitive information
- User-controlled capture frequency

### 5. Keyboard/Mouse Input Patterns
**Impact**: Medium (activity detection)  
**Complexity**: Low  
**Privacy Impact**: Medium  
**Implementation Time**: 3-4 days  

**Captures**:
- Typing speed and rhythm patterns
- Mouse movement and click patterns
- Idle detection and activity levels
- Application focus patterns
- Keyboard shortcuts and hotkeys

**Implementation Strategy**:
```rust
// evdev input monitoring
pub struct InputPatternMonitor {
    input_devices: Vec<EvdevDevice>,
    pattern_analyzer: InputPatternAnalyzer,
    privacy_filter: InputPrivacyFilter,
}

// Privacy-preserving input analysis
impl InputPatternAnalyzer {
    fn analyze_patterns(&self, events: &[InputEvent]) -> InputPatterns {
        InputPatterns {
            typing_speed_wpm: self.calculate_typing_speed(events),
            activity_level: self.assess_activity_level(events),
            focus_duration: self.calculate_focus_periods(events),
            // No actual keystrokes stored
        }
    }
}
```

**Privacy Design**:
- No actual keystroke content captured
- Only timing and pattern metadata
- Configurable monitoring periods
- Application-specific filtering

### 6. Audio Environment Monitor
**Impact**: Low (context awareness)  
**Complexity**: Medium  
**Privacy Impact**: Very High  
**Implementation Time**: 1 week  

**Captures**:
- Microphone activity detection (not content)
- Audio playback events and metadata
- Sound environment classification
- Meeting/call detection

**Implementation Strategy**:
```rust
// PipeWire/PulseAudio integration
pub struct AudioMonitor {
    pipewire_client: PipeWireClient,
    audio_analyzer: AudioAnalyzer,
    privacy_controls: AudioPrivacyControls,
}

// Audio activity detection without content capture
impl AudioMonitor {
    async fn monitor_audio_activity(&self) -> Result<AudioActivityEvent> {
        let volume_levels = self.pipewire_client.get_volume_levels().await?;
        let activity_type = self.audio_analyzer.classify_activity(&volume_levels);
        
        Ok(AudioActivityEvent {
            microphone_active: volume_levels.input > self.privacy_controls.activity_threshold,
            playback_active: volume_levels.output > 0.0,
            activity_classification: activity_type, // music, speech, silence
            // No actual audio content
        })
    }
}
```

### 7. Power and Performance Monitor
**Impact**: Low (system health)  
**Complexity**: Low  
**Privacy Impact**: Low  
**Implementation Time**: 2-3 days  

**Captures**:
- CPU usage and load patterns
- Memory consumption
- Battery levels and charging state
- Thermal conditions
- Performance throttling events

**Implementation Strategy**:
```rust
// sysfs-based system monitoring
pub struct SystemPerformanceMonitor {
    cpu_monitor: CpuMonitor,
    memory_monitor: MemoryMonitor,
    battery_monitor: BatteryMonitor,
    thermal_monitor: ThermalMonitor,
}

// Efficient polling of system statistics
impl SystemPerformanceMonitor {
    async fn collect_system_stats(&self) -> Result<SystemStatsEvent> {
        Ok(SystemStatsEvent {
            cpu_usage_percent: self.cpu_monitor.get_usage().await?,
            memory_usage_mb: self.memory_monitor.get_usage().await?,
            battery_percentage: self.battery_monitor.get_level().await?,
            thermal_state: self.thermal_monitor.get_state().await?,
        })
    }
}
```

### 8. Application State Monitor
**Impact**: Medium (deep integration)  
**Complexity**: Very High  
**Privacy Impact**: High  
**Implementation Time**: 2-4 weeks per application  

**Captures**:
- IDE project and file changes
- Document save events
- Application-specific workflow events
- Plugin and extension activities

**Implementation Strategy**:
```rust
// Plugin-based per-application monitoring
pub trait ApplicationMonitor: StatefulStreamProcessor {
    fn application_name() -> &'static str;
    async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<()>;
}

// Example: VSCode integration
pub struct VSCodeMonitor {
    extension_host: VSCodeExtensionHost,
    workspace_watcher: WorkspaceWatcher,
    checkpoint_manager: CheckpointManager,
    redis_client: RedisClient,
}

impl StatefulStreamProcessor for VSCodeMonitor {
    async fn scan(&mut self, from: Checkpoint, until: TimeHorizon, args: ScanArgs) -> SatelliteResult<()> {
        // Monitor via VSCode extension API
        // Track file operations, debugging sessions, etc.
        // Process events from checkpoint to time horizon
    }
}
```

### 9. Communication Metadata Monitor
**Impact**: High (collaboration tracking)  
**Complexity**: High  
**Privacy Impact**: Very High  
**Implementation Time**: 1-2 weeks per service  

**Captures**:
- Email send/receive events (metadata only)
- Chat message events (metadata only)
- Video call participation
- Calendar event attendance

**Implementation Strategy**:
```rust
// API-based communication monitoring
pub struct CommunicationMonitor {
    email_monitors: Vec<Box<dyn EmailMonitor>>,
    chat_monitors: Vec<Box<dyn ChatMonitor>>,
    calendar_monitors: Vec<Box<dyn CalendarMonitor>>,
}

// Email monitoring via IMAP/SMTP observation
pub struct IMAPMonitor {
    client: ImapClient,
    privacy_filter: EmailPrivacyFilter,
}

impl EmailMonitor for IMAPMonitor {
    async fn monitor_email_events(&self) -> Result<Vec<EmailEvent>> {
        // Monitor mailbox changes, extract metadata only
        // No email content captured
    }
}
```

### 10. Hardware Events Monitor
**Impact**: Low (system awareness)  
**Complexity**: Low  
**Privacy Impact**: Low  
**Implementation Time**: 3-4 days  

**Captures**:
- USB device connections/disconnections
- Bluetooth device pairing and connections
- Display configuration changes
- Hardware failure events

**Implementation Strategy**:
```rust
// udev-based hardware monitoring
pub struct HardwareMonitor {
    udev_context: UdevContext,
    device_tracker: DeviceTracker,
}

impl HardwareMonitor {
    async fn monitor_hardware_events(&self) -> Result<()> {
        let monitor = self.udev_context.monitor_builder()
            .match_subsystem("usb")
            .match_subsystem("bluetooth")
            .match_subsystem("drm")
            .create()?;
            
        // Process udev events and convert to Sinex events
    }
}
```

## Implementation Framework

### Adaptive Batching System
```rust
pub struct AdaptiveBatcher {
    min_batch_size: usize,
    max_batch_size: usize,
    current_rate: f64,
    max_latency: Duration,
}

impl AdaptiveBatcher {
    pub fn calculate_optimal_batch_size(&self) -> usize {
        match self.current_rate {
            r if r > 10000.0 => self.max_batch_size,      // High volume
            r if r > 1000.0 => self.max_batch_size / 2,   // Medium volume
            _ => self.min_batch_size,                      // Low volume
        }
    }
}
```

### Progressive Backpressure Strategy
```rust
pub enum BackpressureMode {
    None,              // < 1K events/sec
    Sampling(f32),     // 1K-10K: sample percentage
    Aggregation,       // 10K-100K: aggregate similar events
    CircuitBreaker,    // >100K: temporary pause
}

pub struct BackpressureController {
    current_mode: BackpressureMode,
    rate_calculator: RateCalculator,
    sample_rate: f32,
}
```

### Hierarchical Privacy Framework
```rust
pub enum PrivacyLevel {
    Public,      // System metrics, anonymous patterns
    Internal,    // File paths, process names
    Sensitive,   // Window titles, command history
    Private,     // Clipboard, input patterns
    Restricted,  // Credentials, personal data
}

pub struct PrivacyEngine {
    level_configs: HashMap<String, PrivacyLevel>,
    content_filters: Vec<Box<dyn ContentFilter>>,
    user_overrides: HashMap<String, PrivacyOverride>,
}
```

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-8)
1. Implement adaptive batching framework
2. Build privacy engine with configurable levels
3. Create event source development toolkit
4. Establish testing infrastructure for new sources

### Phase 2: High-Impact Sources (Weeks 9-16)
1. Browser Activity Monitor (weeks 9-11)
2. Process Execution Tracker (weeks 12-13)
3. Network Activity Monitor (weeks 14-15)
4. Input Pattern Monitor (week 16)

### Phase 3: Context Sources (Weeks 17-24)
1. Screen Capture with OCR (weeks 17-18)
2. Audio Environment Monitor (weeks 19-20)
3. System Performance Monitor (week 21)
4. Hardware Events Monitor (week 22)
5. Integration testing and optimization (weeks 23-24)

### Phase 4: Advanced Sources (Weeks 25-32)
1. Application State Monitors (weeks 25-28)
2. Communication Metadata Monitors (weeks 29-31)
3. Performance optimization and scaling (week 32)

## Success Metrics

- **Coverage**: Increase from 35% → 80%+ of system activity
- **Performance**: Handle 100K+ events/sec sustained load
- **Privacy**: Zero credential/PII leaks, user-controlled levels
- **Reliability**: <0.1% event loss under normal conditions
- **Development Velocity**: New sources implementable in <1 week using toolkit

## Privacy and Ethical Considerations

### Data Minimization Principles
- Capture metadata, not content, wherever possible
- Implement configurable privacy levels per source
- Provide granular user control over what gets captured
- Regular privacy impact assessments

### User Agency
- Clear visibility into what's being captured
- Easy opt-out mechanisms for any source
- Data export and deletion capabilities
- Privacy dashboard for monitoring capture levels

### Security Measures
- Encryption at rest for sensitive event types
- Access control and audit logging
- Secure key management for privacy features
- Regular security assessments and updates

## Dependencies

1. **Plugin Architecture**: Runtime extensible source loading
2. **Privacy Framework**: Configurable privacy controls
3. **Enhanced Worker System**: Handle high-volume event processing
4. **Storage Optimization**: Efficient storage for increased event volume
5. **Configuration System**: Per-source privacy and capture settings

## Future Considerations

### Machine Learning Integration
- Behavioral pattern recognition across sources
- Anomaly detection for security monitoring
- Productivity optimization insights
- Context-aware event prioritization

### Cross-Device Synchronization
- Multi-device event correlation
- Distributed privacy controls
- Conflict resolution for overlapping events
- Unified personal timeline across devices

This TIM represents the path from current 35% coverage to comprehensive 80%+ system observation while maintaining strict privacy controls and user agency over their digital footprint.