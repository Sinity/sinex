// Mock implementations for event sources and monitoring
//
// Provides test implementations of event sources that would normally be
// implemented as satellite processes.

use crate::common::prelude::*;
use async_trait::async_trait;
use serde_json::Value;
use sinex_db::RawEvent;
use sinex_satellite_sdk::{
    EventSourceConfig, SatelliteResult, ScanArgs, StatefulStreamProcessor,
    stream_processor::{Checkpoint, ProcessorType, StreamProcessorContext, ScanReport},
};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Configuration context for event sources
#[derive(Debug, Clone)]
pub struct EventSourceContext {
    config: Value,
    pool: Option<DbPool>,
    _redis: Option<String>, // Redis connection string
}

impl EventSourceContext {
    /// Create new event source context
    pub fn new(config: Value) -> Self {
        Self {
            config,
            pool: None,
            _redis: None,
        }
    }

    /// Add database pool to context
    pub fn with_db_pool(mut self, pool: DbPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Get configuration
    pub fn config(&self) -> &Value {
        &self.config
    }

    /// Get database pool
    pub fn pool(&self) -> Option<&DbPool> {
        self.pool.as_ref()
    }
}

/// Mock filesystem monitor for testing
#[derive(Debug)]
pub struct FilesystemMonitor {
    config: EventSourceContext,
    events: Arc<Mutex<Vec<RawEvent>>>,
    supports_scanner: bool,
}

impl FilesystemMonitor {
    /// Initialize filesystem monitor
    pub async fn initialize(config: EventSourceContext) -> SatelliteResult<Self> {
        Ok(Self {
            config,
            events: Arc::new(Mutex::new(Vec::new())),
            supports_scanner: true,
        })
    }

    /// Check if scanner is supported
    pub fn supports_scanner(&self) -> bool {
        self.supports_scanner
    }

    /// Add test event
    pub async fn add_test_event(&self, event: RawEvent) {
        let mut events = self.events.lock().await;
        events.push(event);
    }

    /// Get collected events
    pub async fn get_events(&self) -> Vec<RawEvent> {
        let events = self.events.lock().await;
        events.clone()
    }
    
    /// Run scanner and send events through channel
    pub async fn run_scanner(
        &mut self,
        tx: mpsc::Sender<RawEvent>,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Call the scan method to populate internal events
        self.scan(
            sinex_satellite_sdk::stream_processor::Checkpoint::None,
            sinex_satellite_sdk::stream_processor::TimeHorizon::Continuous,
            args,
        ).await?;
        
        // Send collected events through the channel
        let events = self.get_events().await;
        let events_count = events.len();
        
        for event in events {
            let _ = tx.send(event).await;
        }
        
        Ok(ScanReport {
            events_generated: events_count as u64,
            duration: start_time.elapsed(),
            source_stats: std::collections::HashMap::from([
                ("files_scanned".to_string(), events_count as u64),
            ]),
            time_range: None,
        })
    }
}

#[async_trait]
impl StatefulStreamProcessor for FilesystemMonitor {
    async fn initialize(&mut self, _ctx: StreamProcessorContext) -> SatelliteResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: sinex_satellite_sdk::stream_processor::Checkpoint,
        _until: sinex_satellite_sdk::stream_processor::TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Mock scanning - generate some test events
        let event = EventFactory::new("filesystem").create_event(
            "file.created",
            json!({
                "path": "/test/file.txt",
                "size": 1024,
                "scan": true
            }),
        );

        self.add_test_event(event).await;
        
        Ok(ScanReport {
            events_processed: 1,
            duration: start_time.elapsed(),
            final_checkpoint: from,
            time_range: None,
            processor_stats: std::collections::HashMap::new(),
            successful_targets: vec!["/test/file.txt".to_string()],
            failed_targets: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "filesystem-monitor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

/// Mock terminal monitor for testing
#[derive(Debug)]
pub struct TerminalMonitor {
    config: EventSourceContext,
    events: Arc<Mutex<Vec<RawEvent>>>,
}

impl TerminalMonitor {
    /// Initialize terminal monitor
    pub async fn initialize(config: EventSourceContext) -> SatelliteResult<Self> {
        Ok(Self {
            config,
            events: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Add test command event
    pub async fn add_command_event(&self, command: &str) {
        let event = EventFactory::new("terminal").create_event(
            "command.executed",
            json!({
                "command": command,
                "exit_code": 0,
                "duration_ms": 100
            }),
        );

        let mut events = self.events.lock().await;
        events.push(event);
    }

    /// Get collected events
    pub async fn get_events(&self) -> Vec<RawEvent> {
        let events = self.events.lock().await;
        events.clone()
    }
}

#[async_trait]
impl StatefulStreamProcessor for TerminalMonitor {
    async fn initialize(&mut self, _ctx: StreamProcessorContext) -> SatelliteResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: sinex_satellite_sdk::stream_processor::Checkpoint,
        _until: sinex_satellite_sdk::stream_processor::TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Mock terminal scanning
        self.add_command_event("ls -la").await;
        self.add_command_event("git status").await;
        
        Ok(ScanReport {
            events_processed: 2,
            duration: start_time.elapsed(),
            final_checkpoint: from,
            time_range: None,
            processor_stats: std::collections::HashMap::from([("commands".to_string(), 2)]),
            successful_targets: vec!["ls -la".to_string(), "git status".to_string()],
            failed_targets: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "terminal-monitor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

/// Mock Redis client for testing
#[derive(Debug, Clone)]
pub struct RedisClient {
    connected: bool,
}

impl RedisClient {
    /// Create new Redis client
    pub async fn new() -> SatelliteResult<Self> {
        Ok(Self { connected: true })
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Publish event to stream
    pub async fn publish_event(&self, _stream: &str, _event: &RawEvent) -> SatelliteResult<String> {
        Ok("1234567890-0".to_string()) // Mock message ID
    }

    /// Get stream length
    pub async fn stream_length(&self, _stream: &str) -> SatelliteResult<usize> {
        Ok(0)
    }
}

/// Mock shell history monitor for testing
#[derive(Debug)]
pub struct ShellHistoryMonitor {
    config: EventSourceContext,
    events: Arc<Mutex<Vec<RawEvent>>>,
    supports_scanner: bool,
}

impl ShellHistoryMonitor {
    /// Initialize shell history monitor
    pub async fn initialize(config: EventSourceContext) -> SatelliteResult<Self> {
        Ok(Self {
            config,
            events: Arc::new(Mutex::new(Vec::new())),
            supports_scanner: true,
        })
    }

    /// Check if scanner is supported
    pub fn supports_scanner(&self) -> bool {
        self.supports_scanner
    }

    /// Add test command event
    pub async fn add_command_event(&self, command: &str) {
        let event = EventFactory::new("shell.history").create_event(
            "command.imported",
            json!({
                "command": command,
                "timestamp": Utc::now().to_rfc3339(),
                "session_id": "test_session"
            }),
        );

        let mut events = self.events.lock().await;
        events.push(event);
    }

    /// Get collected events
    pub async fn get_events(&self) -> Vec<RawEvent> {
        let events = self.events.lock().await;
        events.clone()
    }
    
    /// Run scanner and send events through channel
    pub async fn run_scanner(
        &mut self,
        tx: mpsc::Sender<RawEvent>,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        // Call the scan method to populate internal events
        let scan_report = self.scan(
            sinex_satellite_sdk::stream_processor::Checkpoint::None,
            sinex_satellite_sdk::stream_processor::TimeHorizon::Continuous,
            args,
        ).await?;
        
        // Send collected events through the channel
        let events = self.get_events().await;
        
        for event in events {
            let _ = tx.send(event).await;
        }
        
        Ok(scan_report)
    }
}

#[async_trait]
impl StatefulStreamProcessor for ShellHistoryMonitor {
    async fn initialize(&mut self, _ctx: StreamProcessorContext) -> SatelliteResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: sinex_satellite_sdk::stream_processor::Checkpoint,
        _until: sinex_satellite_sdk::stream_processor::TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Mock shell history scanning
        self.add_command_event("cd /home/user").await;
        self.add_command_event("ls -la").await;
        self.add_command_event("git status").await;
        
        Ok(ScanReport {
            events_processed: 3,
            duration: start_time.elapsed(),
            final_checkpoint: from,
            time_range: None,
            processor_stats: std::collections::HashMap::from([("history_entries".to_string(), 3)]),
            successful_targets: vec!["cd /home/user".to_string(), "ls -la".to_string(), "git status".to_string()],
            failed_targets: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "shell-history-monitor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

/// Mock Atuin history importer for testing
#[derive(Debug)]
pub struct AtuinHistoryImporter {
    config: EventSourceContext,
    events: Arc<Mutex<Vec<RawEvent>>>,
    supports_scanner: bool,
}

impl AtuinHistoryImporter {
    /// Initialize Atuin history importer
    pub async fn initialize(config: EventSourceContext) -> SatelliteResult<Self> {
        Ok(Self {
            config,
            events: Arc::new(Mutex::new(Vec::new())),
            supports_scanner: true,
        })
    }

    /// Check if scanner is supported
    pub fn supports_scanner(&self) -> bool {
        self.supports_scanner
    }

    /// Add test command event
    pub async fn add_command_event(&self, command: &str, exit_code: i32) {
        let event = EventFactory::new("shell.atuin").create_event(
            "command.imported",
            json!({
                "command": command,
                "exit_code": exit_code,
                "duration_ms": 1500,
                "timestamp": Utc::now().to_rfc3339(),
                "session_id": "atuin_test_session",
                "hostname": "test_host"
            }),
        );

        let mut events = self.events.lock().await;
        events.push(event);
    }

    /// Get collected events
    pub async fn get_events(&self) -> Vec<RawEvent> {
        let events = self.events.lock().await;
        events.clone()
    }
    
    /// Run scanner and send events through channel
    pub async fn run_scanner(
        &mut self,
        tx: mpsc::Sender<RawEvent>,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Call the scan method to populate internal events
        self.scan(
            sinex_satellite_sdk::stream_processor::Checkpoint::None,
            sinex_satellite_sdk::stream_processor::TimeHorizon::Continuous,
            args,
        ).await?;
        
        // Send collected events through the channel
        let events = self.get_events().await;
        let events_count = events.len();
        
        for event in events {
            let _ = tx.send(event).await;
        }
        
        Ok(ScanReport {
            events_generated: events_count as u64,
            duration: start_time.elapsed(),
            source_stats: std::collections::HashMap::from([
                ("atuin_entries".to_string(), events_count as u64),
            ]),
            time_range: None,
        })
    }
}

#[async_trait]
impl StatefulStreamProcessor for AtuinHistoryImporter {
    async fn initialize(&mut self, _ctx: StreamProcessorContext) -> SatelliteResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: sinex_satellite_sdk::stream_processor::Checkpoint,
        _until: sinex_satellite_sdk::stream_processor::TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Mock Atuin history scanning
        self.add_command_event("cargo build", 0).await;
        self.add_command_event("cargo test", 1).await;
        self.add_command_event("git commit -m 'fix'", 0).await;
        
        Ok(ScanReport {
            events_processed: 3,
            duration: start_time.elapsed(),
            final_checkpoint: from,
            time_range: None,
            processor_stats: std::collections::HashMap::from([("atuin_entries".to_string(), 3)]),
            successful_targets: vec!["cargo build".to_string(), "cargo test".to_string(), "git commit -m 'fix'".to_string()],
            failed_targets: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "atuin-history-importer"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

/// Mock clipboard monitor for testing
#[derive(Debug)]
pub struct ClipboardMonitor {
    config: EventSourceContext,
    _content: Arc<Mutex<String>>,
}

impl ClipboardMonitor {
    /// Initialize clipboard monitor
    pub async fn initialize(config: EventSourceContext) -> SatelliteResult<Self> {
        Ok(Self {
            config,
            _content: Arc::new(Mutex::new(String::new())),
        })
    }
}

#[async_trait]
impl StatefulStreamProcessor for ClipboardMonitor {
    async fn initialize(&mut self, _ctx: StreamProcessorContext) -> SatelliteResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        from: sinex_satellite_sdk::stream_processor::Checkpoint,
        _until: sinex_satellite_sdk::stream_processor::TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        
        // Mock clipboard scanning - no events for now
        
        Ok(ScanReport {
            events_processed: 0,
            duration: start_time.elapsed(),
            final_checkpoint: from,
            time_range: None,
            processor_stats: std::collections::HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "clipboard-monitor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}
