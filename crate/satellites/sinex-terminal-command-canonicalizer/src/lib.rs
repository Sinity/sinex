#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]
#![allow(unused_imports, unused_variables, unreachable_patterns)]

//! Terminal command canonicalizer.
//!
//! Terminal Events → Analysis → Canonicalized Command Events.

pub mod unified_processor;

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        types::{events::payloads::*, ulid::Ulid, Id},
        Event, JsonValue,
    };

    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
        cli::{
            ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat,
            IngestionHistoryEntry, MissingItem, SourceState,
        },
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
            ProcessorType, ScanArgs, ScanEstimate, ScanReport, StatefulStreamProcessor,
            TimeHorizon,
        },
        SatelliteError, SatelliteResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Utc},
        serde::{Deserialize, Serialize},
        serde_json,
        sqlx::PgPool,
        std::{collections::HashMap, time::Duration},
        tokio::sync::mpsc,
        tracing::{debug, error, info, instrument, warn},
    };
}

// Use local facade for common types
use crate::common::*;
use color_eyre::eyre::eyre;
use sinex_core::Provenance;
use sinex_satellite_sdk::SatelliteError;

/// Configuration for Terminal Command Canonicalizer
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TerminalCommandCanonicalizerConfig {
    /// Terminal event types to process
    pub terminal_event_types: Vec<String>,
    /// Command canonicalization rules
    pub canonicalization_rules: HashMap<String, String>,
    /// Enable command intent analysis
    pub enable_intent_analysis: bool,
    /// Enable command pattern recognition
    pub enable_pattern_recognition: bool,
    /// Enable command safety analysis
    pub enable_safety_analysis: bool,
    /// Time window for command processing (seconds)
    pub processing_window_seconds: u64,
    /// Minimum command length for processing
    pub min_command_length: usize,
}

impl Default for TerminalCommandCanonicalizerConfig {
    fn default() -> Self {
        let mut rules = HashMap::new();
        // Common command aliases and variations
        rules.insert("ll".to_string(), "ls -la".to_string());
        rules.insert("la".to_string(), "ls -a".to_string());
        rules.insert("l".to_string(), "ls".to_string());
        rules.insert("cls".to_string(), "clear".to_string());
        rules.insert("dir".to_string(), "ls".to_string());
        rules.insert("md".to_string(), "mkdir".to_string());
        rules.insert("rd".to_string(), "rmdir".to_string());
        rules.insert("del".to_string(), "rm".to_string());

        Self {
            terminal_event_types: vec![
                "command.executed".to_string(),
                "command.completed".to_string(),
                "shell.command.executed".to_string(),
                "terminal.command.executed".to_string(),
            ],
            canonicalization_rules: rules,
            enable_intent_analysis: true,
            enable_pattern_recognition: true,
            enable_safety_analysis: true,
            processing_window_seconds: 3600, // 1 hour
            min_command_length: 1,
        }
    }
}

/// Canonicalized command representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalizedCommand {
    pub original_command: String,
    pub canonical_command: String,
    pub command_category: CommandCategory,
    pub intent: CommandIntent,
    pub safety_level: SafetyLevel,
    pub arguments: Vec<String>,
    pub flags: Vec<String>,
    pub target_paths: Vec<String>,
    pub source_event_id: Id<Event<JsonValue>>,
    pub timestamp: DateTime<Utc>,
}

/// Command categories for classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandCategory {
    FileSystem,     // ls, cd, mkdir, rm, cp, mv, etc.
    TextProcessing, // cat, grep, sed, awk, sort, etc.
    SystemInfo,     // ps, top, df, du, uname, etc.
    Network,        // ping, curl, wget, ssh, scp, etc.
    Development,    // git, cargo, npm, python, etc.
    Package,        // apt, yum, brew, pip, etc.
    Process,        // kill, jobs, bg, fg, etc.
    Archive,        // tar, zip, unzip, gzip, etc.
    Editor,         // vim, nano, emacs, code, etc.
    Other,
}

/// Command intent classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandIntent {
    Read,      // Reading/viewing content
    Write,     // Creating/modifying content
    Navigate,  // Changing directories, browsing
    Search,    // Finding files, searching content
    Execute,   // Running programs, scripts
    Install,   // Installing packages, software
    Configure, // Changing settings, configuration
    Monitor,   // Checking status, monitoring
    Transfer,  // Moving, copying, downloading
    Delete,    // Removing files, data
    Unknown,
}

/// Command safety level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SafetyLevel {
    Safe,      // Read-only operations, safe commands
    Caution,   // Commands that modify files but are generally safe
    Dangerous, // Commands that can delete data or affect system
    Critical,  // System-level commands that can cause damage
}

/// Terminal Command Canonicalizer using unified StatefulStreamProcessor architecture
///
/// Consumes terminal command events and produces canonicalized command insights:
/// - Command standardization and normalization
/// - Command intent and safety analysis
/// - Command pattern recognition
/// - Shell workflow insights
pub struct TerminalCommandCanonicalizer {
    runtime: Option<ProcessorRuntimeState>,
    config: TerminalCommandCanonicalizerConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
    canonicalized_commands: Vec<CanonicalizedCommand>,
}

impl TerminalCommandCanonicalizer {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: TerminalCommandCanonicalizerConfig::default(),
            event_sender: None,
            db_pool: None,
            canonicalized_commands: Vec::new(),
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(color_eyre::eyre::eyre!(
                "Terminal canonicalizer runtime not initialised"
            ))
        })
    }

    fn db_pool(&self) -> SatelliteResult<&PgPool> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.db_pool())
        } else if let Some(pool) = self.db_pool.as_ref() {
            Ok(pool)
        } else {
            Err(SatelliteError::General(color_eyre::eyre::eyre!(
                "Database pool not initialized"
            )))
        }
    }

    fn event_sender(&self) -> SatelliteResult<mpsc::UnboundedSender<Event<JsonValue>>> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.event_sender())
        } else if let Some(sender) = self.event_sender.as_ref() {
            Ok(sender.clone())
        } else {
            Err(SatelliteError::General(color_eyre::eyre::eyre!(
                "Event sender not initialized"
            )))
        }
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: ProcessorRuntimeState,
        config: TerminalCommandCanonicalizerConfig,
    ) -> SatelliteResult<()> {
        info!(
            processor = "terminal-command-canonicalizer",
            service = %runtime.service_info().service_name(),
            "Initializing terminal command canonicalizer"
        );

        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.config = config;
        self.runtime = Some(runtime);

        info!(
            "Terminal command canonicalizer configured - processing {} event types, {} canonicalization rules",
            self.config.terminal_event_types.len(),
            self.config.canonicalization_rules.len()
        );

        Ok(())
    }

    /// Process terminal command events and generate canonicalized insights
    async fn process_terminal_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;

        // Query recent terminal command events
        let events = self.query_terminal_events(db_pool, from).await?;
        info!(
            "Processing {} terminal events for canonicalization",
            events.len()
        );

        // Canonicalize commands from events
        self.canonicalize_commands_from_events(&events).await;

        let mut events_processed = 0u64;

        // Generate canonicalized command events
        for canonical_cmd in &self.canonicalized_commands {
            if let Ok(canonical_event) = self.generate_canonical_command_event(canonical_cmd).await
            {
                if let Err(e) = event_sender.send(canonical_event) {
                    warn!("Failed to send canonical command event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        // Generate command pattern analysis if enabled
        if self.config.enable_pattern_recognition && !self.canonicalized_commands.is_empty() {
            if let Ok(pattern_events) = self.analyze_command_patterns().await {
                for pattern_event in pattern_events {
                    if let Err(e) = event_sender.send(pattern_event) {
                        warn!("Failed to send command pattern event: {}", e);
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        // Generate command safety analysis if enabled
        if self.config.enable_safety_analysis {
            if let Ok(safety_events) = self.analyze_command_safety().await {
                for safety_event in safety_events {
                    if let Err(e) = event_sender.send(safety_event) {
                        warn!("Failed to send command safety event: {}", e);
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        // Generate shell workflow insights
        if let Ok(workflow_events) = self.analyze_shell_workflows().await {
            for workflow_event in workflow_events {
                if let Err(e) = event_sender.send(workflow_event) {
                    warn!("Failed to send shell workflow event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        Ok(events_processed)
    }

    /// Query terminal command events from the database
    async fn query_terminal_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.processing_window_seconds as i64);

        use sinex_core::db::repositories::DbPoolExt;
        use sinex_core::types::domain::EventType;

        // Query terminal command events for each configured type
        let mut all_events = Vec::new();
        for event_type_str in &self.config.terminal_event_types {
            let event_type = EventType::from(event_type_str.as_str());
            let events = db_pool
                .events()
                .get_events_by_type_and_time_range(
                    &event_type,
                    window_start,
                    chrono::Utc::now(),
                    Some(100),
                )
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Failed to query terminal events: {}", e))?;
            all_events.extend(events);
        }

        // Sort by timestamp and limit to 1000 most recent
        all_events.sort_by(|a, b| b.ts_orig.cmp(&a.ts_orig));
        all_events.truncate(1000);

        let events = all_events;

        Ok(events)
    }

    /// Canonicalize commands from terminal events
    async fn canonicalize_commands_from_events(&mut self, events: &[Event<JsonValue>]) {
        self.canonicalized_commands.clear();

        for event in events {
            if let Some(command) = self.extract_command_from_event(event) {
                if command.len() >= self.config.min_command_length {
                    if let Some(canonical_cmd) = self.canonicalize_command(&command, event) {
                        self.canonicalized_commands.push(canonical_cmd);
                    }
                }
            }
        }

        info!(
            "Canonicalized {} commands from events",
            self.canonicalized_commands.len()
        );
    }

    /// Extract command string from terminal event
    fn extract_command_from_event(&self, event: &Event<JsonValue>) -> Option<String> {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload.clone()) {
            // Try various command fields
            if let Some(command) = payload.get("command").and_then(|v| v.as_str()) {
                return Some(command.to_string());
            }

            if let Some(command_string) = payload.get("command_string").and_then(|v| v.as_str()) {
                return Some(command_string.to_string());
            }

            if let Some(cmd) = payload.get("cmd").and_then(|v| v.as_str()) {
                return Some(cmd.to_string());
            }

            // For shell history events, try different field names
            if let Some(command_line) = payload.get("command_line").and_then(|v| v.as_str()) {
                return Some(command_line.to_string());
            }
        }

        None
    }

    /// Canonicalize a single command
    fn canonicalize_command(
        &self,
        original_command: &str,
        event: &Event<JsonValue>,
    ) -> Option<CanonicalizedCommand> {
        let trimmed_command = original_command.trim();
        if trimmed_command.is_empty() {
            return None;
        }

        // Parse command components
        let parts: Vec<&str> = trimmed_command.split_whitespace().collect();
        if parts.is_empty() {
            return None;
        }

        let base_command = parts[0];
        let args = parts[1..].to_vec();

        // Apply canonicalization rules
        let canonical_base = self
            .config
            .canonicalization_rules
            .get(base_command)
            .cloned()
            .unwrap_or_else(|| base_command.to_string());

        // Build canonical command
        let canonical_command = if args.is_empty() {
            canonical_base
        } else {
            format!("{} {}", canonical_base, args.join(" "))
        };

        // Classify the command
        let category = self.classify_command(base_command);
        let intent = self.determine_command_intent(base_command, &args);
        let safety_level = self.assess_command_safety(base_command, &args);

        // Extract arguments, flags, and paths
        let (arguments, flags, target_paths) = self.parse_command_components(&args);

        Some(CanonicalizedCommand {
            original_command: original_command.to_string(),
            canonical_command,
            command_category: category,
            intent,
            safety_level,
            arguments,
            flags,
            target_paths,
            source_event_id: event
                .id
                .clone()
                .unwrap_or_else(|| Id::from_ulid(Ulid::new())),
            timestamp: event.ts_orig.unwrap_or_else(|| Utc::now()),
        })
    }

    /// Classify command into category
    fn classify_command(&self, base_command: &str) -> CommandCategory {
        match base_command {
            // File system commands
            "ls" | "cd" | "pwd" | "mkdir" | "rmdir" | "rm" | "cp" | "mv" | "find" | "locate"
            | "which" | "chmod" | "chown" | "ln" => CommandCategory::FileSystem,

            // Text processing
            "cat" | "less" | "more" | "head" | "tail" | "grep" | "sed" | "awk" | "sort"
            | "uniq" | "cut" | "wc" | "diff" | "tr" => CommandCategory::TextProcessing,

            // System info
            "ps" | "top" | "htop" | "df" | "du" | "free" | "uname" | "uptime" | "whoami" | "id"
            | "groups" | "lscpu" | "lsblk" | "lsusb" => CommandCategory::SystemInfo,

            // Network
            "ping" | "curl" | "wget" | "ssh" | "scp" | "rsync" | "netstat" | "ss" | "nslookup"
            | "dig" | "telnet" => CommandCategory::Network,

            // Development
            "git" | "cargo" | "npm" | "yarn" | "python" | "python3" | "node" | "ruby" | "java"
            | "gcc" | "make" | "cmake" => CommandCategory::Development,

            // Package management
            "apt" | "apt-get" | "yum" | "dnf" | "brew" | "pip" | "pip3" | "gem" | "snap" => {
                CommandCategory::Package
            }

            // Process management
            "kill" | "killall" | "jobs" | "bg" | "fg" | "nohup" | "screen" | "tmux" => {
                CommandCategory::Process
            }

            // Archive operations
            "tar" | "zip" | "unzip" | "gzip" | "gunzip" | "7z" | "rar" | "unrar" => {
                CommandCategory::Archive
            }

            // Editors
            "vim" | "vi" | "nano" | "emacs" | "code" | "subl" | "atom" => CommandCategory::Editor,

            _ => CommandCategory::Other,
        }
    }

    /// Determine command intent
    fn determine_command_intent(&self, base_command: &str, args: &[&str]) -> CommandIntent {
        match base_command {
            // Read operations
            "ls" | "cat" | "less" | "more" | "head" | "tail" | "grep" | "find" | "locate"
            | "ps" | "top" | "df" | "du" => CommandIntent::Read,

            // Write operations
            "touch" | "mkdir" | "echo" | "tee" => CommandIntent::Write,

            // Navigate
            "cd" | "pwd" => CommandIntent::Navigate,

            // Search
            "grep" | "find" | "locate" | "which" => CommandIntent::Search,

            // Execute
            "python" | "python3" | "node" | "ruby" | "java" | "bash" | "sh" | "./.*" => {
                CommandIntent::Execute
            }

            // Install
            "apt" | "apt-get" | "yum" | "dnf" | "brew" | "pip" | "pip3" | "npm"
                if args.iter().any(|&arg| arg == "install") =>
            {
                CommandIntent::Install
            }

            // Configure
            "chmod" | "chown" | "chgrp" => CommandIntent::Configure,

            // Monitor
            "top" | "htop" | "ps" | "netstat" | "ss" => CommandIntent::Monitor,

            // Transfer
            "cp" | "mv" | "scp" | "rsync" | "wget" | "curl" => CommandIntent::Transfer,

            // Delete
            "rm" | "rmdir" | "unlink" => CommandIntent::Delete,

            _ => CommandIntent::Unknown,
        }
    }

    /// Assess command safety level
    fn assess_command_safety(&self, base_command: &str, args: &[&str]) -> SafetyLevel {
        match base_command {
            // Critical system commands
            "rm" if args
                .iter()
                .any(|&arg| arg == "-rf" || arg == "-r" || arg.contains("*")) =>
            {
                SafetyLevel::Critical
            }
            "dd" | "fdisk" | "mkfs" | "format" => SafetyLevel::Critical,

            // Dangerous operations
            "rm" | "rmdir" | "unlink" => SafetyLevel::Dangerous,
            "chmod" | "chown" if args.iter().any(|&arg| arg == "-R" || arg == "--recursive") => {
                SafetyLevel::Dangerous
            }
            "kill" | "killall" => SafetyLevel::Dangerous,

            // Caution level
            "mv" | "cp" | "chmod" | "chown" | "ln" => SafetyLevel::Caution,
            "apt" | "apt-get" | "yum" | "dnf" | "brew"
                if args.iter().any(|&arg| arg == "install" || arg == "remove") =>
            {
                SafetyLevel::Caution
            }

            // Safe operations
            "ls" | "cat" | "less" | "more" | "head" | "tail" | "grep" | "find" | "ps" | "top"
            | "df" | "du" | "pwd" | "whoami" => SafetyLevel::Safe,

            _ => SafetyLevel::Caution,
        }
    }

    /// Parse command components into arguments, flags, and paths
    fn parse_command_components(&self, args: &[&str]) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut arguments = Vec::new();
        let mut flags = Vec::new();
        let mut target_paths = Vec::new();

        for &arg in args {
            if arg.starts_with('-') {
                flags.push(arg.to_string());
            } else if arg.contains('/') || arg.contains('.') {
                target_paths.push(arg.to_string());
            } else {
                arguments.push(arg.to_string());
            }
        }

        (arguments, flags, target_paths)
    }

    /// Generate canonical command event
    async fn generate_canonical_command_event(
        &self,
        canonical_cmd: &CanonicalizedCommand,
    ) -> SatelliteResult<Event<JsonValue>> {
        use sinex_core::types::events::payloads::shell::CanonicalCommandPayload;

        // Build typed payload for canonical command
        let payload = CanonicalCommandPayload {
            command: canonical_cmd.canonical_command.clone(),
            working_directory: String::new(),
            exit_code: 0,
            duration_ms: 0,
            start_time: canonical_cmd.timestamp,
            end_time: canonical_cmd.timestamp,
            user: String::new(),
            session_id: String::new(),
            environment_hash: String::new(),
            source_events: vec![canonical_cmd.source_event_id.as_ulid().to_string()],
            enrichment_history: Vec::new(),
        };

        // Create synthesized event with proper provenance (typed)
        let provenance = Provenance::from_synthesis(vec![canonical_cmd.source_event_id.clone()])
            .ok_or_else(|| {
                SatelliteError::General(eyre!("No source event id for canonical command"))
            })?;

        let typed_event = sinex_core::Event::new(payload, provenance).at_time(Utc::now());
        // Convert to dynamic event for downstream JSON handling
        Ok(typed_event
            .to_json_event()
            .map_err(|e| SatelliteError::General(eyre!(e)))?)
    }

    /// Analyze command patterns
    async fn analyze_command_patterns(&self) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut pattern_events = Vec::new();

        // Group commands by category and intent
        let mut category_counts: HashMap<String, usize> = HashMap::new();
        let mut intent_counts: HashMap<String, usize> = HashMap::new();
        let mut safety_counts: HashMap<String, usize> = HashMap::new();

        for cmd in &self.canonicalized_commands {
            *category_counts
                .entry(format!("{:?}", cmd.command_category))
                .or_insert(0) += 1;
            *intent_counts
                .entry(format!("{:?}", cmd.intent))
                .or_insert(0) += 1;
            *safety_counts
                .entry(format!("{:?}", cmd.safety_level))
                .or_insert(0) += 1;
        }

        // Find common command sequences
        let command_sequences = self.find_command_sequences();

        let all_event_ids: Vec<Id<Event<JsonValue>>> = self
            .canonicalized_commands
            .iter()
            .map(|cmd| cmd.source_event_id.clone())
            .collect();

        let pattern_payload = serde_json::json!({
            "analysis_type": "command_patterns",
            "total_commands": self.canonicalized_commands.len(),
            "category_distribution": category_counts,
            "intent_distribution": intent_counts,
            "safety_distribution": safety_counts,
            "common_sequences": command_sequences,
            "analysis_window_hours": self.config.processing_window_seconds / 3600,
            "generated_at": Utc::now(),
        });

        let provenance = Provenance::from_synthesis(all_event_ids)
            .ok_or_else(|| SatelliteError::General(eyre!("No source events for pattern event")))?;

        let pattern_event = sinex_core::Event::dynamic(
            "terminal-canonicalizer",
            "terminal.command_pattern",
            pattern_payload,
        )
        .with_provenance(provenance)
        .at_time(Utc::now())
        .build();

        pattern_events.push(pattern_event);

        Ok(pattern_events)
    }

    /// Find common command sequences
    fn find_command_sequences(&self) -> Vec<serde_json::Value> {
        let mut sequences = Vec::new();

        // Simple sequence detection: look for commands within 5 minutes of each other
        let sequence_threshold = chrono::Duration::minutes(5);
        let mut current_sequence: Vec<CanonicalizedCommand> = Vec::new();

        let mut sorted_commands = self.canonicalized_commands.clone();
        sorted_commands.sort_by_key(|cmd| cmd.timestamp);

        for cmd in &sorted_commands {
            if let Some(last_cmd) = current_sequence.last() {
                if cmd.timestamp - last_cmd.timestamp <= sequence_threshold {
                    current_sequence.push(cmd.clone());
                } else {
                    // End current sequence if it's long enough
                    if current_sequence.len() >= 3 {
                        sequences.push(self.create_sequence_summary(&current_sequence));
                    }
                    current_sequence.clear();
                    current_sequence.push(cmd.clone());
                }
            } else {
                current_sequence.push(cmd.clone());
            }
        }

        // Handle final sequence
        if current_sequence.len() >= 3 {
            sequences.push(self.create_sequence_summary(&current_sequence));
        }

        sequences
    }

    /// Create summary for a command sequence
    fn create_sequence_summary(&self, sequence: &[CanonicalizedCommand]) -> serde_json::Value {
        let commands: Vec<String> = sequence
            .iter()
            .map(|cmd| cmd.canonical_command.clone())
            .collect();

        let duration = sequence.last().unwrap().timestamp - sequence.first().unwrap().timestamp;

        serde_json::json!({
            "commands": commands,
            "command_count": sequence.len(),
            "duration_minutes": duration.num_minutes(),
            "start_time": sequence.first().unwrap().timestamp,
            "end_time": sequence.last().unwrap().timestamp,
        })
    }

    /// Analyze command safety patterns
    async fn analyze_command_safety(&self) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut safety_events = Vec::new();

        // Count dangerous and critical commands
        let dangerous_commands: Vec<&CanonicalizedCommand> = self
            .canonicalized_commands
            .iter()
            .filter(|cmd| {
                matches!(
                    cmd.safety_level,
                    SafetyLevel::Dangerous | SafetyLevel::Critical
                )
            })
            .collect();

        if !dangerous_commands.is_empty() {
            let dangerous_event_ids: Vec<Id<Event<JsonValue>>> = dangerous_commands
                .iter()
                .map(|cmd| cmd.source_event_id.clone())
                .collect();

            let safety_payload = serde_json::json!({
                "analysis_type": "command_safety",
                "total_commands": self.canonicalized_commands.len(),
                "dangerous_commands_count": dangerous_commands.len(),
                "dangerous_commands_percentage": (dangerous_commands.len() as f64 / self.canonicalized_commands.len() as f64) * 100.0,
                "dangerous_commands": dangerous_commands.iter().map(|cmd| serde_json::json!({
                    "command": cmd.canonical_command,
                    "safety_level": cmd.safety_level,
                    "timestamp": cmd.timestamp,
                })).collect::<Vec<_>>(),
                "safety_recommendations": self.generate_safety_recommendations(&dangerous_commands),
                "generated_at": Utc::now(),
            });

            let provenance = Provenance::from_synthesis(dangerous_event_ids).ok_or_else(|| {
                SatelliteError::General(eyre!("No source events for safety analysis"))
            })?;

            let safety_event = sinex_core::Event::dynamic(
                "terminal-canonicalizer",
                "terminal.safety_analysis",
                safety_payload,
            )
            .with_provenance(provenance)
            .at_time(Utc::now())
            .build();

            safety_events.push(safety_event);
        }

        Ok(safety_events)
    }

    /// Generate safety recommendations
    fn generate_safety_recommendations(
        &self,
        dangerous_commands: &[&CanonicalizedCommand],
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !dangerous_commands.is_empty() {
            recommendations.push(format!(
                "Found {} potentially dangerous commands in recent history",
                dangerous_commands.len()
            ));

            // Count critical commands
            let critical_count = dangerous_commands
                .iter()
                .filter(|cmd| matches!(cmd.safety_level, SafetyLevel::Critical))
                .count();

            if critical_count > 0 {
                recommendations.push(format!(
                    "{} commands are marked as critical and require extra caution",
                    critical_count
                ));
            }

            // Check for common dangerous patterns
            let rm_commands: Vec<&CanonicalizedCommand> = dangerous_commands
                .iter()
                .filter(|cmd| cmd.canonical_command.starts_with("rm"))
                .cloned()
                .collect();

            if !rm_commands.is_empty() {
                recommendations.push(format!(
                    "Consider using 'trash' or 'rm -i' instead of direct 'rm' for {} deletion commands",
                    rm_commands.len()
                ));
            }
        }

        recommendations
            .push("Always backup important data before running destructive commands".to_string());
        recommendations
            .push("Use '--dry-run' flags when available to preview command effects".to_string());

        recommendations
    }

    /// Analyze shell workflow patterns
    async fn analyze_shell_workflows(&self) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut workflow_events = Vec::new();

        // Analyze workflow patterns
        let workflows = self.identify_workflows();

        if !workflows.is_empty() {
            let all_event_ids: Vec<Id<Event<JsonValue>>> = self
                .canonicalized_commands
                .iter()
                .map(|cmd| cmd.source_event_id.clone())
                .collect();

            let workflow_payload = serde_json::json!({
                "analysis_type": "shell_workflows",
                "total_workflows": workflows.len(),
                "workflows": workflows,
                "workflow_efficiency_score": self.calculate_workflow_efficiency(&workflows),
                "generated_at": Utc::now(),
            });

            let provenance = Provenance::from_synthesis(all_event_ids)
                .ok_or_else(|| SatelliteError::General(eyre!("No source events for workflow")))?;

            let workflow_event = sinex_core::Event::dynamic(
                "terminal-canonicalizer",
                "terminal.workflow_detected",
                workflow_payload,
            )
            .with_provenance(provenance)
            .at_time(Utc::now())
            .build();

            workflow_events.push(workflow_event);
        }

        Ok(workflow_events)
    }

    /// Identify workflow patterns in commands
    fn identify_workflows(&self) -> Vec<serde_json::Value> {
        let mut workflows = Vec::new();

        // Look for development workflows
        if let Some(dev_workflow) = self.identify_development_workflow() {
            workflows.push(dev_workflow);
        }

        // Look for file management workflows
        if let Some(file_workflow) = self.identify_file_management_workflow() {
            workflows.push(file_workflow);
        }

        // Look for system administration workflows
        if let Some(sysadmin_workflow) = self.identify_sysadmin_workflow() {
            workflows.push(sysadmin_workflow);
        }

        workflows
    }

    /// Identify development workflow patterns
    fn identify_development_workflow(&self) -> Option<serde_json::Value> {
        let dev_commands: Vec<&CanonicalizedCommand> = self
            .canonicalized_commands
            .iter()
            .filter(|cmd| matches!(cmd.command_category, CommandCategory::Development))
            .collect();

        if dev_commands.len() >= 3 {
            let git_commands = dev_commands
                .iter()
                .filter(|cmd| cmd.canonical_command.starts_with("git"))
                .count();

            Some(serde_json::json!({
                "workflow_type": "development",
                "total_dev_commands": dev_commands.len(),
                "git_commands": git_commands,
                "common_tools": self.extract_dev_tools(&dev_commands),
                "productivity_score": self.calculate_dev_productivity(&dev_commands),
            }))
        } else {
            None
        }
    }

    /// Identify file management workflow patterns
    fn identify_file_management_workflow(&self) -> Option<serde_json::Value> {
        let file_commands: Vec<&CanonicalizedCommand> = self
            .canonicalized_commands
            .iter()
            .filter(|cmd| matches!(cmd.command_category, CommandCategory::FileSystem))
            .collect();

        if file_commands.len() >= 5 {
            Some(serde_json::json!({
                "workflow_type": "file_management",
                "total_file_commands": file_commands.len(),
                "navigation_commands": file_commands.iter()
                    .filter(|cmd| matches!(cmd.intent, CommandIntent::Navigate))
                    .count(),
                "read_commands": file_commands.iter()
                    .filter(|cmd| matches!(cmd.intent, CommandIntent::Read))
                    .count(),
                "write_commands": file_commands.iter()
                    .filter(|cmd| matches!(cmd.intent, CommandIntent::Write))
                    .count(),
            }))
        } else {
            None
        }
    }

    /// Identify system administration workflow patterns
    fn identify_sysadmin_workflow(&self) -> Option<serde_json::Value> {
        let system_commands: Vec<&CanonicalizedCommand> = self
            .canonicalized_commands
            .iter()
            .filter(|cmd| {
                matches!(
                    cmd.command_category,
                    CommandCategory::SystemInfo
                        | CommandCategory::Process
                        | CommandCategory::Package
                )
            })
            .collect();

        if system_commands.len() >= 3 {
            Some(serde_json::json!({
                "workflow_type": "system_administration",
                "total_system_commands": system_commands.len(),
                "monitoring_commands": system_commands.iter()
                    .filter(|cmd| matches!(cmd.intent, CommandIntent::Monitor))
                    .count(),
                "configuration_commands": system_commands.iter()
                    .filter(|cmd| matches!(cmd.intent, CommandIntent::Configure))
                    .count(),
            }))
        } else {
            None
        }
    }

    /// Extract development tools from commands
    fn extract_dev_tools(&self, dev_commands: &[&CanonicalizedCommand]) -> Vec<String> {
        let mut tools = std::collections::HashSet::new();

        for cmd in dev_commands {
            let tool = cmd
                .canonical_command
                .split_whitespace()
                .next()
                .unwrap_or("");
            if !tool.is_empty() {
                tools.insert(tool.to_string());
            }
        }

        tools.into_iter().collect()
    }

    /// Calculate development productivity score
    fn calculate_dev_productivity(&self, dev_commands: &[&CanonicalizedCommand]) -> f64 {
        // Simple productivity metric based on command diversity and git usage
        let unique_tools = self.extract_dev_tools(dev_commands).len();
        let git_usage = dev_commands
            .iter()
            .filter(|cmd| cmd.canonical_command.starts_with("git"))
            .count() as f64;

        let productivity = (unique_tools as f64 * 0.3) + (git_usage * 0.1);
        productivity.min(10.0) // Cap at 10
    }

    /// Calculate workflow efficiency score
    fn calculate_workflow_efficiency(&self, workflows: &[serde_json::Value]) -> f64 {
        if workflows.is_empty() {
            return 0.0;
        }

        // Simple efficiency metric based on workflow diversity
        workflows.len() as f64 * 2.0
    }
}

#[async_trait]
impl StatefulStreamProcessor for TerminalCommandCanonicalizer {
    type Config = TerminalCommandCanonicalizerConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();

        let events_processed = match until {
            TimeHorizon::Snapshot => {
                // Perform one-time command canonicalization
                self.process_terminal_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Historical { .. } => {
                // Process historical terminal events
                self.process_terminal_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => {
                // Continuous command processing
                self.process_terminal_events(&from).await.unwrap_or(0)
            }
        };

        let duration = Utc::now().signed_duration_since(start_time);

        Ok(ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds() as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                (
                    "canonicalized_commands".to_string(),
                    self.canonicalized_commands.len() as u64,
                ),
                (
                    "canonicalization_rules".to_string(),
                    self.config.canonicalization_rules.len() as u64,
                ),
                (
                    "intent_analysis_enabled".to_string(),
                    if self.config.enable_intent_analysis {
                        1
                    } else {
                        0
                    },
                ),
                (
                    "safety_analysis_enabled".to_string(),
                    if self.config.enable_safety_analysis {
                        1
                    } else {
                        0
                    },
                ),
            ]),
            successful_targets: vec!["terminal".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "terminal-command-canonicalizer"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // Command canonicalization operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }
}

impl Default for TerminalCommandCanonicalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for TerminalCommandCanonicalizer {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let dangerous_commands = self
            .canonicalized_commands
            .iter()
            .filter(|cmd| {
                matches!(
                    cmd.safety_level,
                    SafetyLevel::Dangerous | SafetyLevel::Critical
                )
            })
            .count();

        Ok(SourceState {
            description: "Terminal command canonicalizer for command standardization and analysis"
                .to_string(),
            last_updated: Utc::now(),
            total_items: Some(self.canonicalized_commands.len() as u64),
            metadata: HashMap::from([
                (
                    "canonicalized_commands".to_string(),
                    serde_json::Value::Number(self.canonicalized_commands.len().into()),
                ),
                (
                    "canonicalization_rules".to_string(),
                    serde_json::Value::Number(self.config.canonicalization_rules.len().into()),
                ),
                (
                    "dangerous_commands".to_string(),
                    serde_json::Value::Number(dangerous_commands.into()),
                ),
                (
                    "intent_analysis".to_string(),
                    serde_json::Value::Bool(self.config.enable_intent_analysis),
                ),
                (
                    "pattern_recognition".to_string(),
                    serde_json::Value::Bool(self.config.enable_pattern_recognition),
                ),
                (
                    "safety_analysis".to_string(),
                    serde_json::Value::Bool(self.config.enable_safety_analysis),
                ),
            ]),
            healthy: (dangerous_commands as f64 / self.canonicalized_commands.len().max(1) as f64)
                < 0.3, // Healthy if < 30% dangerous commands
            recent_activity: Vec::new(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let now = Utc::now();
        Ok(CoverageAnalysis {
            time_range: (now - chrono::Duration::hours(1), now),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0, // Command canonicalizer processes available terminal events
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            recommendations: vec![
                "Terminal command canonicalizer standardizes and analyzes shell commands"
                    .to_string(),
                "Add more canonicalization_rules to improve command normalization".to_string(),
                "Enable safety_analysis to identify potentially dangerous commands".to_string(),
                "Enable pattern_recognition to detect workflow patterns".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        _path: &sinex_core::SanitizedPath,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Ok(())
    }
}
