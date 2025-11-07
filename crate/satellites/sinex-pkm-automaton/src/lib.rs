#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]

//! PKM automaton.
//!
//! Knowledge Events → Analysis → Synthesized PKM Insights.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::Event,
        types::{events::payloads::*, Id},
    };

    pub use sinex_processor_runtime::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    };
    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
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

/// Configuration for PKM Automaton
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PKMAutomatonConfig {
    /// Knowledge-related event types to analyze
    pub knowledge_event_types: Vec<String>,
    /// Enable knowledge extraction from documents
    pub enable_knowledge_extraction: bool,
    /// Enable knowledge graph building
    pub enable_knowledge_graph: bool,
    /// Enable learning session tracking
    pub enable_learning_tracking: bool,
    /// Time window for knowledge analysis (seconds)
    pub analysis_window_seconds: u64,
    /// Minimum knowledge items for pattern recognition
    pub min_knowledge_items_for_patterns: usize,
}

impl Default for PKMAutomatonConfig {
    fn default() -> Self {
        Self {
            knowledge_event_types: vec![
                "document.created".to_string(),
                "document.modified".to_string(),
                "document.read".to_string(),
                "file.created".to_string(),
                "file.modified".to_string(),
                "command.executed".to_string(),
                "web.page_visited".to_string(),
                "clipboard.content.captured".to_string(),
            ],
            enable_knowledge_extraction: true,
            enable_knowledge_graph: true,
            enable_learning_tracking: true,
            analysis_window_seconds: 7200, // 2 hours
            min_knowledge_items_for_patterns: 3,
        }
    }
}

/// Knowledge item extracted from events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeItem {
    pub item_type: KnowledgeItemType,
    pub title: String,
    pub content: Option<String>,
    pub keywords: Vec<String>,
    pub related_paths: Vec<String>,
    pub timestamp: DateTime<Utc>,
    pub source_event_id: Id<Event<JsonValue>>,
}

/// Types of knowledge items we can extract
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnowledgeItemType {
    Document,
    Code,
    Command,
    WebPage,
    Note,
    Reference,
    Learning,
}

/// PKM Automaton using unified StatefulStreamProcessor architecture
///
/// Consumes events related to knowledge work and produces PKM insights:
/// - Knowledge extraction from documents and interactions
/// - Learning session tracking and analysis
/// - Knowledge graph relationship building
/// - Personal workflow pattern recognition
pub struct PKMAutomaton {
    runtime: Option<ProcessorRuntimeState>,
    config: PKMAutomatonConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
    knowledge_items: Vec<KnowledgeItem>,
}

impl PKMAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: PKMAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
            knowledge_items: Vec::new(),
        }
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

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(color_eyre::eyre::eyre!(
                "PKM automaton runtime not initialised"
            ))
        })
    }

    /// Process knowledge events and generate PKM insights
    async fn process_knowledge_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;

        // Query recent knowledge events
        let events = self.query_knowledge_events(db_pool, from).await?;
        info!("Processing {} events for PKM analysis", events.len());

        // Extract knowledge items from events
        self.extract_knowledge_items(&events).await;

        let mut events_processed = 0u64;

        // Generate knowledge extraction insights if enabled
        if self.config.enable_knowledge_extraction && !self.knowledge_items.is_empty() {
            if let Ok(extraction_event) = self.generate_knowledge_extraction_insights().await {
                if let Err(e) = event_sender.send(extraction_event) {
                    warn!("Failed to send knowledge extraction event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        // Generate learning session tracking if enabled
        if self.config.enable_learning_tracking {
            if let Ok(learning_events) = self.track_learning_sessions(&events).await {
                for learning_event in learning_events {
                    if let Err(e) = event_sender.send(learning_event) {
                        warn!("Failed to send learning tracking event: {}", e);
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        // Generate knowledge graph insights if enabled
        if self.config.enable_knowledge_graph
            && self.knowledge_items.len() >= self.config.min_knowledge_items_for_patterns
        {
            if let Ok(graph_events) = self.build_knowledge_graph_insights().await {
                for graph_event in graph_events {
                    if let Err(e) = event_sender.send(graph_event) {
                        warn!("Failed to send knowledge graph event: {}", e);
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        // Generate workflow pattern insights
        if let Ok(workflow_events) = self.analyze_workflow_patterns(&events).await {
            for workflow_event in workflow_events {
                if let Err(e) = event_sender.send(workflow_event) {
                    warn!("Failed to send workflow pattern event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        Ok(events_processed)
    }

    /// Query knowledge-related events from the database
    async fn query_knowledge_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.analysis_window_seconds as i64);

        let events = db_pool
            .events()
            .get_recent(
                1000,
                Some(window_start),
                Some(&self.config.knowledge_event_types),
            )
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query events: {}", e))?;

        Ok(events)
    }

    /// Extract knowledge items from events
    async fn extract_knowledge_items(&mut self, events: &[Event<JsonValue>]) {
        self.knowledge_items.clear();

        for event in events {
            if let Some(knowledge_item) = self.extract_knowledge_item_from_event(event) {
                self.knowledge_items.push(knowledge_item);
            }
        }

        info!(
            "Extracted {} knowledge items from events",
            self.knowledge_items.len()
        );
    }

    /// Extract a knowledge item from a single event
    fn extract_knowledge_item_from_event(&self, event: &Event<JsonValue>) -> Option<KnowledgeItem> {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload.clone()) {
            let item_type = self.determine_knowledge_item_type(event, &payload);
            let title = self.extract_title(&payload, event);
            let content = self.extract_content(&payload);
            let keywords = self.extract_keywords(&payload, &content);
            let related_paths = self.extract_related_paths(&payload);

            Some(KnowledgeItem {
                item_type,
                title,
                content,
                keywords,
                related_paths,
                timestamp: event.ts_orig,
                source_event_id: event.id,
            })
        } else {
            None
        }
    }

    /// Determine the type of knowledge item based on event and payload
    fn determine_knowledge_item_type(
        &self,
        event: &Event<JsonValue>,
        payload: &serde_json::Value,
    ) -> KnowledgeItemType {
        let event_type_str = event.event_type.to_string();

        // Check for code-related events
        if event_type_str.contains("command")
            || payload.get("command").is_some()
            || payload.get("command_string").is_some()
        {
            return KnowledgeItemType::Command;
        }

        // Check for web-related events
        if event_type_str.contains("web")
            || event_type_str.contains("url")
            || payload.get("url").is_some()
        {
            return KnowledgeItemType::WebPage;
        }

        // Check file extension for code files
        if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
            if path.ends_with(".rs")
                || path.ends_with(".py")
                || path.ends_with(".js")
                || path.ends_with(".ts")
                || path.ends_with(".go")
                || path.ends_with(".c")
                || path.ends_with(".cpp")
                || path.ends_with(".java")
            {
                return KnowledgeItemType::Code;
            }

            // Check for documentation files
            if path.ends_with(".md")
                || path.ends_with(".txt")
                || path.ends_with(".org")
                || path.ends_with(".tex")
                || path.ends_with(".rst")
            {
                return KnowledgeItemType::Document;
            }
        }

        // Check content for code indicators
        if let Some(content) = payload
            .get("content")
            .or_else(|| payload.get("text"))
            .and_then(|v| v.as_str())
        {
            if content.contains("function ")
                || content.contains("def ")
                || content.contains("#include")
                || content.contains("import ")
            {
                return KnowledgeItemType::Code;
            }
        }

        // Default to document for most content
        KnowledgeItemType::Document
    }

    /// Extract title from payload
    fn extract_title(&self, payload: &serde_json::Value, event: &Event<JsonValue>) -> String {
        // Try explicit title field
        if let Some(title) = payload.get("title").and_then(|v| v.as_str()) {
            return title.to_string();
        }

        // Try path-based title
        if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
            if let Some(filename) = path.split('/').last() {
                return filename.to_string();
            }
        }

        // Try command-based title
        if let Some(command) = payload
            .get("command")
            .or_else(|| payload.get("command_string"))
            .and_then(|v| v.as_str())
        {
            return format!(
                "Command: {}",
                command
                    .split_whitespace()
                    .take(3)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }

        // Try URL-based title
        if let Some(url) = payload.get("url").and_then(|v| v.as_str()) {
            return format!("Web: {}", url);
        }

        // Default title based on event type
        format!(
            "{} - {}",
            event.event_type,
            event.ts_orig.format("%H:%M:%S")
        )
    }

    /// Extract content from payload
    fn extract_content(&self, payload: &serde_json::Value) -> Option<String> {
        payload
            .get("content")
            .or_else(|| payload.get("text"))
            .or_else(|| payload.get("data"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Extract keywords from payload and content
    fn extract_keywords(
        &self,
        payload: &serde_json::Value,
        content: &Option<String>,
    ) -> Vec<String> {
        let mut keywords = Vec::new();

        // Extract explicit keywords
        if let Some(kw_array) = payload.get("keywords").and_then(|v| v.as_array()) {
            for kw in kw_array {
                if let Some(keyword) = kw.as_str() {
                    keywords.push(keyword.to_string());
                }
            }
        }

        // Extract from tags
        if let Some(tags_array) = payload.get("tags").and_then(|v| v.as_array()) {
            for tag in tags_array {
                if let Some(tag_str) = tag.as_str() {
                    keywords.push(tag_str.to_string());
                }
            }
        }

        // Simple keyword extraction from content
        if let Some(text) = content {
            // Extract common technical terms
            let tech_terms = [
                "rust",
                "python",
                "javascript",
                "typescript",
                "go",
                "programming",
                "algorithm",
                "data",
                "structure",
                "function",
                "method",
                "class",
                "variable",
                "loop",
                "condition",
                "test",
                "debug",
            ];

            for term in tech_terms {
                if text.to_lowercase().contains(term) {
                    keywords.push(term.to_string());
                }
            }
        }

        keywords
    }

    /// Extract related file paths from payload
    fn extract_related_paths(&self, payload: &serde_json::Value) -> Vec<String> {
        let mut paths = Vec::new();

        if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
            paths.push(path.to_string());
        }

        if let Some(working_dir) = payload
            .get("working_directory")
            .or_else(|| payload.get("cwd"))
            .and_then(|v| v.as_str())
        {
            paths.push(working_dir.to_string());
        }

        paths
    }

    /// Generate knowledge extraction insights
    async fn generate_knowledge_extraction_insights(&self) -> SatelliteResult<Event<JsonValue>> {
        let total_items = self.knowledge_items.len();

        // Count by type
        let mut type_counts = HashMap::new();
        let mut all_keywords = HashMap::new();
        let mut recent_items = Vec::new();

        for item in &self.knowledge_items {
            *type_counts
                .entry(format!("{:?}", item.item_type))
                .or_insert(0) += 1;

            // Count keywords
            for keyword in &item.keywords {
                *all_keywords.entry(keyword.clone()).or_insert(0) += 1;
            }

            // Keep recent items
            if recent_items.len() < 10 {
                recent_items.push(serde_json::json!({
                    "title": item.title,
                    "type": format!("{:?}", item.item_type),
                    "timestamp": item.timestamp,
                    "keywords": item.keywords,
                }));
            }
        }

        // Get top keywords
        let mut keyword_pairs: Vec<_> = all_keywords.into_iter().collect();
        keyword_pairs.sort_by(|a, b| b.1.cmp(&a.1));
        keyword_pairs.truncate(20);

        let source_event_ids: Vec<Id<Event<JsonValue>>> = self
            .knowledge_items
            .iter()
            .map(|item| item.source_event_id)
            .collect();

        let insights_payload = serde_json::json!({
            "analysis_type": "knowledge_extraction",
            "total_knowledge_items": total_items,
            "type_distribution": type_counts,
            "top_keywords": keyword_pairs,
            "recent_items": recent_items,
            "time_window_hours": self.config.analysis_window_seconds / 3600,
            "generated_at": Utc::now(),
        });

        let event = Event::new(
            "pkm-automaton",
            "pkm.knowledge_extraction",
            insights_payload,
            source_event_ids,
        )
        .with_timestamp(Utc::now());

        Ok(event.into())
    }

    /// Track learning sessions based on event patterns
    async fn track_learning_sessions(
        &self,
        events: &[Event<JsonValue>],
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut learning_events = Vec::new();

        // Simple learning session detection: sequences of related knowledge events
        let mut current_session: Option<LearningSession> = None;
        let session_gap_threshold = chrono::Duration::minutes(30);

        for event in events {
            let is_learning_event = self.is_learning_related_event(event);

            if is_learning_event {
                match &mut current_session {
                    Some(session) => {
                        // Check if this event continues the current session
                        if event.ts_orig - session.last_activity <= session_gap_threshold {
                            session.events.push(event.id);
                            session.last_activity = event.ts_orig;
                        } else {
                            // End current session and start new one
                            if session.events.len() >= 3 {
                                learning_events
                                    .push(self.create_learning_session_event(session).await?);
                            }

                            *session = LearningSession {
                                start_time: event.ts_orig,
                                last_activity: event.ts_orig,
                                events: vec![event.id],
                                topics: Vec::new(),
                            };
                        }
                    }
                    None => {
                        // Start new session
                        current_session = Some(LearningSession {
                            start_time: event.ts_orig,
                            last_activity: event.ts_orig,
                            events: vec![event.id],
                            topics: Vec::new(),
                        });
                    }
                }
            }
        }

        // Close final session if it exists
        if let Some(session) = current_session {
            if session.events.len() >= 3 {
                learning_events.push(self.create_learning_session_event(&session).await?);
            }
        }

        Ok(learning_events)
    }

    /// Check if an event is related to learning activities
    fn is_learning_related_event(&self, event: &Event<JsonValue>) -> bool {
        let event_type_str = event.event_type.to_string();
        let source_str = event.source.to_string();

        // Check event types that suggest learning
        event_type_str.contains("document")
            || event_type_str.contains("file")
            || event_type_str.contains("command")
            || event_type_str.contains("web")
            || source_str.contains("browser")
            || source_str.contains("terminal")
            || source_str.contains("editor")
    }

    /// Create a learning session event
    async fn create_learning_session_event(
        &self,
        session: &LearningSession,
    ) -> SatelliteResult<Event<JsonValue>> {
        let duration_minutes = (session.last_activity - session.start_time).num_minutes();

        let session_payload = serde_json::json!({
            "analysis_type": "learning_session",
            "start_time": session.start_time,
            "end_time": session.last_activity,
            "duration_minutes": duration_minutes,
            "activity_count": session.events.len(),
            "intensity": session.events.len() as f64 / (duration_minutes as f64 / 60.0).max(1.0), // activities per hour
            "generated_at": Utc::now(),
        });

        let event = Event::new(
            "pkm-automaton",
            "pkm.learning_session",
            session_payload,
            session.events.clone(),
        )
        .with_timestamp(Utc::now());

        Ok(event.into())
    }

    /// Build knowledge graph insights
    async fn build_knowledge_graph_insights(&self) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut graph_events = Vec::new();

        // Simple knowledge relationship detection
        let mut relationships = Vec::new();

        // Find items that share keywords or paths
        for (i, item1) in self.knowledge_items.iter().enumerate() {
            for item2 in self.knowledge_items.iter().skip(i + 1) {
                let shared_keywords = item1
                    .keywords
                    .iter()
                    .filter(|k| item2.keywords.contains(k))
                    .count();

                let shared_paths = item1
                    .related_paths
                    .iter()
                    .filter(|p| {
                        item2
                            .related_paths
                            .iter()
                            .any(|p2| p2.contains(p) || p.contains(p2))
                    })
                    .count();

                if shared_keywords >= 2 || shared_paths >= 1 {
                    relationships.push(serde_json::json!({
                        "from_title": item1.title,
                        "to_title": item2.title,
                        "relationship_type": if shared_paths > 0 { "path_related" } else { "keyword_related" },
                        "shared_keywords": shared_keywords,
                        "shared_paths": shared_paths,
                        "from_event_id": item1.source_event_id,
                        "to_event_id": item2.source_event_id,
                    }));
                }
            }
        }

        if !relationships.is_empty() {
            let all_event_ids: Vec<Id<Event<JsonValue>>> = self
                .knowledge_items
                .iter()
                .map(|item| item.source_event_id)
                .collect();

            let graph_payload = serde_json::json!({
                "analysis_type": "knowledge_graph",
                "total_nodes": self.knowledge_items.len(),
                "total_relationships": relationships.len(),
                "relationships": relationships,
                "graph_density": (relationships.len() as f64) / (self.knowledge_items.len() as f64).max(1.0),
                "generated_at": Utc::now(),
            });

            let graph_event = Event::new(
                "pkm-automaton",
                "pkm.knowledge_graph",
                graph_payload,
                all_event_ids,
            )
            .with_timestamp(Utc::now());

            graph_events.push(graph_event.into());
        }

        Ok(graph_events)
    }

    /// Analyze workflow patterns in events
    async fn analyze_workflow_patterns(
        &self,
        events: &[Event<JsonValue>],
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut pattern_events = Vec::new();

        // Simple workflow pattern: detect sequences of related activities
        let mut activity_sequences = HashMap::new();
        let mut current_sequence = Vec::new();

        for event in events {
            let activity_type = self.classify_activity_type(event);
            current_sequence.push((activity_type.clone(), event.ts_orig, event.id));

            // Keep sequences within reasonable length
            if current_sequence.len() > 10 {
                current_sequence.remove(0);
            }

            // Look for patterns in the sequence
            if current_sequence.len() >= 3 {
                let pattern = current_sequence
                    .iter()
                    .map(|(activity, _, _)| activity.as_str())
                    .collect::<Vec<_>>()
                    .join(" -> ");

                *activity_sequences.entry(pattern).or_insert(0) += 1;
            }
        }

        // Generate events for common patterns
        for (pattern, frequency) in activity_sequences {
            if frequency >= 3 {
                // Pattern appeared at least 3 times
                let pattern_event_ids: Vec<Id<Event<JsonValue>>> =
                    events.iter().map(|e| e.id).collect();

                let workflow_payload = serde_json::json!({
                    "analysis_type": "workflow_pattern",
                    "pattern": pattern,
                    "frequency": frequency,
                    "pattern_type": "activity_sequence",
                    "generated_at": Utc::now(),
                });

                let pattern_event = Event::new(
                    "pkm-automaton",
                    "pkm.workflow_pattern",
                    workflow_payload,
                    pattern_event_ids,
                )
                .with_timestamp(Utc::now());

                pattern_events.push(pattern_event.into());
            }
        }

        Ok(pattern_events)
    }

    /// Classify activity type from event
    fn classify_activity_type(&self, event: &Event<JsonValue>) -> String {
        let event_type = event.event_type.to_string();
        let source = event.source.to_string();

        if event_type.contains("command") {
            "terminal".to_string()
        } else if event_type.contains("document") || event_type.contains("file") {
            "document".to_string()
        } else if event_type.contains("web") || source.contains("browser") {
            "web".to_string()
        } else if event_type.contains("clipboard") {
            "clipboard".to_string()
        } else {
            "other".to_string()
        }
    }
}

#[derive(Debug, Clone)]
struct LearningSession {
    start_time: DateTime<Utc>,
    last_activity: DateTime<Utc>,
    events: Vec<Id<Event<JsonValue>>>,
    topics: Vec<String>,
}

#[async_trait]
impl StatefulStreamProcessor for PKMAutomaton {
    type Config = PKMAutomatonConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, runtime) = init.into_runtime();
        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.runtime = Some(runtime);
        self.config = config;

        info!(
            "PKM automaton configured - analyzing {} event types, window: {} hours",
            self.config.knowledge_event_types.len(),
            self.config.analysis_window_seconds / 3600
        );

        Ok(())
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
                // Perform one-time PKM analysis
                self.process_knowledge_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Historical { .. } => {
                // Analyze historical knowledge events
                self.process_knowledge_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => {
                // Continuous PKM processing
                self.process_knowledge_events(&from).await.unwrap_or(0)
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
                    "knowledge_items_extracted".to_string(),
                    serde_json::Value::Number(self.knowledge_items.len().into()),
                ),
                (
                    "analysis_window_hours".to_string(),
                    serde_json::Value::Number((self.config.analysis_window_seconds / 3600).into()),
                ),
                (
                    "knowledge_extraction_enabled".to_string(),
                    serde_json::Value::Bool(self.config.enable_knowledge_extraction),
                ),
                (
                    "learning_tracking_enabled".to_string(),
                    serde_json::Value::Bool(self.config.enable_learning_tracking),
                ),
            ]),
            successful_targets: vec!["pkm".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "pkm-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // PKM analysis operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }
}

impl Default for PKMAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for PKMAutomaton {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "PKM automaton for Personal Knowledge Management insights".to_string(),
            last_updated: Utc::now(),
            total_items: Some(self.knowledge_items.len() as u64),
            metadata: HashMap::from([
                (
                    "knowledge_items".to_string(),
                    self.knowledge_items.len().to_string(),
                ),
                (
                    "analysis_window_hours".to_string(),
                    (self.config.analysis_window_seconds / 3600).to_string(),
                ),
                (
                    "knowledge_extraction".to_string(),
                    self.config.enable_knowledge_extraction.to_string(),
                ),
                (
                    "knowledge_graph".to_string(),
                    self.config.enable_knowledge_graph.to_string(),
                ),
                (
                    "learning_tracking".to_string(),
                    self.config.enable_learning_tracking.to_string(),
                ),
            ]),
            healthy: true,
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
            time_range: (now - chrono::Duration::hours(2), now),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0, // PKM processes available knowledge events
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            recommendations: vec![
                "PKM automaton analyzes knowledge work patterns and learning sessions".to_string(),
                "Adjust knowledge_event_types to focus on specific knowledge sources".to_string(),
                "Enable knowledge_graph for relationship analysis between knowledge items"
                    .to_string(),
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
