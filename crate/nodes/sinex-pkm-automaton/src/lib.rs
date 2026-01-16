#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! PKM automaton.
//!
//! Knowledge Events → Analysis → Synthesized PKM Insights.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::{models::Event, repositories::DbPoolExt},
        types::{Id, JsonValue, Seconds},
        Ulid,
    };

    pub use sinex_processor_runtime::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        SourceState,
    };
    // SDK facade for common processor types
    pub use sinex_node_sdk::{
        confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent},
        event_processor::EventTransport,
        jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
        stream_processor::{
            Checkpoint, EventSender, Node, ProcessorInitContext, ProcessorRuntimeState,
            ProcessorType, ScanArgs, ScanReport, TimeHorizon,
        },
        NodeError, NodeResult, ProcessingModel,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Utc},
        serde::{Deserialize, Serialize},
        serde_json,
        sqlx::PgPool,
        std::{
            collections::{HashMap, VecDeque},
            sync::Arc,
            time::Duration,
        },
        tokio::{sync::mpsc, task::JoinHandle},
        tracing::{error, info, warn},
    };
}

// Use local facade for common types
use crate::common::*;
use sinex_core::environment;

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
    pub analysis_window_seconds: Seconds,
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
            analysis_window_seconds: Seconds::from_secs(7200), // 2 hours
            min_knowledge_items_for_patterns: 3,
        }
    }
}

const DEFAULT_BATCH_SIZE: usize = 128;
const CONFIRMED_CHANNEL_CAPACITY: usize = 1024;
const MAX_HISTORY_ENTRIES: usize = 32;

#[derive(Default)]
struct PkmAutomatonStats {
    inputs_seen: u64,
    outputs_emitted: u64,
    last_activity: Option<DateTime<Utc>>,
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
    pub source_event_id: Option<Id<Event<JsonValue>>>,
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

/// PKM Automaton using unified Node architecture
///
/// Consumes events related to knowledge work and produces PKM insights:
/// - Knowledge extraction from documents and interactions
/// - Learning session tracking and analysis
/// - Knowledge graph relationship building
/// - Personal workflow pattern recognition
pub struct PKMAutomaton {
    runtime: Option<ProcessorRuntimeState>,
    config: PKMAutomatonConfig,
    event_sender: Option<EventSender>,
    db_pool: Option<PgPool>,
    knowledge_items: Vec<KnowledgeItem>,
    incoming_tx: Option<mpsc::Sender<ProvisionalEvent>>,
    incoming_rx: Option<mpsc::Receiver<ProvisionalEvent>>,
    consumer: Option<Arc<JetStreamEventConsumer>>,
    consumer_handle: Option<JoinHandle<()>>,
    history: VecDeque<IngestionHistoryEntry>,
    stats: PkmAutomatonStats,
}

impl PKMAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: PKMAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
            knowledge_items: Vec::new(),
            incoming_tx: None,
            incoming_rx: None,
            consumer: None,
            consumer_handle: None,
            history: VecDeque::new(),
            stats: PkmAutomatonStats::default(),
        }
    }

    fn runtime(&self) -> NodeResult<&ProcessorRuntimeState> {
        self.runtime
            .as_ref()
            .ok_or_else(|| NodeError::Lifecycle("PKM automaton runtime not initialized".into()))
    }

    fn db_pool(&self) -> NodeResult<&PgPool> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.db_pool())
        } else if let Some(pool) = self.db_pool.as_ref() {
            Ok(pool)
        } else {
            Err(NodeError::General(color_eyre::eyre::eyre!(
                "Database pool not initialized"
            )))
        }
    }

    fn event_sender(&self) -> NodeResult<EventSender> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.event_sender())
        } else if let Some(sender) = self.event_sender.as_ref() {
            Ok(sender.clone())
        } else {
            Err(NodeError::General(color_eyre::eyre::eyre!(
                "Event sender not initialized"
            )))
        }
    }

    fn ensure_event_channel(&mut self) {
        if self.incoming_tx.is_none() || self.incoming_rx.is_none() {
            let (tx, rx) = mpsc::channel(CONFIRMED_CHANNEL_CAPACITY);
            self.incoming_tx = Some(tx);
            self.incoming_rx = Some(rx);
        }
    }

    fn record_history(&mut self, entry: IngestionHistoryEntry) {
        self.history.push_front(entry);
        while self.history.len() > MAX_HISTORY_ENTRIES {
            self.history.pop_back();
        }
    }

    fn record_input(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        self.stats.inputs_seen = self.stats.inputs_seen.saturating_add(count as u64);
        self.stats.last_activity = Some(Utc::now());
    }

    fn record_output(&mut self, count: u64) {
        if count == 0 {
            return;
        }
        self.stats.outputs_emitted = self.stats.outputs_emitted.saturating_add(count);
        self.stats.last_activity = Some(Utc::now());
    }

    fn recent_activity(&self) -> Vec<ActivityEntry> {
        self.history
            .iter()
            .take(5)
            .map(|entry| ActivityEntry {
                timestamp: entry.completed_at.unwrap_or(entry.started_at),
                description: format!("Processed {} events", entry.events_generated),
                data: entry.scan_report.as_ref().map(|report| {
                    serde_json::json!({
                        "events_processed": report.events_processed,
                        "warnings": report.warnings,
                    })
                }),
            })
            .collect()
    }

    async fn ensure_consumer(&mut self) -> NodeResult<()> {
        if let Some(handle) = self.consumer_handle.as_ref() {
            if !handle.is_finished() {
                return Ok(());
            }
        }

        self.consumer_handle = None;
        self.consumer = None;

        let runtime = self.runtime()?;
        let transport = runtime.transport().clone();
        let service_name = runtime.service_info().service_name().to_string();

        let nats_publisher = match transport {
            EventTransport::Nats(publisher) => publisher,
        };

        self.ensure_event_channel();
        let sender = self
            .incoming_tx
            .clone()
            .ok_or_else(|| NodeError::Processing("Confirmed event channel unavailable".into()))?;

        let handler = Arc::new(ChannelConfirmedEventHandler::new(sender));
        let env = environment().clone();
        let config = JetStreamEventConsumerConfig {
            processing_model: ProcessingModel::LeaderStandby,
            batch_size: DEFAULT_BATCH_SIZE,
            confirmation_timeout: Duration::from_secs(60),
            consumer_name: format!("{}-pkm-automaton", service_name.replace('.', "_")),
            enable_provisional_processing: false,
            ..Default::default()
        };

        let consumer = Arc::new(JetStreamEventConsumer::new(
            nats_publisher.nats_client().clone(),
            env,
            config,
            handler,
            None,
        ));

        let consumer_run = consumer.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = consumer_run.run().await {
                error!("PKM automaton JetStream consumer exited: {err}");
            }
        });

        self.consumer = Some(consumer);
        self.consumer_handle = Some(handle);

        Ok(())
    }

    /// Process knowledge events and generate PKM insights
    async fn process_knowledge_events(&mut self, from: &Checkpoint) -> NodeResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;

        // Query recent knowledge events
        let events = self.query_knowledge_events(db_pool, from).await?;
        info!("Processing {} events for PKM analysis", events.len());
        self.emit_insights_for_events(&events, &event_sender).await
    }

    async fn run_continuous(&mut self, from: Checkpoint) -> NodeResult<u64> {
        self.ensure_consumer().await?;
        let mut receiver = self.incoming_rx.take().ok_or_else(|| {
            NodeError::Processing("Confirmed events channel not initialized".into())
        })?;

        let mut processed = 0u64;
        while let Some(provisional) = receiver.recv().await {
            processed += self.process_confirmed_event(provisional).await?;
        }

        info!("Confirmed event channel closed; exiting PKM automaton continuous loop");
        self.incoming_tx = None;
        self.consumer_handle = None;
        self.consumer = None;
        drop(from);

        Ok(processed)
    }

    async fn process_confirmed_event(&mut self, provisional: ProvisionalEvent) -> NodeResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;
        let event_id = Id::from_ulid(provisional.event_id);

        let persisted = match db_pool.events().get_by_id(event_id).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                warn!("Confirmed event missing from database; skipping PKM update");
                return Ok(0);
            }
            Err(err) => {
                return Err(NodeError::Processing(format!(
                    "Failed to load confirmed event: {err}"
                )))
            }
        };

        let events = vec![persisted];
        self.emit_insights_for_events(&events, &event_sender).await
    }

    async fn emit_insights_for_events(
        &mut self,
        events: &[Event<JsonValue>],
        event_sender: &EventSender,
    ) -> NodeResult<u64> {
        self.record_input(events.len());
        // Extract knowledge items from events
        self.extract_knowledge_items(events).await;

        let mut events_processed = 0u64;

        // Generate knowledge extraction insights if enabled
        if self.config.enable_knowledge_extraction && !self.knowledge_items.is_empty() {
            if let Ok(extraction_event) = self.generate_knowledge_extraction_insights().await {
                match event_sender.send(extraction_event).await {
                    Ok(_) => events_processed += 1,
                    Err(e) => warn!("Failed to send knowledge extraction event: {}", e),
                }
            }
        }

        // Generate learning session tracking if enabled
        if self.config.enable_learning_tracking {
            if let Ok(learning_events) = self.track_learning_sessions(events).await {
                for learning_event in learning_events {
                    match event_sender.send(learning_event).await {
                        Ok(_) => events_processed += 1,
                        Err(e) => warn!("Failed to send learning tracking event: {}", e),
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
                    match event_sender.send(graph_event).await {
                        Ok(_) => events_processed += 1,
                        Err(e) => warn!("Failed to send knowledge graph event: {}", e),
                    }
                }
            }
        }

        // Generate workflow pattern insights
        if let Ok(workflow_events) = self.analyze_workflow_patterns(events).await {
            for workflow_event in workflow_events {
                match event_sender.send(workflow_event).await {
                    Ok(_) => events_processed += 1,
                    Err(e) => warn!("Failed to send workflow pattern event: {}", e),
                }
            }
        }

        self.record_output(events_processed);
        Ok(events_processed)
    }

    /// Query knowledge-related events from the database
    async fn query_knowledge_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let window_start = Utc::now()
            - chrono::Duration::seconds(self.config.analysis_window_seconds.as_secs() as i64);

        let events = db_pool
            .events()
            .get_recent(1000)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query events: {}", e))?
            .into_iter()
            .filter(|event| event.ts_orig.map(|ts| ts > window_start).unwrap_or(true))
            .filter(|event| {
                self.config
                    .knowledge_event_types
                    .iter()
                    .any(|t| event.event_type.as_str() == t)
            })
            .collect();

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
                timestamp: event.ts_orig.unwrap_or_else(Utc::now),
                source_event_id: event.id.clone(),
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
            event.ts_orig.unwrap_or_else(Utc::now).format("%H:%M:%S")
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
    async fn generate_knowledge_extraction_insights(&self) -> NodeResult<Event<JsonValue>> {
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
            .filter_map(|item| item.source_event_id.clone())
            .collect();

        let insights_payload = serde_json::json!({
            "analysis_type": "knowledge_extraction",
            "total_knowledge_items": total_items,
            "type_distribution": type_counts,
            "top_keywords": keyword_pairs,
            "recent_items": recent_items,
            "time_window_hours": self.config.analysis_window_seconds.as_secs() / 3600,
            "generated_at": Utc::now(),
        });

        let event = Event::dynamic(
            "pkm-automaton",
            "pkm.knowledge_extraction",
            insights_payload,
        )
        .from_parents(source_event_ids.into_iter())?
        .at_time(Utc::now())
        .build()?;

        Ok(event.into())
    }

    /// Track learning sessions based on event patterns
    async fn track_learning_sessions(
        &self,
        events: &[Event<JsonValue>],
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let mut learning_events = Vec::new();

        // Simple learning session detection: sequences of related knowledge events
        let mut current_session: Option<LearningSession> = None;
        let session_gap_threshold = chrono::Duration::minutes(30);

        for event in events {
            let is_learning_event = self.is_learning_related_event(event);
            let ts = event.ts_orig.unwrap_or_else(Utc::now);
            let id_opt = event.id.clone();

            if is_learning_event {
                match &mut current_session {
                    Some(session) => {
                        // Check if this event continues the current session
                        if ts - session.last_activity <= session_gap_threshold {
                            if let Some(id) = id_opt.clone() {
                                session.events.push(id);
                            }
                            session.last_activity = ts;
                        } else {
                            // End current session and start new one
                            if session.events.len() >= 3 {
                                learning_events
                                    .push(self.create_learning_session_event(session).await?);
                            }

                            *session = LearningSession {
                                start_time: ts,
                                last_activity: ts,
                                events: id_opt.clone().into_iter().collect(),
                            };
                        }
                    }
                    None => {
                        // Start new session
                        current_session = Some(LearningSession {
                            start_time: ts,
                            last_activity: ts,
                            events: id_opt.into_iter().collect(),
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
    ) -> NodeResult<Event<JsonValue>> {
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

        let parents = session.events.clone();
        let event = Event::dynamic("pkm-automaton", "pkm.learning_session", session_payload)
            .from_parents(parents)?
            .at_time(Utc::now())
            .build()?;

        Ok(event.into())
    }

    /// Build knowledge graph insights
    async fn build_knowledge_graph_insights(&self) -> NodeResult<Vec<Event<JsonValue>>> {
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
                            .any(|p2| p2.contains(p.as_str()) || p.contains(p2.as_str()))
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
                .filter_map(|item| item.source_event_id.clone())
                .collect();

            let graph_payload = serde_json::json!({
                "analysis_type": "knowledge_graph",
                "total_nodes": self.knowledge_items.len(),
                "total_relationships": relationships.len(),
                "relationships": relationships,
                "graph_density": (relationships.len() as f64) / (self.knowledge_items.len() as f64).max(1.0),
                "generated_at": Utc::now(),
            });

            let graph_event = Event::dynamic("pkm-automaton", "pkm.knowledge_graph", graph_payload)
                .from_parents(all_event_ids)?
                .at_time(Utc::now())
                .build()?;

            graph_events.push(graph_event.into());
        }

        Ok(graph_events)
    }

    /// Analyze workflow patterns in events
    async fn analyze_workflow_patterns(
        &self,
        events: &[Event<JsonValue>],
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let mut pattern_events = Vec::new();

        // Simple workflow pattern: detect sequences of related activities
        let mut activity_sequences = HashMap::new();
        let mut current_sequence = Vec::new();

        for event in events {
            let activity_type = self.classify_activity_type(event);
            current_sequence.push((
                activity_type.clone(),
                event.ts_orig.unwrap_or_else(Utc::now),
                event.id.clone(),
            ));

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
                    events.iter().filter_map(|e| e.id.clone()).collect();

                let workflow_payload = serde_json::json!({
                    "analysis_type": "workflow_pattern",
                    "pattern": pattern,
                    "frequency": frequency,
                    "pattern_type": "activity_sequence",
                    "generated_at": Utc::now(),
                });

                let pattern_event =
                    Event::dynamic("pkm-automaton", "pkm.workflow_pattern", workflow_payload)
                        .from_parents(pattern_event_ids)?
                        .at_time(Utc::now())
                        .build()?;

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
}

#[async_trait]
impl Node for PKMAutomaton {
    type Config = PKMAutomatonConfig;

    async fn initialize(&mut self, init: ProcessorInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.runtime = Some(runtime);
        self.config = config;

        info!(
            "PKM automaton configured - analyzing {} event types, window: {} hours",
            self.config.knowledge_event_types.len(),
            self.config.analysis_window_seconds.as_secs() / 3600
        );

        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let start_time = Utc::now();

        let events_processed = match until {
            TimeHorizon::Snapshot => {
                // Perform one-time PKM analysis
                self.process_knowledge_events(&from).await?
            }
            TimeHorizon::Historical { .. } => {
                // Analyze historical knowledge events
                self.process_knowledge_events(&from).await?
            }
            TimeHorizon::Continuous => {
                // Continuous PKM processing
                self.run_continuous(from.clone()).await?
            }
        };

        let duration = Utc::now().signed_duration_since(start_time);

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds() as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                (
                    "knowledge_items_extracted".to_string(),
                    self.knowledge_items.len() as u64,
                ),
                (
                    "analysis_window_hours".to_string(),
                    (self.config.analysis_window_seconds.as_secs() / 3600) as u64,
                ),
                (
                    "knowledge_extraction_enabled".to_string(),
                    self.config.enable_knowledge_extraction as u64,
                ),
                (
                    "learning_tracking_enabled".to_string(),
                    self.config.enable_learning_tracking as u64,
                ),
            ]),
            successful_targets: vec!["pkm".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        };

        self.record_history(IngestionHistoryEntry {
            id: Ulid::new().to_string(),
            started_at: start_time,
            completed_at: Some(Utc::now()),
            events_generated: report.events_processed,
            scan_report: Some(report.clone()),
            error: None,
        });

        Ok(report)
    }

    fn processor_name(&self) -> &str {
        "pkm-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
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
        let last_updated = self.stats.last_activity.unwrap_or_else(Utc::now);

        Ok(SourceState {
            description: "PKM automaton for Personal Knowledge Management insights".to_string(),
            last_updated,
            total_items: Some(self.stats.inputs_seen),
            metadata: HashMap::from([
                (
                    "knowledge_items".to_string(),
                    serde_json::json!(self.knowledge_items.len()),
                ),
                (
                    "analysis_window_hours".to_string(),
                    serde_json::json!(self.config.analysis_window_seconds.as_secs() / 3600),
                ),
                (
                    "knowledge_extraction".to_string(),
                    serde_json::json!(self.config.enable_knowledge_extraction),
                ),
                (
                    "knowledge_graph".to_string(),
                    serde_json::json!(self.config.enable_knowledge_graph),
                ),
                (
                    "learning_tracking".to_string(),
                    serde_json::json!(self.config.enable_learning_tracking),
                ),
                (
                    "inputs_seen".to_string(),
                    serde_json::json!(self.stats.inputs_seen),
                ),
                (
                    "outputs_emitted".to_string(),
                    serde_json::json!(self.stats.outputs_emitted),
                ),
            ]),
            healthy: true,
            recent_activity: self.recent_activity(),
        })
    }

    fn get_ingestion_history(
        &self,
        limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        let limit = usize::try_from(limit).unwrap_or(0);
        let take = if limit == 0 {
            self.history.len()
        } else {
            std::cmp::min(limit, self.history.len())
        };
        Ok(self.history.iter().take(take).cloned().collect())
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let now = Utc::now();
        let (start, end) = time_range.unwrap_or_else(|| {
            (
                now - chrono::Duration::seconds(
                    self.config.analysis_window_seconds.as_secs().max(60) as i64,
                ),
                now,
            )
        });
        let source_total = self.stats.inputs_seen;
        let sinex_total = self.stats.outputs_emitted;
        let capped = std::cmp::min(source_total, sinex_total);
        let coverage_percentage = if source_total == 0 {
            0.0
        } else {
            (capped as f64 / source_total as f64) * 100.0
        };
        Ok(CoverageAnalysis {
            time_range: (start, end),
            source_total,
            sinex_total,
            coverage_percentage,
            missing_count: source_total.saturating_sub(capped),
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

#[derive(Clone)]
struct ChannelConfirmedEventHandler {
    sender: mpsc::Sender<ProvisionalEvent>,
}

impl ChannelConfirmedEventHandler {
    fn new(sender: mpsc::Sender<ProvisionalEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl ConfirmedEventHandler for ChannelConfirmedEventHandler {
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> NodeResult<()> {
        match self.sender.try_send(event.clone()) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("PKM confirmed event channel full; dropping event");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(NodeError::Processing(
                "Failed to forward confirmed PKM event: channel closed".into(),
            )),
        }
    }
}
