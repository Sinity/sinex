#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Content automaton entry points.
//!
//! Content Events → Analysis → Synthesized Content Insights.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::{models::{Event, EventBuilder}, repositories::DbPoolExt},
        types::{Bytes, Id, JsonValue, Seconds},
        Ulid,
    };

    // SDK facade for common processor types
    pub use sinex_node_sdk::{
        automaton_base::{
            ActivityEntry, AutomatonFields, ChannelConfirmedEventHandler, IngestionHistoryEntry,
        },
        confirmation_handler::ProvisionalEvent,
        event_processor::EventTransport,
        jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
        stream_processor::{
            Checkpoint, EventSender, Node, ProcessorInitContext, ProcessorType, ScanArgs,
            ScanReport, TimeHorizon,
        },
        NodeError, NodeResult, ProcessingModel,
    };
    pub use sinex_processor_runtime::{CoverageAnalysis, ExplorationProvider, ExportFormat, SourceState};

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Utc},
        serde::{Deserialize, Serialize},
        serde_json,
        sqlx::PgPool,
        std::{
            collections::HashMap,
            sync::Arc,
            time::Duration,
        },
        tracing::{error, info, warn},
    };
}

// Use local facade for common types
use crate::common::*;
use sinex_core::environment;

/// Configuration for Content Automaton
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContentAutomatonConfig {
    /// Event types containing content to analyze
    pub target_event_types: Vec<String>,
    /// Enable text content analysis
    pub enable_text_analysis: bool,
    /// Enable media content analysis
    pub enable_media_analysis: bool,
    /// Enable content classification
    pub enable_content_classification: bool,
    /// Content processing window in seconds
    pub processing_window_seconds: Seconds,
    /// Maximum content size to analyze (bytes)
    pub max_content_size_bytes: Bytes,
}

impl Default for ContentAutomatonConfig {
    fn default() -> Self {
        Self {
            target_event_types: vec![
                "document.created".to_string(),
                "document.modified".to_string(),
                "clipboard.content.captured".to_string(),
                "file.created".to_string(),
                "file.modified".to_string(),
            ],
            enable_text_analysis: true,
            enable_media_analysis: false, // Disabled by default due to complexity
            enable_content_classification: true,
            processing_window_seconds: Seconds::from_secs(3600), // 1 hour
            max_content_size_bytes: Bytes::from_mebibytes(10),   // 10MB
        }
    }
}

const DEFAULT_BATCH_SIZE: usize = 128;

/// Content Automaton using unified Node architecture
///
/// Consumes events containing content and produces content analysis insights:
/// - Text content analysis (language detection, sentiment, keywords)
/// - Media content metadata extraction
/// - Content classification and categorization
/// - Content similarity detection
pub struct ContentAutomaton {
    fields: AutomatonFields<ContentAutomatonConfig>,
}

impl ContentAutomaton {
    pub fn new() -> Self {
        Self {
            fields: AutomatonFields::new(),
        }
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: sinex_node_sdk::stream_processor::ProcessorRuntimeState,
        config: ContentAutomatonConfig,
    ) -> NodeResult<()> {
        info!(
            processor = "content-automaton",
            service = %runtime.service_info().service_name(),
            "Initializing content automaton"
        );

        self.fields.db_pool = Some(runtime.db_pool().clone());
        self.fields.event_sender = Some(runtime.event_sender());
        self.fields.config = config;
        self.fields.runtime = Some(runtime);

        info!(
            "Content automaton configured - analyzing {} event types, max content size: {} bytes",
            self.fields.config.target_event_types.len(),
            self.fields.config.max_content_size_bytes.as_u64()
        );

        Ok(())
    }

    fn recent_activity(&self) -> Vec<ActivityEntry> {
        self.fields
            .history
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
        if let Some(handle) = self.fields.consumer_handle.as_ref() {
            if !handle.is_finished() {
                return Ok(());
            }
        }

        self.fields.consumer_handle = None;
        self.fields.consumer = None;

        let runtime = self.fields.runtime()?;
        let transport = runtime.transport().clone();
        let service_name = runtime.service_info().service_name().to_string();

        let nats_publisher = match transport {
            EventTransport::Nats(publisher) => publisher,
        };

        self.fields.ensure_event_channel();
        let sender = self
            .fields
            .incoming_tx
            .clone()
            .ok_or_else(|| NodeError::Processing("Confirmed event channel unavailable".into()))?;

        let handler = Arc::new(ChannelConfirmedEventHandler::new(sender));
        let env = environment().clone();
        let config = JetStreamEventConsumerConfig {
            processing_model: ProcessingModel::LeaderStandby,
            batch_size: DEFAULT_BATCH_SIZE,
            confirmation_timeout: Duration::from_secs(60),
            consumer_name: format!("{}-content-automaton", service_name.replace('.', "_")),
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
                error!("Content automaton JetStream consumer exited: {err}");
            }
        });

        self.fields.set_consumer(consumer, handle);

        Ok(())
    }

    /// Process content events and generate content insights
    async fn process_content_events(&mut self, from: &Checkpoint) -> NodeResult<u64> {
        let db_pool = self.fields.db_pool()?;
        let event_sender = self.fields.event_sender()?;

        // Query recent content events for analysis
        let events = self.query_content_events(db_pool, from).await?;
        info!("Processing {} content events for analysis", events.len());
        self.fields.stats.record_input(events.len());

        let mut events_processed = 0u64;

        for event in &events {
            events_processed += self.emit_analysis_for_event(event, &event_sender).await?;
        }

        // Generate content similarity analysis for the batch
        if events.len() > 1 {
            if let Ok(similarity_events) = self.analyze_content_similarity(&events).await {
                for similarity_event in similarity_events {
                    match event_sender.send(similarity_event).await {
                        Ok(_) => events_processed += 1,
                        Err(e) => warn!("Failed to send content similarity event: {}", e),
                    }
                }
            }
        }

        self.fields.stats.record_output(events_processed);
        Ok(events_processed)
    }

    async fn run_continuous(&mut self, from: Checkpoint) -> NodeResult<u64> {
        self.ensure_consumer().await?;
        let mut receiver = self.fields.take_incoming_rx().ok_or_else(|| {
            NodeError::Processing("Confirmed events channel not initialized".into())
        })?;

        let mut processed = 0u64;
        while let Some(provisional) = receiver.recv().await {
            processed += self.process_confirmed_event(provisional).await?;
        }

        info!("Confirmed event channel closed; exiting content automaton continuous loop");
        self.fields.incoming_tx = None;
        self.fields.consumer_handle = None;
        self.fields.consumer = None;
        drop(from);

        Ok(processed)
    }

    async fn process_confirmed_event(&mut self, provisional: ProvisionalEvent) -> NodeResult<u64> {
        let db_pool = self.fields.db_pool()?;
        let event_sender = self.fields.event_sender()?;
        let event_id = Id::from_ulid(provisional.event_id);

        let persisted = match db_pool.events().get_by_id(event_id).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                warn!("Confirmed event missing from database; skipping content analysis");
                return Ok(0);
            }
            Err(err) => {
                return Err(NodeError::Processing(format!(
                    "Failed to load confirmed event: {err}"
                )))
            }
        };

        self.fields.stats.record_input(1);
        let processed = self
            .emit_analysis_for_event(&persisted, &event_sender)
            .await?;
        self.fields.stats.record_output(processed);
        Ok(processed)
    }

    async fn emit_analysis_for_event(
        &self,
        event: &Event<JsonValue>,
        event_sender: &EventSender,
    ) -> NodeResult<u64> {
        let mut events_processed = 0u64;

        if let Some(content) = self.extract_content_from_event(event) {
            if content.len() > self.fields.config.max_content_size_bytes.as_usize() {
                warn!(
                    "Skipping content analysis - size {} exceeds limit {}",
                    content.len(),
                    self.fields.config.max_content_size_bytes.as_u64()
                );
                return Ok(0);
            }

            if self.fields.config.enable_text_analysis {
                if let Ok(analysis_event) = self.analyze_text_content(&content, event).await {
                    match event_sender.send(analysis_event).await {
                        Ok(_) => events_processed += 1,
                        Err(e) => warn!("Failed to send text analysis event: {}", e),
                    }
                }
            }

            if self.fields.config.enable_content_classification {
                if let Ok(classification_event) = self.classify_content(&content, event).await {
                    match event_sender.send(classification_event).await {
                        Ok(_) => events_processed += 1,
                        Err(e) => warn!("Failed to send content classification event: {}", e),
                    }
                }
            }
        }

        Ok(events_processed)
    }

    /// Query content events from the database for analysis
    async fn query_content_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let window_start = Utc::now()
            - chrono::Duration::seconds(self.fields.config.processing_window_seconds.as_secs() as i64);

        let events = db_pool
            .events()
            .get_recent(1000)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query events: {}", e))?
            .into_iter()
            .filter(|event| event.ts_orig.map(|ts| ts > window_start).unwrap_or(true))
            .filter(|event| {
                self.fields.config
                    .target_event_types
                    .iter()
                    .any(|t| event.event_type.as_str() == t)
            })
            .collect();

        Ok(events)
    }

    /// Extract content from event payload for analysis
    fn extract_content_from_event(&self, event: &Event<JsonValue>) -> Option<String> {
        // This is a simplified content extraction - in reality we'd need
        // sophisticated content extraction based on event type and payload structure

        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload.clone()) {
            // Try to extract text content from common fields
            if let Some(content) = payload.get("content").and_then(|v| v.as_str()) {
                return Some(content.to_string());
            }

            if let Some(text) = payload.get("text").and_then(|v| v.as_str()) {
                return Some(text.to_string());
            }

            if let Some(data) = payload.get("data").and_then(|v| v.as_str()) {
                return Some(data.to_string());
            }

            // For file events, we might need to read file content - simplified here
            if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
                info!("Skipping content analysis for path-only event: {}", path);
            }
        }

        None
    }

    /// Analyze text content and generate insights
    async fn analyze_text_content(
        &self,
        content: &str,
        source_event: &Event<JsonValue>,
    ) -> NodeResult<Event<JsonValue>> {
        // Simple text analysis - in reality this would be much more sophisticated
        let word_count = content.split_whitespace().count();
        let char_count = content.chars().count();
        let line_count = content.lines().count();

        // Simple keyword extraction (most common words)
        let mut word_freq: HashMap<String, usize> = HashMap::new();
        for word in content.split_whitespace() {
            let clean_word = word
                .to_lowercase()
                .chars()
                .filter(|c| c.is_alphabetic())
                .collect::<String>();
            if clean_word.len() > 3 {
                // Skip short words
                *word_freq.entry(clean_word).or_insert(0) += 1;
            }
        }

        let mut keywords: Vec<_> = word_freq.into_iter().collect();
        keywords.sort_by(|a, b| b.1.cmp(&a.1));
        keywords.truncate(10); // Top 10 keywords

        // Simple language detection heuristic
        let language = self.detect_language_simple(content);

        let analysis_payload = serde_json::json!({
            "analysis_type": "text_analysis",
            "source_event_id": source_event.id,
            "word_count": word_count,
            "character_count": char_count,
            "line_count": line_count,
            "detected_language": language,
            "top_keywords": keywords,
            "content_preview": content.chars().take(200).collect::<String>(),
            "generated_at": Utc::now(),
        });

        let parents = source_event.id.clone().into_iter().collect::<Vec<_>>();

        // Create synthesized event with proper provenance
        let event = EventBuilder::new(
            "content-automaton".into(),
            "content.analyzed".into(),
            analysis_payload,
        )
        .from_parents(parents)?
        .at_time(Utc::now())
        .build()?;

        Ok(event.into())
    }

    /// Classify content into categories
    async fn classify_content(
        &self,
        content: &str,
        source_event: &Event<JsonValue>,
    ) -> NodeResult<Event<JsonValue>> {
        // Simple content classification heuristics
        let mut categories = Vec::new();

        // Code detection
        if content.contains("function ")
            || content.contains("def ")
            || content.contains("#include")
            || content.contains("import ")
        {
            categories.push("code".to_string());
        }

        // Configuration files
        if content.contains("[") && content.contains("]") && content.contains("=") {
            categories.push("configuration".to_string());
        }

        // Documentation
        if content.contains("# ") || content.contains("## ") || content.contains("```") {
            categories.push("documentation".to_string());
        }

        // Log files
        if content.contains("ERROR") || content.contains("INFO") || content.contains("DEBUG") {
            categories.push("log".to_string());
        }

        // Email/communication
        if content.contains("@") && content.contains(".com") {
            categories.push("communication".to_string());
        }

        // Default category
        if categories.is_empty() {
            categories.push("general_text".to_string());
        }

        let classification_payload = serde_json::json!({
            "analysis_type": "content_classification",
            "source_event_id": source_event.id,
            "categories": categories,
            "confidence": 0.7, // Simplified confidence score
            "content_length": content.len(),
            "generated_at": Utc::now(),
        });

        let parents = source_event.id.clone().into_iter().collect::<Vec<_>>();

        // Create synthesized event with proper provenance
        let event = EventBuilder::new(
            "content-automaton".into(),
            "content.classified".into(),
            classification_payload,
        )
        .from_parents(parents)?
        .at_time(Utc::now())
        .build()?;

        Ok(event.into())
    }

    /// Analyze content similarity between events
    async fn analyze_content_similarity(
        &self,
        events: &[Event<JsonValue>],
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        let mut similarity_events = Vec::new();

        // Simple similarity analysis - compare content lengths and detect potential duplicates
        let mut content_map: HashMap<String, Vec<Id<Event<JsonValue>>>> = HashMap::new();

        for event in events {
            if let (Some(content), Some(id)) =
                (self.extract_content_from_event(event), event.id.clone())
            {
                // Simple content fingerprint (first 100 chars)
                let fingerprint = content.chars().take(100).collect::<String>();
                content_map.entry(fingerprint).or_default().push(id);
            }
        }

        // Find groups with multiple events (potential duplicates or similar content)
        for (fingerprint, event_ids) in content_map {
            if event_ids.len() > 1 {
                let similarity_payload = serde_json::json!({
                    "analysis_type": "content_similarity",
                    "similarity_type": "potential_duplicate",
                    "event_group_size": event_ids.len(),
                    "content_fingerprint": fingerprint,
                    "similar_event_ids": event_ids,
                    "generated_at": Utc::now(),
                });

                let similarity_event = EventBuilder::new(
                    "content-automaton".into(),
                    "content.similarity_detected".into(),
                    similarity_payload,
                )
                .from_parents(event_ids)?
                .at_time(Utc::now())
                .build()?;

                similarity_events.push(similarity_event.into());
            }
        }

        Ok(similarity_events)
    }

    /// Simple language detection heuristic
    fn detect_language_simple(&self, content: &str) -> String {
        // Very simple language detection based on common words
        let english_words = [
            "the", "and", "for", "are", "but", "not", "you", "all", "can", "had", "was", "one",
        ];
        let english_count = english_words
            .iter()
            .map(|word| content.to_lowercase().matches(word).count())
            .sum::<usize>();

        if english_count > content.split_whitespace().count() / 20 {
            "english".to_string()
        } else {
            "unknown".to_string()
        }
    }
}

#[async_trait]
impl Node for ContentAutomaton {
    type Config = ContentAutomatonConfig;

    async fn initialize(&mut self, init: ProcessorInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.initialise_with_runtime_state(runtime, config).await
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
                // Perform one-time content analysis
                self.process_content_events(&from).await?
            }
            TimeHorizon::Historical { .. } => {
                // Analyze historical content events
                self.process_content_events(&from).await?
            }
            TimeHorizon::Continuous => {
                // Continuous content processing
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
                ("content_events_processed".to_string(), events_processed),
                (
                    "max_content_size_bytes".to_string(),
                    self.fields.config.max_content_size_bytes.as_u64(),
                ),
                (
                    "text_analysis_enabled".to_string(),
                    self.fields.config.enable_text_analysis as u64,
                ),
            ]),
            successful_targets: vec!["content".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        };

        self.fields.record_history(IngestionHistoryEntry {
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
        "content-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        // Content analysis operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }
}

impl Default for ContentAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for ContentAutomaton {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let last_updated = self.fields.stats.last_activity.unwrap_or_else(Utc::now);

        Ok(SourceState {
            description: "Content automaton for text and media content analysis".to_string(),
            last_updated,
            total_items: Some(self.fields.stats.inputs_seen),
            metadata: HashMap::from([
                (
                    "target_event_types".to_string(),
                    serde_json::json!(self.fields.config.target_event_types),
                ),
                (
                    "text_analysis".to_string(),
                    serde_json::json!(self.fields.config.enable_text_analysis),
                ),
                (
                    "media_analysis".to_string(),
                    serde_json::json!(self.fields.config.enable_media_analysis),
                ),
                (
                    "content_classification".to_string(),
                    serde_json::json!(self.fields.config.enable_content_classification),
                ),
                (
                    "max_content_size".to_string(),
                    serde_json::json!(self.fields.config.max_content_size_bytes),
                ),
                (
                    "inputs_seen".to_string(),
                    serde_json::json!(self.fields.stats.inputs_seen),
                ),
                (
                    "outputs_emitted".to_string(),
                    serde_json::json!(self.fields.stats.outputs_emitted),
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
            self.fields.history.len()
        } else {
            std::cmp::min(limit, self.fields.history.len())
        };
        Ok(self.fields.history.iter().take(take).cloned().collect())
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let now = Utc::now();
        let (start, end) = time_range.unwrap_or_else(|| {
            (
                now - chrono::Duration::seconds(
                    self.fields.config.processing_window_seconds.as_secs().max(60) as i64,
                ),
                now,
            )
        });
        let source_total = self.fields.stats.inputs_seen;
        let sinex_total = self.fields.stats.outputs_emitted;
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
                "Content automaton analyzes text and media content from events".to_string(),
                "Adjust target_event_types to focus on specific content sources".to_string(),
                "Increase max_content_size_bytes to analyze larger content".to_string(),
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
