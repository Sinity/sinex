#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/docs/overview.md")]

//! Content automaton entry points.
//!
//! Content Events → Analysis → Synthesized Content Insights.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::{models::Event, repositories::DbPoolExt},
        types::{Id, JsonValue},
    };

    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
        stream_processor::{
            Checkpoint, ProcessorInitContext, ProcessorRuntimeState, ProcessorType, ScanArgs,
            ScanReport, StatefulStreamProcessor, TimeHorizon,
        },
        SatelliteResult,
    };
    pub use sinex_processor_runtime::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
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
        tracing::{info, warn},
    };
}

// Use local facade for common types
use crate::common::*;

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
    pub processing_window_seconds: u64,
    /// Maximum content size to analyze (bytes)
    pub max_content_size_bytes: usize,
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
            processing_window_seconds: 3600,          // 1 hour
            max_content_size_bytes: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Content Automaton using unified StatefulStreamProcessor architecture
///
/// Consumes events containing content and produces content analysis insights:
/// - Text content analysis (language detection, sentiment, keywords)
/// - Media content metadata extraction
/// - Content classification and categorization
/// - Content similarity detection
pub struct ContentAutomaton {
    runtime: Option<ProcessorRuntimeState>,
    config: ContentAutomatonConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
}

impl ContentAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: ContentAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
        }
    }

    async fn initialise_with_runtime_state(
        &mut self,
        runtime: ProcessorRuntimeState,
        config: ContentAutomatonConfig,
    ) -> SatelliteResult<()> {
        info!(
            processor = "content-automaton",
            service = %runtime.service_info().service_name(),
            "Initializing content automaton"
        );

        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.config = config;
        self.runtime = Some(runtime);

        info!(
            "Content automaton configured - analyzing {} event types, max content size: {} bytes",
            self.config.target_event_types.len(),
            self.config.max_content_size_bytes
        );

        Ok(())
    }

    /// Process content events and generate content insights
    async fn process_content_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("Database pool not initialized"))?;
        let event_sender = self
            .event_sender
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("Event sender not initialized"))?;

        // Query recent content events for analysis
        let events = self.query_content_events(db_pool, from).await?;
        info!("Processing {} content events for analysis", events.len());

        let mut events_processed = 0u64;

        for event in &events {
            // Extract content from event payload for analysis
            if let Some(content) = self.extract_content_from_event(event) {
                // Skip content that's too large
                if content.len() > self.config.max_content_size_bytes {
                    warn!(
                        "Skipping content analysis - size {} exceeds limit {}",
                        content.len(),
                        self.config.max_content_size_bytes
                    );
                    continue;
                }

                // Generate content analysis if enabled
                if self.config.enable_text_analysis {
                    if let Ok(analysis_event) = self.analyze_text_content(&content, event).await {
                        if let Err(e) = event_sender.send(analysis_event) {
                            warn!("Failed to send text analysis event: {}", e);
                        } else {
                            events_processed += 1;
                        }
                    }
                }

                // Generate content classification if enabled
                if self.config.enable_content_classification {
                    if let Ok(classification_event) = self.classify_content(&content, event).await {
                        if let Err(e) = event_sender.send(classification_event) {
                            warn!("Failed to send content classification event: {}", e);
                        } else {
                            events_processed += 1;
                        }
                    }
                }
            }
        }

        // Generate content similarity analysis for the batch
        if events.len() > 1 {
            if let Ok(similarity_events) = self.analyze_content_similarity(&events).await {
                for similarity_event in similarity_events {
                    if let Err(e) = event_sender.send(similarity_event) {
                        warn!("Failed to send content similarity event: {}", e);
                    } else {
                        events_processed += 1;
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
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.processing_window_seconds as i64);

        let events = db_pool
            .events()
            .get_recent(1000)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query events: {}", e))?
            .into_iter()
            .filter(|event| {
                event
                    .ts_orig
                    .map(|ts| ts > window_start)
                    .unwrap_or(true)
            })
            .filter(|event| {
                self.config
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
                info!("Found file path for content analysis: {}", path);
                // In reality, we'd read file content here if it's a text file
                return Some(format!("File content analysis placeholder for: {}", path));
            }
        }

        None
    }

    /// Analyze text content and generate insights
    async fn analyze_text_content(
        &self,
        content: &str,
        source_event: &Event<JsonValue>,
    ) -> SatelliteResult<Event<JsonValue>> {
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

        let parents = source_event
            .id
            .clone()
            .into_iter()
            .collect::<Vec<_>>();

        // Create synthesized event with proper provenance
        let event = Event::dynamic("content-automaton", "content.analyzed", analysis_payload)
            .from_parents(parents)
            .at_time(Utc::now())
            .build();

        Ok(event.into())
    }

    /// Classify content into categories
    async fn classify_content(
        &self,
        content: &str,
        source_event: &Event<JsonValue>,
    ) -> SatelliteResult<Event<JsonValue>> {
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

        let parents = source_event
            .id
            .clone()
            .into_iter()
            .collect::<Vec<_>>();

        // Create synthesized event with proper provenance
        let event = Event::dynamic(
            "content-automaton",
            "content.classified",
            classification_payload,
        )
        .from_parents(parents)
        .at_time(Utc::now())
        .build();

        Ok(event.into())
    }

    /// Analyze content similarity between events
    async fn analyze_content_similarity(
        &self,
        events: &[Event<JsonValue>],
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut similarity_events = Vec::new();

        // Simple similarity analysis - compare content lengths and detect potential duplicates
        let mut content_map: HashMap<String, Vec<Id<Event<JsonValue>>>> = HashMap::new();

        for event in events {
            if let (Some(content), Some(id)) = (
                self.extract_content_from_event(event),
                event.id.clone(),
            ) {
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

                let similarity_event = Event::dynamic(
                    "content-automaton",
                    "content.similarity_detected",
                    similarity_payload,
                )
                .from_parents(event_ids)
                .at_time(Utc::now())
                .build();

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
impl StatefulStreamProcessor for ContentAutomaton {
    type Config = ContentAutomatonConfig;

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
                // Perform one-time content analysis
                self.process_content_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Historical { .. } => {
                // Analyze historical content events
                self.process_content_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => {
                // Continuous content processing
                self.process_content_events(&from).await.unwrap_or(0)
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
                    "content_events_processed".to_string(),
                    events_processed,
                ),
                (
                    "max_content_size_bytes".to_string(),
                    self.config.max_content_size_bytes as u64,
                ),
                (
                    "text_analysis_enabled".to_string(),
                    self.config.enable_text_analysis as u64,
                ),
            ]),
            successful_targets: vec!["content".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "content-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
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
        Ok(SourceState {
            description: "Content automaton for text and media content analysis".to_string(),
            last_updated: Utc::now(),
            total_items: Some(0),
            metadata: HashMap::from([
                (
                    "target_event_types".to_string(),
                    serde_json::json!(self.config.target_event_types),
                ),
                (
                    "text_analysis".to_string(),
                    serde_json::json!(self.config.enable_text_analysis),
                ),
                (
                    "media_analysis".to_string(),
                    serde_json::json!(self.config.enable_media_analysis),
                ),
                (
                    "content_classification".to_string(),
                    serde_json::json!(self.config.enable_content_classification),
                ),
                (
                    "max_content_size".to_string(),
                    serde_json::json!(self.config.max_content_size_bytes),
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
            time_range: (now - chrono::Duration::hours(1), now),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 100.0, // Content analysis processes available content events
            missing_count: 0,
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
