#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]

//! Search automaton.
//!
//! Content Events → Indexing → Synthesized Search Events.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::Event,
        types::{events::payloads::*, Id},
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
    pub use sinex_processor_runtime::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
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

/// Configuration for Search Automaton
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SearchAutomatonConfig {
    /// Event types containing searchable content
    pub searchable_event_types: Vec<String>,
    /// Enable full-text search indexing
    pub enable_fulltext_indexing: bool,
    /// Enable semantic search capabilities
    pub enable_semantic_search: bool,
    /// Enable search analytics
    pub enable_search_analytics: bool,
    /// Time window for search indexing (seconds)
    pub indexing_window_seconds: u64,
    /// Minimum content length for indexing (characters)
    pub min_content_length: usize,
    /// Maximum search index size (entries)
    pub max_index_size: usize,
}

impl Default for SearchAutomatonConfig {
    fn default() -> Self {
        Self {
            searchable_event_types: vec![
                "document.created".to_string(),
                "document.modified".to_string(),
                "file.created".to_string(),
                "file.modified".to_string(),
                "clipboard.content.captured".to_string(),
                "command.executed".to_string(),
                "web.page_visited".to_string(),
                "terminal.output.captured".to_string(),
            ],
            enable_fulltext_indexing: true,
            enable_semantic_search: false, // Disabled by default due to complexity
            enable_search_analytics: true,
            indexing_window_seconds: 3600, // 1 hour
            min_content_length: 10,
            max_index_size: 10000,
        }
    }
}

/// Search index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchIndexEntry {
    pub entry_id: String,
    pub title: String,
    pub content: String,
    pub keywords: Vec<String>,
    pub source_event_id: Id<Event<JsonValue>>,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub search_score: f64,
}

/// Search query pattern for analytics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQueryPattern {
    pub pattern_type: String,
    pub query_terms: Vec<String>,
    pub frequency: usize,
    pub related_content_types: Vec<String>,
    pub avg_relevance_score: f64,
}

/// Search Automaton using unified StatefulStreamProcessor architecture
///
/// Consumes events containing searchable content and produces search insights:
/// - Full-text search index building from event content
/// - Search query pattern analysis
/// - Content discoverability insights
/// - Search optimization recommendations
pub struct SearchAutomaton {
    runtime: Option<ProcessorRuntimeState>,
    config: SearchAutomatonConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
    search_index: Vec<SearchIndexEntry>,
}

impl SearchAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: SearchAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
            search_index: Vec::new(),
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(color_eyre::eyre::eyre!(
                "Search automaton runtime not initialised"
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

    /// Process searchable events and generate search insights
    async fn process_search_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;

        // Query recent searchable events
        let events = self.query_searchable_events(db_pool, from).await?;
        info!("Processing {} events for search indexing", events.len());

        // Build or update search index from events
        self.build_search_index(&events).await;

        let mut events_processed = 0u64;

        // Generate full-text search index if enabled
        if self.config.enable_fulltext_indexing && !self.search_index.is_empty() {
            if let Ok(index_event) = self.generate_search_index_event().await {
                if let Err(e) = event_sender.send(index_event) {
                    warn!("Failed to send search index event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        // Generate search analytics if enabled
        if self.config.enable_search_analytics {
            if let Ok(analytics_events) = self.generate_search_analytics(&events).await {
                for analytics_event in analytics_events {
                    if let Err(e) = event_sender.send(analytics_event) {
                        warn!("Failed to send search analytics event: {}", e);
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        // Generate content discoverability insights
        if let Ok(discoverability_events) = self.analyze_content_discoverability().await {
            for discoverability_event in discoverability_events {
                if let Err(e) = event_sender.send(discoverability_event) {
                    warn!("Failed to send content discoverability event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        Ok(events_processed)
    }

    /// Query searchable events from the database
    async fn query_searchable_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.indexing_window_seconds as i64);

        let events = db_pool
            .events()
            .get_recent(
                1000,
                Some(window_start),
                Some(&self.config.searchable_event_types),
            )
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query events: {}", e))?;

        Ok(events)
    }

    /// Build or update search index from events
    async fn build_search_index(&mut self, events: &[Event<JsonValue>]) {
        self.search_index.clear();

        for event in events {
            if let Some(index_entry) = self.create_search_index_entry(event) {
                if index_entry.content.len() >= self.config.min_content_length {
                    self.search_index.push(index_entry);
                }
            }
        }

        // Limit index size
        if self.search_index.len() > self.config.max_index_size {
            // Sort by search score and keep the best entries
            self.search_index.sort_by(|a, b| {
                b.search_score
                    .partial_cmp(&a.search_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            self.search_index.truncate(self.config.max_index_size);
        }

        info!(
            "Built search index with {} entries",
            self.search_index.len()
        );
    }

    /// Create a search index entry from an event
    fn create_search_index_entry(&self, event: &Event<JsonValue>) -> Option<SearchIndexEntry> {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload.clone()) {
            let title = self.extract_title(&payload, event);
            let content = self.extract_searchable_content(&payload)?;
            let keywords = self.extract_search_keywords(&payload, &content);
            let search_score = self.calculate_search_score(&content, &keywords, event);

            Some(SearchIndexEntry {
                entry_id: format!("{}_{}", event.id.as_ulid(), event.ts_orig.timestamp()),
                title,
                content,
                keywords,
                source_event_id: event.id,
                event_type: event.event_type.to_string(),
                timestamp: event.ts_orig,
                search_score,
            })
        } else {
            None
        }
    }

    /// Extract title for search indexing
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
                    .take(5)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }

        // Try URL-based title
        if let Some(url) = payload.get("url").and_then(|v| v.as_str()) {
            return format!("Web: {}", url);
        }

        // Default title
        format!(
            "{} - {}",
            event.event_type,
            event.ts_orig.format("%Y-%m-%d %H:%M")
        )
    }

    /// Extract searchable content from payload
    fn extract_searchable_content(&self, payload: &serde_json::Value) -> Option<String> {
        // Try various content fields
        if let Some(content) = payload.get("content").and_then(|v| v.as_str()) {
            return Some(content.to_string());
        }

        if let Some(text) = payload.get("text").and_then(|v| v.as_str()) {
            return Some(text.to_string());
        }

        if let Some(data) = payload.get("data").and_then(|v| v.as_str()) {
            return Some(data.to_string());
        }

        if let Some(output) = payload.get("output").and_then(|v| v.as_str()) {
            return Some(output.to_string());
        }

        // For command events, include the command itself as searchable
        if let Some(command) = payload
            .get("command")
            .or_else(|| payload.get("command_string"))
            .and_then(|v| v.as_str())
        {
            return Some(command.to_string());
        }

        None
    }

    /// Extract keywords for search indexing
    fn extract_search_keywords(&self, payload: &serde_json::Value, content: &str) -> Vec<String> {
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
        // Extract words longer than 3 characters that appear frequently
        let mut word_counts = HashMap::new();
        for word in content.split_whitespace() {
            let clean_word = word
                .to_lowercase()
                .chars()
                .filter(|c| c.is_alphabetic())
                .collect::<String>();
            if clean_word.len() > 3 {
                *word_counts.entry(clean_word).or_insert(0) += 1;
            }
        }

        // Add frequent words as keywords
        for (word, count) in word_counts {
            if count >= 2 || keywords.len() < 10 {
                keywords.push(word);
            }
        }

        keywords
    }

    /// Calculate search relevance score for content
    fn calculate_search_score(
        &self,
        content: &str,
        keywords: &[String],
        event: &Event<JsonValue>,
    ) -> f64 {
        let mut score = 0.0;

        // Content length factor (longer content might be more valuable)
        score += (content.len() as f64 / 1000.0).min(2.0);

        // Keyword density factor
        score += (keywords.len() as f64 / 10.0).min(1.0);

        // Event type factor
        match event.event_type.to_string().as_str() {
            s if s.contains("document") => score += 2.0,
            s if s.contains("file") => score += 1.5,
            s if s.contains("command") => score += 1.0,
            s if s.contains("web") => score += 1.2,
            _ => score += 0.5,
        }

        // Recency factor (newer content gets higher score)
        let hours_old = (Utc::now() - event.ts_orig).num_hours() as f64;
        let recency_factor = (1.0 / (1.0 + hours_old / 24.0)).max(0.1);
        score *= recency_factor;

        score
    }

    /// Generate search index event
    async fn generate_search_index_event(&self) -> SatelliteResult<Event<JsonValue>> {
        let total_entries = self.search_index.len();

        // Create index summary
        let mut content_type_distribution = HashMap::new();
        let mut avg_score_by_type = HashMap::new();
        let mut type_counts = HashMap::new();
        let mut type_scores = HashMap::new();

        for entry in &self.search_index {
            *content_type_distribution
                .entry(entry.event_type.clone())
                .or_insert(0) += 1;
            *type_scores
                .entry(entry.event_type.clone())
                .or_insert(Vec::new())
                .push(entry.search_score);
            *type_counts.entry(entry.event_type.clone()).or_insert(0) += 1;
        }

        // Calculate average scores by type
        for (event_type, scores) in type_scores {
            let avg_score = scores.iter().sum::<f64>() / scores.len() as f64;
            avg_score_by_type.insert(event_type, avg_score);
        }

        // Get top entries
        let mut top_entries = self.search_index.clone();
        top_entries.sort_by(|a, b| {
            b.search_score
                .partial_cmp(&a.search_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        top_entries.truncate(10);

        let top_entries_summary: Vec<_> = top_entries
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "title": entry.title,
                    "event_type": entry.event_type,
                    "search_score": entry.search_score,
                    "keywords": entry.keywords,
                    "timestamp": entry.timestamp,
                })
            })
            .collect();

        let source_event_ids: Vec<Id<Event<JsonValue>>> = self
            .search_index
            .iter()
            .map(|entry| entry.source_event_id)
            .collect();

        let index_payload = serde_json::json!({
            "analysis_type": "search_index",
            "total_entries": total_entries,
            "content_type_distribution": content_type_distribution,
            "avg_score_by_type": avg_score_by_type,
            "top_entries": top_entries_summary,
            "index_size_limit": self.config.max_index_size,
            "indexing_window_hours": self.config.indexing_window_seconds / 3600,
            "generated_at": Utc::now(),
        });

        let event = Event::new(
            "search-automaton",
            "search.index_built",
            index_payload,
            source_event_ids,
        )
        .with_timestamp(Utc::now());

        Ok(event.into())
    }

    /// Generate search analytics
    async fn generate_search_analytics(
        &self,
        events: &[Event<JsonValue>],
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut analytics_events = Vec::new();

        // Analyze search patterns in the content
        let search_patterns = self.analyze_search_patterns();

        if !search_patterns.is_empty() {
            let all_event_ids: Vec<Id<Event<JsonValue>>> = events.iter().map(|e| e.id).collect();

            let analytics_payload = serde_json::json!({
                "analysis_type": "search_analytics",
                "search_patterns": search_patterns,
                "total_patterns": search_patterns.len(),
                "analysis_period_hours": self.config.indexing_window_seconds / 3600,
                "generated_at": Utc::now(),
            });

            let analytics_event = Event::new(
                "search-automaton",
                "search.analytics",
                analytics_payload,
                all_event_ids,
            )
            .with_timestamp(Utc::now());

            analytics_events.push(analytics_event.into());
        }

        Ok(analytics_events)
    }

    /// Analyze search patterns in content
    fn analyze_search_patterns(&self) -> Vec<SearchQueryPattern> {
        let mut patterns = Vec::new();

        // Group entries by similar keywords
        let mut keyword_groups: HashMap<String, Vec<&SearchIndexEntry>> = HashMap::new();

        for entry in &self.search_index {
            for keyword in &entry.keywords {
                keyword_groups
                    .entry(keyword.clone())
                    .or_default()
                    .push(entry);
            }
        }

        // Generate patterns for keywords that appear frequently
        for (keyword, entries) in keyword_groups {
            if entries.len() >= 3 {
                // Pattern needs at least 3 occurrences
                let related_content_types: Vec<String> = entries
                    .iter()
                    .map(|e| e.event_type.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();

                let avg_relevance_score =
                    entries.iter().map(|e| e.search_score).sum::<f64>() / entries.len() as f64;

                patterns.push(SearchQueryPattern {
                    pattern_type: "keyword_cluster".to_string(),
                    query_terms: vec![keyword],
                    frequency: entries.len(),
                    related_content_types,
                    avg_relevance_score,
                });
            }
        }

        // Sort by frequency and keep top patterns
        patterns.sort_by(|a, b| b.frequency.cmp(&a.frequency));
        patterns.truncate(20);

        patterns
    }

    /// Analyze content discoverability
    async fn analyze_content_discoverability(&self) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut discoverability_events = Vec::new();

        // Analyze how easily content can be discovered
        let mut low_discoverability_entries = Vec::new();
        let mut high_discoverability_entries = Vec::new();

        for entry in &self.search_index {
            if entry.keywords.len() < 2 && entry.search_score < 1.0 {
                low_discoverability_entries.push(entry);
            } else if entry.keywords.len() >= 5 && entry.search_score > 2.0 {
                high_discoverability_entries.push(entry);
            }
        }

        if !low_discoverability_entries.is_empty() || !high_discoverability_entries.is_empty() {
            let all_entry_ids: Vec<Id<Event<JsonValue>>> = self
                .search_index
                .iter()
                .map(|entry| entry.source_event_id)
                .collect();

            let discoverability_payload = serde_json::json!({
                "analysis_type": "content_discoverability",
                "total_indexed_items": self.search_index.len(),
                "low_discoverability_count": low_discoverability_entries.len(),
                "high_discoverability_count": high_discoverability_entries.len(),
                "low_discoverability_percentage": (low_discoverability_entries.len() as f64 / self.search_index.len() as f64) * 100.0,
                "recommendations": self.generate_discoverability_recommendations(&low_discoverability_entries),
                "generated_at": Utc::now(),
            });

            let discoverability_event = Event::new(
                "search-automaton",
                "search.content_discoverability",
                discoverability_payload,
                all_entry_ids,
            )
            .with_timestamp(Utc::now());

            discoverability_events.push(discoverability_event.into());
        }

        Ok(discoverability_events)
    }

    /// Generate recommendations for improving content discoverability
    fn generate_discoverability_recommendations(
        &self,
        low_discoverability_entries: &[&SearchIndexEntry],
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !low_discoverability_entries.is_empty() {
            recommendations.push(format!(
                "{} items have low discoverability due to insufficient keywords",
                low_discoverability_entries.len()
            ));

            // Analyze common characteristics of low-discoverability content
            let mut event_types = HashMap::new();
            for entry in low_discoverability_entries {
                *event_types.entry(&entry.event_type).or_insert(0) += 1;
            }

            if let Some((most_common_type, count)) =
                event_types.iter().max_by_key(|(_, &count)| count)
            {
                recommendations.push(format!(
                    "Most affected content type: {} ({} items)",
                    most_common_type, count
                ));
                recommendations.push(format!(
                    "Consider adding more descriptive metadata to {} events",
                    most_common_type
                ));
            }
        }

        recommendations.push("Enable semantic search for better content understanding".to_string());
        recommendations
            .push("Consider increasing min_content_length for better quality indexing".to_string());

        recommendations
    }
}

#[async_trait]
impl StatefulStreamProcessor for SearchAutomaton {
    type Config = SearchAutomatonConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        info!("Initializing search automaton");

        let (config, runtime) = init.into_runtime();
        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.runtime = Some(runtime);
        self.config = config;

        info!(
            "Search automaton configured - indexing {} event types, max index size: {}",
            self.config.searchable_event_types.len(),
            self.config.max_index_size
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
                // Perform one-time search indexing
                self.process_search_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Historical { .. } => {
                // Index historical searchable events
                self.process_search_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => {
                // Continuous search processing
                self.process_search_events(&from).await.unwrap_or(0)
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
                    "search_index_entries".to_string(),
                    serde_json::Value::Number(self.search_index.len().into()),
                ),
                (
                    "max_index_size".to_string(),
                    serde_json::Value::Number(self.config.max_index_size.into()),
                ),
                (
                    "fulltext_indexing_enabled".to_string(),
                    serde_json::Value::Bool(self.config.enable_fulltext_indexing),
                ),
                (
                    "search_analytics_enabled".to_string(),
                    serde_json::Value::Bool(self.config.enable_search_analytics),
                ),
            ]),
            successful_targets: vec!["search".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "search-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // Search indexing operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }
}

impl Default for SearchAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for SearchAutomaton {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let avg_search_score = if !self.search_index.is_empty() {
            self.search_index
                .iter()
                .map(|e| e.search_score)
                .sum::<f64>()
                / self.search_index.len() as f64
        } else {
            0.0
        };

        Ok(SourceState {
            description: "Search automaton for content indexing and search analytics".to_string(),
            last_updated: Utc::now(),
            total_items: Some(self.search_index.len() as u64),
            metadata: HashMap::from([
                (
                    "search_index_entries".to_string(),
                    self.search_index.len().to_string(),
                ),
                (
                    "max_index_size".to_string(),
                    self.config.max_index_size.to_string(),
                ),
                (
                    "avg_search_score".to_string(),
                    format!("{:.2}", avg_search_score),
                ),
                (
                    "fulltext_indexing".to_string(),
                    self.config.enable_fulltext_indexing.to_string(),
                ),
                (
                    "semantic_search".to_string(),
                    self.config.enable_semantic_search.to_string(),
                ),
                (
                    "search_analytics".to_string(),
                    self.config.enable_search_analytics.to_string(),
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
            coverage_percentage: 100.0, // Search processes available searchable events
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            recommendations: vec![
                "Search automaton builds full-text search indices from event content".to_string(),
                "Adjust searchable_event_types to focus on specific content sources".to_string(),
                "Enable semantic_search for more sophisticated content understanding".to_string(),
                "Increase max_index_size to retain more searchable content".to_string(),
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

/// Type alias for compatibility with processor_main! macro
pub type SearchProcessor = SearchAutomaton;
