#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Search automaton.
//!
//! Content Events → Indexing → Synthesized Search Events.

mod common {
    // Core facades
    pub use sinex_core::{
        db::models::{Event, EventId, Provenance},
        db::repositories::DbPoolExt,
        types::{domain::EventType, Id, Seconds},
        JsonValue,
    };

    // Runtime/SDK facades
    pub use sinex_processor_runtime::cli::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        SourceState,
    };
    pub use sinex_node_sdk::{
        stream_processor::{
            Checkpoint, EventSender, ProcessorCapabilities, ProcessorInitContext,
            ProcessorRuntimeState, ProcessorType, ScanArgs, ScanReport, Node,
            TimeHorizon,
        },
        NodeError, NodeResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Duration as ChronoDuration, Utc},
        serde::{Deserialize, Serialize},
        serde_json,
        sqlx::PgPool,
        std::time::Duration,
        tokio::sync::mpsc,
        tracing::{error, info, warn},
    };
}

use crate::common::*;
use serde_json::json;
use sinex_core::{environment, types::Result as CoreResult, Ulid};
use sinex_node_sdk::{
    confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent},
    event_processor::EventTransport,
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    ProcessingModel,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::task::JoinHandle;

const MAX_SEARCH_EVENTS: usize = 1024;
const MAX_PROVENANCE_IDS: usize = 8;
const DEFAULT_BATCH_SIZE: usize = 128;
const CONFIRMED_CHANNEL_CAPACITY: usize = 1024;
const MAX_HISTORY_ENTRIES: usize = 32;

#[derive(Default)]
struct SearchAutomatonStats {
    inputs_seen: u64,
    outputs_emitted: u64,
    last_activity: Option<DateTime<Utc>>,
}

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
    pub indexing_window_seconds: Seconds,
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
            enable_semantic_search: false,
            enable_search_analytics: true,
            indexing_window_seconds: Seconds::from_secs(3600),
            min_content_length: 10,
            max_index_size: 10_000,
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

/// Search Automaton using unified Node architecture
pub struct SearchAutomaton {
    runtime: Option<ProcessorRuntimeState>,
    config: SearchAutomatonConfig,
    event_sender: Option<EventSender>,
    db_pool: Option<PgPool>,
    search_index: Vec<SearchIndexEntry>,
    recent_events: VecDeque<Event<JsonValue>>,
    recent_event_ids: HashSet<Ulid>,
    incoming_tx: Option<mpsc::Sender<ProvisionalEvent>>,
    incoming_rx: Option<mpsc::Receiver<ProvisionalEvent>>,
    consumer: Option<Arc<JetStreamEventConsumer>>,
    consumer_handle: Option<JoinHandle<()>>,
    history: VecDeque<IngestionHistoryEntry>,
    stats: SearchAutomatonStats,
}

impl SearchAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: SearchAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
            search_index: Vec::new(),
            recent_events: VecDeque::new(),
            recent_event_ids: HashSet::new(),
            incoming_tx: None,
            incoming_rx: None,
            consumer: None,
            consumer_handle: None,
            history: VecDeque::new(),
            stats: SearchAutomatonStats::default(),
        }
    }

    fn runtime(&self) -> NodeResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            NodeError::Lifecycle("Search automaton runtime not initialized".into())
        })
    }

    fn db_pool(&self) -> NodeResult<&PgPool> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.db_pool())
        } else if let Some(pool) = self.db_pool.as_ref() {
            Ok(pool)
        } else {
            Err(NodeError::Processing(
                "Database pool not initialized".into(),
            ))
        }
    }

    fn event_sender(&self) -> NodeResult<EventSender> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.event_sender())
        } else if let Some(sender) = self.event_sender.as_ref() {
            Ok(sender.clone())
        } else {
            Err(NodeError::Processing(
                "Event sender not initialized".into(),
            ))
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
        let sender = self.incoming_tx.clone().ok_or_else(|| {
            NodeError::Processing("Confirmed event channel unavailable".into())
        })?;

        let handler = Arc::new(ChannelConfirmedEventHandler::new(sender));
        let env = environment().clone();
        let config = JetStreamEventConsumerConfig {
            processing_model: ProcessingModel::LeaderStandby,
            batch_size: DEFAULT_BATCH_SIZE,
            confirmation_timeout: Duration::from_secs(60),
            consumer_name: format!("{}-search-automaton", service_name.replace('.', "_")),
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
                error!("Search automaton JetStream consumer exited: {err}");
            }
        });

        self.consumer = Some(consumer);
        self.consumer_handle = Some(handle);

        Ok(())
    }

    async fn process_snapshot(&mut self, end_time: DateTime<Utc>) -> NodeResult<u64> {
        let db_pool = self.db_pool()?;
        let events = self
            .query_searchable_events(db_pool, end_time)
            .await
            .map_err(|err| {
                NodeError::Processing(format!("Failed to query search events: {err}"))
            })?;

        if events.is_empty() {
            return Ok(0);
        }

        self.capture_events(events);
        self.emit_search_outputs().await
    }

    async fn run_continuous(&mut self, from: Checkpoint) -> NodeResult<u64> {
        let mut processed = self.process_snapshot(Utc::now()).await.unwrap_or(0);
        self.ensure_consumer().await?;

        let mut receiver = self.incoming_rx.take().ok_or_else(|| {
            NodeError::Processing("Confirmed events channel not initialized".into())
        })?;

        while let Some(provisional) = receiver.recv().await {
            processed += self.process_confirmed_event(provisional).await?;
        }

        info!("Confirmed event channel closed; exiting search continuous loop");
        self.incoming_tx = None;
        self.consumer_handle = None;
        self.consumer = None;
        drop(from);

        self.record_output(processed);
        Ok(processed)
    }

    async fn process_confirmed_event(
        &mut self,
        provisional: ProvisionalEvent,
    ) -> NodeResult<u64> {
        let db_pool = self.db_pool()?;
        let event_id = EventId::from_ulid(provisional.event_id);

        let persisted = match db_pool.events().get_by_id(event_id).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                warn!("Confirmed search event missing from database");
                return Ok(0);
            }
            Err(err) => {
                return Err(NodeError::Processing(format!(
                    "Failed to load confirmed search event: {err}"
                )))
            }
        };

        self.capture_events(vec![persisted]);
        self.emit_search_outputs().await
    }

    async fn emit_search_outputs(&mut self) -> NodeResult<u64> {
        let mut processed = 0u64;
        let sender = self.event_sender()?;

        if self.config.enable_fulltext_indexing && !self.search_index.is_empty() {
            match self.generate_search_index_event() {
                Ok(event) => {
                    if let Err(err) = sender.send(event).await {
                        warn!(error = %err, "Failed to send search index event");
                    } else {
                        processed += 1;
                    }
                }
                Err(err) => warn!("Failed to build search index event: {err}"),
            }
        }

        if self.config.enable_search_analytics {
            let snapshot = self.recent_snapshot();
            match self.generate_search_analytics(&snapshot).await {
                Ok(events) => {
                    for event in events {
                        if let Err(err) = sender.send(event).await {
                            warn!(error = %err, "Failed to send search analytics event");
                        } else {
                            processed += 1;
                        }
                    }
                }
                Err(err) => warn!("Failed to build search analytics: {err}"),
            }
        }

        match self.analyze_content_discoverability().await {
            Ok(events) => {
                for event in events {
                    if let Err(err) = sender.send(event).await {
                        warn!(error = %err, "Failed to send discoverability event");
                    } else {
                        processed += 1;
                    }
                }
            }
            Err(err) => warn!("Failed to analyze discoverability: {err}"),
        }

        Ok(processed)
    }

    fn capture_events(&mut self, mut events: Vec<Event<JsonValue>>) {
        events.sort_by_key(|event| event_timestamp(event));
        let mut added = 0usize;
        for event in events {
            if let Some(key) = event.id.as_ref().map(|id| *id.as_ulid()) {
                if self.recent_event_ids.insert(key) {
                    self.recent_events.push_back(event);
                    added += 1;
                }
            }
        }
        self.record_input(added);
        self.prune_recent_events();
        self.rebuild_search_index();
    }

    fn prune_recent_events(&mut self) {
        let cutoff = Utc::now()
            - ChronoDuration::seconds(self.config.indexing_window_seconds.as_secs().max(60) as i64);
        while let Some(front) = self.recent_events.front() {
            let outdated = event_timestamp(front) < cutoff;
            if outdated || self.recent_events.len() > MAX_SEARCH_EVENTS {
                if let Some(evicted) = self.recent_events.pop_front() {
                    if let Some(id) = evicted.id.as_ref() {
                        self.recent_event_ids.remove(id.as_ulid());
                    }
                }
            } else {
                break;
            }
        }
        while self.recent_events.len() > MAX_SEARCH_EVENTS {
            if let Some(evicted) = self.recent_events.pop_front() {
                if let Some(id) = evicted.id.as_ref() {
                    self.recent_event_ids.remove(id.as_ulid());
                }
            }
        }
    }

    fn rebuild_search_index(&mut self) {
        self.search_index.clear();
        let mut entries: Vec<SearchIndexEntry> = self
            .recent_events
            .iter()
            .rev()
            .filter_map(|event| self.create_search_index_entry(event))
            .collect();

        entries.sort_by(|a, b| {
            b.search_score
                .partial_cmp(&a.search_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        entries.truncate(self.config.max_index_size);
        self.search_index = entries;
    }

    fn recent_snapshot(&self) -> Vec<Event<JsonValue>> {
        let mut snapshot: Vec<_> = self.recent_events.iter().cloned().collect();
        snapshot.sort_by_key(|event| event_timestamp(event));
        snapshot
    }

    async fn query_searchable_events(
        &self,
        db_pool: &PgPool,
        end_time: DateTime<Utc>,
    ) -> CoreResult<Vec<Event<JsonValue>>> {
        let start_time = end_time
            - ChronoDuration::seconds(self.config.indexing_window_seconds.as_secs().max(60) as i64);

        let mut collected = Vec::new();
        if self.config.searchable_event_types.is_empty() {
            let mut events = db_pool
                .events()
                .get_by_time_range(
                    start_time,
                    end_time,
                    sinex_core::types::Pagination::new(Some(MAX_SEARCH_EVENTS as i64), None),
                )
                .await?;
            collected.append(&mut events);
        } else {
            for event_type_str in &self.config.searchable_event_types {
                let event_type = EventType::from(event_type_str.as_str());
                let mut events = db_pool
                    .events()
                    .get_events_by_type_and_time_range(
                        &event_type,
                        start_time,
                        end_time,
                        sinex_core::types::Pagination::new(
                            Some((MAX_SEARCH_EVENTS / 2) as i64),
                            None,
                        ),
                    )
                    .await?;
                collected.append(&mut events);
            }
        }

        collected.sort_by_key(|event| event_timestamp(event));
        dedup_events(&mut collected);
        if collected.len() > MAX_SEARCH_EVENTS {
            collected.drain(..collected.len() - MAX_SEARCH_EVENTS);
        }

        Ok(collected)
    }

    /// Create a search index entry from an event
    fn create_search_index_entry(&self, event: &Event<JsonValue>) -> Option<SearchIndexEntry> {
        if !self.config.searchable_event_types.is_empty()
            && !self
                .config
                .searchable_event_types
                .iter()
                .any(|ty| ty == event.event_type.as_ref())
        {
            return None;
        }

        let payload = serde_json::from_value::<serde_json::Value>(event.payload.clone()).ok()?;
        let id = event.id.as_ref()?.clone();
        let title = self.extract_title(&payload, event);
        let content = self.extract_searchable_content(&payload)?;
        if content.len() < self.config.min_content_length {
            return None;
        }
        let keywords = self.extract_search_keywords(&payload, &content);
        let search_score = self.calculate_search_score(&content, &keywords, event);
        let entry_id = format!("{}_{}", id.as_ulid(), event_timestamp(event).timestamp());

        Some(SearchIndexEntry {
            entry_id,
            title,
            content,
            keywords,
            source_event_id: id,
            event_type: event.event_type.to_string(),
            timestamp: event_timestamp(event),
            search_score,
        })
    }

    fn extract_title(&self, payload: &serde_json::Value, event: &Event<JsonValue>) -> String {
        if let Some(title) = payload.get("title").and_then(|v| v.as_str()) {
            return title.to_string();
        }
        if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
            if let Some(filename) = path.split('/').last() {
                return filename.to_string();
            }
        }
        if let Some(command) = payload.get("command").and_then(|v| v.as_str()) {
            return format!(
                "Command: {}",
                command
                    .split_whitespace()
                    .take(5)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        if let Some(url) = payload.get("url").and_then(|v| v.as_str()) {
            return format!("Web: {url}");
        }
        format!(
            "{} - {}",
            event.event_type,
            event_timestamp(event).format("%Y-%m-%d %H:%M")
        )
    }

    fn extract_searchable_content(&self, payload: &serde_json::Value) -> Option<String> {
        for key in [
            "content",
            "text",
            "data",
            "output",
            "command",
            "command_string",
        ] {
            if let Some(value) = payload.get(key).and_then(|v| v.as_str()) {
                return Some(value.to_string());
            }
        }
        None
    }

    fn extract_search_keywords(&self, payload: &serde_json::Value, content: &str) -> Vec<String> {
        let mut keywords = Vec::new();
        if let Some(explicit) = payload.get("keywords").and_then(|v| v.as_array()) {
            for kw in explicit.iter().filter_map(|value| value.as_str()) {
                keywords.push(kw.to_string());
            }
        }
        if let Some(tags) = payload.get("tags").and_then(|v| v.as_array()) {
            for tag in tags.iter().filter_map(|value| value.as_str()) {
                keywords.push(tag.to_string());
            }
        }
        let mut word_counts = HashMap::new();
        for word in content.split_whitespace() {
            let cleaned = word
                .to_lowercase()
                .chars()
                .filter(|c| c.is_alphanumeric())
                .collect::<String>();
            if cleaned.len() > 3 {
                *word_counts.entry(cleaned).or_insert(0) += 1;
            }
        }
        for (word, count) in word_counts {
            if count >= 2 || keywords.len() < 10 {
                keywords.push(word);
            }
        }
        keywords
    }

    fn calculate_search_score(
        &self,
        content: &str,
        keywords: &[String],
        event: &Event<JsonValue>,
    ) -> f64 {
        let mut score = 0.0;
        score += (content.len() as f64 / 1000.0).min(2.0);
        score += (keywords.len() as f64 / 10.0).min(1.0);
        match event.event_type.to_string().as_str() {
            s if s.contains("document") => score += 2.0,
            s if s.contains("file") => score += 1.5,
            s if s.contains("command") => score += 1.0,
            s if s.contains("web") => score += 1.2,
            _ => score += 0.5,
        }
        let hours_old = (Utc::now() - event_timestamp(event)).num_hours() as f64;
        let recency = (1.0 / (1.0 + hours_old / 24.0)).max(0.2);
        score * recency
    }

    fn provenance_from_index(&self) -> Provenance {
        let ids: Vec<EventId> = self
            .search_index
            .iter()
            .take(MAX_PROVENANCE_IDS)
            .map(|entry| entry.source_event_id.clone())
            .collect();
        provenance_from_ids(&ids)
    }

    async fn generate_search_analytics(
        &self,
        events: &[Event<JsonValue>],
    ) -> NodeResult<Vec<Event<JsonValue>>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut content_types = HashMap::new();
        for event in events {
            *content_types
                .entry(event.event_type.to_string())
                .or_insert(0usize) += 1;
        }

        let top_types: Vec<_> = content_types
            .iter()
            .map(|(typ, count)| json!({ "event_type": typ, "count": count }))
            .collect();

        let payload = json!({
            "analysis_type": "search.analytics",
            "top_content_types": top_types,
            "index_size": self.search_index.len(),
            "semantic_enabled": self.config.enable_semantic_search,
        });

        let provenance = self.provenance_from_index();
        let analytics_event =
            Event::create("search-automaton", "search.analytics", payload, provenance)
                .at_time(Utc::now());

        Ok(vec![analytics_event])
    }

    async fn analyze_content_discoverability(&self) -> NodeResult<Vec<Event<JsonValue>>> {
        if self.search_index.is_empty() {
            return Ok(Vec::new());
        }

        let mut event_types = HashMap::new();
        for entry in &self.search_index {
            *event_types
                .entry(entry.event_type.clone())
                .or_insert(0usize) += 1;
        }

        let mut issues = Vec::new();
        for (event_type, count) in event_types {
            if count < 3 {
                issues.push(json!({
                    "event_type": event_type,
                    "message": "Sparse coverage",
                }));
            }
        }

        if issues.is_empty() {
            return Ok(Vec::new());
        }

        let payload = json!({
            "analysis_type": "search.discoverability",
            "issues": issues,
            "recommendations": [
                "Capture richer metadata for underrepresented types",
                "Enable semantic_search for better clustering",
            ],
        });

        let event = Event::create(
            "search-automaton",
            "search.discoverability",
            payload,
            self.provenance_from_index(),
        )
        .at_time(Utc::now());

        Ok(vec![event])
    }

    fn generate_search_index_event(&self) -> NodeResult<Event<JsonValue>> {
        if self.search_index.is_empty() {
            return Err(NodeError::Processing(
                "Search index empty; nothing to emit".into(),
            ));
        }

        let total_entries = self.search_index.len();
        let mut content_type_distribution = HashMap::new();
        let mut type_scores = HashMap::new();

        for entry in &self.search_index {
            *content_type_distribution
                .entry(entry.event_type.clone())
                .or_insert(0usize) += 1;
            type_scores
                .entry(entry.event_type.clone())
                .or_insert(Vec::new())
                .push(entry.search_score);
        }

        let avg_score_by_type: HashMap<_, _> = type_scores
            .into_iter()
            .map(|(event_type, scores)| {
                let avg = scores.iter().sum::<f64>() / scores.len() as f64;
                (event_type, avg)
            })
            .collect();

        let mut top_entries = self.search_index.clone();
        top_entries.sort_by(|a, b| {
            b.search_score
                .partial_cmp(&a.search_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        top_entries.truncate(10);

        let top_summary: Vec<_> = top_entries
            .iter()
            .map(|entry| {
                json!({
                    "title": entry.title,
                    "event_type": entry.event_type,
                    "search_score": entry.search_score,
                    "keywords": entry.keywords,
                    "timestamp": entry.timestamp,
                })
            })
            .collect();

        let payload = json!({
            "analysis_type": "search.index",
            "total_entries": total_entries,
            "content_type_distribution": content_type_distribution,
            "avg_score_by_type": avg_score_by_type,
            "top_entries": top_summary,
            "index_size_limit": self.config.max_index_size,
            "indexing_window_hours": self.config.indexing_window_seconds.as_secs() / 3600,
            "generated_at": Utc::now(),
        });

        Ok(Event::create(
            "search-automaton",
            "search.index_built",
            payload,
            self.provenance_from_index(),
        )
        .at_time(Utc::now()))
    }
}

#[async_trait]
impl Node for SearchAutomaton {
    type Config = SearchAutomatonConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.runtime = Some(runtime);
        self.config = config;
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
            TimeHorizon::Snapshot => self.process_snapshot(Utc::now()).await.unwrap_or(0),
            TimeHorizon::Historical { end_time } => {
                self.process_snapshot(end_time).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => self.run_continuous(from).await.unwrap_or(0),
        };

        let duration = Utc::now().signed_duration_since(start_time);

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds().max(0) as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                (
                    "search_index_entries".to_string(),
                    self.search_index.len() as u64,
                ),
                (
                    "fulltext_indexing_enabled".to_string(),
                    self.config.enable_fulltext_indexing as u64,
                ),
                (
                    "search_analytics_enabled".to_string(),
                    self.config.enable_search_analytics as u64,
                ),
            ]),
            successful_targets: vec!["search".to_string()],
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
        "search-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_snapshot: true,
            supports_historical: true,
            manages_own_continuous_loop: true,
            ..ProcessorCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        if let Some(consumer) = self.consumer.take() {
            consumer.stop().await;
        }
        if let Some(handle) = self.consumer_handle.take() {
            if let Err(err) = handle.await {
                warn!("Failed to join search consumer task: {err}");
            }
        }
        self.incoming_tx = None;
        self.incoming_rx = None;
        Ok(())
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

        let last_updated = self.stats.last_activity.unwrap_or_else(Utc::now);

        Ok(SourceState {
            description: "Search automaton for content indexing and search analytics".to_string(),
            last_updated,
            total_items: Some(self.search_index.len() as u64),
            metadata: HashMap::from([
                (
                    "search_index_entries".to_string(),
                    json!(self.search_index.len()),
                ),
                (
                    "max_index_size".to_string(),
                    json!(self.config.max_index_size),
                ),
                ("avg_search_score".to_string(), json!(avg_search_score)),
                (
                    "fulltext_indexing".to_string(),
                    json!(self.config.enable_fulltext_indexing),
                ),
                (
                    "semantic_search".to_string(),
                    json!(self.config.enable_semantic_search),
                ),
                (
                    "search_analytics".to_string(),
                    json!(self.config.enable_search_analytics),
                ),
                ("inputs_seen".to_string(), json!(self.stats.inputs_seen)),
                (
                    "outputs_emitted".to_string(),
                    json!(self.stats.outputs_emitted),
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
                now - ChronoDuration::seconds(
                    self.config.indexing_window_seconds.as_secs().max(60) as i64,
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
                "Adjust searchable_event_types to focus on specific sources".to_string(),
                "Enable semantic_search for more sophisticated content understanding".to_string(),
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

fn event_timestamp(event: &Event<JsonValue>) -> DateTime<Utc> {
    event.ts_orig.unwrap_or_else(|| {
        event
            .id
            .as_ref()
            .map(|id| id.timestamp())
            .unwrap_or_else(Utc::now)
    })
}

fn dedup_events(events: &mut Vec<Event<JsonValue>>) {
    let mut seen: HashSet<Ulid> = HashSet::new();
    events.retain(|event| match event.id.as_ref() {
        Some(id) => seen.insert(*id.as_ulid()),
        None => false,
    });
}

fn provenance_from_ids(ids: &[EventId]) -> Provenance {
    if let Some(first) = ids.first().cloned() {
        Provenance::from_synthesis_safe(first, ids.iter().skip(1).cloned().collect())
    } else {
        default_provenance()
    }
}

fn default_provenance() -> Provenance {
    let bootstrap = EventId::from_ulid(
        Ulid::from_bytes([
            0x01, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ])
        .expect("valid ULID bytes"),
    );
    Provenance::from_synthesis_safe(bootstrap, vec![])
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
                warn!("Search automaton confirmed event channel full; dropping event");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(NodeError::Processing(
                "Failed to forward confirmed search event: channel closed".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_core::types::Id;
    use sinex_test_utils::{sinex_test, TestResult};
    use tokio::runtime::Runtime;

    fn make_event(event_type: &str, minutes_ago: i64, content: &str) -> Event<JsonValue> {
        let mut event = Event::create(
            "test",
            event_type,
            json!({ "content": content, "title": "Test" }),
            default_provenance(),
        )
        .at_time(Utc::now() - ChronoDuration::minutes(minutes_ago));
        event.id = Some(Id::new());
        event
    }

    #[sinex_test]
    fn capture_events_prunes_deduplicates() -> TestResult<()> {
        let mut automaton = SearchAutomaton::new();
        let first = make_event("document.created", 10, "hello world");
        let mut duplicate = first.clone();
        duplicate.ts_orig = Some(Utc::now());

        automaton.capture_events(vec![first.clone(), duplicate]);
        assert_eq!(automaton.recent_events.len(), 1);
        assert_eq!(automaton.search_index.len(), 1);
        Ok(())
    }

    #[sinex_test]
    fn search_index_event_contains_summary() -> TestResult<()> {
        let mut automaton = SearchAutomaton::new();
        let event = make_event("document.created", 5, "contents for indexing");
        automaton.capture_events(vec![event]);
        let index_event = automaton.generate_search_index_event().unwrap();
        assert_eq!(index_event.event_type.to_string(), "search.index_built");
        Ok(())
    }

    #[sinex_test]
    fn search_analytics_event_emitted() -> TestResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut automaton = SearchAutomaton::new();
            automaton.capture_events(vec![make_event(
                "document.created",
                1,
                "rich searchable content with repeated words content content",
            )]);
            let snapshot = automaton.recent_snapshot();
            let analytics = automaton
                .generate_search_analytics(&snapshot)
                .await
                .expect("analytics generation should succeed");
            assert_eq!(analytics.len(), 1);
            assert_eq!(
                analytics[0].event_type.to_string(),
                "search.analytics",
                "expected analytics event"
            );
        });
        Ok(())
    }

    #[sinex_test]
    fn sparse_content_types_trigger_discoverability_event() -> TestResult<()> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut automaton = SearchAutomaton::new();
            automaton.capture_events(vec![
                make_event("document.created", 1, "alpha beta gamma delta epsilon"),
                make_event(
                    "web.page_visited",
                    1,
                    "http request response content sample",
                ),
            ]);

            let discoverability = automaton
                .analyze_content_discoverability()
                .await
                .expect("analysis should succeed");
            assert!(
                !discoverability.is_empty(),
                "sparse types should emit discoverability issues"
            );
            assert_eq!(
                discoverability[0].event_type.to_string(),
                "search.discoverability"
            );
        });
        Ok(())
    }
}
