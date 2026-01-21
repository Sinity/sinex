#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Analytics automaton entry points.
//!
//! Events → Analysis → Synthesized Events.

mod common {
    pub use sinex_core::{
        db::models::{Event, EventBuilder, EventId},
        db::repositories::DbPoolExt,
        types::{domain::EventType, Seconds},
        JsonValue,
    };
    pub use sinex_processor_runtime::cli::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    };

    pub use sinex_node_sdk::{
        automaton_base::{
            bootstrap_provenance, event_ids_from_events, provenance_from_ids, AutomatonFields,
            ChannelConfirmedEventHandler, MAX_PROVENANCE_IDS,
        },
        stream_processor::{
            Checkpoint, EventSender, Node, NodeCapabilities, NodeInitContext, NodeRuntimeState,
            NodeType, ScanArgs, ScanReport, TimeHorizon,
        },
        NodeError, NodeResult,
    };

    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Duration as ChronoDuration, Utc},
        serde::{Deserialize, Serialize},
        sqlx::PgPool,
        std::time::Duration,
        tracing::{error, info, warn},
    };
}

use crate::common::*;
use serde_json::json;
use sinex_core::{environment, types::Result as CoreResult, Ulid};
use sinex_node_sdk::{
    confirmation_handler::ProvisionalEvent,
    event_processor::EventTransport,
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    ProcessingModel,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

const MAX_ANALYTICS_EVENTS: usize = 768;
const DEFAULT_BATCH_SIZE: usize = 128;

// Provenance utilities imported from sinex_node_sdk::automaton_base

/// Configuration for Analytics Automaton
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnalyticsAutomatonConfig {
    /// Event types to analyze (empty = analyze all)
    pub target_event_types: Vec<String>,
    /// Analysis window size in seconds
    pub analysis_window_seconds: Seconds,
    /// Minimum events required for pattern analysis
    pub min_events_for_pattern: usize,
    /// Enable specific analysis modules
    pub enable_frequency_analysis: bool,
    pub enable_pattern_detection: bool,
    pub enable_correlation_analysis: bool,
}

impl Default for AnalyticsAutomatonConfig {
    fn default() -> Self {
        Self {
            target_event_types: vec![],
            analysis_window_seconds: Seconds::from_secs(3600),
            min_events_for_pattern: 5,
            enable_frequency_analysis: true,
            enable_pattern_detection: true,
            enable_correlation_analysis: false,
        }
    }
}

/// Analytics Automaton using unified Node architecture
///
/// Consumes events from the event stream and produces analytical insights:
/// - Frequency analysis of event patterns
/// - Temporal pattern detection
/// - Cross-domain correlation analysis
/// - Usage insights and behavioral patterns
pub struct AnalyticsAutomaton {
    /// Common automaton infrastructure (runtime, config, channels, stats, history)
    fields: AutomatonFields<AnalyticsAutomatonConfig>,
    /// Automaton-specific analytics state
    state: AnalyticsState,
}

impl AnalyticsAutomaton {
    pub fn new() -> Self {
        Self {
            fields: AutomatonFields::new(),
            state: AnalyticsState::default(),
        }
    }

    // Delegate to AutomatonFields for common infrastructure
    fn runtime(&self) -> NodeResult<&NodeRuntimeState> {
        self.fields.runtime()
    }

    fn db_pool(&self) -> NodeResult<&PgPool> {
        self.fields.db_pool()
    }

    fn event_sender(&self) -> NodeResult<EventSender> {
        self.fields.event_sender()
    }

    async fn ensure_consumer(&mut self) -> NodeResult<()> {
        // Check if existing consumer is still running
        if let Some(handle) = self.fields.consumer_handle.as_ref() {
            if !handle.is_finished() {
                return Ok(());
            }
        }

        // Clean up old consumer
        self.fields.consumer = None;
        self.fields.consumer_handle = None;

        let runtime = self.runtime()?;
        let transport = runtime.transport().clone();
        let service_name = runtime.service_info().service_name().to_string();

        let nats_publisher = match transport {
            EventTransport::Nats(publisher) => publisher,
        };

        self.fields.ensure_event_channel();
        let sender =
            self.fields.incoming_tx.clone().ok_or_else(|| {
                NodeError::Processing("Confirmed event channel unavailable".into())
            })?;

        let handler = Arc::new(ChannelConfirmedEventHandler::new(sender));
        let env = environment().clone();
        let config = JetStreamEventConsumerConfig {
            processing_model: ProcessingModel::LeaderStandby,
            batch_size: DEFAULT_BATCH_SIZE,
            confirmation_timeout: Duration::from_secs(60),
            consumer_name: format!("{}-analytics-automaton", service_name.replace('.', "_")),
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
                error!("Analytics automaton JetStream consumer exited: {err}");
            }
        });

        self.fields.set_consumer(consumer, handle);

        Ok(())
    }

    async fn analyze_snapshot(&mut self, end_time: DateTime<Utc>) -> NodeResult<u64> {
        let db_pool = self.db_pool()?;
        let events = self
            .query_events_for_window(db_pool, end_time)
            .await
            .map_err(|err| NodeError::Processing(format!("Failed to query events: {err}")))?;

        if events.is_empty() {
            return Ok(0);
        }

        self.fields.stats.record_input(events.len());
        self.state.integrate(
            events,
            self.fields.config.analysis_window_seconds,
            MAX_ANALYTICS_EVENTS,
        );
        self.emit_insights().await
    }

    async fn run_continuous(&mut self, from: Checkpoint) -> NodeResult<u64> {
        // Seed state with current window before streaming
        let mut processed = self.analyze_snapshot(Utc::now()).await.unwrap_or(0);

        self.ensure_consumer().await?;
        let mut receiver = self.fields.take_incoming_rx().ok_or_else(|| {
            NodeError::Processing("Confirmed events channel not initialized".into())
        })?;

        while let Some(provisional) = receiver.recv().await {
            processed += self.process_confirmed_event(provisional).await?;
        }

        info!("Confirmed event channel closed; exiting analytics continuous loop");
        self.fields.incoming_tx = None;
        self.fields.consumer_handle = None;
        self.fields.consumer = None;
        drop(from);

        Ok(processed)
    }

    async fn process_confirmed_event(&mut self, provisional: ProvisionalEvent) -> NodeResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;
        let event_id = provisional.event_id;

        let persisted = match db_pool.events().get_by_id(event_id).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                warn!("Confirmed event missing from database; skipping analytics update");
                return Ok(0);
            }
            Err(err) => {
                return Err(NodeError::Processing(format!(
                    "Failed to load confirmed event: {err}"
                )))
            }
        };

        self.fields.stats.record_input(1);
        self.state.integrate(
            vec![persisted],
            self.fields.config.analysis_window_seconds,
            MAX_ANALYTICS_EVENTS,
        );

        self.emit_with_sender(&event_sender).await
    }

    async fn emit_insights(&mut self) -> NodeResult<u64> {
        let sender = self.event_sender()?;
        self.emit_with_sender(&sender).await
    }

    async fn emit_with_sender(&mut self, sender: &EventSender) -> NodeResult<u64> {
        let snapshot = self.state.snapshot();
        if snapshot.is_empty() {
            return Ok(0);
        }

        let mut events_processed = 0u64;

        if self.fields.config.enable_frequency_analysis {
            if let Some(event) = self.generate_frequency_analysis(&snapshot) {
                match sender.send(event).await {
                    Ok(_) => events_processed += 1,
                    Err(err) => warn!(error = %err, "Failed to send frequency analysis event"),
                }
            }
        }

        if self.fields.config.enable_pattern_detection {
            for pattern_event in self.detect_patterns(&snapshot) {
                match sender.send(pattern_event).await {
                    Ok(_) => events_processed += 1,
                    Err(err) => warn!(error = %err, "Failed to send pattern detection event"),
                }
            }
        }

        if self.fields.config.enable_correlation_analysis {
            if let Some(event) = self.detect_correlations(&snapshot) {
                match sender.send(event).await {
                    Ok(_) => events_processed += 1,
                    Err(err) => warn!(error = %err, "Failed to send correlation analysis event"),
                }
            }
        }

        self.fields.stats.record_output(events_processed);
        Ok(events_processed)
    }

    async fn query_events_for_window(
        &self,
        db_pool: &PgPool,
        end_time: DateTime<Utc>,
    ) -> CoreResult<Vec<Event<JsonValue>>> {
        let start_time = end_time
            - ChronoDuration::seconds(
                self.fields.config.analysis_window_seconds.as_secs().max(60) as i64
            );

        let mut collected = Vec::new();
        if self.fields.config.target_event_types.is_empty() {
            let mut events = db_pool
                .events()
                .get_by_time_range(
                    start_time,
                    end_time,
                    sinex_core::types::Pagination::new(Some(MAX_ANALYTICS_EVENTS as i64), None),
                )
                .await?;
            collected.append(&mut events);
        } else {
            for event_type_str in &self.fields.config.target_event_types {
                let event_type = EventType::from(event_type_str.as_str());
                let mut events = db_pool
                    .events()
                    .get_events_by_type_and_time_range(
                        &event_type,
                        start_time,
                        end_time,
                        sinex_core::types::Pagination::new(
                            Some((MAX_ANALYTICS_EVENTS / 2) as i64),
                            None,
                        ),
                    )
                    .await?;
                collected.append(&mut events);
            }
        }

        collected.sort_by_key(|event| event_timestamp(event));
        dedup_events(&mut collected);
        if collected.len() > MAX_ANALYTICS_EVENTS {
            collected.drain(..collected.len() - MAX_ANALYTICS_EVENTS);
        }

        Ok(collected)
    }

    fn generate_frequency_analysis(&self, events: &[Event<JsonValue>]) -> Option<Event<JsonValue>> {
        if events.is_empty() {
            return None;
        }

        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut per_source: HashMap<String, usize> = HashMap::new();
        for event in events {
            *counts.entry(event.event_type.to_string()).or_default() += 1;
            *per_source.entry(event.source.to_string()).or_default() += 1;
        }

        let mut top_event_types: Vec<_> = counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        top_event_types.sort_by(|a, b| b.1.cmp(&a.1));
        top_event_types.truncate(5);

        let mut top_sources: Vec<_> = per_source.iter().map(|(k, v)| (k.clone(), *v)).collect();
        top_sources.sort_by(|a, b| b.1.cmp(&a.1));
        top_sources.truncate(5);

        let total = events.len() as f64;
        let window_seconds = self.fields.config.analysis_window_seconds.as_secs().max(60);
        let window_minutes = (window_seconds as f64) / 60.0;
        let events_per_minute = (total / window_minutes).max(0.1);

        let anomalies: Vec<_> = top_event_types
            .iter()
            .filter(|(_, count)| (*count as f64 / total) > 0.5)
            .map(|(event_type, count)| {
                json!({
                    "event_type": event_type,
                    "share": (*count as f64 / total),
                })
            })
            .collect();

        let payload = json!({
            "analysis_type": "frequency",
            "events_per_minute": events_per_minute,
            "top_event_types": top_event_types,
            "top_sources": top_sources,
            "anomalies": anomalies,
            "window_seconds": window_seconds,
        });

        Some(self.build_synthesized_event("analytics.frequency", payload, events))
    }

    fn detect_patterns(&self, events: &[Event<JsonValue>]) -> Vec<Event<JsonValue>> {
        if events.len() < self.fields.config.min_events_for_pattern {
            return Vec::new();
        }

        let mut transitions: HashMap<(String, String), PatternStats> = HashMap::new();
        for window in events.windows(2) {
            let first = &window[0];
            let second = &window[1];
            let key = (first.event_type.to_string(), second.event_type.to_string());
            let entry = transitions.entry(key).or_default();
            entry.count += 1;
            entry.total_delta_ms += (event_timestamp(second) - event_timestamp(first))
                .num_milliseconds()
                .max(0);
            entry.sample_ids.extend(event_ids_from_events(
                window.iter().collect::<Vec<&Event<JsonValue>>>(),
                MAX_PROVENANCE_IDS,
            ));
            entry.last_seen = Some(event_timestamp(second));
        }

        let mut patterns = Vec::new();
        for ((from, to), stats) in transitions {
            if stats.count < self.fields.config.min_events_for_pattern.saturating_sub(1) {
                continue;
            }

            let avg_delta = if stats.count > 0 {
                (stats.total_delta_ms as f64 / stats.count as f64) as i64
            } else {
                0
            };

            let payload = json!({
                "pattern_type": "transition",
                "from_event": from,
                "to_event": to,
                "occurrences": stats.count,
                "avg_delta_ms": avg_delta,
                "last_seen": stats.last_seen,
            });

            let provenance = provenance_from_ids(&stats.sample_ids);
            let event =
                EventBuilder::dynamic("analytics-automaton", "analytics.pattern.detected", payload)
                    .with_provenance(provenance)
                    .at_time(Utc::now())
                    .build()
                    .expect("infallible: provenance set via with_provenance");
            patterns.push(event);
        }

        patterns
    }

    fn detect_correlations(&self, events: &[Event<JsonValue>]) -> Option<Event<JsonValue>> {
        if events.len() < 2 {
            return None;
        }

        let mut correlation_map: HashMap<(String, String), CorrelationStats> = HashMap::new();
        let lookahead = (self.fields.config.analysis_window_seconds.as_secs() / 6).max(120);
        let horizon = ChronoDuration::seconds(lookahead as i64);

        for (idx, event) in events.iter().enumerate() {
            let base_ts = event_timestamp(event);
            for peer in events.iter().skip(idx + 1) {
                let delta = event_timestamp(peer) - base_ts;
                if delta > horizon {
                    break;
                }

                if event.event_type == peer.event_type {
                    continue;
                }

                let mut pair = (event.event_type.to_string(), peer.event_type.to_string());
                if pair.0 > pair.1 {
                    pair = (pair.1, pair.0);
                }

                let entry = correlation_map.entry(pair).or_default();
                entry.count += 1;
                entry.total_gap_ms += delta.num_milliseconds().max(0);
                entry
                    .sample_ids
                    .extend(event_ids_from_events(vec![event, peer], MAX_PROVENANCE_IDS));
            }
        }

        let mut top_pairs: Vec<_> = correlation_map.into_iter().collect();
        top_pairs.sort_by(|a, b| b.1.count.cmp(&a.1.count));
        top_pairs.truncate(5);

        if top_pairs.is_empty() {
            return None;
        }

        let summary: Vec<_> = top_pairs
            .iter()
            .map(|((from, to), stats)| {
                let avg_gap_ms = if stats.count > 0 {
                    (stats.total_gap_ms as f64 / stats.count as f64) as i64
                } else {
                    0
                };
                json!({
                    "event_a": from,
                    "event_b": to,
                    "occurrences": stats.count,
                    "avg_gap_ms": avg_gap_ms,
                })
            })
            .collect();

        let payload = json!({
            "analysis_type": "correlation",
            "pairs": summary,
            "window_seconds": self.fields.config.analysis_window_seconds.as_secs(),
        });

        let provenance = provenance_from_ids(&top_pairs[0].1.sample_ids);
        Some(
            EventBuilder::dynamic("analytics-automaton", "analytics.correlation", payload)
                .with_provenance(provenance)
                .at_time(Utc::now())
                .build()
                .expect("infallible: provenance set via with_provenance"),
        )
    }
}

#[async_trait]
impl Node for AnalyticsAutomaton {
    type Config = AnalyticsAutomatonConfig;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.fields.db_pool = Some(runtime.db_pool().clone());
        self.fields.event_sender = Some(runtime.event_sender());
        self.fields.runtime = Some(runtime);
        self.fields.config = config;
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
            TimeHorizon::Snapshot => self.analyze_snapshot(Utc::now()).await.unwrap_or(0),
            TimeHorizon::Historical { end_time } => {
                self.analyze_snapshot(end_time).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => self.run_continuous(from).await.unwrap_or(0),
        };

        let duration = Utc::now().signed_duration_since(start_time);
        let snapshot_size = self.state.len();

        let report = ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds().max(0) as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                ("window_events".to_string(), snapshot_size as u64),
                (
                    "frequency_enabled".to_string(),
                    self.fields.config.enable_frequency_analysis as u64,
                ),
                (
                    "pattern_enabled".to_string(),
                    self.fields.config.enable_pattern_detection as u64,
                ),
                (
                    "correlation_enabled".to_string(),
                    self.fields.config.enable_correlation_analysis as u64,
                ),
            ]),
            successful_targets: vec!["analytics".to_string()],
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
        "analytics-automaton"
    }

    fn processor_type(&self) -> NodeType {
        NodeType::Automaton
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_snapshot: true,
            supports_historical: true,
            supports_continuous: true,
            manages_own_continuous_loop: true,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        if let Some(consumer) = self.fields.consumer.take() {
            consumer.stop().await;
        }
        if let Some(handle) = self.fields.consumer_handle.take() {
            if let Err(err) = handle.await {
                warn!("Failed to join analytics consumer task: {err}");
            }
        }
        self.fields.incoming_tx = None;
        self.fields.incoming_rx = None;
        Ok(())
    }
}

impl ExplorationProvider for AnalyticsAutomaton {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let window_events = self.state.len() as u64;
        let last_updated = self.fields.stats.last_activity.unwrap_or_else(Utc::now);

        Ok(SourceState {
            description: "Analytics automaton summarizing frequency/pattern data".to_string(),
            last_updated,
            total_items: Some(self.fields.stats.inputs_seen),
            metadata: HashMap::from([
                (
                    "analysis_window_seconds".to_string(),
                    json!(self.fields.config.analysis_window_seconds),
                ),
                ("window_events".to_string(), json!(window_events)),
                (
                    "frequency_enabled".to_string(),
                    json!(self.fields.config.enable_frequency_analysis),
                ),
                (
                    "pattern_enabled".to_string(),
                    json!(self.fields.config.enable_pattern_detection),
                ),
                (
                    "correlation_enabled".to_string(),
                    json!(self.fields.config.enable_correlation_analysis),
                ),
                (
                    "inputs_seen".to_string(),
                    json!(self.fields.stats.inputs_seen),
                ),
                (
                    "outputs_emitted".to_string(),
                    json!(self.fields.stats.outputs_emitted),
                ),
            ]),
            healthy: true,
            recent_activity: self.fields.recent_activity(),
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
                now - ChronoDuration::seconds(
                    self.fields.config.analysis_window_seconds.as_secs().max(60) as i64,
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
                "Tune analysis_window_seconds to control responsiveness".to_string(),
                "Enable correlation analysis for multi-stream insights".to_string(),
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

#[derive(Default)]
struct AnalyticsState {
    window: VecDeque<Event<JsonValue>>,
    window_ids: HashSet<Ulid>,
}

impl AnalyticsState {
    fn integrate(
        &mut self,
        mut events: Vec<Event<JsonValue>>,
        window_seconds: Seconds,
        max_events: usize,
    ) {
        events.sort_by_key(|event| event_timestamp(event));
        for event in events {
            if let Some(key) = event.id.as_ref().map(|id| *id.as_ulid()) {
                if self.window_ids.insert(key) {
                    self.window.push_back(event);
                }
            }
        }
        self.prune(window_seconds, max_events);
    }

    fn prune(&mut self, window_seconds: Seconds, max_events: usize) {
        let cutoff = Utc::now() - ChronoDuration::seconds(window_seconds.as_secs().max(60) as i64);
        while let Some(front) = self.window.front() {
            let outdated = event_timestamp(front) < cutoff;
            if outdated || self.window.len() > max_events {
                if let Some(evicted) = self.window.pop_front() {
                    if let Some(id) = evicted.id.as_ref() {
                        self.window_ids.remove(id.as_ulid());
                    }
                }
            } else {
                break;
            }
        }
        while self.window.len() > max_events {
            if let Some(evicted) = self.window.pop_front() {
                if let Some(id) = evicted.id.as_ref() {
                    self.window_ids.remove(id.as_ulid());
                }
            }
        }
    }

    fn snapshot(&self) -> Vec<Event<JsonValue>> {
        let mut snapshot: Vec<_> = self.window.iter().cloned().collect();
        snapshot.sort_by_key(|event| event_timestamp(event));
        snapshot
    }

    fn len(&self) -> usize {
        self.window.len()
    }
}

#[derive(Default)]
struct PatternStats {
    count: usize,
    total_delta_ms: i64,
    last_seen: Option<DateTime<Utc>>,
    sample_ids: Vec<EventId>,
}

#[derive(Default)]
struct CorrelationStats {
    count: usize,
    total_gap_ms: i64,
    sample_ids: Vec<EventId>,
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

// Provenance utilities (event_ids_from_events, provenance_from_ids, bootstrap_provenance)
// imported from sinex_node_sdk::automaton_base

impl AnalyticsAutomaton {
    fn build_synthesized_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        sample: &[Event<JsonValue>],
    ) -> Event<JsonValue> {
        let provenance = provenance_from_ids(&event_ids_from_events(
            sample.iter().collect::<Vec<&Event<JsonValue>>>(),
            MAX_PROVENANCE_IDS,
        ));
        EventBuilder::dynamic("analytics-automaton", event_type, payload)
            .with_provenance(provenance)
            .at_time(Utc::now())
            .build()
            .expect("infallible: provenance set via with_provenance")
    }
}

// ChannelConfirmedEventHandler is now imported from sinex_node_sdk::automaton_base

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_core::types::Id;
    use sinex_test_utils::{sinex_test, TestResult};

    fn test_event(event_type: &str, minutes_ago: i64) -> Event<JsonValue> {
        let provenance = bootstrap_provenance();
        let payload = json!({ "value": 1 });
        let mut event = EventBuilder::dynamic("test", event_type, payload)
            .with_provenance(provenance)
            .at_time(Utc::now() - ChronoDuration::minutes(minutes_ago))
            .build()
            .expect("infallible: test provenance set");
        event.id = Some(Id::new());
        event
    }

    #[sinex_test]
    fn frequency_analysis_finds_top_types() -> TestResult<()> {
        let automaton = AnalyticsAutomaton::new();
        let events = vec![
            test_event("fs.created", 5),
            test_event("fs.created", 4),
            test_event("terminal.command", 3),
        ];

        let report = automaton.generate_frequency_analysis(&events).unwrap();
        assert_eq!(report.event_type.to_string(), "analytics.frequency");
        Ok(())
    }

    #[sinex_test]
    fn pattern_detection_emits_transition_event() -> TestResult<()> {
        let mut automaton = AnalyticsAutomaton::new();
        automaton.fields.config.min_events_for_pattern = 2;
        let events = vec![
            test_event("a", 5),
            test_event("b", 4),
            test_event("a", 3),
            test_event("b", 2),
        ];

        let patterns = automaton.detect_patterns(&events);
        assert!(!patterns.is_empty());
        assert_eq!(
            patterns[0].event_type.to_string(),
            "analytics.pattern.detected"
        );
        Ok(())
    }

    #[sinex_test]
    fn correlation_detection_picks_pairs() -> TestResult<()> {
        let mut automaton = AnalyticsAutomaton::new();
        let config = &mut automaton.fields.config;
        config.analysis_window_seconds = Seconds::from_secs(600);
        config.enable_correlation_analysis = true;
        let events = vec![
            test_event("cmd.run", 4),
            test_event("fs.created", 3),
            test_event("cmd.run", 2),
            test_event("fs.created", 1),
        ];

        let correlation = automaton.detect_correlations(&events).unwrap();
        assert_eq!(correlation.event_type.to_string(), "analytics.correlation");
        Ok(())
    }

    #[sinex_test]
    fn analytics_state_prunes_old_events() -> TestResult<()> {
        let mut state = AnalyticsState::default();
        let old_event = test_event("fs.created", 10);
        state.integrate(vec![old_event], Seconds::from_secs(30), 16);
        assert_eq!(state.len(), 0, "old events should be evicted");
        Ok(())
    }

    #[sinex_test]
    fn analytics_state_dedupes_event_ids() -> TestResult<()> {
        let mut state = AnalyticsState::default();
        let event = test_event("terminal.command", 1);
        let duplicate = event.clone();
        state.integrate(vec![event.clone(), duplicate], Seconds::from_secs(600), 16);
        assert_eq!(state.len(), 1, "duplicate ULIDs should be ignored");

        // Integrating another unique event should increase length
        let other = test_event("fs.created", 1);
        state.integrate(vec![other], Seconds::from_secs(600), 16);
        assert_eq!(state.len(), 2);
        Ok(())
    }
}
