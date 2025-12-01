#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/docs/overview.md")]

//! Analytics automaton entry points.
//!
//! Events → Analysis → Synthesized Events.

mod common {
    pub use sinex_core::{
        db::models::{Event, EventId, Provenance},
        db::repositories::DbPoolExt,
        types::domain::EventType,
        JsonValue,
    };
    pub use sinex_processor_runtime::cli::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    };

    pub use sinex_satellite_sdk::{
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
            ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor, TimeHorizon,
        },
        SatelliteError, SatelliteResult,
    };

    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Duration as ChronoDuration, Utc},
        serde::{Deserialize, Serialize},
        sqlx::PgPool,
        std::time::Duration,
        tokio::sync::mpsc,
        tracing::{error, info, warn},
    };
}

use crate::common::*;
use serde_json::json;
use sinex_core::{environment, types::Result as CoreResult, Ulid};
use sinex_satellite_sdk::{
    confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent},
    event_processor::EventTransport,
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    ProcessingModel,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::task::JoinHandle;

const MAX_ANALYTICS_EVENTS: usize = 768;
const DEFAULT_BATCH_SIZE: usize = 128;
const MAX_PROVENANCE_IDS: usize = 8;

/// Configuration for Analytics Automaton
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnalyticsAutomatonConfig {
    /// Event types to analyze (empty = analyze all)
    pub target_event_types: Vec<String>,
    /// Analysis window size in seconds
    pub analysis_window_seconds: u64,
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
            analysis_window_seconds: 3600,
            min_events_for_pattern: 5,
            enable_frequency_analysis: true,
            enable_pattern_detection: true,
            enable_correlation_analysis: false,
        }
    }
}

/// Analytics Automaton using unified StatefulStreamProcessor architecture
///
/// Consumes events from the event stream and produces analytical insights:
/// - Frequency analysis of event patterns
/// - Temporal pattern detection
/// - Cross-domain correlation analysis
/// - Usage insights and behavioral patterns
pub struct AnalyticsAutomaton {
    runtime: Option<ProcessorRuntimeState>,
    config: AnalyticsAutomatonConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
    state: AnalyticsState,
    incoming_tx: Option<mpsc::UnboundedSender<ProvisionalEvent>>,
    incoming_rx: Option<mpsc::UnboundedReceiver<ProvisionalEvent>>,
    consumer: Option<Arc<JetStreamEventConsumer>>,
    consumer_handle: Option<JoinHandle<()>>,
}

impl AnalyticsAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: AnalyticsAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
            state: AnalyticsState::default(),
            incoming_tx: None,
            incoming_rx: None,
            consumer: None,
            consumer_handle: None,
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("Analytics automaton runtime not initialised".into())
        })
    }

    fn db_pool(&self) -> SatelliteResult<&PgPool> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.db_pool())
        } else if let Some(pool) = self.db_pool.as_ref() {
            Ok(pool)
        } else {
            Err(SatelliteError::Processing(
                "Database pool not initialized".into(),
            ))
        }
    }

    fn event_sender(&self) -> SatelliteResult<mpsc::UnboundedSender<Event<JsonValue>>> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.event_sender())
        } else if let Some(sender) = self.event_sender.as_ref() {
            Ok(sender.clone())
        } else {
            Err(SatelliteError::Processing(
                "Event sender not initialized".into(),
            ))
        }
    }

    fn ensure_event_channel(&mut self) {
        if self.incoming_tx.is_none() || self.incoming_rx.is_none() {
            let (tx, rx) = mpsc::unbounded_channel();
            self.incoming_tx = Some(tx);
            self.incoming_rx = Some(rx);
        }
    }

    async fn ensure_consumer(&mut self) -> SatelliteResult<()> {
        if let Some(handle) = self.consumer_handle.as_ref() {
            if !handle.is_finished() {
                return Ok(());
            }
        }

        self.consumer = None;
        self.consumer_handle = None;

        let runtime = self.runtime()?;
        let transport = runtime.transport().clone();
        let service_name = runtime.service_info().service_name().to_string();

        let nats_publisher = match transport {
            EventTransport::Nats(publisher) => publisher,
        };

        self.ensure_event_channel();
        let sender = self.incoming_tx.clone().ok_or_else(|| {
            SatelliteError::Processing("Confirmed event channel unavailable".into())
        })?;

        let handler = Arc::new(ChannelConfirmedEventHandler::new(sender));
        let env = environment().clone();
        let config = JetStreamEventConsumerConfig {
            processing_model: ProcessingModel::LeaderStandby,
            batch_size: DEFAULT_BATCH_SIZE,
            confirmation_timeout: Duration::from_secs(60),
            consumer_name: format!("{}-analytics-automaton", service_name.replace('.', "_")),
            enable_provisional_processing: false,
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

        self.consumer = Some(consumer);
        self.consumer_handle = Some(handle);

        Ok(())
    }

    async fn analyze_snapshot(&mut self, end_time: DateTime<Utc>) -> SatelliteResult<u64> {
        let db_pool = self.db_pool()?;
        let events = self
            .query_events_for_window(db_pool, end_time)
            .await
            .map_err(|err| SatelliteError::Processing(format!("Failed to query events: {err}")))?;

        if events.is_empty() {
            return Ok(0);
        }

        self.state.integrate(
            events,
            self.config.analysis_window_seconds,
            MAX_ANALYTICS_EVENTS,
        );
        self.emit_insights().await
    }

    async fn run_continuous(&mut self, from: Checkpoint) -> SatelliteResult<u64> {
        // Seed state with current window before streaming
        let mut processed = self.analyze_snapshot(Utc::now()).await.unwrap_or(0);

        self.ensure_consumer().await?;
        let mut receiver = self.incoming_rx.take().ok_or_else(|| {
            SatelliteError::Processing("Confirmed events channel not initialized".into())
        })?;

        while let Some(provisional) = receiver.recv().await {
            processed += self.process_confirmed_event(provisional).await?;
        }

        info!("Confirmed event channel closed; exiting analytics continuous loop");
        self.incoming_tx = None;
        self.consumer_handle = None;
        self.consumer = None;
        drop(from);

        Ok(processed)
    }

    async fn process_confirmed_event(
        &mut self,
        provisional: ProvisionalEvent,
    ) -> SatelliteResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;
        let event_id = EventId::from_ulid(provisional.event_id);

        let persisted = match db_pool.events().get_by_id(event_id).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                warn!("Confirmed event missing from database; skipping analytics update");
                return Ok(0);
            }
            Err(err) => {
                return Err(SatelliteError::Processing(format!(
                    "Failed to load confirmed event: {err}"
                )))
            }
        };

        self.state.integrate(
            vec![persisted],
            self.config.analysis_window_seconds,
            MAX_ANALYTICS_EVENTS,
        );

        self.emit_with_sender(&event_sender).await
    }

    async fn emit_insights(&mut self) -> SatelliteResult<u64> {
        let sender = self.event_sender()?;
        self.emit_with_sender(&sender).await
    }

    async fn emit_with_sender(
        &mut self,
        sender: &mpsc::UnboundedSender<Event<JsonValue>>,
    ) -> SatelliteResult<u64> {
        let snapshot = self.state.snapshot();
        if snapshot.is_empty() {
            return Ok(0);
        }

        let mut events_processed = 0u64;

        if self.config.enable_frequency_analysis {
            if let Some(event) = self.generate_frequency_analysis(&snapshot) {
                if sender.send(event).is_ok() {
                    events_processed += 1;
                } else {
                    warn!("Failed to send frequency analysis event");
                }
            }
        }

        if self.config.enable_pattern_detection {
            for pattern_event in self.detect_patterns(&snapshot) {
                if sender.send(pattern_event).is_ok() {
                    events_processed += 1;
                } else {
                    warn!("Failed to send pattern detection event");
                }
            }
        }

        if self.config.enable_correlation_analysis {
            if let Some(event) = self.detect_correlations(&snapshot) {
                if sender.send(event).is_ok() {
                    events_processed += 1;
                } else {
                    warn!("Failed to send correlation analysis event");
                }
            }
        }

        Ok(events_processed)
    }

    async fn query_events_for_window(
        &self,
        db_pool: &PgPool,
        end_time: DateTime<Utc>,
    ) -> CoreResult<Vec<Event<JsonValue>>> {
        let start_time =
            end_time - ChronoDuration::seconds(self.config.analysis_window_seconds.max(60) as i64);

        let mut collected = Vec::new();
        if self.config.target_event_types.is_empty() {
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
            for event_type_str in &self.config.target_event_types {
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
        let window_minutes = (self.config.analysis_window_seconds.max(60) as f64) / 60.0;
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
            "window_seconds": self.config.analysis_window_seconds,
        });

        Some(self.build_synthesized_event("analytics.frequency", payload, events))
    }

    fn detect_patterns(&self, events: &[Event<JsonValue>]) -> Vec<Event<JsonValue>> {
        if events.len() < self.config.min_events_for_pattern {
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
            if stats.count < self.config.min_events_for_pattern.saturating_sub(1) {
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
            let event = Event::create(
                "analytics-automaton",
                "analytics.pattern.detected",
                payload,
                provenance,
            )
            .at_time(Utc::now());
            patterns.push(event);
        }

        patterns
    }

    fn detect_correlations(&self, events: &[Event<JsonValue>]) -> Option<Event<JsonValue>> {
        if events.len() < 2 {
            return None;
        }

        let mut correlation_map: HashMap<(String, String), CorrelationStats> = HashMap::new();
        let lookahead = (self.config.analysis_window_seconds / 6).max(120);
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
            "window_seconds": self.config.analysis_window_seconds,
        });

        let provenance = provenance_from_ids(&top_pairs[0].1.sample_ids);
        Some(
            Event::create(
                "analytics-automaton",
                "analytics.correlation",
                payload,
                provenance,
            )
            .at_time(Utc::now()),
        )
    }
}

#[async_trait]
impl StatefulStreamProcessor for AnalyticsAutomaton {
    type Config = AnalyticsAutomatonConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
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
    ) -> SatelliteResult<ScanReport> {
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

        Ok(ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds().max(0) as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                ("window_events".into(), snapshot_size as u64),
                (
                    "frequency_enabled".into(),
                    self.config.enable_frequency_analysis as u64,
                ),
                (
                    "pattern_enabled".into(),
                    self.config.enable_pattern_detection as u64,
                ),
                (
                    "correlation_enabled".into(),
                    self.config.enable_correlation_analysis as u64,
                ),
            ]),
            successful_targets: vec!["analytics".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "analytics-automaton"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_snapshot: true,
            supports_historical: true,
            supports_continuous: true,
            manages_own_continuous_loop: true,
            ..ProcessorCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        if let Some(consumer) = self.consumer.take() {
            consumer.stop().await;
        }
        if let Some(handle) = self.consumer_handle.take() {
            if let Err(err) = handle.await {
                warn!("Failed to join analytics consumer task: {err}");
            }
        }
        self.incoming_tx = None;
        self.incoming_rx = None;
        Ok(())
    }
}

/// Type alias for compatibility with processor_main! macro
pub type AnalyticsProcessor = AnalyticsAutomaton;

impl ExplorationProvider for AnalyticsAutomaton {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let window_events = self.state.len() as u64;
        Ok(SourceState {
            description: "Analytics automaton summarizing frequency/pattern data".to_string(),
            last_updated: Utc::now(),
            total_items: Some(window_events),
            metadata: HashMap::from([
                (
                    "analysis_window_seconds".to_string(),
                    json!(self.config.analysis_window_seconds),
                ),
                ("window_events".to_string(), json!(window_events)),
                (
                    "frequency_enabled".to_string(),
                    json!(self.config.enable_frequency_analysis),
                ),
                (
                    "pattern_enabled".to_string(),
                    json!(self.config.enable_pattern_detection),
                ),
                (
                    "correlation_enabled".to_string(),
                    json!(self.config.enable_correlation_analysis),
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
            time_range: (
                now - ChronoDuration::seconds(self.config.analysis_window_seconds.max(60) as i64),
                now,
            ),
            source_total: self.state.len() as u64,
            sinex_total: self.state.len() as u64,
            coverage_percentage: 100.0,
            missing_count: 0,
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
        window_seconds: u64,
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

    fn prune(&mut self, window_seconds: u64, max_events: usize) {
        let cutoff = Utc::now() - ChronoDuration::seconds(window_seconds.max(60) as i64);
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

fn event_ids_from_events(events: Vec<&Event<JsonValue>>, max: usize) -> Vec<EventId> {
    let mut ids = Vec::new();
    for event in events {
        if let Some(id) = event.id.as_ref().cloned() {
            ids.push(id);
            if ids.len() >= max {
                break;
            }
        }
    }
    ids
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
            0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ])
        .expect("valid ULID bytes"),
    );
    Provenance::from_synthesis_safe(bootstrap, vec![])
}

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
        Event::create("analytics-automaton", event_type, payload, provenance).at_time(Utc::now())
    }
}

#[derive(Clone)]
struct ChannelConfirmedEventHandler {
    sender: mpsc::UnboundedSender<ProvisionalEvent>,
}

impl ChannelConfirmedEventHandler {
    fn new(sender: mpsc::UnboundedSender<ProvisionalEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl ConfirmedEventHandler for ChannelConfirmedEventHandler {
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> SatelliteResult<()> {
        self.sender.send(event.clone()).map_err(|err| {
            SatelliteError::Processing(format!(
                "Failed to forward confirmed analytics event: {err}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_core::types::Id;
    use sinex_test_utils::sinex_test;

    fn test_event(event_type: &str, minutes_ago: i64) -> Event<JsonValue> {
        let provenance = default_provenance();
        let payload = json!({ "value": 1 });
        let mut event = Event::create("test", event_type, payload, provenance)
            .at_time(Utc::now() - ChronoDuration::minutes(minutes_ago));
        event.id = Some(Id::new());
        event
    }

    #[sinex_test]
    fn frequency_analysis_finds_top_types() -> color_eyre::Result<()> {
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
    fn pattern_detection_emits_transition_event() -> color_eyre::Result<()> {
        let mut automaton = AnalyticsAutomaton::new();
        automaton.config.min_events_for_pattern = 2;
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
    #[allow(unused_mut)]
    fn correlation_detection_picks_pairs() -> color_eyre::Result<()> {
        let mut automaton = AnalyticsAutomaton::new();
        automaton.config.analysis_window_seconds = 600;
        automaton.config.enable_correlation_analysis = true;
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
    fn analytics_state_prunes_old_events() -> color_eyre::Result<()> {
        let mut state = AnalyticsState::default();
        let old_event = test_event("fs.created", 10);
        state.integrate(vec![old_event], 30, 16);
        assert_eq!(state.len(), 0, "old events should be evicted");
        Ok(())
    }

    #[sinex_test]
    fn analytics_state_dedupes_event_ids() -> color_eyre::Result<()> {
        let mut state = AnalyticsState::default();
        let event = test_event("terminal.command", 1);
        let duplicate = event.clone();
        state.integrate(vec![event.clone(), duplicate], 600, 16);
        assert_eq!(state.len(), 1, "duplicate ULIDs should be ignored");

        // Integrating another unique event should increase length
        let other = test_event("fs.created", 1);
        state.integrate(vec![other], 600, 16);
        assert_eq!(state.len(), 2);
        Ok(())
    }
}
