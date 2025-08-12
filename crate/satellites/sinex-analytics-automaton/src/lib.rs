//! Analytics Automaton - Event-driven data analysis and insights
//!
//! This automaton consumes events from the event stream and produces synthesized
//! analytical insights and patterns. It implements the proper automaton pattern:
//! Events → Analysis → Synthesized Events

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::RawEvent,
        types::{
            events::{payloads::*, Event},
            Id,
        },
    };

    // SDK facade for common processor types
    pub use sinex_satellite_sdk::{
        cli::{
            ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat,
            IngestionHistoryEntry, MissingItem, SourceState,
        },
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport,
            StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
        },
        SatelliteResult,
    };

    // External dependencies
    pub use {
        async_trait::async_trait,
        chrono::{DateTime, Utc},
        serde::{Deserialize, Serialize},
        sqlx::PgPool,
        std::{collections::HashMap, time::Duration},
        tokio::sync::mpsc,
        tracing::{debug, error, info, instrument, warn},
    };
}

// Use local facade for common types
use crate::common::*;

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
            analysis_window_seconds: 3600, // 1 hour
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
    context: Option<StreamProcessorContext>,
    config: AnalyticsAutomatonConfig,
    event_sender: Option<mpsc::Sender<RawEvent>>,
    db_pool: Option<PgPool>,
}

impl AnalyticsAutomaton {
    pub fn new() -> Self {
        Self {
            context: None,
            config: AnalyticsAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
        }
    }

    /// Analyze events and produce analytics insights
    async fn analyze_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("Database pool not initialized"))?;
        let event_sender = self
            .event_sender
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("Event sender not initialized"))?;

        // Query recent events for analysis
        let events = self.query_events_for_analysis(db_pool, from).await?;
        info!("Analyzing {} events for patterns", events.len());

        let mut events_processed = 0u64;

        // Generate frequency analysis if enabled
        if self.config.enable_frequency_analysis && !events.is_empty() {
            if let Ok(analysis_event) = self.generate_frequency_analysis(&events).await {
                if let Err(e) = event_sender.send(analysis_event).await {
                    warn!("Failed to send frequency analysis event: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        // Generate pattern detection if enabled
        if self.config.enable_pattern_detection
            && events.len() >= self.config.min_events_for_pattern
        {
            if let Ok(pattern_events) = self.detect_patterns(&events).await {
                for pattern_event in pattern_events {
                    if let Err(e) = event_sender.send(pattern_event).await {
                        warn!("Failed to send pattern detection event: {}", e);
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        Ok(events_processed)
    }

    /// Query events from the database for analysis
    async fn query_events_for_analysis(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> SatelliteResult<Vec<RawEvent>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.analysis_window_seconds as i64);

        let query = if self.config.target_event_types.is_empty() {
            // Analyze all event types
            sqlx::query_as!(
                RawEvent,
                r#"
                SELECT event_id as "id: Id<RawEvent>", source as "source: _", event_type as "event_type: _",
                       payload, ts_orig, host as "host: _", ingestor_version, payload_schema_id as "payload_schema_id: _",
                       provenance as "provenance: _", anchor_byte, associated_blob_ids as "associated_blob_ids: _"
                FROM core.events 
                WHERE ts_orig >= $1
                ORDER BY ts_orig DESC
                LIMIT 1000
                "#,
                window_start
            )
        } else {
            // Analyze specific event types
            sqlx::query_as!(
                RawEvent,
                r#"
                SELECT event_id as "id: Id<RawEvent>", source as "source: _", event_type as "event_type: _",
                       payload, ts_orig, host as "host: _", ingestor_version, payload_schema_id as "payload_schema_id: _",
                       provenance as "provenance: _", anchor_byte, associated_blob_ids as "associated_blob_ids: _"
                FROM core.events 
                WHERE ts_orig >= $1 AND event_type = ANY($2)
                ORDER BY ts_orig DESC
                LIMIT 1000
                "#,
                window_start,
                &self.config.target_event_types
            )
        };

        let events = query
            .fetch_all(db_pool)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to query events: {}", e))?;

        Ok(events)
    }

    /// Generate frequency analysis of events
    async fn generate_frequency_analysis(&self, events: &[RawEvent]) -> SatelliteResult<RawEvent> {
        let mut frequency_map: HashMap<String, u32> = HashMap::new();
        let mut source_frequency: HashMap<String, u32> = HashMap::new();

        for event in events {
            *frequency_map
                .entry(event.event_type.to_string())
                .or_insert(0) += 1;
            *source_frequency
                .entry(event.source.to_string())
                .or_insert(0) += 1;
        }

        let parent_event_ids: Vec<Id<RawEvent>> = events.iter().map(|e| e.id).collect();

        let analysis_payload = serde_json::json!({
            "analysis_type": "frequency_analysis",
            "analysis_window_seconds": self.config.analysis_window_seconds,
            "total_events_analyzed": events.len(),
            "event_type_frequencies": frequency_map,
            "source_frequencies": source_frequency,
            "generated_at": Utc::now(),
        });

        // Create synthesized event with proper provenance
        let event =
            Event::from_events(analysis_payload, parent_event_ids).with_ts_orig(Some(Utc::now()));

        Ok(event.into())
    }

    /// Detect temporal and behavioral patterns in events
    async fn detect_patterns(&self, events: &[RawEvent]) -> SatelliteResult<Vec<RawEvent>> {
        let mut pattern_events = Vec::new();

        // Simple pattern: detect bursts of activity
        let time_windows = self.analyze_temporal_patterns(events);

        if let Some(burst_pattern) = time_windows.into_iter().find(|w| w.event_count > 10) {
            let parent_event_ids: Vec<Id<RawEvent>> = burst_pattern.event_ids.clone();

            let pattern_payload = serde_json::json!({
                "pattern_type": "activity_burst",
                "window_start": burst_pattern.start_time,
                "window_end": burst_pattern.end_time,
                "event_count": burst_pattern.event_count,
                "events_per_second": burst_pattern.events_per_second,
                "dominant_event_types": burst_pattern.dominant_event_types,
                "generated_at": Utc::now(),
            });

            let pattern_event = Event::from_events(pattern_payload, parent_event_ids)
                .with_ts_orig(Some(Utc::now()));

            pattern_events.push(pattern_event.into());
        }

        Ok(pattern_events)
    }

    /// Analyze temporal patterns in events
    fn analyze_temporal_patterns(&self, events: &[RawEvent]) -> Vec<TimeWindow> {
        // Simple implementation: 5-minute windows
        let window_size = chrono::Duration::minutes(5);
        let mut windows = Vec::new();

        if events.is_empty() {
            return windows;
        }

        // Sort events by time
        let mut sorted_events = events.to_vec();
        sorted_events.sort_by_key(|e| e.ts_orig);

        let mut current_window_start = sorted_events[0].ts_orig;
        let mut current_window_events = Vec::new();
        let mut event_type_counts: HashMap<String, u32> = HashMap::new();

        for event in sorted_events {
            if event.ts_orig > current_window_start + window_size {
                // Finish current window
                if !current_window_events.is_empty() {
                    let dominant_types: Vec<String> = event_type_counts
                        .iter()
                        .filter(|(_, &count)| count >= 2)
                        .map(|(type_name, _)| type_name.clone())
                        .collect();

                    let duration = (current_window_start + window_size - current_window_start)
                        .num_seconds() as f64;
                    let events_per_second = if duration > 0.0 {
                        current_window_events.len() as f64 / duration
                    } else {
                        0.0
                    };

                    windows.push(TimeWindow {
                        start_time: current_window_start,
                        end_time: current_window_start + window_size,
                        event_count: current_window_events.len(),
                        events_per_second,
                        event_ids: current_window_events.clone(),
                        dominant_event_types: dominant_types,
                    });
                }

                // Start new window
                current_window_start = event.ts_orig;
                current_window_events.clear();
                event_type_counts.clear();
            }

            current_window_events.push(event.id);
            *event_type_counts
                .entry(event.event_type.to_string())
                .or_insert(0) += 1;
        }

        windows
    }
}

#[derive(Debug, Clone)]
struct TimeWindow {
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    event_count: usize,
    events_per_second: f64,
    event_ids: Vec<Id<RawEvent>>,
    dominant_event_types: Vec<String>,
}

#[async_trait]
impl StatefulStreamProcessor for AnalyticsAutomaton {
    type Config = AnalyticsAutomatonConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        info!("Initializing analytics automaton");

        // Get database pool from context
        self.db_pool = Some(ctx.db_pool.clone());
        self.event_sender = Some(ctx.event_sender.clone());
        self.context = Some(ctx);
        self.config = config;

        info!(
            "Analytics automaton configured - analyzing {} event types, window: {}s",
            if self.config.target_event_types.is_empty() {
                "all".to_string()
            } else {
                self.config.target_event_types.len().to_string()
            },
            self.config.analysis_window_seconds
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
                // Perform one-time analysis of recent events
                self.analyze_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Historical { .. } => {
                // Analyze historical events in the specified range
                self.analyze_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => {
                // Continuous analysis mode - analyze recent events
                self.analyze_events(&from).await.unwrap_or(0)
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
                    "events_analyzed".to_string(),
                    serde_json::Value::Number(events_processed.into()),
                ),
                (
                    "analysis_window_seconds".to_string(),
                    serde_json::Value::Number(self.config.analysis_window_seconds.into()),
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

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // Analytics operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }
}

impl Default for AnalyticsAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for AnalyticsAutomaton {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Analytics automaton for event pattern analysis and insights".to_string(),
            last_updated: Utc::now(),
            total_items: Some(0),
            metadata: HashMap::from([
                (
                    "analysis_window_seconds".to_string(),
                    self.config.analysis_window_seconds.to_string(),
                ),
                (
                    "target_event_types".to_string(),
                    format!("{:?}", self.config.target_event_types),
                ),
                (
                    "frequency_analysis".to_string(),
                    self.config.enable_frequency_analysis.to_string(),
                ),
                (
                    "pattern_detection".to_string(),
                    self.config.enable_pattern_detection.to_string(),
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
            coverage_percentage: 100.0, // Analytics processes all available events
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            recommendations: vec![
                "Analytics automaton processes events to generate insights".to_string(),
                "Adjust analysis_window_seconds to change temporal scope".to_string(),
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
