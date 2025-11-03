#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../doc/overview.md")]
#![doc = include_str!("../../../../docs/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/doc/overview.md")]

//! Analytics automaton entry points.
//!
//! Events → Analysis → Synthesized Events.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::Event,
        types::{events::payloads::*, Id},
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
            StreamProcessorContext, TimeHorizon,
        },
        SatelliteError, SatelliteResult,
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
    runtime: Option<ProcessorRuntimeState>,
    config: AnalyticsAutomatonConfig,
    event_sender: Option<mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
}

impl AnalyticsAutomaton {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: AnalyticsAutomatonConfig::default(),
            event_sender: None,
            db_pool: None,
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::General(color_eyre::eyre::eyre!(
                "Analytics automaton runtime not initialised"
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
        config: AnalyticsAutomatonConfig,
    ) -> SatelliteResult<()> {
        info!(
            processor = "analytics-automaton",
            service = %runtime.service_info().service_name(),
            "Initializing analytics automaton"
        );

        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.config = config;
        self.runtime = Some(runtime);

        Ok(())
    }

    /// Analyze events and produce analytics insights
    async fn analyze_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self.db_pool()?;
        let event_sender = self.event_sender()?;

        // Query recent events for analysis
        let events = self.query_events_for_analysis(db_pool, from).await?;
        info!("Analyzing {} events for patterns", events.len());

        let mut events_processed = 0u64;

        // Generate frequency analysis if enabled
        if self.config.enable_frequency_analysis && !events.is_empty() {
            if let Ok(analysis_event) = self.generate_frequency_analysis(&events).await {
                if let Err(e) = event_sender.send(analysis_event) {
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
                    if let Err(e) = event_sender.send(pattern_event) {
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
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.analysis_window_seconds as i64);

        let query = if self.config.target_event_types.is_empty() {
            // Analyze all event types
            vec![] // TODO: Fix query
        } else {
            vec![] // TODO: Fix query
        };

        Ok(query)
    }
}

#[async_trait]
impl StatefulStreamProcessor for AnalyticsAutomaton {
    type Config = AnalyticsAutomatonConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        let runtime = ctx.to_runtime_state();
        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.runtime = Some(runtime);
        self.config = config;
        Ok(())
    }

    async fn initialize_with_runtime(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, raw_config, service_info, handles, work_dir_utf8) = init.into_parts();
        let runtime = ProcessorRuntimeState::new(service_info, handles, raw_config, work_dir_utf8);
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
            TimeHorizon::Snapshot => self.analyze_events(&from).await.unwrap_or(0),
            TimeHorizon::Historical { .. } => self.analyze_events(&from).await.unwrap_or(0),
            TimeHorizon::Continuous => self.analyze_events(&from).await.unwrap_or(0),
        };

        let duration = Utc::now().signed_duration_since(start_time);

        Ok(ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds().max(0) as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                (
                    "target_event_types".to_string(),
                    serde_json::Value::Number((self.config.target_event_types.len() as u64).into()),
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

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_snapshot: true,
            supports_historical: true,
            supports_continuous: true,
            ..ProcessorCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

/// Type alias for compatibility with processor_main! macro
pub type AnalyticsProcessor = AnalyticsAutomaton;
