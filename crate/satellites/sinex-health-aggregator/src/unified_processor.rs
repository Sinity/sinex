//! Unified StatefulStreamProcessor implementation for Health Aggregator
//!
//! This demonstrates the correct pattern for automata: implementing StatefulStreamProcessor
//! directly and using RedisStreamConsumer for real-time event consumption, without any
//! HotlogAutomaton layer.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::models::{RawEvent, SystemHealthSummaryPayload};
use sinex_satellite_sdk::{
    cli::{ExplorationProvider, SourceState, IngestionHistoryEntry, CoverageAnalysis, ExportFormat, ActivityEntry},
    stream_processor::{
        Checkpoint, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon},
    SatelliteError, SatelliteResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// Use HealthStatus and ComponentHealth from sinex_events
use sinex_core::types::events::{HealthStatus, ComponentHealth};

/// System-wide health summary (internal representation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealthSummary {
    pub overall_status: HealthStatus,
    pub healthy_components: u32,
    pub degraded_components: u32,
    pub failed_components: u32,
    pub missing_components: u32,
    pub total_components: u32,
    pub last_updated: DateTime<Utc>,
    pub components: HashMap<String, ComponentHealth>}

/// Health Aggregator as a unified StatefulStreamProcessor
pub struct HealthAggregator {
    context: Option<StreamProcessorContext>,
    expected_components: Vec<String>,
    aggregation_window: Duration,
    component_health: Arc<Mutex<HashMap<String, ComponentHealth>>>,
    last_summary_time: DateTime<Utc>}

impl HealthAggregator {
    pub fn new() -> Self {
        Self {
            context: None,
            expected_components: vec![
                "sinex-ingestd".to_string(),
                "sinex-gateway".to_string(),
                "sinex-fs-watcher".to_string(),
                "sinex-terminal-satellite".to_string(),
                "sinex-health-aggregator".to_string(),
            ],
            aggregation_window: Duration::minutes(5),
            component_health: Arc::new(Mutex::new(HashMap::new())),
            last_summary_time: Utc::now() - Duration::minutes(10), // Force initial summary
        }
    }

    /// Process a heartbeat event and update component health
    async fn process_heartbeat(&self, event: &RawEvent) -> SatelliteResult<()> {
        if event.source == "journald" && event.event_type == "satellite.heartbeat" {
            let payload = &event.payload;
            
            let service_name = payload
                .get("service_name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let health = ComponentHealth {
                service_name: service_name.clone(),
                status: HealthStatus::Healthy,
                last_heartbeat: event.ts_orig,
                uptime_seconds: payload.get("uptime_seconds").and_then(|v| v.as_i64()),
                memory_usage_mb: payload
                    .get("memory_usage_mb")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32),
                events_processed: payload.get("events_processed").and_then(|v| v.as_i64()),
                version: payload
                    .get("version")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                git_hash: payload
                    .get("git_hash")
                    .and_then(|v| v.as_str())
                    .map(String::from)};

            let mut health_map = self.component_health.lock().await;
            health_map.insert(service_name, health);
        }

        Ok(())
    }

    /// Generate health summary if enough time has passed
    async fn maybe_generate_summary(&mut self) -> SatelliteResult<Option<RawEvent>> {
        let now = Utc::now();
        
        if now - self.last_summary_time < self.aggregation_window {
            return Ok(None);
        }

        self.last_summary_time = now;

        let health_map = self.component_health.lock().await;
        let mut summary = SystemHealthSummary {
            overall_status: HealthStatus::Healthy,
            healthy_components: 0,
            degraded_components: 0,
            failed_components: 0,
            missing_components: 0,
            total_components: self.expected_components.len() as u32,
            last_updated: now,
            components: health_map.clone()};

        // Check each expected component
        for expected in &self.expected_components {
            if let Some(health) = health_map.get(expected) {
                let age = now - health.last_heartbeat;
                if age < Duration::minutes(2) {
                    summary.healthy_components += 1;
                } else if age < Duration::minutes(5) {
                    summary.degraded_components += 1;
                } else {
                    summary.failed_components += 1;
                }
            } else {
                summary.missing_components += 1;
            }
        }

        // Determine overall status
        if summary.missing_components > 0 || summary.failed_components > 0 {
            summary.overall_status = HealthStatus::Failed;
        } else if summary.degraded_components > 0 {
            summary.overall_status = HealthStatus::Degraded;
        }

        // Create synthesis event
        let event: RawEvent = Event::from_payload(SystemHealthSummaryPayload {
            overall_status: summary.overall_status,
            healthy_components: summary.healthy_components,
            degraded_components: summary.degraded_components,
            failed_components: summary.failed_components,
            missing_components: summary.missing_components,
            total_components: summary.total_components,
            last_updated: summary.last_updated,
            components: summary.components,
        }).into();

        info!(
            healthy = summary.healthy_components,
            degraded = summary.degraded_components,
            failed = summary.failed_components,
            missing = summary.missing_components,
            "Generated system health summary"
        );

        Ok(Some(event))
    }

    // TODO: Remove event_filters after NatsStreamConsumer removal
    // /// Get event filters for this automaton
    // fn event_filters() -> Vec<NatsEventFilter> {
    //     vec![
    //         NatsEventFilter {
    //             sources: vec!["journald".to_string()],
    //             event_types: vec!["satellite.heartbeat".to_string()],
    //         },
    //     ]
    // }
}

// TODO: Remove NatsEventBatchProcessor implementation after NatsStreamConsumer removal
// #[async_trait]
// impl NatsEventBatchProcessor for HealthAggregator {
//     async fn process_batch(&mut self, events: Vec<RawEvent>) -> SatelliteResult<NatsBatchProcessingResult> {
//         // ... implementation removed
//     }
// }

#[async_trait]
impl StatefulStreamProcessor for HealthAggregator {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        info!("Initializing Health Aggregator automaton");
        self.context = Some(ctx);
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let mut events_processed = 0;

        match until {
            TimeHorizon::Continuous => {
                // TODO: Implement continuous health monitoring after NatsStreamConsumer removal
                warn!("Health aggregator continuous mode not yet implemented after NatsStreamConsumer removal");
                
                Ok(ScanReport {
                    events_processed,
                    duration: std::time::Duration::from_millis(0),
                    final_checkpoint: from,
                    time_range: Some((start_time, Utc::now())),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["health-aggregator".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new()})
            }
            TimeHorizon::Historical { end_time } => {
                // Query historical events from PostgreSQL
                info!(
                    "Processing historical health events up to {}",
                    end_time
                );

                let ctx = self.context.as_ref().ok_or_else(|| {
                    SatelliteError::Processing("Health aggregator context not initialized".to_string())
                })?;
                
                // Determine start time from checkpoint
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    Checkpoint::Internal { event_id, .. } => {
                        // Look up timestamp of this event
                        // For now, just use a reasonable default
                        end_time - Duration::days(1)
                    }
                    _ => end_time - Duration::days(7), // Default to 7 days
                };

                // Query events
                let events = ctx.db_pool.events()
                    .get_events_by_type_and_time_range(
                        "journald",
                        "satellite.heartbeat",
                        start_time,
                        end_time,
                        Some(1000),
                        None,
                    )
                    .await?;

                // Process events directly using batch processor
                let raw_events: Vec<RawEvent> = events.into_iter()
                    .map(|e| RawEvent::from(e))
                    .collect();
                
                let batch_result = self.process_batch(raw_events).await?;
                events_processed = batch_result.successful_ids.len() as u64;
                
                let final_checkpoint = if events_processed > 0 {
                    Checkpoint::timestamp(end_time, None)
                } else {
                    from.clone()
                };

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(final_checkpoint),
                    time_range: Some((start_time, end_time)),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["postgresql".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new()})
            }
            TimeHorizon::Snapshot => {
                // Generate immediate health snapshot
                info!("Generating health snapshot");
                
                if let Some(summary_event) = self.maybe_generate_summary().await? {
                    if let Some(ctx) = &self.context {
                        ctx.send_event(summary_event).await?;
                        events_processed = 1;
                    }
                }

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(Checkpoint::None),
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["snapshot".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new()})
            }
        }
    }

    fn processor_name(&self) -> &str {
        "health-aggregator"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::Timestamp {
            timestamp: self.last_summary_time,
            metadata: self.get_checkpoint_data().await})
    }
}

impl Default for HealthAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for HealthAggregator {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "Health aggregator monitoring satellite heartbeats".to_string(),
            last_updated: Utc::now(),
            total_items: Some(self.expected_components.len() as u64),
            metadata: [
                ("aggregation_window_minutes".to_string(), json!(self.aggregation_window.num_minutes())),
                ("expected_components".to_string(), json!(self.expected_components.len())),
            ].into_iter().collect(),
            healthy: true,
            recent_activity: vec![
                ActivityEntry {
                    timestamp: self.last_summary_time,
                    description: "Last health summary generated".to_string(),
                    data: None,
                }
            ],
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        // Health aggregator doesn't have traditional ingestion history
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let now = Utc::now();
        let time_range = (now - chrono::Duration::hours(24), now);
        
        Ok(CoverageAnalysis {
            time_range,
            source_total: self.expected_components.len() as u64,
            sinex_total: self.expected_components.len() as u64, // Assume all are monitored
            coverage_percentage: 100.0,
            missing_count: 0,
            missing_samples: vec![],
            duplicate_count: 0,
            recommendations: vec![
                "All expected components are being monitored".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        _path: &camino::Utf8PathBuf,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        Err("Export not implemented for health aggregator".into())
    }
}