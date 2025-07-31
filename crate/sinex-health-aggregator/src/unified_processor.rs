//! Unified StatefulStreamProcessor implementation for Health Aggregator
//!
//! This demonstrates the correct pattern for automata: implementing StatefulStreamProcessor
//! directly and using RedisStreamConsumer for real-time event consumption, without any
//! HotlogAutomaton layer.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_db::models::{Event, RawEvent, SystemHealthSummaryPayload};
use sinex_satellite_sdk::{
    redis_stream_consumer::{
        BatchProcessingResult, EventBatchProcessor, RedisConsumerConfig, RedisStreamConsumer,
        EventFilter as StreamEventFilter},
    stream_processor::{
        Checkpoint, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon},
    SatelliteError, SatelliteResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// Use HealthStatus and ComponentHealth from sinex_events
use sinex_events::{HealthStatus, ComponentHealth};

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
        let event = Event::from(SystemHealthSummaryPayload {
            overall_status: summary.overall_status,
            healthy_components: summary.healthy_components,
            degraded_components: summary.degraded_components,
            failed_components: summary.failed_components,
            missing_components: summary.missing_components,
            total_components: summary.total_components,
            last_updated: summary.last_updated,
            components: summary.components,
        });

        info!(
            healthy = summary.healthy_components,
            degraded = summary.degraded_components,
            failed = summary.failed_components,
            missing = summary.missing_components,
            "Generated system health summary"
        );

        Ok(Some(event))
    }

    /// Get event filters for this automaton
    fn event_filters() -> Vec<StreamEventFilter> {
        vec![
            StreamEventFilter::new(
                Some("journald".to_string()),
                Some("satellite.heartbeat".to_string()),
            ),
        ]
    }
}

#[async_trait]
impl EventBatchProcessor for HealthAggregator {
    async fn process_batch(&mut self, events: Vec<RawEvent>) -> SatelliteResult<BatchProcessingResult> {
        let mut successful_ids = Vec::new();
        let mut failed_ids = Vec::new();
        
        for event in events {
            let event_id = event.id.to_string();
            
            match self.process_heartbeat(&event).await {
                Ok(_) => successful_ids.push(event_id),
                Err(e) => {
                    warn!("Failed to process heartbeat event {}: {}", event_id, e);
                    failed_ids.push((event_id, e.to_string()));
                }
            }
        }

        // Check if we should generate a summary
        if let Some(summary_event) = self.maybe_generate_summary().await? {
            if let Some(ctx) = &self.context {
                ctx.send_event(summary_event).await?;
            }
        }

        Ok(BatchProcessingResult {
            successful_ids,
            failed_ids,
            retry_ids: Vec::new(),
            checkpoint_data: None})
    }

    async fn get_checkpoint_data(&self) -> Option<serde_json::Value> {
        Some(json!({
            "last_summary_time": self.last_summary_time.to_rfc3339(),
            "component_count": self.component_health.lock().await.len()}))
    }
}

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
                // Real-time processing from Redis Stream
                info!("Starting continuous health monitoring from Redis Stream");
                
                let ctx = self.context.as_ref().ok_or_else(|| {
                    SatelliteError::Processing("Health aggregator context not initialized".to_string())
                })?;
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "health-aggregator".to_string(),
                    Self::event_filters(),
                );

                // This will run indefinitely for continuous mode
                let final_checkpoint = redis_consumer
                    .consume_continuous(from, self, args.shutdown_signal)
                    .await?;

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(final_checkpoint),
                    time_range: Some((start_time, Utc::now())),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["redis-stream".to_string()],
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

                // Process using batch processor
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "health-aggregator".to_string(),
                    Self::event_filters(),
                );

                let final_checkpoint = redis_consumer
                    .consume_historical(events, self, 100)
                    .await?;

                events_processed = events.len();

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