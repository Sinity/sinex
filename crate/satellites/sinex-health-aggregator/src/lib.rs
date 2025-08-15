//! Health Aggregator - Event-driven system health monitoring and aggregation
//!
//! This automaton consumes health-related events from various system components
//! and produces synthesized health status reports. It implements the proper automaton pattern:
//! Health Events → Aggregation → Synthesized Health Status Events

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::{Event, RawEvent},
        types::{events::payloads::*, Id},
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
        serde_json,
        sqlx::PgPool,
        std::{collections::HashMap, time::Duration},
        tokio::sync::mpsc,
        tracing::{debug, error, info, instrument, warn},
    };
}

// Use local facade for common types
use crate::common::*;

/// Configuration for Health Aggregator
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthAggregatorConfig {
    /// Health check intervals in seconds for different components
    pub component_check_intervals: HashMap<String, u64>,
    /// Health aggregation window in seconds
    pub aggregation_window_seconds: u64,
    /// Threshold for marking a component as unhealthy (missed checks)
    pub unhealthy_threshold_minutes: u64,
    /// Enable system-wide health status generation
    pub enable_system_health_status: bool,
    /// Enable component-specific health reports
    pub enable_component_health_reports: bool,
}

impl Default for HealthAggregatorConfig {
    fn default() -> Self {
        let mut component_intervals = HashMap::new();
        component_intervals.insert("database".to_string(), 60); // 1 minute
        component_intervals.insert("filesystem".to_string(), 300); // 5 minutes
        component_intervals.insert("network".to_string(), 120); // 2 minutes
        component_intervals.insert("services".to_string(), 180); // 3 minutes

        Self {
            component_check_intervals: component_intervals,
            aggregation_window_seconds: 3600, // 1 hour
            unhealthy_threshold_minutes: 10,  // 10 minutes without health updates
            enable_system_health_status: true,
            enable_component_health_reports: true,
        }
    }
}

/// Health status for a system component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub component_name: String,
    pub last_seen: DateTime<Utc>,
    pub status: HealthStatus,
    pub metrics: HashMap<String, f64>,
    pub recent_events: Vec<Id<RawEvent>>,
}

/// Health status enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Health Aggregator using unified StatefulStreamProcessor architecture
///
/// Consumes health events from system components and produces aggregated health insights:
/// - Component health status tracking
/// - System-wide health aggregation
/// - Health trend analysis
/// - Alert generation for unhealthy components
pub struct HealthAggregator {
    context: Option<StreamProcessorContext>,
    config: HealthAggregatorConfig,
    event_sender: Option<mpsc::Sender<RawEvent>>,
    db_pool: Option<PgPool>,
    component_health: HashMap<String, ComponentHealth>,
}

impl HealthAggregator {
    pub fn new() -> Self {
        Self {
            context: None,
            config: HealthAggregatorConfig::default(),
            event_sender: None,
            db_pool: None,
            component_health: HashMap::new(),
        }
    }

    /// Aggregate health events and produce health status reports
    async fn aggregate_health_events(&mut self, from: &Checkpoint) -> SatelliteResult<u64> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("Database pool not initialized"))?;
        let event_sender = self
            .event_sender
            .as_ref()
            .ok_or_else(|| color_eyre::eyre::eyre!("Event sender not initialized"))?;

        // Query recent health events
        let health_events = self.query_health_events(db_pool, from).await?;
        info!(
            "Processing {} health events for aggregation",
            health_events.len()
        );

        // Update component health status from events
        self.update_component_health(&health_events).await;

        let mut events_processed = 0u64;

        // Generate component health reports if enabled
        if self.config.enable_component_health_reports {
            for (component_name, health) in &self.component_health {
                if let Ok(health_report) = self
                    .generate_component_health_report(component_name, health)
                    .await
                {
                    if let Err(e) = event_sender.send(health_report).await {
                        warn!(
                            "Failed to send component health report for {}: {}",
                            component_name, e
                        );
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        // Generate system-wide health status if enabled
        if self.config.enable_system_health_status {
            if let Ok(system_health) = self.generate_system_health_status().await {
                if let Err(e) = event_sender.send(system_health).await {
                    warn!("Failed to send system health status: {}", e);
                } else {
                    events_processed += 1;
                }
            }
        }

        // Generate health alerts for unhealthy components
        let alert_events = self.generate_health_alerts().await?;
        for alert_event in alert_events {
            if let Err(e) = event_sender.send(alert_event).await {
                warn!("Failed to send health alert: {}", e);
            } else {
                events_processed += 1;
            }
        }

        Ok(events_processed)
    }

    /// Query health-related events from the database
    async fn query_health_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> SatelliteResult<Vec<RawEvent>> {
        let window_start =
            Utc::now() - chrono::Duration::seconds(self.config.aggregation_window_seconds as i64);

        // Query events that might contain health information
        let health_event_types = vec![
            "system.health".to_string(),
            "service.status".to_string(),
            "database.health".to_string(),
            "filesystem.health".to_string(),
            "network.health".to_string(),
            "process.status".to_string(),
            "system.error".to_string(),
            "service.error".to_string(),
        ];

        use sinex_core::db::repositories::events::EventRecordExt;
        use sinex_schema::schema::records::EventRecord;

        let event_records = sqlx::query_as!(
            EventRecord,
            r#"
            SELECT 
                id as "id!: Uuid",
                ts_ingest as "ts_ingest!",
                ts_orig,
                source as "source!",
                event_type as "event_type!",
                host as "host!",
                payload as "payload!",
                ingestor_version,
                payload_schema_id,
                payload_schema_name,
                payload_schema_version,
                source_event_ids as "source_event_ids?: Vec<Uuid>",
                source_material_id,
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids as "associated_blob_ids?: Vec<Uuid>"
            FROM core.events 
            WHERE ts_orig >= $1 
            AND (event_type = ANY($2) OR payload::text ILIKE '%health%' OR payload::text ILIKE '%status%')
            ORDER BY ts_orig DESC
            LIMIT 1000
            "#,
            window_start,
            &health_event_types
        )
        .fetch_all(db_pool).await
        .map_err(|e| color_eyre::eyre::eyre!("Failed to query health events: {}", e))?;

        let events: Vec<RawEvent> = event_records
            .into_iter()
            .map(|record| record.to_event())
            .collect();

        Ok(events)
    }

    /// Update component health status based on events
    async fn update_component_health(&mut self, events: &[RawEvent]) {
        for event in events {
            if let Some(component_info) = self.extract_component_health_info(event) {
                let component_health = self
                    .component_health
                    .entry(component_info.component_name.clone())
                    .or_insert_with(|| ComponentHealth {
                        component_name: component_info.component_name.clone(),
                        last_seen: event.ts_orig.unwrap_or_else(Utc::now),
                        status: HealthStatus::Unknown,
                        metrics: HashMap::new(),
                        recent_events: Vec::new(),
                    });

                // Update health information
                component_health.last_seen = event.ts_orig.unwrap_or_else(Utc::now);
                component_health.status = component_info.status;
                component_health.metrics.extend(component_info.metrics);
                if let Some(event_id) = event.id {
                    component_health.recent_events.push(event_id);
                }

                // Keep only recent events (last 10)
                if component_health.recent_events.len() > 10 {
                    component_health
                        .recent_events
                        .drain(..component_health.recent_events.len() - 10);
                }
            }
        }

        // Check for stale components (haven't reported in a while)
        let stale_threshold =
            Utc::now() - chrono::Duration::minutes(self.config.unhealthy_threshold_minutes as i64);
        for component_health in self.component_health.values_mut() {
            if component_health.last_seen < stale_threshold {
                component_health.status = HealthStatus::Critical;
            }
        }
    }

    /// Extract component health information from an event
    fn extract_component_health_info(&self, event: &RawEvent) -> Option<ComponentHealthInfo> {
        if let Ok(payload) = serde_json::from_value::<serde_json::Value>(event.payload.clone()) {
            // Try to extract component health information
            let component_name = self.determine_component_name(event, &payload)?;
            let status = self.determine_health_status(&payload);
            let metrics = self.extract_health_metrics(&payload);

            Some(ComponentHealthInfo {
                component_name,
                status,
                metrics,
            })
        } else {
            None
        }
    }

    /// Determine component name from event and payload
    fn determine_component_name(
        &self,
        event: &RawEvent,
        payload: &serde_json::Value,
    ) -> Option<String> {
        // Check explicit component field
        if let Some(component) = payload.get("component").and_then(|v| v.as_str()) {
            return Some(component.to_string());
        }

        // Infer from source
        let source_str = event.source.to_string();
        if source_str.contains("database")
            || source_str.contains("postgres")
            || source_str.contains("sqlx")
        {
            return Some("database".to_string());
        }
        if source_str.contains("filesystem") || source_str.contains("file") {
            return Some("filesystem".to_string());
        }
        if source_str.contains("network") || source_str.contains("http") {
            return Some("network".to_string());
        }
        if source_str.contains("system") || source_str.contains("service") {
            return Some("services".to_string());
        }

        // Infer from event type
        let event_type_str = event.event_type.to_string();
        if event_type_str.contains("database") {
            Some("database".to_string())
        } else if event_type_str.contains("file") || event_type_str.contains("fs") {
            Some("filesystem".to_string())
        } else if event_type_str.contains("network") {
            Some("network".to_string())
        } else if event_type_str.contains("service") || event_type_str.contains("process") {
            Some("services".to_string())
        } else {
            Some("unknown".to_string())
        }
    }

    /// Determine health status from payload
    fn determine_health_status(&self, payload: &serde_json::Value) -> HealthStatus {
        // Check explicit status field
        if let Some(status_str) = payload.get("status").and_then(|v| v.as_str()) {
            return match status_str.to_lowercase().as_str() {
                "healthy" | "ok" | "success" | "running" => HealthStatus::Healthy,
                "warning" | "degraded" | "slow" => HealthStatus::Warning,
                "critical" | "error" | "failed" | "down" => HealthStatus::Critical,
                _ => HealthStatus::Unknown,
            };
        }

        // Check for error indicators
        if payload.get("error").is_some()
            || payload.get("exception").is_some()
            || payload
                .get("failed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        {
            return HealthStatus::Critical;
        }

        // Check success indicators
        if payload
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            || payload
                .get("healthy")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        {
            return HealthStatus::Healthy;
        }

        HealthStatus::Unknown
    }

    /// Extract health metrics from payload
    fn extract_health_metrics(&self, payload: &serde_json::Value) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();

        // Extract numeric fields that might be metrics
        if let Some(obj) = payload.as_object() {
            for (key, value) in obj {
                if let Some(num_value) = value.as_f64() {
                    match key.as_str() {
                        "cpu_usage" | "memory_usage" | "disk_usage" | "network_usage"
                        | "response_time" | "error_rate" | "throughput" | "latency"
                        | "connection_count" | "queue_size" => {
                            metrics.insert(key.clone(), num_value);
                        }
                        _ => {
                            // Include any numeric metric
                            if key.contains("_usage")
                                || key.contains("_rate")
                                || key.contains("_time")
                                || key.contains("_count")
                            {
                                metrics.insert(key.clone(), num_value);
                            }
                        }
                    }
                }
            }
        }

        metrics
    }

    /// Generate health report for a specific component
    async fn generate_component_health_report(
        &self,
        component_name: &str,
        health: &ComponentHealth,
    ) -> SatelliteResult<RawEvent> {
        let report_payload = serde_json::json!({
            "report_type": "component_health",
            "component_name": component_name,
            "status": health.status,
            "last_seen": health.last_seen,
            "metrics": health.metrics,
            "recent_event_count": health.recent_events.len(),
            "minutes_since_last_update": (Utc::now() - health.last_seen).num_minutes(),
            "generated_at": Utc::now(),
        });

        // Create synthesized event with proper provenance from recent events
        let event = Event::from_events(report_payload, health.recent_events.clone())
            .with_ts_orig(Some(Utc::now()));

        Ok(event.into())
    }

    /// Generate system-wide health status
    async fn generate_system_health_status(&self) -> SatelliteResult<RawEvent> {
        let total_components = self.component_health.len();
        let healthy_components = self
            .component_health
            .values()
            .filter(|h| matches!(h.status, HealthStatus::Healthy))
            .count();
        let warning_components = self
            .component_health
            .values()
            .filter(|h| matches!(h.status, HealthStatus::Warning))
            .count();
        let critical_components = self
            .component_health
            .values()
            .filter(|h| matches!(h.status, HealthStatus::Critical))
            .count();

        let overall_status = if critical_components > 0 {
            HealthStatus::Critical
        } else if warning_components > 0 {
            HealthStatus::Warning
        } else if healthy_components == total_components && total_components > 0 {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        };

        let all_event_ids: Vec<Id<RawEvent>> = self
            .component_health
            .values()
            .flat_map(|h| h.recent_events.iter().cloned())
            .collect();

        let system_health_payload = serde_json::json!({
            "report_type": "system_health",
            "overall_status": overall_status,
            "total_components": total_components,
            "healthy_components": healthy_components,
            "warning_components": warning_components,
            "critical_components": critical_components,
            "health_score": if total_components > 0 { (healthy_components as f64 / total_components as f64) * 100.0 } else { 0.0 },
            "component_summary": self.component_health.iter()
                .map(|(name, health)| (name, health.status.clone()))
                .collect::<HashMap<_, _>>(),
            "generated_at": Utc::now(),
        });

        let event = Event::from_synthesis(
            "health-aggregator",
            "health.system_status",
            system_health_payload,
            all_event_ids,
        )
        .with_timestamp(Utc::now());

        Ok(event.into())
    }

    /// Generate health alerts for unhealthy components
    async fn generate_health_alerts(&self) -> SatelliteResult<Vec<RawEvent>> {
        let mut alerts = Vec::new();
        let alert_threshold =
            Utc::now() - chrono::Duration::minutes(self.config.unhealthy_threshold_minutes as i64);

        for (component_name, health) in &self.component_health {
            let should_alert = match health.status {
                HealthStatus::Critical => true,
                HealthStatus::Warning => health.last_seen < alert_threshold,
                _ => false,
            };

            if should_alert {
                let alert_payload = serde_json::json!({
                    "alert_type": "health_alert",
                    "component_name": component_name,
                    "alert_level": match health.status {
                        HealthStatus::Critical => "critical",
                        HealthStatus::Warning => "warning",
                        _ => "unknown",
                    },
                    "last_seen": health.last_seen,
                    "minutes_since_update": (Utc::now() - health.last_seen).num_minutes(),
                    "current_status": health.status,
                    "recent_metrics": health.metrics,
                    "generated_at": Utc::now(),
                });

                let alert_event = Event::from_events(alert_payload, health.recent_events.clone())
                    .with_ts_orig(Some(Utc::now()));

                alerts.push(alert_event.into());
            }
        }

        Ok(alerts)
    }
}

#[derive(Debug, Clone)]
struct ComponentHealthInfo {
    component_name: String,
    status: HealthStatus,
    metrics: HashMap<String, f64>,
}

#[async_trait]
impl StatefulStreamProcessor for HealthAggregator {
    type Config = HealthAggregatorConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        info!("Initializing health aggregator");

        // Get database pool from context
        self.db_pool = Some(ctx.db_pool.clone());
        self.event_sender = Some(ctx.event_sender.clone());
        self.context = Some(ctx);
        self.config = config;

        info!(
            "Health aggregator configured - monitoring {} components, aggregation window: {}s",
            self.config.component_check_intervals.len(),
            self.config.aggregation_window_seconds
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
                // Perform one-time health aggregation
                self.aggregate_health_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Historical { .. } => {
                // Analyze historical health events
                self.aggregate_health_events(&from).await.unwrap_or(0)
            }
            TimeHorizon::Continuous => {
                // Continuous health monitoring
                self.aggregate_health_events(&from).await.unwrap_or(0)
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
                    "health_events_processed".to_string(),
                    serde_json::Value::Number(events_processed.into()),
                ),
                (
                    "monitored_components".to_string(),
                    serde_json::Value::Number(self.component_health.len().into()),
                ),
                (
                    "healthy_components".to_string(),
                    serde_json::Value::Number(
                        self.component_health
                            .values()
                            .filter(|h| matches!(h.status, HealthStatus::Healthy))
                            .count()
                            .into(),
                    ),
                ),
                (
                    "critical_components".to_string(),
                    serde_json::Value::Number(
                        self.component_health
                            .values()
                            .filter(|h| matches!(h.status, HealthStatus::Critical))
                            .count()
                            .into(),
                    ),
                ),
            ]),
            successful_targets: vec!["health".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "health-aggregator"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // Health aggregation operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }
}

impl Default for HealthAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for HealthAggregator {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let healthy_count = self
            .component_health
            .values()
            .filter(|h| matches!(h.status, HealthStatus::Healthy))
            .count();
        let total_components = self.component_health.len();

        Ok(SourceState {
            description: "Health aggregator for system component health monitoring".to_string(),
            last_updated: Utc::now(),
            total_items: Some(total_components as u64),
            metadata: HashMap::from([
                (
                    "monitored_components".to_string(),
                    total_components.to_string(),
                ),
                ("healthy_components".to_string(), healthy_count.to_string()),
                (
                    "aggregation_window_seconds".to_string(),
                    self.config.aggregation_window_seconds.to_string(),
                ),
                (
                    "unhealthy_threshold_minutes".to_string(),
                    self.config.unhealthy_threshold_minutes.to_string(),
                ),
            ]),
            healthy: total_components == 0 || healthy_count as f64 / total_components as f64 > 0.5,
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
            coverage_percentage: 100.0, // Health aggregation processes available health events
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            recommendations: vec![
                "Health aggregator monitors system component health status".to_string(),
                "Configure component_check_intervals for specific monitoring needs".to_string(),
                "Adjust unhealthy_threshold_minutes to change alert sensitivity".to_string(),
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
