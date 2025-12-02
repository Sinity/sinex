#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/automaton.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../../../lib/sinex-satellite-sdk/docs/overview.md")]

//! Health aggregator automaton.
//!
//! Health Events → Aggregation → Synthesized Health Status Events.

// Local facade module to reduce import verbosity
mod common {
    // Core types facade
    pub use sinex_core::{
        db::models::{EventId, Provenance},
        types::{Id, Ulid},
        Event, JsonValue,
    };

    // Runtime/SDK facades
    pub use sinex_processor_runtime::cli::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    };
    pub use sinex_satellite_sdk::{
        stream_processor::{
            Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorRuntimeState,
            ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor, TimeHorizon,
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
        tracing::{error, info, warn},
    };
}

// Use local facade for common types
use crate::common::*;
use sinex_satellite_sdk::{
    confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    ProcessingModel, SatelliteError,
};
use std::sync::Arc;

// Default batch size for health event processing
const DEFAULT_HEALTH_BATCH_SIZE: usize = 128;
use tokio::sync::mpsc;

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
    pub recent_events: Vec<EventId>,
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
    runtime: Option<ProcessorRuntimeState>,
    config: HealthAggregatorConfig,
    event_sender: Option<tokio::sync::mpsc::UnboundedSender<Event<JsonValue>>>,
    db_pool: Option<PgPool>,
    component_health: HashMap<String, ComponentHealth>,
    incoming_tx: Option<mpsc::UnboundedSender<ProvisionalEvent>>,
    incoming_rx: Option<mpsc::UnboundedReceiver<ProvisionalEvent>>,
    consumer: Option<Arc<JetStreamEventConsumer>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
}

impl HealthAggregator {
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: HealthAggregatorConfig::default(),
            event_sender: None,
            db_pool: None,
            component_health: HashMap::new(),
            incoming_tx: None,
            incoming_rx: None,
            consumer: None,
            consumer_handle: None,
        }
    }

    fn runtime(&self) -> SatelliteResult<&ProcessorRuntimeState> {
        self.runtime.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("Processor runtime not initialized".to_string())
        })
    }

    /// Initialize the confirmed event channel if needed.
    fn ensure_event_channel(&mut self) {
        if self.incoming_tx.is_none() || self.incoming_rx.is_none() {
            let (tx, rx) = mpsc::unbounded_channel();
            self.incoming_tx = Some(tx);
            self.incoming_rx = Some(rx);
        }
    }

    /// Lazily start a JetStream consumer that forwards confirmed events into the local channel.
    async fn ensure_consumer(&mut self) -> SatelliteResult<()> {
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

        // Only NATS transport is supported in JetStream mode
        let nats_publisher = match transport {
            sinex_satellite_sdk::event_processor::EventTransport::Nats(publisher) => publisher,
        };

        self.ensure_event_channel();
        let sender = self.incoming_tx.clone().ok_or_else(|| {
            SatelliteError::Processing("Confirmed event channel unavailable".to_string())
        })?;

        let handler = Arc::new(ChannelConfirmedEventHandler::new(sender));
        let env = sinex_core::environment().clone();
        let config = JetStreamEventConsumerConfig {
            processing_model: ProcessingModel::LeaderStandby,
            batch_size: DEFAULT_HEALTH_BATCH_SIZE,
            confirmation_timeout: Duration::from_secs(60),
            consumer_name: format!("{}-health-aggregator", service_name.replace('.', "_")),
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
                error!("Health aggregator JetStream consumer exited: {err}");
            }
        });

        self.consumer = Some(consumer);
        self.consumer_handle = Some(handle);

        Ok(())
    }

    /// Emit component/system health reports based on current state.
    async fn emit_reports(
        &self,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event<JsonValue>>,
    ) -> SatelliteResult<u64> {
        let mut events_processed = 0u64;

        if self.config.enable_component_health_reports {
            for (component_name, health) in &self.component_health {
                if let Ok(event) = self
                    .generate_component_health_report(component_name, health)
                    .await
                {
                    if let Err(err) = event_sender.send(event) {
                        warn!(
                            "Failed to send component health report for {}: {}",
                            component_name, err
                        );
                    } else {
                        events_processed += 1;
                    }
                }
            }
        }

        if self.config.enable_system_health_status {
            if let Ok(event) = self.generate_system_health_status().await {
                if let Err(err) = event_sender.send(event) {
                    warn!("Failed to send system health status: {}", err);
                } else {
                    events_processed += 1;
                }
            }
        }

        let alerts = self.generate_health_alerts().await?;
        for alert in alerts {
            if let Err(err) = event_sender.send(alert) {
                warn!("Failed to send health alert: {}", err);
            } else {
                events_processed += 1;
            }
        }

        Ok(events_processed)
    }

    /// Process a confirmed event flowing from JetStream.
    async fn process_confirmed_event(
        &mut self,
        provisional: ProvisionalEvent,
    ) -> SatelliteResult<u64> {
        use sinex_core::db::repositories::DbPoolExt;

        let db_pool = self.db_pool.as_ref().ok_or_else(|| {
            SatelliteError::Processing("Database pool not initialized".to_string())
        })?;
        let event_sender = self
            .event_sender
            .as_ref()
            .ok_or_else(|| SatelliteError::Processing("Event sender not initialized".to_string()))?
            .clone();

        let event_id = EventId::from_ulid(provisional.event_id);

        let persisted_event = match db_pool.events().get_by_id(event_id.clone()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                warn!(
                    event_id = %event_id,
                    "Confirmed event not yet visible in database; skipping health update"
                );
                return Ok(0);
            }
            Err(err) => {
                return Err(SatelliteError::Processing(format!(
                    "Failed to load confirmed event {}: {}",
                    event_id, err
                )))
            }
        };

        let events = vec![persisted_event];
        self.update_component_health(&events).await;
        self.emit_reports(&event_sender).await
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
            .ok_or_else(|| color_eyre::eyre::eyre!("Event sender not initialized"))?
            .clone();

        // Query recent health events
        let health_events = self.query_health_events(db_pool, from).await?;
        info!(
            "Processing {} health events for aggregation",
            health_events.len()
        );

        // Update component health status from events
        self.update_component_health(&health_events).await;

        self.emit_reports(&event_sender).await
    }

    /// Query health-related events from the database
    async fn query_health_events(
        &self,
        db_pool: &PgPool,
        _from: &Checkpoint,
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
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

        use sinex_core::db::repositories::DbPoolExt;
        use sinex_core::types::domain::EventType;

        // Query health events for each type
        let mut all_events = Vec::new();
        for event_type_str in &health_event_types {
            let event_type = EventType::from(event_type_str.as_str());
            let events = db_pool
                .events()
                .get_events_by_type_and_time_range(
                    &event_type,
                    window_start,
                    chrono::Utc::now(),
                    sinex_core::types::Pagination::new(Some(100), None),
                )
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Failed to query health events: {}", e))?;
            all_events.extend(events);
        }

        // Sort by timestamp and limit to 1000 most recent
        all_events.sort_by(|a, b| b.ts_orig.cmp(&a.ts_orig));
        all_events.truncate(1000);

        let events = all_events;

        Ok(events)
    }

    /// Update component health status based on events
    async fn update_component_health(&mut self, events: &[Event<JsonValue>]) {
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
                if let Some(event_id) = &event.id {
                    component_health.recent_events.push(event_id.clone());
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
    fn extract_component_health_info(
        &self,
        event: &Event<JsonValue>,
    ) -> Option<ComponentHealthInfo> {
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
        event: &Event<JsonValue>,
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
    ) -> SatelliteResult<Event<JsonValue>> {
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
        let provenance =
            Provenance::from_synthesis(health.recent_events.clone()).unwrap_or_else(|| {
                // Fallback to system bootstrap if no recent events
                let system_bootstrap_id = EventId::from_ulid(
                    Ulid::from_bytes([
                        0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ])
                    .unwrap(),
                );
                Provenance::from_synthesis_safe(system_bootstrap_id, vec![])
            });

        let event = Event::create(
            "health-aggregator",
            "health.component_report",
            report_payload,
            provenance,
        );

        Ok(event)
    }

    /// Generate system-wide health status
    async fn generate_system_health_status(&self) -> SatelliteResult<Event<JsonValue>> {
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

        let all_event_ids: Vec<Id<Event<JsonValue>>> = self
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

        let provenance = Provenance::from_synthesis(all_event_ids).unwrap_or_else(|| {
            let system_bootstrap_id = EventId::from_ulid(
                Ulid::from_bytes([
                    0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00,
                ])
                .unwrap(),
            );
            Provenance::from_synthesis_safe(system_bootstrap_id, vec![])
        });

        let event = Event::create(
            "health-aggregator",
            "health.system_status",
            system_health_payload,
            provenance,
        );

        Ok(event)
    }

    /// Generate health alerts for unhealthy components
    async fn generate_health_alerts(&self) -> SatelliteResult<Vec<Event<JsonValue>>> {
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

                let provenance = Provenance::from_synthesis(health.recent_events.clone())
                    .unwrap_or_else(|| {
                        let system_bootstrap_id = EventId::from_ulid(
                            Ulid::from_bytes([
                                0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x00,
                            ])
                            .unwrap(),
                        );
                        Provenance::from_synthesis_safe(system_bootstrap_id, vec![])
                    });

                let alert_event = Event::create(
                    "health-aggregator",
                    "health.alert",
                    alert_payload,
                    provenance,
                );

                alerts.push(alert_event);
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
            SatelliteError::Processing(format!("Failed to forward confirmed event: {}", err))
        })?;
        Ok(())
    }
}

#[async_trait]
impl StatefulStreamProcessor for HealthAggregator {
    type Config = HealthAggregatorConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        info!("Initializing health aggregator");
        let (config, runtime) = init.into_runtime();
        self.db_pool = Some(runtime.db_pool().clone());
        self.event_sender = Some(runtime.event_sender());
        self.runtime = Some(runtime);
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
                self.ensure_consumer().await?;

                let mut receiver = self.incoming_rx.take().ok_or_else(|| {
                    SatelliteError::Processing(
                        "Confirmed events channel not initialized".to_string(),
                    )
                })?;

                let mut processed = self.aggregate_health_events(&from).await.unwrap_or(0);

                while let Some(provisional) = receiver.recv().await {
                    processed += self.process_confirmed_event(provisional).await?;
                }

                info!("Confirmed event channel closed; exiting continuous aggregation loop");
                self.incoming_tx = None;
                self.incoming_rx = None;
                self.consumer_handle = None;
                self.consumer = None;
                processed
            }
        };

        let duration = Utc::now().signed_duration_since(start_time);

        Ok(ScanReport {
            events_processed,
            duration: Duration::from_millis(duration.num_milliseconds() as u64),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                ("health_events_processed".to_string(), events_processed),
                (
                    "monitored_components".to_string(),
                    self.component_health.len() as u64,
                ),
                (
                    "healthy_components".to_string(),
                    self.component_health
                        .values()
                        .filter(|h| matches!(h.status, HealthStatus::Healthy))
                        .count() as u64,
                ),
                (
                    "critical_components".to_string(),
                    self.component_health
                        .values()
                        .filter(|h| matches!(h.status, HealthStatus::Critical))
                        .count() as u64,
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

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_historical: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // Health aggregation operates on recent data, no persistent checkpoint needed
        Ok(Checkpoint::None)
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Shutting down health aggregator");

        if let Some(consumer) = self.consumer.take() {
            consumer.stop().await;
        }

        if let Some(handle) = self.consumer_handle.take() {
            if let Err(err) = handle.await {
                warn!("Failed to join consumer task: {err}");
            }
        }

        self.incoming_tx = None;
        self.incoming_rx = None;

        Ok(())
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
                    serde_json::Value::Number(serde_json::Number::from(total_components)),
                ),
                (
                    "healthy_components".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(healthy_count)),
                ),
                (
                    "aggregation_window_seconds".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(
                        self.config.aggregation_window_seconds,
                    )),
                ),
                (
                    "unhealthy_threshold_minutes".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(
                        self.config.unhealthy_threshold_minutes,
                    )),
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
