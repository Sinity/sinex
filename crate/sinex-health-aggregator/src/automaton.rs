use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sinex_db::models::{Event, SystemHealthSummaryPayload, HealthStatus as PayloadHealthStatus, ComponentHealth};
use sinex_satellite_sdk::{
    automaton::{
        EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent,
        ProcessingResult,
    },
    SatelliteResult,
};
use std::collections::HashMap;
use tracing::{debug, info, warn};

// Use HealthStatus from the payload module
type HealthStatus = PayloadHealthStatus;

// Use ComponentHealth from the payload module
// (already imported above)

// Use SystemHealthSummaryPayload from the payload module
type SystemHealthSummary = SystemHealthSummaryPayload;

/// Health aggregator automaton that processes satellite heartbeat events
/// and generates system health summary synthesis events
pub struct HealthAggregatorAutomaton {
    context: Option<HotlogAutomatonContext>,
    // Track expected components (could be loaded from config)
    expected_components: Vec<String>,
    // Health status aggregation window
    aggregation_window: Duration,
}

impl HealthAggregatorAutomaton {
    pub fn new() -> Self {
        Self {
            context: None,
            expected_components: vec![
                services::INGESTD.to_string(),
                services::GATEWAY.to_string(),
                services::FS_WATCHER.to_string(),
                services::TERMINAL_SATELLITE.to_string(),
                services::HEALTH_AGGREGATOR.to_string(),
            ],
            aggregation_window: Duration::minutes(5),
        }
    }

    /// Process a satellite heartbeat event and extract health information
    fn process_heartbeat_event(
        &self,
        event: &HotlogAutomatonEvent,
    ) -> SatelliteResult<Option<ComponentHealth>> {
        // Extract service information from journald satellite heartbeat events
        let payload = &event.event.payload;

        // Handle satellite.heartbeat events from journald
        if event.event.source == "journald" && event.event.event_type == "satellite.heartbeat" {
            let service_name = payload
                .get("service_name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let uptime_seconds = payload.get("uptime_seconds").and_then(|v| v.as_i64());

            let memory_usage_mb = payload
                .get("memory_usage_mb")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            let events_processed = payload.get("events_processed").and_then(|v| v.as_i64());

            let version = payload
                .get("version")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let git_hash = payload
                .get("git_hash")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let component_health = ComponentHealth {
                service_name: service_name.to_string(),
                status: HealthStatus::Healthy, // Heartbeat means it's alive
                last_heartbeat: event.event.ts_orig.unwrap_or_else(Utc::now),
                uptime_seconds,
                memory_usage_mb,
                events_processed,
                version,
                git_hash,
            };

            debug!(
                service = %service_name,
                uptime = ?uptime_seconds,
                "Processed heartbeat for component"
            );

            return Ok(Some(component_health));
        }

        Ok(None)
    }

    /// Generate a system health summary synthesis event
    async fn generate_health_summary(
        &self,
        components: HashMap<String, ComponentHealth>,
    ) -> SatelliteResult<RawEvent> {
        let now = Utc::now();
        let _cutoff = now - self.aggregation_window;

        // Analyze component health
        let mut healthy_count = 0;
        let mut degraded_count = 0;
        let mut failed_count = 0;
        let mut missing_count = 0;

        // Update component statuses based on heartbeat recency
        let mut updated_components = components.clone();
        for (_name, component) in updated_components.iter_mut() {
            let time_since_heartbeat = now - component.last_heartbeat;

            if time_since_heartbeat > Duration::minutes(10) {
                component.status = HealthStatus::Failed;
            } else if time_since_heartbeat > Duration::minutes(5) {
                component.status = HealthStatus::Degraded;
            } else {
                component.status = HealthStatus::Healthy;
            }

            match component.status {
                HealthStatus::Healthy => healthy_count += 1,
                HealthStatus::Degraded => degraded_count += 1,
                HealthStatus::Failed => failed_count += 1,
                HealthStatus::Missing => missing_count += 1,
            }
        }

        // Check for missing expected components
        for expected in &self.expected_components {
            if !updated_components.contains_key(expected) {
                missing_count += 1;
                updated_components.insert(
                    expected.clone(),
                    ComponentHealth {
                        service_name: expected.clone(),
                        status: HealthStatus::Missing,
                        last_heartbeat: DateTime::<Utc>::MIN_UTC,
                        uptime_seconds: None,
                        memory_usage_mb: None,
                        events_processed: None,
                        version: None,
                        git_hash: None,
                    },
                );
            }
        }

        let total_components = updated_components.len() as u32;

        // Determine overall system status
        let overall_status = if failed_count > 0 || missing_count > 0 {
            HealthStatus::Failed
        } else if degraded_count > 0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        // Create synthesis event with typed payload
        let mut synthesis_event = Event::from(SystemHealthSummaryPayload {
            overall_status,
            healthy_components: healthy_count,
            degraded_components: degraded_count,
            failed_components: failed_count,
            missing_components: missing_count,
            total_components,
            last_updated: now,
            components: updated_components,
        });
        synthesis_event = synthesis_event.with_ts_orig(Some(now));

        info!(
            overall_status = ?overall_status,
            healthy = healthy_count,
            degraded = degraded_count,
            failed = failed_count,
            missing = missing_count,
            "Generated system health summary"
        );

        Ok(synthesis_event)
    }
}

#[async_trait]
impl HotlogAutomaton for HealthAggregatorAutomaton {
    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing Health Aggregator Automaton");
        self.context = Some(ctx);
        Ok(())
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult> {
        let _ctx = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Processing("Health aggregator context not initialized".to_string())
        })?;

        // Process heartbeat events and maintain component health state
        if let Some(component_health) = self.process_heartbeat_event(&event)? {
            debug!(
                component = %component_health.service_name,
                status = ?component_health.status,
                "Updated component health"
            );

            // Store component health in checkpoint data for persistence
            let checkpoint_data = json!({
                "last_processed_heartbeat": {
                    "service_name": component_health.service_name,
                    "timestamp": component_health.last_heartbeat,
                    "status": component_health.status
                }
            });

            return Ok(ProcessingResult::Success {
                checkpoint_data: Some(checkpoint_data),
            });
        }

        // For other event types, just skip
        Ok(ProcessingResult::Skip {
            reason: "Not a satellite heartbeat event".to_string(),
        })
    }

    async fn process_batch(
        &mut self,
        events: Vec<HotlogAutomatonEvent>,
    ) -> SatelliteResult<Vec<ProcessingResult>> {
        let ctx = self.context.as_ref().ok_or_else(|| {
            SatelliteError::Processing("Health aggregator context not initialized".to_string())
        })?;
        let mut results = Vec::new();
        let mut components_in_batch = HashMap::new();

        // Process all heartbeat events in the batch
        for event in &events {
            if let Some(component_health) = self.process_heartbeat_event(event)? {
                components_in_batch.insert(component_health.service_name.clone(), component_health);
            }
        }

        // If we found heartbeat events, generate a health summary
        if !components_in_batch.is_empty() {
            let health_summary = self.generate_health_summary(components_in_batch).await?;

            // Emit the health summary synthesis event
            match ctx.emit_synthesis_event(health_summary).await {
                Ok(_) => {
                    info!("Successfully emitted system health summary");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to emit health summary event");
                }
            }
        }

        // Generate processing results for each input event
        for event in events {
            let result = self.process_event(event).await?;
            results.push(result);
        }

        Ok(results)
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Listen for satellite heartbeat events from journald
            EventFilter::new(
                Some(sinex_events::sources::JOURNALD.to_string()),
                Some("satellite.heartbeat".to_string()),
            ),
            // Also listen for any sinex system events that might be relevant
            EventFilter::new(Some(sinex_events::sources::SINEX.to_string()), None),
        ]
    }

    fn automaton_name(&self) -> &str {
        services::HEALTH_AGGREGATOR
    }
}

impl Default for HealthAggregatorAutomaton {
    fn default() -> Self {
        Self::new()
    }
}
