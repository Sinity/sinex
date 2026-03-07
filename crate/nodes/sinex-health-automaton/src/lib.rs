#![doc = include_str!("../docs/README.md")]

//! Modernized `AutomatonNode` implementation for the Health Aggregator.

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::{Deserialize, Serialize};
use sinex_node_sdk::NodeEventContext;
use sinex_node_sdk::{AutomatonNode, NodeLogicError};
use sinex_primitives::JsonValue;
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::HashMap;
use std::str::FromStr;

/// Configuration for the health aggregator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthAggregatorConfig {
    /// Component-specific check intervals (in seconds)
    #[serde(default = "default_component_intervals")]
    pub component_check_intervals: HashMap<String, u64>,

    /// Aggregation window for health statistics (in seconds)
    #[serde(default = "default_aggregation_window_secs")]
    pub aggregation_window_seconds: u64,

    /// Threshold for marking component as unhealthy (in minutes)
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold_minutes: u64,

    /// Enable system-wide health status emission
    #[serde(default = "default_true")]
    pub enable_system_health_status: bool,

    /// Enable per-component health reports
    #[serde(default = "default_true")]
    pub enable_component_health_reports: bool,
}

fn default_component_intervals() -> HashMap<String, u64> {
    let mut intervals = HashMap::new();
    intervals.insert("default".to_string(), 60); // 1 minute default
    intervals
}

fn default_aggregation_window_secs() -> u64 {
    300 // 5 minutes
}

fn default_unhealthy_threshold() -> u64 {
    5 // 5 minutes
}

fn default_true() -> bool {
    true
}

impl Default for HealthAggregatorConfig {
    fn default() -> Self {
        Self {
            component_check_intervals: default_component_intervals(),
            aggregation_window_seconds: default_aggregation_window_secs(),
            unhealthy_threshold_minutes: default_unhealthy_threshold(),
            enable_system_health_status: default_true(),
            enable_component_health_reports: default_true(),
        }
    }
}

impl HealthAggregatorConfig {
    /// Load configuration from environment variables and TOML files
    pub fn from_env() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("health_aggregator.toml").nested())
            .merge(Env::prefixed("SINEX_HEALTH_AGGREGATOR_"))
            .extract()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HealthState {
    pub component_health: HashMap<String, ComponentHealth>,
    pub last_window_emission: Option<Timestamp>,
    pub config: HealthAggregatorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub component_name: String,
    pub current_status: ComponentHealthStatus,
    pub status_since: Timestamp,
    pub last_seen: Timestamp,
    pub last_check_emission: Option<Timestamp>,
    pub transition_count: u64,
    pub events: Vec<HealthEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthEvent {
    pub timestamp: Timestamp,
    pub previous_status: ComponentHealthStatus,
    pub current_status: ComponentHealthStatus,
    pub event_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentHealthStatus {
    Unknown,
    Healthy,
    Degraded,
    Failed,
}

impl ComponentHealthStatus {
    fn from_input(status: Option<&str>) -> Self {
        status
            .and_then(|s| Self::from_str(s).ok())
            .unwrap_or(Self::Unknown)
    }
}

impl FromStr for ComponentHealthStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "unknown" => Ok(Self::Unknown),
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "failed" => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

#[derive(Default)]
pub struct HealthAggregator {
    pub config: HealthAggregatorConfig,
}

impl AutomatonNode for HealthAggregator {
    type State = HealthState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "health-aggregator"
    }
    fn input_event_type(&self) -> &'static str {
        "health.status"
    }
    fn output_event_type(&self) -> &'static str {
        "health.aggregated_report"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &NodeEventContext,
    ) -> Result<Option<Self::Output>, NodeLogicError> {
        let now = context.ts_orig.unwrap_or_else(Timestamp::now);

        // Ensure state has config
        if state.config.aggregation_window_seconds == 0 {
            state.config = self.config.clone();
        }

        // Parse health.status event
        let component = input
            .get("component")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let previous_status = ComponentHealthStatus::from_input(
            input.get("previous_status").and_then(|v| v.as_str()),
        );

        let current_status =
            ComponentHealthStatus::from_input(input.get("current_status").and_then(|v| v.as_str()));

        // Check for periodic reports (before mutably borrowing component_health)
        let mut periodic_reports = Vec::new();

        // 1. Window-based emission (every aggregation_window_seconds)
        let should_emit_window = state.last_window_emission.is_none_or(|last| {
            let elapsed = now - last;
            elapsed >= Duration::seconds(state.config.aggregation_window_seconds as i64)
        });

        if should_emit_window && state.config.enable_system_health_status {
            periodic_reports.push(self.create_system_status(state, now));
            state.last_window_emission = Some(now);
        }

        // Get or create component health tracking
        let component_health = state
            .component_health
            .entry(component.clone())
            .or_insert_with(|| ComponentHealth {
                component_name: component.clone(),
                current_status,
                status_since: now,
                last_seen: now,
                last_check_emission: None,
                transition_count: 0,
                events: Vec::new(),
            });

        // Track status transition
        let status_changed = component_health.current_status != current_status;
        if status_changed {
            component_health.current_status = current_status;
            component_health.status_since = now;
            component_health.transition_count += 1;
        }

        component_health.last_seen = now;

        // Add event to sliding window
        component_health.events.push(HealthEvent {
            timestamp: now,
            previous_status,
            current_status,
            event_id: context.event_id.to_string(),
        });

        // Prune events outside aggregation window
        let window_start = now - Duration::seconds(state.config.aggregation_window_seconds as i64);

        component_health
            .events
            .retain(|e| e.timestamp >= window_start);

        // Check for immediate alert: component transitioned to Failed
        let mut immediate_alert = None;
        if status_changed && matches!(current_status, ComponentHealthStatus::Failed) {
            immediate_alert = Some(self.create_alert(
                &component,
                current_status,
                now,
                "Component entered failed state",
            ));
        }

        // 2. Component-specific check interval
        let check_interval = state
            .config
            .component_check_intervals
            .get(&component)
            .or_else(|| state.config.component_check_intervals.get("default"))
            .copied()
            .unwrap_or(60);

        let should_emit_component = component_health.last_check_emission.is_none_or(|last| {
            let elapsed = now - last;
            elapsed >= Duration::seconds(check_interval as i64)
        });

        if should_emit_component && state.config.enable_component_health_reports {
            periodic_reports.push(self.create_component_report(component_health, now));
            component_health.last_check_emission = Some(now);
        }

        // Combine all outputs
        if let Some(alert) = immediate_alert {
            Ok(Some(alert))
        } else if !periodic_reports.is_empty() {
            Ok(Some(periodic_reports[0].clone()))
        } else {
            Ok(None)
        }
    }
}

impl HealthAggregator {
    /// Create immediate alert event for component status change
    fn create_alert(
        &self,
        component: &str,
        status: ComponentHealthStatus,
        timestamp: Timestamp,
        reason: &str,
    ) -> JsonValue {
        serde_json::json!({
            "alert_type": "component_status_change",
            "component": component,
            "status": status,
            "timestamp": timestamp.format_rfc3339(),
            "reason": reason,
            "severity": if matches!(status, ComponentHealthStatus::Failed) { "critical" } else { "warning" },
        })
    }

    /// Create system-wide health status report
    fn create_system_status(&self, state: &HealthState, timestamp: Timestamp) -> JsonValue {
        let total_components = state.component_health.len();
        let healthy = state
            .component_health
            .values()
            .filter(|c| matches!(c.current_status, ComponentHealthStatus::Healthy))
            .count();
        let degraded = state
            .component_health
            .values()
            .filter(|c| matches!(c.current_status, ComponentHealthStatus::Degraded))
            .count();
        let failed = state
            .component_health
            .values()
            .filter(|c| matches!(c.current_status, ComponentHealthStatus::Failed))
            .count();

        let overall_status = if failed > 0 {
            ComponentHealthStatus::Failed
        } else if degraded > 0 {
            ComponentHealthStatus::Degraded
        } else {
            ComponentHealthStatus::Healthy
        };

        serde_json::json!({
            "report_type": "system_health_status",
            "timestamp": timestamp.format_rfc3339(),
            "overall_status": overall_status,
            "total_components": total_components,
            "healthy_count": healthy,
            "degraded_count": degraded,
            "failed_count": failed,
            "components": state
                .component_health
                .iter()
                .map(|(name, health)| {
                    serde_json::json!({
                        "name": name,
                        "status": health.current_status,
                        "status_since": health.status_since.format_rfc3339(),
                        "last_seen": health.last_seen.format_rfc3339(),
                    })
                })
                .collect::<Vec<_>>(),
        })
    }

    /// Create component-specific health report
    fn create_component_report(
        &self,
        component_health: &ComponentHealth,
        timestamp: Timestamp,
    ) -> JsonValue {
        let window_start =
            timestamp - Duration::seconds(self.config.aggregation_window_seconds as i64);

        let events_in_window = component_health
            .events
            .iter()
            .filter(|e| e.timestamp >= window_start)
            .count();

        let transitions_in_window = component_health
            .events
            .iter()
            .filter(|e| e.timestamp >= window_start && e.previous_status != e.current_status)
            .count();

        serde_json::json!({
            "report_type": "component_health_report",
            "timestamp": timestamp.format_rfc3339(),
            "component": component_health.component_name,
            "current_status": component_health.current_status,
            "status_since": component_health.status_since.format_rfc3339(),
            "last_seen": component_health.last_seen.format_rfc3339(),
            "total_transitions": component_health.transition_count,
            "events_in_window": events_in_window,
            "transitions_in_window": transitions_in_window,
            "window_seconds": self.config.aggregation_window_seconds,
        })
    }
}
