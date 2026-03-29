#![doc = include_str!("../docs/README.md")]

//! Health aggregator — [`ScopeReconcilerNode`] implementation.
//!
//! Model classification: **ScopeReconciler** — groups health events by component
//! (the scope key), maintains per-component state, and emits reports when
//! conditions are met (status transitions, periodic intervals). During replay,
//! invalidating a component scope recomputes all health reports for that component.

use serde::{Deserialize, Serialize};
use sinex_node_sdk::derived_node::{
    DerivedOutput, DerivedTriggerContext, ScopeReconcilerNodeAdapter,
};
use sinex_node_sdk::{NodeLogicError, ScopeReconcilerNode};
use sinex_primitives::JsonValue;
use sinex_primitives::Uuid;
use sinex_primitives::domain::SyntheticTemporalPolicy;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::{Duration, Timestamp};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::warn;

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
    intervals.insert("default".to_string(), 60);
    intervals
}

fn default_aggregation_window_secs() -> u64 {
    300
}

fn default_unhealthy_threshold() -> u64 {
    5
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
    /// Load configuration from environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_AGGREGATION_WINDOW_SECONDS",
            &mut config.aggregation_window_seconds,
        );
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_UNHEALTHY_THRESHOLD_MINUTES",
            &mut config.unhealthy_threshold_minutes,
        );
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_ENABLE_SYSTEM_HEALTH_STATUS",
            &mut config.enable_system_health_status,
        );
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_ENABLE_COMPONENT_HEALTH_REPORTS",
            &mut config.enable_component_health_reports,
        );

        match std::env::var("SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS") {
            Ok(value) => match serde_json::from_str::<HashMap<String, u64>>(&value) {
                Ok(intervals) => match validate_component_intervals(intervals) {
                    Ok(validated) => config.component_check_intervals = validated,
                    Err(error) => {
                        warn!(
                            env = "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
                            value = %value,
                            %error,
                            "Invalid component check interval override; using default"
                        );
                    }
                },
                Err(error) => {
                    warn!(
                        env = "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
                        value = %value,
                        %error,
                        "Invalid component check interval override; using default"
                    );
                }
            },
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                warn!(
                    env = "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
                    "Component check interval override is not valid UTF-8; using default"
                );
            }
        }

        config
    }
}

fn apply_env_override<T>(key: &str, target: &mut T)
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(value) => match value.parse::<T>() {
            Ok(parsed) => *target = parsed,
            Err(error) => {
                warn!(
                    env = key,
                    value = %value,
                    %error,
                    "Invalid health automaton env override; using default"
                );
            }
        },
        Err(std::env::VarError::NotPresent) => {}
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                env = key,
                "Health automaton env override is not valid UTF-8; using default"
            );
        }
    }
}

fn validate_component_intervals(
    intervals: HashMap<String, u64>,
) -> Result<HashMap<String, u64>, String> {
    for (component, interval) in &intervals {
        if component.trim().is_empty() {
            return Err("component interval override contains an empty component name".to_string());
        }
        if *interval == 0 {
            return Err(format!(
                "component interval override for '{component}' must be positive"
            ));
        }
    }
    Ok(intervals)
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
    fn parse_field(input: &JsonValue, field: &str) -> Result<Self, NodeLogicError> {
        let value = input.get(field).ok_or_else(|| {
            NodeLogicError::InputParsing(format!(
                "health status payload is missing required field '{field}'"
            ))
        })?;
        let status = value.as_str().ok_or_else(|| {
            NodeLogicError::InputParsing(format!(
                "health status field '{field}' must be a string"
            ))
        })?;
        Self::from_str(status).map_err(|()| {
            NodeLogicError::InputParsing(format!(
                "health status field '{field}' has invalid value '{status}'"
            ))
        })
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

impl ScopeReconcilerNode for HealthAggregator {
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

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Metadata
    }

    fn scope_keys(&self, input: &Self::Input, context: &DerivedTriggerContext) -> Vec<String> {
        // Keep malformed payloads isolated on the live path so they do not all collide into a
        // shared "unknown" scope before reconcile rejects them into DLQ.
        let component = input
            .get("component")
            .and_then(|v| v.as_str())
            .filter(|component| !component.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("__invalid_component__:{}", context.trigger_uuid()));
        vec![component]
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, NodeLogicError> {
        let now = context.require_ts_orig()?;
        let component = parse_component_name(&input)?.to_string();
        if component != scope_key {
            return Err(NodeLogicError::InputParsing(format!(
                "health status scope key '{scope_key}' does not match payload component '{component}'"
            )));
        }

        // Ensure state has config
        if state.config.aggregation_window_seconds == 0 {
            state.config = self.config.clone();
        }

        let previous_status = ComponentHealthStatus::parse_field(&input, "previous_status")?;
        let current_status = ComponentHealthStatus::parse_field(&input, "current_status")?;

        // Get or create component health tracking
        let mut immediate_alert = None;
        let mut component_report = None;
        {
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
                event_id: context.trigger_uuid().to_string(),
            });

            // Prune events outside aggregation window
            let window_start =
                now - Duration::seconds(state.config.aggregation_window_seconds as i64);
            component_health.events.retain(|e| e.timestamp >= window_start);

            let window_event_ids =
                Self::collect_event_ids(&component_health.events, &component, "component report")?;

            // Immediate alert for component failure
            if status_changed && matches!(current_status, ComponentHealthStatus::Failed) {
                immediate_alert = Some(
                    DerivedOutput::reconciled(
                        self.create_alert(
                            &component,
                            current_status,
                            now,
                            "Component entered failed state",
                        ),
                        now,
                        window_event_ids.clone(),
                        component.clone(),
                    )
                    .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
                    .with_equivalence_key(format!("alert:{component}:{}", now.format_rfc3339())),
                );
            }

            // Component-specific check interval
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
                component_report = Some(
                    DerivedOutput::reconciled(
                        self.create_component_report(component_health, now),
                        now,
                        window_event_ids,
                        component.clone(),
                    )
                    .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective),
                );
                component_health.last_check_emission = Some(now);
            }
        }

        // Check for periodic system-wide report after the current event has updated state.
        let mut periodic_reports = Vec::new();

        let should_emit_window = state.last_window_emission.is_none_or(|last| {
            let elapsed = now - last;
            elapsed >= Duration::seconds(state.config.aggregation_window_seconds as i64)
        });

        if should_emit_window && state.config.enable_system_health_status {
            let mut all_event_ids = Vec::new();
            for component_health in state.component_health.values() {
                all_event_ids.extend(Self::collect_event_ids(
                    &component_health.events,
                    &component_health.component_name,
                    "system status",
                )?);
            }
            periodic_reports.push(
                DerivedOutput::reconciled(
                    self.create_system_status(state, now),
                    now,
                    all_event_ids,
                    component.clone(),
                )
                .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective),
            );
            state.last_window_emission = Some(now);
        }

        if let Some(report) = component_report {
            periodic_reports.push(report);
        }

        let mut outputs = Vec::with_capacity(periodic_reports.len() + usize::from(immediate_alert.is_some()));
        if let Some(alert) = immediate_alert {
            outputs.push(alert);
        }
        outputs.extend(periodic_reports);
        Ok(outputs)
    }
}

impl HealthAggregator {
    fn collect_event_ids(
        events: &[HealthEvent],
        component: &str,
        report_kind: &str,
    ) -> Result<Vec<Uuid>, NodeLogicError> {
        events
            .iter()
            .map(|event| {
                Uuid::parse_str(&event.event_id).map_err(|error| {
                    NodeLogicError::Processing(format!(
                        "health aggregator {report_kind} for component '{component}' contains invalid event_id '{}': {error}",
                        event.event_id
                    ))
                })
            })
            .collect()
    }

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
        let unknown = total_components.saturating_sub(healthy + degraded + failed);

        let overall_status = if failed > 0 {
            ComponentHealthStatus::Failed
        } else if degraded > 0 {
            ComponentHealthStatus::Degraded
        } else if healthy == total_components && total_components > 0 {
            ComponentHealthStatus::Healthy
        } else {
            ComponentHealthStatus::Unknown
        };

        serde_json::json!({
            "report_type": "system_health_status",
            "timestamp": timestamp.format_rfc3339(),
            "overall_status": overall_status,
            "total_components": total_components,
            "healthy_count": healthy,
            "degraded_count": degraded,
            "failed_count": failed,
            "unknown_count": unknown,
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

/// Node type alias for use with `node_entrypoint!`.
pub type HealthAggregatorNode = ScopeReconcilerNodeAdapter<HealthAggregator>;

fn parse_component_name(input: &JsonValue) -> Result<&str, NodeLogicError> {
    let component = input.get("component").ok_or_else(|| {
        NodeLogicError::InputParsing(
            "health status payload is missing required field 'component'".to_string(),
        )
    })?;
    let component = component.as_str().ok_or_else(|| {
        NodeLogicError::InputParsing("health status field 'component' must be a string".to_string())
    })?;
    if component.trim().is_empty() {
        return Err(NodeLogicError::InputParsing(
            "health status field 'component' must not be empty".to_string(),
        ));
    }
    Ok(component)
}
