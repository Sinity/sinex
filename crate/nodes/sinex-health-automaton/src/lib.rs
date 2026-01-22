#![doc = include_str!("../docs/README.md")]

//! Modernized `SimpleNode` implementation for the Health Aggregator.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use sinex_core::JsonValue;
use sinex_node_sdk::simple_node::{
    SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper,
};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for the health aggregator
#[derive(Debug, Clone, Deserialize)]
pub struct HealthAggregatorConfig {
    /// Component-specific check intervals (in seconds)
    #[serde(default = "default_component_intervals")]
    pub component_check_intervals: HashMap<String, u64>,

    /// Aggregation window for health statistics (in seconds)
    #[serde(default = "default_aggregation_window")]
    pub aggregation_window_seconds: Duration,

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

fn default_aggregation_window() -> Duration {
    Duration::from_secs(300) // 5 minutes
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
            aggregation_window_seconds: default_aggregation_window(),
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub component_name: String,
    pub last_seen: DateTime<Utc>,
    pub status: HealthStatus,
    pub metrics: HashMap<String, f64>,
    pub recent_events: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

#[derive(Default)]
pub struct HealthAggregator;

#[async_trait]
impl SimpleNode for HealthAggregator {
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
        context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        let component_name = input
            .get("component")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let health = state
            .component_health
            .entry(component_name.clone())
            .or_insert_with(|| ComponentHealth {
                component_name: component_name.clone(),
                last_seen: context.ts_orig.unwrap_or_else(Utc::now),
                status: HealthStatus::Unknown,
                metrics: HashMap::new(),
                recent_events: Vec::new(),
            });

        health.last_seen = context.ts_orig.unwrap_or_else(Utc::now);
        health.recent_events.push(context.event_id.to_string());
        if health.recent_events.len() > 10 {
            health.recent_events.remove(0);
        }

        // Logic to generate report every N events
        if state.component_health.len() % 5 == 0 {
            Ok(Some(serde_json::to_value(&state.component_health).unwrap()))
        } else {
            Ok(None)
        }
    }
}

pub type HealthAggregatorNode = SimpleNodeWrapper<HealthAggregator>;
