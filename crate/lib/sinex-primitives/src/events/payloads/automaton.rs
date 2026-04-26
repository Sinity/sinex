//! Automaton event payloads.
//!
//! Keep this module tied to event types emitted by real derived-node crates.
//! Future automata should add payload contracts when their emitters land, not
//! ahead of implementation.

use crate::Timestamp;
use crate::activity::ActivitySourceKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::BTreeMap;
use std::str::FromStr;

// ============================================================================
// Health Aggregator Payloads
// ============================================================================

/// Health status vocabulary emitted by the health aggregator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthAggregatedStatus {
    Unknown,
    Healthy,
    Degraded,
    Failed,
}

impl FromStr for HealthAggregatedStatus {
    type Err = ();

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_ascii_lowercase().as_str() {
            "unknown" => Ok(Self::Unknown),
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "failed" => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

/// Health alert discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthAlertType {
    ComponentStatusChange,
}

/// Health alert severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthAlertSeverity {
    Critical,
    Warning,
}

/// Health report discriminator for non-alert aggregate reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthAggregatedReportType {
    SystemHealthStatus,
    ComponentHealthReport,
}

/// Component snapshot embedded in system-wide health reports.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HealthComponentSnapshot {
    pub name: String,
    pub status: HealthAggregatedStatus,
    pub status_since: Timestamp,
    pub last_seen: Timestamp,
}

/// Immediate health alert report.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HealthAggregatedAlertPayload {
    pub alert_type: HealthAlertType,
    pub component: String,
    pub status: HealthAggregatedStatus,
    pub timestamp: Timestamp,
    pub reason: String,
    pub severity: HealthAlertSeverity,
}

/// System-wide health summary report.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HealthAggregatedSystemStatusPayload {
    pub report_type: HealthAggregatedReportType,
    pub timestamp: Timestamp,
    pub overall_status: HealthAggregatedStatus,
    pub total_components: usize,
    pub healthy_count: usize,
    pub degraded_count: usize,
    pub failed_count: usize,
    pub unknown_count: usize,
    pub components: Vec<HealthComponentSnapshot>,
}

/// Per-component health report.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct HealthAggregatedComponentReportPayload {
    pub report_type: HealthAggregatedReportType,
    pub timestamp: Timestamp,
    pub component: String,
    pub current_status: HealthAggregatedStatus,
    pub status_since: Timestamp,
    pub last_seen: Timestamp,
    pub total_transitions: u64,
    pub events_in_window: usize,
    pub transitions_in_window: usize,
    pub window_seconds: u64,
}

/// Reports emitted by `sinex-health-automaton`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[serde(untagged)]
#[event_payload(
    source = "health-aggregator",
    event_type = "health.aggregated_report",
    version = "1.0.0"
)]
pub enum HealthAggregatedReportPayload {
    Alert(HealthAggregatedAlertPayload),
    SystemStatus(HealthAggregatedSystemStatusPayload),
    ComponentReport(HealthAggregatedComponentReportPayload),
}

// ============================================================================
// Activity Window / Session Payloads
// ============================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityWindowCloseReason {
    Gap,
    MaxDuration,
    MaxEventCount,
}

/// Completed bounded activity window derived from trusted activity signals.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "derived.activity-window",
    event_type = "activity.window.summary",
    version = "1.0.0"
)]
pub struct ActivityWindowSummaryPayload {
    pub window_id: String,
    pub window_start: Timestamp,
    pub window_end: Timestamp,
    pub duration_secs: u64,
    pub event_count: u64,
    pub source_count: u64,
    pub sources: Vec<String>,
    pub activity_sources: Vec<ActivitySourceKind>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub primary_source: ActivitySourceKind,
    pub close_reason: ActivityWindowCloseReason,
}

/// Completed activity session derived from trusted activity signals.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "derived.session-detector",
    event_type = "activity.session.boundary",
    version = "1.0.0"
)]
pub struct ActivitySessionBoundaryPayload {
    pub session_id: String,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub duration_secs: u64,
    pub event_count: u64,
    pub window_count: u64,
    pub source_count: u64,
    pub sources: Vec<String>,
    pub activity_sources: Vec<ActivitySourceKind>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub primary_source: ActivitySourceKind,
}

/// Completed hourly activity rollup derived from bounded activity windows.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "derived.hourly-summarizer",
    event_type = "activity.summary.hourly",
    version = "1.0.0"
)]
pub struct ActivityHourlySummaryPayload {
    pub hour_id: String,
    pub hour_start: Timestamp,
    pub hour_end: Timestamp,
    pub duration_secs: u64,
    pub window_count: u64,
    pub event_count: u64,
    pub source_count: u64,
    pub sources: Vec<String>,
    pub top_sources: Vec<String>,
    pub source_window_counts: BTreeMap<String, u64>,
    pub activity_sources: Vec<ActivitySourceKind>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub focus_time_secs_by_source: BTreeMap<ActivitySourceKind, u64>,
    pub primary_source: ActivitySourceKind,
}

/// Completed daily activity rollup derived from hourly activity summaries.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "derived.daily-summarizer",
    event_type = "activity.summary.daily",
    version = "1.0.0"
)]
pub struct ActivityDailySummaryPayload {
    pub day_id: String,
    pub day_start: Timestamp,
    pub day_end: Timestamp,
    pub duration_secs: u64,
    pub hour_count: u64,
    pub window_count: u64,
    pub event_count: u64,
    pub source_count: u64,
    pub sources: Vec<String>,
    pub top_sources: Vec<String>,
    pub source_window_counts: BTreeMap<String, u64>,
    pub activity_sources: Vec<ActivitySourceKind>,
    pub activity_source_counts: BTreeMap<ActivitySourceKind, u64>,
    pub focus_time_secs_by_source: BTreeMap<ActivitySourceKind, u64>,
    pub primary_source: ActivitySourceKind,
}
