//! Derived activity: state intervals, metric figures, and hourly and daily
//! health figures.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A span during which some state held: a focused window, a workspace, an
/// ActivityWatch bout. Derived from transitions by the
/// interval derivation. Identified by its start evidence; the end may still
/// be unknown (`ended_at` is `None` while the state holds).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "state.interval",
    key = ["interval_id"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct StateInterval {
    /// `<state kind>:<subject>:<start record location or start time>`.
    pub interval_id: String,
    pub state_kind: String,
    pub subject_id: Option<String>,
    pub label: Option<String>,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub start_event_type: String,
    /// What ended it: the next transition, a fence, a stop, or the observed
    /// duration (`None` while open).
    pub end_event_type: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// One metric over one civil hour or logical day: how many input events it
/// counted and the metric's number for them, in total or for one value of a
/// breakdown field. Hourly figures are computed from the metric's input
/// events, daily ones from the day's hourly figures. A period nothing was
/// counted in has no figure; whether that means zero or unobserved depends on
/// whether the sources were observing, which is read with the figures.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "metric.figure",
    key = ["metric", "grain", "period_start", "dimension", "dimension_value"],
    replace = newer,
    time = period_start,
    quality = intrinsic
)]
pub struct MetricFigure {
    /// The metric's name (`shell.commands`).
    pub metric: String,
    /// `hour` or `day`.
    pub grain: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    /// The logical day the period belongs to (`YYYY-MM-DD`).
    pub day: String,
    /// The breakdown field (`cwd`), and the value this figure counts for;
    /// both empty for the total.
    pub dimension: String,
    pub dimension_value: String,
    /// The metric's number; `None` where it is undefined (a ratio of zero
    /// commands with a known exit code).
    pub value: Option<f64>,
    /// A ratio's parts, summed from hours to the day.
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    /// Input events counted.
    pub rows: u64,
}

/// Figures over one series of health points in one civil hour or logical
/// day: how many points started in the period and, for each numeric value
/// position, their count, sum, minimum and maximum. Hourly figures are
/// computed from the points, daily ones from the day's hourly figures.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "health.figure",
    key = ["grain", "period_start", "provider", "series"],
    replace = newer,
    time = period_start,
    quality = intrinsic
)]
pub struct HealthFigure {
    /// `hour` or `day`.
    pub grain: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    /// The logical day the period belongs to (`YYYY-MM-DD`).
    pub day: String,
    pub provider: String,
    /// The points' data type.
    pub category: String,
    /// Where the points came from: a Fit data source.
    pub series: String,
    pub points: u64,
    pub values: Vec<FigureValue>,
}

/// One numeric value position of a series' points, over a period.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FigureValue {
    pub position: u32,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}
