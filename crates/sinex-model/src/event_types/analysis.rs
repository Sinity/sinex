//! Canonical questions and the stored products maintained from their evidence.
use crate::Timestamp;
pub mod selection;
use schemars::JsonSchema;
pub use selection::QuerySelection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version = 2, type = "analysis.request", key = ["request"], replace = repeat, time = started_at, quality = operator)]
pub struct AnalysisRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<QuerySelection>,
    pub request: String,
    pub operation: String,
    pub recipe: Option<String>,
    pub from: String,
    pub to: String,
    pub metrics: Vec<String>,
    pub min_observed_fraction: Option<f64>,
    pub zone: String,
    pub day_start: String,
    pub grants: Vec<String>,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub by: String,
    pub requested_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "analysis.panel", key = ["request"], replace = newer, time = started_at, quality = intrinsic)]
pub struct AnalysisPanel {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_versions: Option<Value>,
    pub coverage_evidence: Value,
    pub read_as: Option<i64>,
    pub request: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub day_setting: String,
    pub units: Value,
    pub cells: Value,
    pub coverage: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "recipe.answer", key = ["request"], replace = newer, time = started_at, quality = intrinsic)]
pub struct RecipeAnswer {
    pub recipe_version: u32,
    pub method_versions: Value,
    pub parameters: Value,
    pub day_setting: String,
    pub coverage_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_versions: Option<Value>,
    pub coverage_evidence: Value,
    pub read_as: Option<i64>,
    pub request: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub recipe: String,
    pub tables: Value,
    pub findings: Value,
    pub coverage: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "analysis.finding", key = ["request", "index"], replace = newer, time = started_at, quality = intrinsic)]
pub struct Finding {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_versions: Option<Value>,
    pub read_as: Option<i64>,
    pub request: String,
    pub index: u64,
    pub started_at: Timestamp,
    pub finding: Value,
}
