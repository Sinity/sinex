//! Read-only declarations of admitted query formats from revision b609f3ef.
//! They are never part of the current registration catalog.
use crate::{KindDeclaration, KindRef, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub name: String,
    pub field_type: ParameterType,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParameterType {
    Text,
    Integer,
    Number,
    Bool,
    Time,
    Duration,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Saved {
    pub name: String,
    pub query: String,
    pub parameters: Vec<Parameter>,
    pub by: String,
    pub written_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QuerySelection {
    pub query: String,
    pub now: Timestamp,
    /// Only an explicitly historical question fixes the producer position.
    pub read_as: Option<i64>,
    pub zone: String,
    pub day_start: String,
    pub grants: Vec<String>,
    /// The resolved window, or stable shell-session/mark record coordinates.
    pub selected: Value,
    /// Exact declaration references and hashes used to bind the question.
    pub declarations: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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

pub(super) fn original(reference: &KindRef) -> Option<KindDeclaration> {
    static KINDS: std::sync::OnceLock<Vec<KindDeclaration>> = std::sync::OnceLock::new();
    KINDS
        .get_or_init(|| {
            serde_json::from_str(include_str!("formats/kinds-v2.json"))
                .expect("frozen query declarations decode")
        })
        .iter()
        .find(|declaration| declaration.kind == *reference)
        .cloned()
}

pub(super) fn validate(kind: &KindRef, payload: &Value) -> Result<(), super::QueryError> {
    match (kind.name.as_str(), kind.version) {
        ("query.saved", 2) => serde_json::from_value::<Saved>(payload.clone()).map(|_| ()),
        ("analysis.request", 2) => {
            serde_json::from_value::<AnalysisRequest>(payload.clone()).map(|_| ())
        }
        ("context.pack", 2) => {
            serde_json::from_value::<crate::event_types::work::ContextPack>(payload.clone())
                .map(|_| ())
        }
        _ => {
            return Err(super::QueryError::new(
                "unknown original query payload format",
            ));
        }
    }
    .map_err(|error| super::QueryError::new(error.to_string()))
}
