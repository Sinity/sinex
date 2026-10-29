//! A query's resolved selection, retained independently of presentation pages.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
