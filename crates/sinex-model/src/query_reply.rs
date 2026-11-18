//! Query pages carried by every client, independent of database execution.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// One page of an answer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct QueryPage {
    pub table: String,
    pub query: String,
    pub read_as: String,
    pub resolved: ResolvedRead,
    pub columns: Vec<String>,
    pub rows: Vec<Json>,
    /// Rows in the whole answer.
    pub total: i64,
    pub page: QueryPageInfo,
    /// What a recall found per source, and which sources had no data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recall: Option<Json>,
}

/// What the query used: `now`, the zone, the day start and the commit
/// position it read at.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ResolvedRead {
    pub now: String,
    pub zone: String,
    pub day_start: String,
    pub commit_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct QueryPageInfo {
    pub size: u64,
    /// `None` on the last page.
    pub cursor: Option<String>,
}
