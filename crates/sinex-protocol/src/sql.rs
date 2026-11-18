//! Read-only SQL requests and their bounded pages.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::query_reply::QueryPageInfo;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Sql {
    pub sql: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SqlReply {
    pub rows: Vec<Value>,
    pub count: usize,
    pub page: QueryPageInfo,
}
