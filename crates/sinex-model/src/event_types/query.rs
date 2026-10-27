//! Moment marks and saved questions are ordinary authored records.
use crate::Timestamp;
use crate::query::saved::Parameter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(type = "operator.mark", version = 2, key = location, replace = repeat, time = at, quality = operator, class = guarded)]
pub struct Mark {
    pub host: String,
    pub at: Timestamp,
    pub text: Option<String>,
    pub cwd: Option<String>,
    pub command: Option<String>,
    pub app: Option<String>,
    pub title: Option<String>,
    pub by: String,
    pub written_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(type = "query.saved", version = 2, key = ["name", "by"], replace = newer, time = written_at, quality = intrinsic, class = guarded)]
pub struct Saved {
    pub name: String,
    pub query: String,
    pub parameters: Vec<Parameter>,
    pub by: String,
    pub written_at: Timestamp,
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum QueryRecord { Mark(Mark), Saved(Saved) }
}
