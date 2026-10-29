//! Moment marks and saved questions are ordinary authored records.
use crate::Timestamp;
use crate::query::saved::Parameter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version = 2, type = "operator.mark", key = location, replace = repeat, time = at, quality = operator)]
pub struct Mark {
    #[event(class = guarded)]
    pub host: String,
    #[event(class = guarded)]
    pub at: Timestamp,
    #[event(class = guarded)]
    pub text: Option<String>,
    #[event(class = guarded)]
    pub cwd: Option<String>,
    #[event(class = guarded)]
    pub command: Option<String>,
    #[event(class = guarded)]
    pub app: Option<String>,
    #[event(class = guarded)]
    pub title: Option<String>,
    #[event(class = guarded)]
    pub by: String,
    #[event(class = guarded)]
    pub written_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[event(class = guarded)]
    pub command_record: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[event(class = guarded)]
    pub focus_record: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version = 2, type = "query.saved", key = ["name", "by"], replace = newer, time = written_at, quality = intrinsic)]
pub struct Saved {
    #[event(class = guarded)]
    pub name: String,
    #[event(class = guarded)]
    pub query: String,
    #[event(class = guarded)]
    pub parameters: Vec<Parameter>,
    #[event(class = guarded)]
    pub by: String,
    #[event(class = guarded)]
    pub written_at: Timestamp,
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum QueryRecord { Mark(Mark), Saved(Saved) }
}
