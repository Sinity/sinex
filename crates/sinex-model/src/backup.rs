//! A core backup boundary. Producer custody is a separate, explicitly incomplete cut.
use crate::{OperationId, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProducerCustody {
    Incomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Hold {
    pub id: OperationId,
    pub actor: String,
    pub created_at: Timestamp,
    pub released_at: Option<Timestamp>,
    pub commit_position: i64,
    pub blob_changes: i64,
    pub schema_version: i32,
    pub producer_custody: ProducerCustody,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Segment {
    pub name: String,
    pub committed_end: u64,
}
