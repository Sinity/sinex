//! An immutable core backup cut. Producer custody and archive durability are separate.
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Database {
    pub name: String,
    pub oid: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct File {
    pub path: String,
    pub bytes: u64,
    pub blake3: String,
}

/// Published only after both dumps and every committed segment prefix are durable.
/// This is local byte custody, never an assertion that Borg or a producer acknowledged it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Manifest {
    pub format: u32,
    pub boundary: Hold,
    pub database: Database,
    pub source_revision: String,
    pub files: Vec<File>,
}
