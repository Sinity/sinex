//! Raw parser replay and admitted-input coverage are distinct modes.
use crate::{ProjectionIdentity, ReplayScopePage, ReplayScopeQuery, SourceRef};
use serde::{Deserialize, Serialize};
use sinex_model::EventId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayQuery {
    Materials {
        query: ReplayScopeQuery,
    },
    Inputs {
        cache: String,
        cursor: Option<String>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ReplayReply {
    Materials { page: ReplayScopePage },
    Inputs { result: InputReplayReply },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum InputReplayReply {
    Page {
        position: i64,
        scope: String,
        inputs: Vec<InputReference>,
        continuation: ReplayContinuation,
    },
    Incomplete {
        reason: String,
        basis: ReplayBasisStatus,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReplayContinuation {
    Next {
        cursor: String,
    },
    Complete {
        projection: FullProjectionComplete,
        raw_inputs: crate::CoverageStatus,
    },
}
/// Owner certification of the entire indexed scoped snapshot. It is usable as
/// an applied prefix only after the client atomically applies every page.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FullProjectionComplete {
    pub stamp: String,
    pub position: i64,
    pub scope: String,
    pub catalog: String,
}
/// Fetch must return exactly this slot and its stored payload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InputReference {
    pub event: EventId,
    pub commit_seq: i64,
    pub archived: bool,
    pub lifecycle_position: Option<i64>,
    pub projection: ProjectionIdentity,
    pub kind: sinex_model::KindRef,
    pub kind_definition_hash: String,
    pub source: Option<SourceRef>,
    pub source_fingerprint: Option<String>,
}

/// Both states fence current output. An invalidated operation discards its
/// partial chain; the optional scope never asserts a scope change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReplayBasisStatus {
    Invalidated { scope: Option<String> },
    Unverified,
}
