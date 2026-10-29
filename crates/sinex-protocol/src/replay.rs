//! Raw parser replay and current-policy admitted-input coverage are distinct modes.
use crate::{CoverageProof, ProjectionIdentity, ReplayScopePage, ReplayScopeQuery, SourceRef};
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
        policy_epoch: i64,
        scope: String,
        inputs: Vec<InputReference>,
        continuation: ReplayContinuation,
    },
    Incomplete {
        reason: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReplayContinuation {
    Next { cursor: String },
    Complete { basis: CoverageProof },
}
/// Fetch must return exactly this original slot and current-policy projection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InputReference {
    pub event: EventId,
    pub commit_seq: i64,
    pub archived: bool,
    pub lifecycle_position: Option<i64>,
    pub projection: ProjectionIdentity,
    pub source: SourceRef,
    pub source_fingerprint: String,
}
