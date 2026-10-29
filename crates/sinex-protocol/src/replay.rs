//! Raw parser replay and current-policy admitted-input coverage are distinct modes.
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
        policy_epoch: i64,
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
/// Fetch must return exactly this original slot and current-policy projection.
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
/// partial chain; these optional identities never assert an epoch change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ReplayBasisStatus {
    Invalidated {
        policy_epoch: Option<i64>,
        scope: Option<String>,
    },
    Unverified,
}
