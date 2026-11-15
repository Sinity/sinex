//! Producer replay scopes, and the current-state snapshot a consumer installs
//! before following a confirmed stream that starts at a current-state origin.
use crate::{ConfirmedEvent, ReplayScopePage, ReplayScopeQuery, WithheldEvent};
use serde::{Deserialize, Serialize};
use sinex_model::EventId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayQuery {
    Materials {
        query: ReplayScopeQuery,
    },
    /// The next page of the snapshot; no cursor starts a new one.
    Snapshot {
        cursor: Option<SnapshotCursor>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ReplayReply {
    Materials { page: ReplayScopePage },
    Snapshot { reply: SnapshotReply },
}

/// The last confirmed-stream message before a snapshot's database read, read
/// from the broker. Every event it delivered is in the snapshot, so following
/// starts after it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LastStreamMessage {
    pub sequence: u64,
    /// [`crate::DeliveredCompletion::fingerprint`] of that message.
    pub fingerprint: String,
}

/// Where a snapshot stands. It is plain data: each page Sinex recomputes the
/// scope and catalog from the producer's current grants and the stream
/// instance from the broker, and answers [`SnapshotReply::Restart`] when any
/// of them changed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotCursor {
    /// The commit position the whole snapshot is read at.
    pub position: i64,
    pub scope: String,
    pub catalog: String,
    pub stream_instance: String,
    /// `None` when the stream held no message before the read.
    pub last: Option<LastStreamMessage>,
    /// The last event already delivered, by commit position and ID.
    pub after: Option<(i64, EventId)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SnapshotReply {
    Page {
        events: Vec<SnapshotEvent>,
        next: SnapshotNext,
    },
    /// The scope, catalog or stream instance changed, or an event inside the
    /// snapshot was tombstoned since its position: discard the partial
    /// snapshot and start again.
    Restart { reason: String },
    /// Sinex could not answer now (database or broker unavailable); keep the
    /// cursor and ask again.
    Unavailable { reason: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SnapshotNext {
    More { cursor: SnapshotCursor },
    Complete { complete: SnapshotComplete },
}

/// The snapshot is complete at `position`. A consumer that applied every page
/// names [`SnapshotStart`] in its next consumer request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotComplete {
    pub position: i64,
    pub scope: String,
    pub stream_instance: String,
    pub last: Option<LastStreamMessage>,
}

/// Follow the stream after an applied snapshot: delivery starts after `last`
/// (at the first sequence when `None`), and the request's `after` is the
/// snapshot's position.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotStart {
    pub stream_instance: String,
    pub last: Option<LastStreamMessage>,
}

/// One reading as of the snapshot position, in the stream's form, with its
/// lifecycle state at that position.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SnapshotEvent {
    pub event: SnapshotReading,
    /// Archived at or before the snapshot position.
    pub archived: bool,
    /// The position of its last lifecycle change at or before the snapshot
    /// position, if any.
    pub lifecycle_position: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "form", rename_all = "snake_case")]
pub enum SnapshotReading {
    Inline(Box<ConfirmedEvent>),
    Withheld(WithheldEvent),
}
