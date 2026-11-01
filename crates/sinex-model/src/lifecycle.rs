//! The lifecycle's operations as they are requested and stored: which act,
//! over which scope. Readings and materials are live, archived (reversible)
//! or tombstoned (irreversible, reached only from archived), and whole
//! provenance chains move between those states together.
use crate::{EventId, MaterialId, OperationId, RunId, Timestamp};
use serde::{Deserialize, Serialize};

/// Which lifecycle act an operation performs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, crate::DbCheck)]
#[serde(rename_all = "snake_case")]
pub enum Act {
    /// Live to archived.
    Archive,
    /// Archived to live.
    Restore,
    /// Archived to tombstoned; irreversible.
    Tombstone,
    /// The inverse of an earlier operation: archive what it made live, then
    /// restore what it archived.
    Undo,
}

/// What an operation acts on, as given and stored with it. A scope never
/// defaults to everything: each form names its members.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "snake_case", deny_unknown_fields)]
pub enum Scope {
    /// Readings named by reference, or the rows a query selected.
    Events {
        events: Vec<EventId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
    },
    /// Whole materials: their status and every reading read from them.
    Materials { materials: Vec<MaterialId> },
    /// Every reading one run produced.
    Run { run: RunId },
    /// One source's readings, optionally within a time range of `ts_orig`.
    Source {
        source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<Timestamp>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<Timestamp>,
    },
    /// The chains an earlier operation moved: for restore, those it archived;
    /// for undo, everything it changed.
    Operation { operation: OperationId },
}

/// How an operation ended. A running operation has no result yet; one whose
/// driver stopped without an end resumes like a `stopped_early` one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, crate::DbCheck)]
#[serde(rename_all = "snake_case")]
pub enum OperationResult {
    Ok,
    StoppedEarly,
}

/// A material's lifecycle status, which cascades to every reading read from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, crate::DbCheck)]
#[serde(rename_all = "snake_case")]
pub enum MaterialStatus {
    Live,
    Archived,
    Tombstoned,
}
