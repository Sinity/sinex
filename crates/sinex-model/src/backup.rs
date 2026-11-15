//! The manifest of one prepared backup: the hold it was taken at, the copied
//! files, and the producer state copied with it. Archiving is the backup tool's job.
use crate::{OperationId, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Hold {
    pub id: OperationId,
    pub actor: String,
    pub created_at: Timestamp,
    pub released_at: Option<Timestamp>,
    pub commit_position: i64,
    pub blob_changes: i64,
    pub schema_version: i32,
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

/// The files copied from one configured producer, or why they could not be.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProducerSnapshot {
    pub producer: String,
    /// Identifies the producer process that answered; a restart changes it.
    pub run_id: Option<String>,
    pub files: Vec<File>,
    pub unavailable: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Manifest {
    pub format: u32,
    pub boundary: Hold,
    pub database: Database,
    pub source_revision: String,
    pub files: Vec<File>,
    pub required_producers: Option<Vec<String>>,
    /// Every required producer's state was copied.
    pub producers_complete: bool,
    pub producer_snapshots: Vec<ProducerSnapshot>,
}

/// What the daemon hands the client that copies a backup: the hold, the
/// exported SQL snapshot the copy reads under, and the producer state already
/// copied into the output while producers were paused. The daemon keeps the
/// snapshot's exporting transaction open for as long as the client's
/// connection stays open, and no longer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CopyJob {
    pub boundary: Hold,
    /// The exported snapshot's name, for `SET TRANSACTION SNAPSHOT` and
    /// `pg_dump --snapshot`.
    pub snapshot: String,
    pub database: Database,
    /// The daemon's database URL without any password; a password comes from
    /// the copying client's own environment.
    pub database_url: String,
    /// The content store whose committed segment prefixes are copied.
    pub store: String,
    pub output: String,
    pub source_revision: String,
    pub producer_snapshots: Vec<ProducerSnapshot>,
    pub required_producers: Option<Vec<String>>,
}
