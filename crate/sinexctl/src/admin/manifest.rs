//! Snapshot manifest types and JSON serialisation.

use serde::{Deserialize, Serialize};

/// Unique identifier for a snapshot (`UUIDv7` — sortable by creation time).
pub type SnapshotId = String;

/// Top-level manifest written into every snapshot archive as `manifest.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    /// `UUIDv7` identifier assigned at snapshot creation time.
    pub snapshot_id: SnapshotId,
    /// RFC 3339 timestamp of when the snapshot was started.
    pub created_at: String,
    /// Sinex version string from `CARGO_PKG_VERSION`.
    pub sinex_version: String,
    /// Short git SHA, if obtainable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
    /// Hostname of the machine that produced the snapshot.
    pub host: String,
    /// Snapshot mode: `"quiesce"` or `"live"`.
    pub mode: String,
    /// Evidence that the writer-service preflight made the `quiesce` mode claim true.
    ///
    /// Older archives predate this receipt, so its absence is evidence that the
    /// archive cannot prove its own quiescence rather than evidence of a live
    /// capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiesce_receipt: Option<QuiesceReceipt>,
    /// Source IDs known at snapshot time.
    #[serde(default)]
    pub source_ids: Vec<String>,
    /// Per-component capture records.
    pub components: Vec<ComponentRecord>,
    /// Aggregate size summary.
    pub totals: Totals,
}

/// Durable result of the writer-service stop and verification preflight.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuiesceReceipt {
    /// Writer services observed active immediately before the snapshot started.
    pub active_writer_units_before: Vec<String>,
    /// Writer services this command explicitly stopped. Empty when the
    /// preflight found the deployment already quiescent.
    pub stopped_writer_units: Vec<String>,
    /// Writer services still active after preflight, and after the stop when
    /// one was required. A successfully produced quiesced archive always
    /// records an empty list here.
    pub active_writer_units_after: Vec<String>,
}

/// Record for a single captured component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentRecord {
    /// Component name (`postgres`, `nats`, `cas`, `state`).
    pub name: String,
    /// Path inside the staging directory / archive (relative).
    pub path: String,
    /// Uncompressed size in bytes of everything at `path`.
    pub bytes: u64,
    /// BLAKE3 hex digest of the component root (file or directory tree hash).
    pub blake3: String,
    /// Extra component-specific metadata (e.g. row counts for postgres).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<ComponentExtras>,
}

/// Optional component-specific metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ComponentExtras {
    /// NATS `JetStream` state member paths.
    ///
    /// Keep this before `Postgres`: `PostgresExtras::row_counts` is optional,
    /// so serde's untagged matching would otherwise accept every NATS extras
    /// object as an empty Postgres record and discard `member_paths`.
    Nats(NatsExtras),
    /// Runtime state metadata.
    State(StateExtras),
    /// CAS blob count.
    ///
    /// Keep this before `Postgres`: `PostgresExtras::row_counts` is optional,
    /// so serde's untagged matching would otherwise accept the CAS object as
    /// an empty PostgreSQL record and silently discard `blob_count`.
    Cas(CasExtras),
    /// `PostgreSQL` row counts per table.
    Postgres(PostgresExtras),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresExtras {
    /// Exact row counts keyed by `schema.table`.
    /// `None` means capture could not obtain evidence.
    #[serde(default)]
    pub row_counts: Option<std::collections::BTreeMap<String, i64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsExtras {
    /// Files observed under the captured `JetStream` state root.
    pub member_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasExtras {
    /// Number of blobs in the repository.
    pub blob_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateExtras {
    /// Source contracts registered in the `sinexctl` binary that created the snapshot.
    pub source_ids: Vec<String>,
    /// Whether runtime private-mode state was present in the captured state.
    pub private_mode_state_present: bool,
}

/// Aggregate size totals for the snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Totals {
    /// Sum of all component uncompressed sizes.
    pub uncompressed_bytes: u64,
    /// Final compressed archive size — `null` when not yet known (dry-run mode).
    pub archive_bytes: Option<u64>,
}
