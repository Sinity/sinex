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
    /// Snapshot mode — currently always `"quiesce"`.
    pub mode: String,
    /// Per-component capture records.
    pub components: Vec<ComponentRecord>,
    /// Aggregate size summary.
    pub totals: Totals,
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
    /// `PostgreSQL` row counts per table.
    Postgres(PostgresExtras),
    /// CAS blob count.
    Cas(CasExtras),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresExtras {
    /// Live row count estimates keyed by `schema.table`.
    pub row_counts: std::collections::BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CasExtras {
    /// Number of blobs in the repository.
    pub blob_count: u64,
}

/// Aggregate size totals for the snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Totals {
    /// Sum of all component uncompressed sizes.
    pub uncompressed_bytes: u64,
    /// Final compressed archive size — `null` when not yet known (dry-run mode).
    pub archive_bytes: Option<u64>,
}
