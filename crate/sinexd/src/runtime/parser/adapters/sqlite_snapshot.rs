//! `SQLite` database-file snapshot lane.
//!
//! # Purpose
//!
//! [`SqliteRowAdapter`] (and the [`AdapterBackedSource`] that hosts it) capture
//! row-projection bytes into a rotating stream material. That lane preserves
//! per-row provenance: every event anchors into a byte range of a long-lived
//! material whose contents are the JSON serialisations of the rows.
//!
//! For long-horizon **reinterpretation** — re-running a new parser against the
//! same evidence, recovering blob columns that the current query projects out,
//! or auditing what was actually on disk at a moment in time — the row-export
//! lane is weak evidence. It contains only the columns and rows the parser
//! chose to read. The actual `SQLite` database file is stronger evidence.
//!
//! `SqliteSnapshotLane` captures the **`SQLite` database file as a single source
//! material** on a periodic timer. It runs in parallel with the row-export
//! lane: events stay anchored in their row-export materials, but a second
//! material lineage records the substrate. Per-snapshot content hashing skips
//! identical successive snapshots so a quiet DB does not produce churn.
//!
//! # Lifecycle
//!
//! 1. Caller (`AdapterBackedSource` if `InputShapeAdapter::snapshot_lane` is
//!    `Some`) spawns a tokio task running [`SqliteSnapshotLane::run`].
//! 2. The task loops: sleep `interval` → read DB file → hash contents → if
//!    different from last snapshot, register a new source material and write
//!    the file bytes as ≤512KB slices.
//! 3. Task exits when the parent's shutdown receiver flips, or when the channel
//!    handle is dropped.
//!
//! # Why a separate lane?
//!
//! The row-export rotation-window material answers "what did the rows look
//! like as they were captured?". The file snapshot answers "what was the
//! literal database on disk at this point?". They are different evidence
//! claims with different reinterpretation properties; storing them as
//! distinct material lineages keeps both available.
//!
//! Per-row stage events do NOT reference snapshot materials. Snapshot
//! materials carry no event lineage of their own — they exist as durable
//! substrate, queryable by `(material_id, source_identifier)` only.
//! Re-deriving events from a snapshot is a separate (manual) replay path.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use sinex_primitives::SinexError;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;

use crate::runtime::RuntimeResult;
use crate::runtime::acquisition_manager::AcquisitionManager;

/// Maximum NATS frame payload — must match
/// `AcquisitionManager::MAX_NATS_PAYLOAD_BYTES`. Slices larger than this are
/// rejected by `append_slice`, so the snapshot lane chunks file reads to this
/// boundary.
///
/// Kept as a private const so we don't leak the runtime constraint into the
/// public type signature.
const SNAPSHOT_SLICE_BYTES: usize = 256 * 1024;

/// Per-source snapshot configuration.
///
/// Embedded as `snapshot` inside [`SqliteRowConfig`].  When `interval_seconds`
/// is `0` the lane is disabled (the default) and no snapshots are produced.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqliteSnapshotConfig {
    /// Polling interval in seconds. `0` disables the snapshot lane.
    #[serde(default)]
    pub interval_seconds: u64,

    /// Skip snapshots whose content is byte-identical to the previously
    /// captured one. Defaults to `true`. Set `false` for sources that should
    /// produce a snapshot every interval regardless of content (rare, but
    /// useful for forensic timelines that need wall-clock anchors even when
    /// the DB is idle).
    #[serde(default = "default_dedup")]
    pub dedup_by_content_hash: bool,
}

fn default_dedup() -> bool {
    true
}

impl Default for SqliteSnapshotConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 0,
            dedup_by_content_hash: true,
        }
    }
}

impl SqliteSnapshotConfig {
    /// True if the lane is enabled (`interval_seconds > 0`).
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.interval_seconds > 0
    }

    /// Configured interval as a [`Duration`].
    #[must_use]
    pub const fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_seconds)
    }
}

/// Description of a snapshot lane handed to [`AdapterBackedSource`] by
/// `InputShapeAdapter::snapshot_lane`.
///
/// The struct is opaque enough that callers can construct it from any source
/// identifier / path / interval combination, not just the SQLite-specific
/// path. We keep the type concrete (rather than a trait object) because the
/// only kind currently exercised is the file-snapshot kind.
#[derive(Debug, Clone)]
pub struct SnapshotLaneSpec {
    /// Path to the file or directory to snapshot.
    pub path: PathBuf,

    /// Source identifier embedded in `raw.source_material_registry`. Should be
    /// stable across runs for the same source; usually
    /// `"<source_id>.snapshot"`.
    pub source_identifier: String,

    /// How often to capture a snapshot.
    pub interval: Duration,

    /// Skip back-to-back identical snapshots.
    pub dedup_by_content_hash: bool,
}

impl SnapshotLaneSpec {
    /// Build a spec from a `SQLite` path and snapshot config. Returns `None` if
    /// the config is not enabled.
    #[must_use]
    pub fn from_sqlite_config(
        path: impl AsRef<Path>,
        source_id: &str,
        config: &SqliteSnapshotConfig,
    ) -> Option<Self> {
        if !config.enabled() {
            return None;
        }
        Some(Self {
            path: path.as_ref().to_path_buf(),
            source_identifier: format!("{source_id}.snapshot"),
            interval: config.interval(),
            dedup_by_content_hash: config.dedup_by_content_hash,
        })
    }
}

/// Latest successfully captured SQLite snapshot material for a source.
///
/// `AdapterBackedSource` owns one shared handle and gives a clone to the
/// parallel snapshot lane. The lane updates it after material finalization;
/// row-event materialization reads it to create `BACKED_BY` links from row
/// stream materials to the strongest available substrate material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteSnapshotEvidence {
    pub material_id: Id<SourceMaterial>,
    pub source_identifier: String,
    pub source_path: String,
    pub content_hash_blake3: String,
    pub size_bytes: usize,
}

#[derive(Debug, Default)]
struct LatestSqliteSnapshotEvidenceInner {
    latest: RwLock<Option<SqliteSnapshotEvidence>>,
}

/// Shared latest-snapshot state for the decoupled SQLite snapshot lane.
#[derive(Debug, Clone, Default)]
pub struct LatestSqliteSnapshotEvidence {
    inner: Arc<LatestSqliteSnapshotEvidenceInner>,
}

impl LatestSqliteSnapshotEvidence {
    /// Store the latest successful snapshot evidence.
    pub fn update(&self, evidence: SqliteSnapshotEvidence) {
        match self.inner.latest.write() {
            Ok(mut latest) => {
                *latest = Some(evidence);
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "latest SQLite snapshot evidence lock poisoned; dropping update"
                );
            }
        }
    }

    /// Return the latest successful snapshot evidence, if any.
    #[must_use]
    pub fn latest(&self) -> Option<SqliteSnapshotEvidence> {
        match self.inner.latest.read() {
            Ok(latest) => latest.clone(),
            Err(error) => {
                warn!(
                    error = %error,
                    "latest SQLite snapshot evidence lock poisoned; treating as absent"
                );
                None
            }
        }
    }
}

/// The running snapshot lane.
///
/// Instantiated by [`AdapterBackedSource::initialize`] when the adapter
/// returns `Some(SnapshotLaneSpec)`. Run via [`Self::run`] inside a
/// `tokio::spawn`; the parent holds the resulting `JoinHandle` and a shutdown
/// `watch` sender to terminate the lane cooperatively.
pub struct SqliteSnapshotLane {
    spec: SnapshotLaneSpec,
    acquisition_manager: Arc<AcquisitionManager>,
    latest_evidence: Option<LatestSqliteSnapshotEvidence>,
    /// Hash of the most recent snapshot's content, if any.
    last_hash: Option<[u8; 32]>,
    /// Count of snapshots successfully published. Exposed for tests.
    snapshots_captured: u64,
}

impl SqliteSnapshotLane {
    /// Create a new lane bound to the given acquisition manager.
    #[must_use]
    pub fn new(spec: SnapshotLaneSpec, acquisition_manager: Arc<AcquisitionManager>) -> Self {
        Self {
            spec,
            acquisition_manager,
            latest_evidence: None,
            last_hash: None,
            snapshots_captured: 0,
        }
    }

    /// Publish successful snapshot material IDs to an adapter-side linker.
    #[must_use]
    pub fn with_latest_evidence(mut self, latest: LatestSqliteSnapshotEvidence) -> Self {
        self.latest_evidence = Some(latest);
        self
    }

    /// Number of snapshots successfully captured. Exposed for tests.
    #[must_use]
    #[cfg(test)]
    pub const fn snapshots_captured(&self) -> u64 {
        self.snapshots_captured
    }

    /// Run the snapshot loop until `shutdown` fires.
    ///
    /// Errors during a single capture are logged but do not abort the loop —
    /// the next interval is still tried. This matches the "best effort,
    /// long-horizon" character of the snapshot lane.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> RuntimeResult<()> {
        info!(
            path = %self.spec.path.display(),
            source_identifier = %self.spec.source_identifier,
            interval_s = self.spec.interval.as_secs(),
            "SqliteSnapshotLane starting"
        );

        // Capture once immediately so a short-running test (or a freshly
        // restarted source) can prove at least one snapshot landed
        // without waiting a full interval.
        if let Err(e) = self.capture_once().await {
            warn!(
                path = %self.spec.path.display(),
                error = %e,
                "Initial snapshot failed; continuing"
            );
        }

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        info!(
                            path = %self.spec.path.display(),
                            captured = self.snapshots_captured,
                            "SqliteSnapshotLane shutdown signal received"
                        );
                        return Ok(());
                    }
                }
                () = tokio::time::sleep(self.spec.interval) => {
                    if let Err(e) = self.capture_once().await {
                        warn!(
                            path = %self.spec.path.display(),
                            error = %e,
                            "Snapshot capture failed; continuing"
                        );
                    }
                }
            }
        }
    }

    /// Capture exactly one snapshot. Exposed (pub) so tests can drive the
    /// lane without spinning a tokio timer.
    pub async fn capture_once(&mut self) -> RuntimeResult<()> {
        // Read the file fully into memory. SQLite DB files routinely fit in
        // RAM (atuin ~tens of MB, activitywatch ~hundreds of MB worst case);
        // streaming hash + slice could be added later for genuinely
        // multi-GB sources.
        let bytes = match tokio::fs::read(&self.spec.path).await {
            Ok(b) => b,
            Err(e) => {
                return Err(SinexError::io(format!(
                    "snapshot lane: failed to read {}: {e}",
                    self.spec.path.display()
                )));
            }
        };

        let hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        if self.spec.dedup_by_content_hash && self.last_hash == Some(hash) {
            debug!(
                path = %self.spec.path.display(),
                hash = %hex_short(&hash),
                "Snapshot content unchanged; skipping"
            );
            return Ok(());
        }

        let metadata = json!({
            "lane": "sqlite_file_snapshot",
            "source_path": self.spec.path.display().to_string(),
            "content_hash_blake3": hex_full(&hash),
            "size_bytes": bytes.len(),
        });

        let mut handle = self
            .acquisition_manager
            .build_material(&self.spec.source_identifier)
            .with_metadata(metadata)
            .begin()
            .await?;

        // Chunk into ≤SNAPSHOT_SLICE_BYTES frames; one frame per
        // `append_slice` call. Slices share one material; finalize records
        // the total content hash.
        let mut written: usize = 0;
        while written < bytes.len() {
            let end = (written + SNAPSHOT_SLICE_BYTES).min(bytes.len());
            self.acquisition_manager
                .append_slice(&mut handle, &bytes[written..end])
                .await?;
            written = end;
        }

        let material_id = handle.material_id;
        self.acquisition_manager
            .finalize(handle, "snapshot-lane-interval")
            .await?;

        self.last_hash = Some(hash);
        self.snapshots_captured += 1;
        if let Some(latest) = &self.latest_evidence {
            latest.update(SqliteSnapshotEvidence {
                material_id: Id::<SourceMaterial>::from_uuid(material_id),
                source_identifier: self.spec.source_identifier.clone(),
                source_path: self.spec.path.display().to_string(),
                content_hash_blake3: hex_full(&hash),
                size_bytes: bytes.len(),
            });
        }
        info!(
            path = %self.spec.path.display(),
            size_bytes = bytes.len(),
            captured_total = self.snapshots_captured,
            "SqliteSnapshotLane captured snapshot"
        );
        Ok(())
    }
}

/// Short hex prefix for log lines.
fn hex_short(bytes: &[u8]) -> String {
    let n = bytes.len().min(8);
    let mut s = String::with_capacity(n * 2);
    for b in &bytes[..n] {
        use std::fmt::Write;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

/// Full hex of a byte slice. Used for material metadata, not log lines.
fn hex_full(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

#[cfg(test)]
#[path = "sqlite_snapshot_test.rs"]
mod tests;
