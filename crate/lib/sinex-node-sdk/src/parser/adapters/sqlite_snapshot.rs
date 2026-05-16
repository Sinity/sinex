//! SQLite database-file snapshot lane.
//!
//! # Purpose
//!
//! [`SqliteRowAdapter`] (and the [`AdapterBackedIngestor`] that hosts it) capture
//! row-projection bytes into a rotating stream material. That lane preserves
//! per-row provenance: every event anchors into a byte range of a long-lived
//! material whose contents are the JSON serialisations of the rows.
//!
//! For long-horizon **reinterpretation** — re-running a new parser against the
//! same evidence, recovering blob columns that the current query projects out,
//! or auditing what was actually on disk at a moment in time — the row-export
//! lane is weak evidence. It contains only the columns and rows the parser
//! chose to read. The actual SQLite database file is stronger evidence.
//!
//! `SqliteSnapshotLane` captures the **SQLite database file as a single source
//! material** on a periodic timer. It runs in parallel with the row-export
//! lane: events stay anchored in their row-export materials, but a second
//! material lineage records the substrate. Per-snapshot content hashing skips
//! identical successive snapshots so a quiet DB does not produce churn.
//!
//! # Lifecycle
//!
//! 1. Caller (`AdapterBackedIngestor` if `InputShapeAdapter::snapshot_lane` is
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
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use sinex_primitives::SinexError;

use crate::NodeResult;
use crate::acquisition_manager::AcquisitionManager;

/// Maximum NATS frame payload — must match
/// `AcquisitionManager::MAX_NATS_PAYLOAD_BYTES`. Slices larger than this are
/// rejected by `append_slice`, so the snapshot lane chunks file reads to this
/// boundary.
///
/// Kept as a private const so we don't leak the runtime constraint into the
/// public type signature.
const SNAPSHOT_SLICE_BYTES: usize = 256 * 1024;

/// Default snapshot interval: 1 hour matches the default rotation-window
/// max-age. Snapshots and rotations land at similar cadences, but on
/// independent timers.
pub const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(3600);

/// Per-source-unit snapshot configuration.
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

/// Description of a snapshot lane handed to [`AdapterBackedIngestor`] by
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
    /// stable across runs for the same source unit; usually
    /// `"<source_unit_id>.snapshot"`.
    pub source_identifier: String,

    /// How often to capture a snapshot.
    pub interval: Duration,

    /// Skip back-to-back identical snapshots.
    pub dedup_by_content_hash: bool,
}

impl SnapshotLaneSpec {
    /// Build a spec from a SQLite path and snapshot config. Returns `None` if
    /// the config is not enabled.
    #[must_use]
    pub fn from_sqlite_config(
        path: impl AsRef<Path>,
        source_unit_id: &str,
        config: &SqliteSnapshotConfig,
    ) -> Option<Self> {
        if !config.enabled() {
            return None;
        }
        Some(Self {
            path: path.as_ref().to_path_buf(),
            source_identifier: format!("{source_unit_id}.snapshot"),
            interval: config.interval(),
            dedup_by_content_hash: config.dedup_by_content_hash,
        })
    }
}

/// The running snapshot lane.
///
/// Instantiated by [`AdapterBackedIngestor::initialize`] when the adapter
/// returns `Some(SnapshotLaneSpec)`. Run via [`Self::run`] inside a
/// `tokio::spawn`; the parent holds the resulting `JoinHandle` and a shutdown
/// `watch` sender to terminate the lane cooperatively.
pub struct SqliteSnapshotLane {
    spec: SnapshotLaneSpec,
    acquisition_manager: Arc<AcquisitionManager>,
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
            last_hash: None,
            snapshots_captured: 0,
        }
    }

    /// Number of snapshots successfully captured. Exposed for tests.
    #[must_use]
    pub const fn snapshots_captured(&self) -> u64 {
        self.snapshots_captured
    }

    /// Run the snapshot loop until `shutdown` fires.
    ///
    /// Errors during a single capture are logged but do not abort the loop —
    /// the next interval is still tried. This matches the "best effort,
    /// long-horizon" character of the snapshot lane.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> NodeResult<()> {
        info!(
            path = %self.spec.path.display(),
            source_identifier = %self.spec.source_identifier,
            interval_s = self.spec.interval.as_secs(),
            "SqliteSnapshotLane starting"
        );

        // Capture once immediately so a short-running test (or a freshly
        // restarted source unit) can prove at least one snapshot landed
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
    pub async fn capture_once(&mut self) -> NodeResult<()> {
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

        self.acquisition_manager
            .finalize(handle, "snapshot-lane-interval")
            .await?;

        self.last_hash = Some(hash);
        self.snapshots_captured += 1;
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
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use xtask::sandbox::prelude::*;

    fn make_acquisition_manager(
        work_dir: &Path,
        nats_client: async_nats::Client,
        label: &str,
    ) -> Arc<AcquisitionManager> {
        let namespace = format!("{label}-{}", sinex_primitives::primitives::Uuid::new_v4());
        Arc::new(
            AcquisitionManager::new_with_namespace(
                nats_client,
                crate::acquisition_manager::RotationPolicy::default(),
                label.to_string(),
                Some(namespace),
            )
            .with_work_dir(work_dir),
        )
    }

    fn make_sqlite_db_with_payload(payload: &str) -> NamedTempFile {
        let f = NamedTempFile::with_suffix(".db").unwrap();
        let conn = rusqlite::Connection::open(f.path()).unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE k (v TEXT);
             INSERT INTO k (v) VALUES ('{payload}');",
        ))
        .unwrap();
        f
    }

    #[sinex_test]
    async fn snapshot_disabled_by_default() -> TestResult<()> {
        let cfg = SqliteSnapshotConfig::default();
        assert!(!cfg.enabled());
        let spec = SnapshotLaneSpec::from_sqlite_config("/tmp/x.db", "test.unit", &cfg);
        assert!(spec.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn snapshot_spec_built_when_enabled() -> TestResult<()> {
        let cfg = SqliteSnapshotConfig {
            interval_seconds: 60,
            dedup_by_content_hash: true,
        };
        assert!(cfg.enabled());
        let spec = SnapshotLaneSpec::from_sqlite_config("/tmp/x.db", "test.unit", &cfg).unwrap();
        assert_eq!(spec.source_identifier, "test.unit.snapshot");
        assert_eq!(spec.interval, Duration::from_secs(60));
        assert!(spec.dedup_by_content_hash);
        Ok(())
    }

    #[sinex_test]
    async fn capture_once_produces_one_material(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-one");

        let db = make_sqlite_db_with_payload("hello");
        let spec = SnapshotLaneSpec {
            path: db.path().to_path_buf(),
            source_identifier: "test.atuin.snapshot".to_string(),
            interval: Duration::from_secs(3600),
            dedup_by_content_hash: true,
        };
        let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

        assert_eq!(lane.snapshots_captured(), 0);
        lane.capture_once().await?;
        assert_eq!(lane.snapshots_captured(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn capture_dedups_unchanged_db(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-dedup");

        let db = make_sqlite_db_with_payload("hello");
        let spec = SnapshotLaneSpec {
            path: db.path().to_path_buf(),
            source_identifier: "test.atuin.snapshot".to_string(),
            interval: Duration::from_secs(3600),
            dedup_by_content_hash: true,
        };
        let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

        lane.capture_once().await?;
        lane.capture_once().await?;
        lane.capture_once().await?;
        // First capture lands; subsequent identical-hash captures dedup.
        assert_eq!(lane.snapshots_captured(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn capture_emits_new_material_when_content_changes(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-changes");

        // Reuse the same path across two distinct DBs by writing through the
        // same NamedTempFile (path stable). We rebuild the DB to mutate
        // content while keeping the path the same.
        let path = tempfile::NamedTempFile::with_suffix(".db").unwrap();
        {
            let conn = rusqlite::Connection::open(path.path()).unwrap();
            conn.execute_batch("CREATE TABLE k (v TEXT); INSERT INTO k VALUES ('a');")
                .unwrap();
        }

        let spec = SnapshotLaneSpec {
            path: path.path().to_path_buf(),
            source_identifier: "test.atuin.snapshot".to_string(),
            interval: Duration::from_secs(3600),
            dedup_by_content_hash: true,
        };
        let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

        lane.capture_once().await?;
        assert_eq!(lane.snapshots_captured(), 1);

        // Mutate the DB so the file hash changes.
        {
            let conn = rusqlite::Connection::open(path.path()).unwrap();
            conn.execute_batch("INSERT INTO k VALUES ('b');").unwrap();
        }

        lane.capture_once().await?;
        assert_eq!(lane.snapshots_captured(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn capture_with_dedup_disabled_always_emits(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-nodedup");

        let db = make_sqlite_db_with_payload("xyz");
        let spec = SnapshotLaneSpec {
            path: db.path().to_path_buf(),
            source_identifier: "test.atuin.snapshot".to_string(),
            interval: Duration::from_secs(3600),
            dedup_by_content_hash: false,
        };
        let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

        lane.capture_once().await?;
        lane.capture_once().await?;
        lane.capture_once().await?;
        assert_eq!(lane.snapshots_captured(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn missing_path_returns_error(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-missing");

        let spec = SnapshotLaneSpec {
            path: PathBuf::from("/definitely/does/not/exist.db"),
            source_identifier: "test.atuin.snapshot".to_string(),
            interval: Duration::from_secs(3600),
            dedup_by_content_hash: true,
        };
        let mut lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

        assert!(lane.capture_once().await.is_err());
        assert_eq!(lane.snapshots_captured(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn run_loop_exits_on_shutdown(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let work_dir = tempfile::tempdir()?;
        let manager = make_acquisition_manager(work_dir.path(), ctx.nats_client(), "snap-run");

        let db = make_sqlite_db_with_payload("loopy");
        let spec = SnapshotLaneSpec {
            path: db.path().to_path_buf(),
            source_identifier: "test.atuin.snapshot".to_string(),
            // Long interval — only the initial-capture should run before shutdown.
            interval: Duration::from_secs(3600),
            dedup_by_content_hash: true,
        };
        let lane = SqliteSnapshotLane::new(spec, Arc::clone(&manager));

        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move { lane.run(rx).await });

        // Give the lane a beat to do its initial capture, then shut it down.
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(true).unwrap();
        let result = task.await.expect("task join")?;
        let _ = result;
        Ok(())
    }
}
