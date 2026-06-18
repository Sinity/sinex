//! Generic [`AdapterBackedSource`] — wires an [`InputShapeAdapter`] to a
//! [`MaterialParser`] as a full [`SourceDriver`].
//!
//! # Purpose
//!
//! Wave-B source folds need one line per source:
//!
//! ```rust,ignore
//! register_source!(
//!     source_id: "terminal.atuin-history",
//!     adapter:        SqliteRowAdapter,
//!     parser:         AtuinHistoryRecord,
//! );
//! ```
//!
//! `AdapterBackedSource<A, P>` is the `SourceDriver` implementation that
//! backs every such registration. It handles:
//!
//! - Snapshot and historical scans (drive adapter stream → parse → emit).
//! - Continuous mode for append-only adapters (tail loop with shutdown signal).
//! - Cursor persistence via the standard `SourceDriver` state mechanism.
//! - Conversion of `ParsedEventIntent` → `Event<JsonValue>` → `emit()`.
//! - Long-lived source-material lifecycle: records without their own material
//!   provenance are appended to the same [`AppendStreamAcquirer`], which
//!   auto-rotates at 100 MB or 1 hour (configurable). This prevents
//!   `O(poll_count)` material rows.
//! - Already-materialized records keep their adapter-supplied source material
//!   and anchor, which lets file-content adapters preserve byte-provenance
//!   rather than re-wrapping file observations in a metadata stream.
//!
//! # Config shape
//!
//! The source JSON config is deserialized into [`AdapterSourceConfig<A::Config>`]:
//!
//! ```json
//! {
//!   "path": "/path/to/file",
//!   "binding_flags": { "private_mode_active": false }
//! }
//! ```
//!
//! The `adapter` fields are flattened so adapter-specific keys live at the
//! top level — matching the plain `{ "path": "..." }` shape that existing
//! runtime configs use. The optional `binding_flags` map carries runtime flags
//! for `#[suppress_if]` predicates (the `BindingConfig` concern), which is
//! separate from the adapter's typed config.
//!
//! # Design constraints
//!
//! - `A::Cursor` must be serialisable so the runtime checkpoint machinery can
//!   persist and restore it.
//! - `P` must be `Default + MaterialParser`. Both hold for every
//!   `#[derive(SourceRecord)]` struct and for imperative parsers that `impl
//!   Default`.
//! - This struct does NOT own transport or admission — it calls
//!   `runtime.event_emitter().emit()` exactly as every other source does.
//!
//! # Material lifecycle
//!
//! A single [`AppendStreamAcquirer`] is held across all drain cycles (snapshot,
//! historical, and every continuous poll). Record bytes are appended to the
//! growing material; [`AppendStreamAcquirer`] handles size/time-based rotation
//! transparently. This ensures `raw.source_material_registry` grows at
//! `O(rotation_count)`, not `O(poll_count)`.
//!
//! When `run_continuous` exits cleanly (shutdown signal), the current material
//! is finalized. On source drop the [`AppendStreamAcquirer`] finalizes via its
//! own `finalize` path.
//!
//! For adapters that return structured rows (e.g. `SqliteRowAdapter`), the
//! "bytes" written to the material are the JSON serialisation of the record,
//! giving a content-addressable provenance trail for each logical row.
//!
//! # Continuous mode
//!
//! Adapters that do not natively stream (e.g. `SqliteRowAdapter`,
//! `StaticFileAdapter`) are polled on a configurable interval (default 30 s).
//! Adapters that natively support streaming should implement their own
//! `SourceDriver` instead.

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value as JsonValue, json};
use tracing::{debug, info, warn};

use sinex_primitives::events::Event;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::builder::{EventBuilder, NoProvenance};
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, ParsedEventIntent, ParserContext};
use sinex_primitives::primitives::Uuid;
use sinex_primitives::privacy::{
    RuntimePrivateModeState, load_private_mode_state, save_private_mode_state,
};
use sinex_primitives::rpc::sources::SourceCaveat;
use sinex_primitives::temporal::Timestamp;

#[cfg(feature = "db")]
use sinex_db::DbPoolExt;

use crate::runtime::RuntimeResult;
use crate::runtime::acquisition_manager::{
    AcquisitionManager, AppendStreamAcquirer, RotationPolicy,
};
use crate::runtime::parser::adapters::{LatestSqliteSnapshotEvidence, SqliteSnapshotLane};
use crate::runtime::parser::{
    BindingConfig, DriftEvent, InputShapeAdapter, InputShapeAdapterExt, MaterialParser,
    SourceRecord, SourceRecordFingerprint,
};
use crate::runtime::source_driver::SourceDriver;
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, RuntimeCapabilities, RuntimeContext, ScanArgs, ScanReport,
    TimeHorizon,
};
use std::path::PathBuf;
use std::sync::Arc;

const MAX_RECENT_INPUT_DRIFTS: usize = 16;
const PRIVATE_MODE_CONTROL_SUBJECT: &str = "sinex.control.privacy.private_mode";

// =============================================================================
// Typed runtime config — wraps adapter config + optional binding flags
// =============================================================================

/// RuntimeModule-level config for [`AdapterBackedSource`].
///
/// The adapter config is stored as raw JSON (`serde_json::Value`) and
/// deserialized into `A::Config` during `initialize`. This avoids requiring
/// `A::Config: Default` (which many adapter configs cannot satisfy because
/// they have mandatory fields like `path` or `table`).
///
/// The optional `binding_flags` map carries runtime values for `#[suppress_if]`
/// predicates in `DeclarativeParser`-backed parsers. It is separate from the
/// adapter config and defaults to empty.
///
/// # Serde shape
///
/// The adapter config fields live at the top level (flat); `binding_flags` is
/// an optional nested map. Existing runtime configs (e.g. `{ "path": "..." }`)
/// continue to work without modification.
///
/// ```json
/// {
///   "path": "/home/user/.weechat/logs/irc.log",
///   "binding_flags": { "private_mode_active": false },
///   "continuous_poll_interval_secs": 30,
///   "private_mode_state_dir": "/var/lib/sinex",
///   "private_mode_source_class": "desktop",
///   "private_mode_fail_closed": true
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdapterSourceConfig {
    /// Adapter-specific config fields. Flattened so they live at the top
    /// level of the JSON object. Deserialized into `A::Config` at
    /// `initialize` time.
    #[serde(flatten)]
    pub adapter: JsonValue,

    /// Optional runtime flags for `BindingConfig`-aware parsers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub binding_flags: BTreeMap<String, bool>,

    /// Poll interval for adapter-backed continuous mode.
    ///
    /// Adapters without native streaming are drained, then the source sleeps for
    /// this interval before polling again. Defaults to 30 seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuous_poll_interval_secs: Option<u64>,

    /// Optional state root used to derive `private_mode_active` from the
    /// persisted runtime private-mode file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_mode_state_dir: Option<PathBuf>,

    /// Optional source-class override used when matching private-mode scope.
    /// Defaults to the prefix before the first `.` in the source id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_mode_source_class: Option<String>,

    /// Whether unreadable or malformed private-mode state should suppress
    /// acquisition. Defaults to fail-closed when `private_mode_state_dir` is set.
    ///
    /// Lower-sensitivity source contracts may set this to `false` deliberately, but
    /// the unavailable-state caveat still reaches binding-aware parsers through
    /// `private_mode_state_unavailable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_mode_fail_closed: Option<bool>,
}

impl AdapterSourceConfig {
    /// Convert the `binding_flags` map into a [`BindingConfig`] for use with
    /// `DeclarativeParser::evaluate`.
    #[must_use]
    pub fn to_binding_config(&self) -> BindingConfig {
        let mut bc = BindingConfig::new();
        for (name, &value) in &self.binding_flags {
            bc = bc.with_flag(name, value);
        }
        bc
    }

    /// Continuous-mode poll interval, with validation for explicit values.
    pub fn continuous_poll_interval(&self) -> Result<Duration, crate::runtime::SinexError> {
        let seconds = self.continuous_poll_interval_secs.unwrap_or(30);
        if seconds == 0 {
            return Err(crate::runtime::SinexError::configuration(
                "AdapterBackedSource continuous_poll_interval_secs must be greater than zero",
            ));
        }
        Ok(Duration::from_secs(seconds))
    }

    /// Convert static binding flags and persisted private-mode state into a
    /// parser [`BindingConfig`].
    pub fn to_binding_config_for_source(
        &self,
        source_id: &str,
    ) -> Result<BindingConfig, crate::runtime::SinexError> {
        let mut bc = self.to_binding_config();
        let Some(state_dir) = &self.private_mode_state_dir else {
            return Ok(bc);
        };

        let state = match load_private_mode_state(state_dir) {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(
                    source_id,
                    state_dir = %state_dir.display(),
                    error = %error,
                    "private-mode state unavailable for adapter-backed source"
                );
                bc = bc.with_flag("private_mode_state_unavailable", true);
                if self.private_mode_fail_closed.unwrap_or(true) {
                    bc = bc.with_flag("private_mode_active", true);
                }
                return Ok(bc);
            }
        };
        let source_class = self
            .private_mode_source_class
            .as_deref()
            .unwrap_or_else(|| {
                source_id
                    .split_once('.')
                    .map_or(source_id, |(class, _)| class)
            });
        let source = source_id;
        let scoped = state.affected_source_classes.is_empty()
            || state
                .affected_source_classes
                .iter()
                .any(|class| class == source_class || class == source);
        bc = bc.with_flag(
            "private_mode_active",
            state.is_active_at(sinex_primitives::temporal::Timestamp::now()) && scoped,
        );
        Ok(bc)
    }

    /// Deserialize the flattened adapter JSON into the typed adapter config.
    pub fn into_adapter_config<C: DeserializeOwned>(self) -> Result<C, serde_json::Error> {
        serde_json::from_value(self.adapter)
    }
}

// =============================================================================
// Adapter module state (checkpoint-persisted)
// =============================================================================

/// Checkpoint state for [`AdapterBackedSource`].
///
/// Contains the adapter cursor (opaque to the runtime) and event counters.
/// Serialized as the `SourceDriverState<S>::user_state` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "C: Clone + Serialize + DeserializeOwned")]
pub struct AdapterModuleState<C>
where
    C: Clone + Serialize + DeserializeOwned,
{
    /// Last cursor returned by `adapter.cursor_after(record)`.
    pub cursor: Option<C>,

    /// Total events emitted across all scans.
    pub total_events_emitted: u64,

    /// Last adapter-reported input fingerprint, used to detect substrate
    /// shape drift across checkpointed drain cycles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_input_fingerprint: Option<SourceRecordFingerprint>,

    /// Bounded history of recently observed input-shape drift events.
    ///
    /// This is checkpoint-persisted operator evidence: logs are still emitted
    /// for live diagnosis, while this field keeps the most recent drift records
    /// available for later readiness and CLI/RPC surfaces.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_input_drifts: Vec<DriftEvent>,
}

impl<C> Default for AdapterModuleState<C>
where
    C: Clone + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            cursor: None,
            total_events_emitted: 0,
            last_input_fingerprint: None,
            recent_input_drifts: Vec::new(),
        }
    }
}

impl<C> AdapterModuleState<C>
where
    C: Clone + Serialize + DeserializeOwned,
{
    fn record_input_drift(&mut self, drift: DriftEvent) {
        self.recent_input_drifts.push(drift);
        if self.recent_input_drifts.len() > MAX_RECENT_INPUT_DRIFTS {
            let excess = self.recent_input_drifts.len() - MAX_RECENT_INPUT_DRIFTS;
            self.recent_input_drifts.drain(0..excess);
        }
    }

    /// Return readiness caveats for the latest checkpointed input-shape drift.
    ///
    /// Readiness consumers should summarize the latest observed drift rather
    /// than reclassifying raw drift deltas independently. The bounded history
    /// remains available for future operator listings.
    #[must_use]
    pub fn latest_input_drift_caveats(&self) -> Vec<SourceCaveat> {
        self.recent_input_drifts
            .last()
            .map(DriftEvent::readiness_caveats)
            .unwrap_or_default()
    }
}

// =============================================================================
// AdapterBackedSource
// =============================================================================

/// A generic source driver that wraps `(A: InputShapeAdapter, P: MaterialParser)`.
///
/// Type parameters:
/// - `A` — the input-shape adapter (e.g. `SqliteRowAdapter`,
///   `AppendOnlyFileAdapter`).
/// - `P` — the material parser (any type implementing `MaterialParser`, including
///   `#[derive(SourceRecord)]` structs and imperative parsers).
///
/// The adapter and parser are constructed via `Default`, then configured during
/// `initialize`. The runtime config is deserialized into
/// `AdapterSourceConfig<A::Config>`; the source id is hard-coded at
/// registration time via the `register_source!` macro.
pub struct AdapterBackedSource<A, P>
where
    A: InputShapeAdapter + Default + InputShapeAdapterExt,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    /// Human-readable source id, baked in at registration time.
    source_id: &'static str,

    /// The adapter instance. Constructed in `Default`, configured in
    /// `initialize`.
    adapter: A,

    /// The parser instance. Constructed in `Default`.
    parser: P,

    /// Adapter config deserialized from the runtime config at `initialize`.
    config: Option<A::Config>,

    /// Original runtime config, retained so runtime-derived binding flags such as
    /// `private_mode_active` can be refreshed before each acquisition.
    runtime_config: Option<AdapterSourceConfig>,

    /// `BindingConfig` derived from `binding_flags` in the runtime config.
    /// Refreshed before each acquisition so live private-mode toggles do not
    /// require source restart.
    binding_config: BindingConfig,

    /// Runtime handles captured during `initialize`.
    runtime: Option<RuntimeContext>,

    /// Long-lived stream acquirer that grows one source material across many
    /// drain cycles. Rotates automatically at the configured size/time limits.
    /// Initialized lazily on the first drain call after `initialize`.
    stream_acquirer: Option<AppendStreamAcquirer>,

    /// Shared acquisition manager used by `stream_acquirer`. Kept as `Arc` so
    /// the acquirer and any test helpers can share ownership.
    acquisition_manager: Option<Arc<AcquisitionManager>>,

    /// Rotation policy applied to the stream acquirer.
    rotation_policy: RotationPolicy,

    /// Optional parallel snapshot-lane task. Spawned in `initialize` when the
    /// adapter returns a [`SnapshotLaneSpec`]. The lane runs an independent
    /// timer that captures the underlying substrate (currently only the
    /// `SQLite` DB file) into a separate source-material lineage. Per-row
    /// drain is unaffected.
    snapshot_task: Option<tokio::task::JoinHandle<RuntimeResult<()>>>,

    /// Sender that shuts down the snapshot-lane task. Held alongside
    /// `snapshot_task`; both are `Some` together or both are `None`.
    snapshot_shutdown: Option<tokio::sync::watch::Sender<bool>>,

    /// Latest successful SQLite snapshot captured by the optional snapshot
    /// lane. Row-stream materialization reads this to create `BACKED_BY`
    /// evidence links from row materials to the strongest substrate material.
    sqlite_snapshot_evidence: LatestSqliteSnapshotEvidence,

    /// NATS control listener that mirrors private-mode broadcasts into the
    /// configured local state directory for this adapter-backed source.
    private_mode_control_task: Option<tokio::task::JoinHandle<()>>,

    /// Sleep duration between continuous-mode adapter drains.
    poll_interval: Duration,

    _phantom: PhantomData<()>,
}

struct MaterializedAdapterRecord {
    record: SourceRecord,
    material_id: Id<SourceMaterial>,
    anchor_byte: i64,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    /// BLAKE3 hash of the record payload bytes (#1447). `None` for adapter
    /// paths where the byte range cannot be cheaply isolated, e.g. directory-
    /// entry anchors that carry only a path. Derived events stay `None`
    /// regardless.
    anchor_payload_hash: Option<[u8; 32]>,
}

impl<A, P> AdapterBackedSource<A, P>
where
    A: InputShapeAdapter + Default + InputShapeAdapterExt,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    /// Create a new adapter-backed source for the given source id.
    ///
    /// Called by `register_source!` via `Default::default()` and the
    /// `new` constructor. Callers should normally use the macro, not this
    /// constructor directly.
    #[must_use]
    pub fn new(source_id: &'static str) -> Self {
        Self {
            source_id,
            adapter: A::default(),
            parser: P::default(),
            config: None,
            runtime_config: None,
            binding_config: BindingConfig::default(),
            runtime: None,
            stream_acquirer: None,
            acquisition_manager: None,
            rotation_policy: RotationPolicy::default(),
            snapshot_task: None,
            snapshot_shutdown: None,
            sqlite_snapshot_evidence: LatestSqliteSnapshotEvidence::default(),
            private_mode_control_task: None,
            poll_interval: Duration::from_secs(30),
            _phantom: PhantomData,
        }
    }

    /// Create a new adapter-backed source with a custom rotation policy.
    ///
    /// Useful in tests to trigger rotation without writing 100 MB of data.
    #[must_use]
    pub fn with_rotation_policy(mut self, policy: RotationPolicy) -> Self {
        self.rotation_policy = policy;
        self
    }

    /// Force-rotate the current source material immediately.
    ///
    /// Intended for tests that need to verify rotation semantics without
    /// waiting for size/time thresholds. Finalizes the current in-progress
    /// material so the next drain starts a fresh one.
    ///
    /// No-op if no material has been opened yet (stream acquirer is `None`).
    pub async fn rotate_for_test(&mut self) -> RuntimeResult<()> {
        if let Some(acquirer) = self.stream_acquirer.as_mut() {
            acquirer.finalize("forced-rotation-for-test").await?;
        }
        Ok(())
    }

    /// Return the material ID of the currently active in-flight material, if any.
    ///
    /// Used in tests to verify that multiple drain cycles share the same material.
    #[must_use]
    pub fn current_material_id(&self) -> Option<Uuid> {
        self.stream_acquirer
            .as_ref()
            .and_then(super::super::acquisition_manager::AppendStreamAcquirer::current_material_id)
    }

    /// Observe adapter-level input shape before draining records.
    ///
    /// This is advisory: shape observation should surface drift, but a
    /// fingerprinting failure must not prevent ingestion from reading the
    /// underlying source.
    fn observe_input_fingerprint(
        &self,
        config: &A::Config,
        state: &mut AdapterModuleState<A::Cursor>,
        source_id: &sinex_primitives::parser::SourceId,
    ) {
        match self.adapter.input_fingerprint(config) {
            Ok(Some(current)) => {
                if let Some(previous) = &state.last_input_fingerprint
                    && let Some(mut drift) =
                        SourceRecordFingerprint::diff(source_id.clone(), previous, &current)
                {
                    drift.required_input_keys = self.parser.required_input_keys();
                    warn!(
                        source = self.source_id,
                        format = drift.format.as_str(),
                        previous_hash = drift.previous_hash.as_str(),
                        current_hash = drift.current_hash.as_str(),
                        added_keys = ?&drift.added_keys,
                        removed_keys = ?&drift.removed_keys,
                        required_input_keys = ?&drift.required_input_keys,
                        type_changes = ?&drift.type_changes,
                        "input shape drift detected"
                    );
                    state.record_input_drift(drift);
                }
                state.last_input_fingerprint = Some(current);
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    source = self.source_id,
                    adapter_kind = A::KIND.as_str(),
                    error = %e,
                    "input fingerprint failed; continuing without shape drift check"
                );
            }
        }
    }

    /// Refresh runtime-derived binding flags before an acquisition attempt.
    ///
    /// Static flags remain stable, but fields derived from the private-mode
    /// state file must be re-read so live source poll loops can react
    /// to operator toggles without waiting for process restart.
    fn refresh_binding_config(&mut self) -> RuntimeResult<()> {
        let Some(config) = &self.runtime_config else {
            return Ok(());
        };
        self.binding_config = config.to_binding_config_for_source(self.source_id)?;
        Ok(())
    }

    fn stop_private_mode_control_listener(&mut self) {
        if let Some(task) = self.private_mode_control_task.take() {
            task.abort();
        }
    }

    /// Ensure the `AppendStreamAcquirer` is initialized, creating it from the
    /// acquisition manager if necessary.
    ///
    /// Returns a mutable reference to the acquirer, or an error if the source
    /// has not been initialized yet.
    #[allow(clippy::expect_used)]
    fn ensure_stream_acquirer(&mut self) -> RuntimeResult<&mut AppendStreamAcquirer> {
        if self.stream_acquirer.is_none() {
            let manager = self.acquisition_manager.as_ref().ok_or_else(|| {
                crate::runtime::SinexError::lifecycle(
                    "AdapterBackedSource: acquisition_manager not set (initialize not called)",
                )
            })?;
            self.stream_acquirer = Some(AppendStreamAcquirer::new(Arc::clone(manager)));
        }
        // SAFETY: we just set it above if it was None
        Ok(self
            .stream_acquirer
            .as_mut()
            .expect("stream_acquirer initialized above"))
    }

    async fn materialize_adapter_record(
        &mut self,
        record: SourceRecord,
    ) -> RuntimeResult<MaterializedAdapterRecord> {
        if record.material_id.to_uuid() != Uuid::nil() {
            let (anchor_byte, offset_start, offset_end) =
                anchor_offsets_for_materialized_record(&record.anchor);
            // Pre-materialized adapter records (file-drop content staging,
            // SQLite row snapshots) already own their anchor; hash the record
            // payload bytes as the integrity witness. Records with non-byte
            // anchors (directory entries, git objects) carry only a logical
            // identifier in `bytes`, so we still hash whatever the adapter
            // chose to emit — verify just re-runs the same hashing on the
            // same byte range and confirms consistency, not authenticity.
            let anchor_payload_hash = blake3::hash(record.bytes.as_slice()).as_bytes().to_owned();
            return Ok(MaterializedAdapterRecord {
                material_id: record.material_id,
                record,
                anchor_byte,
                offset_start,
                offset_end,
                anchor_payload_hash: Some(anchor_payload_hash),
            });
        }

        // Append record bytes to the long-lived stream material. The acquirer
        // returns a source-material anchor that precisely locates this record
        // within the growing material blob. The acquirer handles size/time-based
        // rotation transparently, so raw.source_material_registry grows at
        // O(rotation_count) across drain cycles rather than O(poll_count).
        let record_bytes = record.bytes.as_slice();
        let anchor_payload_hash = blake3::hash(record_bytes).as_bytes().to_owned();
        let source_id_for_anchor = self.source_id;
        let anchor = self
            .ensure_stream_acquirer()?
            .append_with_anchor(record_bytes, source_id_for_anchor)
            .await
            .map_err(|error| {
                crate::runtime::SinexError::processing("append_with_anchor failed")
                    .with_context("source_id", self.source_id)
                    .with_std_error(&error)
            })?;

        Ok(MaterializedAdapterRecord {
            record,
            material_id: Id::<SourceMaterial>::from_uuid(anchor.material_id),
            anchor_byte: anchor.offset_start,
            offset_start: Some(anchor.offset_start),
            offset_end: Some(anchor.offset_end),
            anchor_payload_hash: Some(anchor_payload_hash),
        })
    }

    async fn link_latest_sqlite_snapshot_backing_material(
        &self,
        row_material_id: Id<SourceMaterial>,
    ) {
        let Some(snapshot) = self.sqlite_snapshot_evidence.latest() else {
            return;
        };

        let row_material_uuid = row_material_id.to_uuid();
        let snapshot_material_uuid = snapshot.material_id.to_uuid();
        if row_material_uuid == snapshot_material_uuid {
            return;
        }

        #[cfg(feature = "db")]
        {
            let Some(pool) = self
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.handles().db_pool().map(std::clone::Clone::clone))
            else {
                debug!(
                    source = self.source_id,
                    row_material_id = %row_material_uuid,
                    snapshot_material_id = %snapshot_material_uuid,
                    "SQLite snapshot evidence link skipped; runtime has no DB pool"
                );
                return;
            };

            let metadata = json!({
                "evidence_role": "sqlite_snapshot",
                "source_identifier": snapshot.source_identifier,
                "source_path": snapshot.source_path,
                "content_hash_blake3": snapshot.content_hash_blake3,
                "size_bytes": snapshot.size_bytes,
            });

            match pool
                .source_materials()
                .link_backing_material(row_material_uuid, snapshot_material_uuid, metadata)
                .await
            {
                Ok(_) => debug!(
                    source = self.source_id,
                    row_material_id = %row_material_uuid,
                    snapshot_material_id = %snapshot_material_uuid,
                    "linked SQLite row material to snapshot evidence"
                ),
                Err(error) => warn!(
                    source = self.source_id,
                    row_material_id = %row_material_uuid,
                    snapshot_material_id = %snapshot.material_id,
                    error = %error,
                    "failed to link SQLite row material to snapshot evidence"
                ),
            }
        }

        #[cfg(not(feature = "db"))]
        {
            let _ = row_material_id;
        }
    }

    /// Open the adapter, drain all records through the parser, emit each
    /// `ParsedEventIntent` via the runtime, and append record bytes to the
    /// long-lived stream material.
    ///
    /// The stream acquirer is reused across drain calls; it rotates the
    /// underlying source material automatically at the configured size/time
    /// thresholds. This ensures `raw.source_material_registry` grows at
    /// `O(rotation_count)` rather than `O(poll_count)`.
    /// On adapter-open failure the material is cancelled before returning the
    /// error.
    ///
    /// Returns total events emitted.
    async fn drain_adapter(
        &mut self,
        cursor: Option<A::Cursor>,
        state: &mut AdapterModuleState<A::Cursor>,
    ) -> RuntimeResult<u64> {
        self.refresh_binding_config()?;
        if self.binding_config.is_truthy("private_mode_active") {
            info!(
                source = self.source_id,
                adapter_kind = A::KIND.as_str(),
                "private mode active for source; skipping adapter acquisition"
            );
            return Ok(0);
        }

        let config = self.config.as_ref().ok_or_else(|| {
            crate::runtime::SinexError::lifecycle(
                "AdapterBackedSource: adapter config not set (initialize not called)",
            )
        })?;

        // Clone the event emitter out of runtime so we don't hold an
        // immutable borrow of self across the later mutable
        // `ensure_stream_acquirer()` call (Slice A introduced the &mut self
        // path). EventEmitter is Clone (cheap — it's an Arc-shaped handle).
        let event_emitter = self
            .runtime
            .as_ref()
            .ok_or_else(|| {
                crate::runtime::SinexError::lifecycle(
                    "AdapterBackedSource: runtime not available (initialize not called)",
                )
            })?
            .event_emitter()
            .clone();

        let source_id = sinex_primitives::parser::SourceId::new(self.source_id).map_err(|e| {
            crate::runtime::SinexError::validation("invalid source_id in AdapterBackedSource")
                .with_std_error(&e)
        })?;

        self.observe_input_fingerprint(config, state, &source_id);

        let operation_id = Uuid::now_v7();
        let job_id = Uuid::now_v7();
        let host = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown".to_string());

        // We pass a placeholder material_id to adapter::open() since the actual
        // material_id is determined lazily by the stream acquirer when records
        // arrive. The placeholder is never used in production events — each
        // record's real anchor is returned by append_with_anchor() below.
        let placeholder_material_id = Id::<SourceMaterial>::from_uuid(Uuid::nil());

        // Open the adapter stream. Runtime acquisition is offered so
        // content-bearing adapters can stage their own material and return
        // already-anchored records; ordinary row/log adapters inherit the
        // default open_with_acquisition() implementation and continue through
        // the append-stream materialization path.
        let mut stream = match self
            .adapter
            .open_with_acquisition(
                placeholder_material_id,
                config,
                cursor,
                self.acquisition_manager.clone(),
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return Err(
                    crate::runtime::SinexError::processing("adapter open failed")
                        .with_context("source_id", self.source_id)
                        .with_context("adapter_kind", A::KIND.as_str())
                        .with_context("error", e.to_string()),
                );
            }
        };

        let mut emitted: u64 = 0;

        while let Some(record_result) = stream.next().await {
            self.refresh_binding_config()?;
            if self.binding_config.is_truthy("private_mode_active") {
                info!(
                    source = self.source_id,
                    adapter_kind = A::KIND.as_str(),
                    emitted,
                    "private mode became active during adapter drain; stopping acquisition"
                );
                return Ok(emitted);
            }

            let record = match record_result {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "Adapter stream error — skipping record"
                    );
                    continue;
                }
            };

            // Compute the next cursor before parsing, but only commit it after
            // the source bytes have been anchored in material storage. Parser
            // failures may still advance the cursor because the record was
            // observed and preserved; append failures must remain retryable.
            let next_cursor = match self.adapter.cursor_after(&record) {
                Ok(c) => Some(c),
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "cursor_after failed — checkpoint may regress"
                    );
                    None
                }
            };

            let materialized = match self.materialize_adapter_record(record).await {
                Ok(materialized) => materialized,
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "record materialization failed — skipping record so material provenance can be retried"
                    );
                    continue;
                }
            };

            let material_id = materialized.material_id;
            self.link_latest_sqlite_snapshot_backing_material(material_id)
                .await;
            if let Some(cursor) = next_cursor {
                state.cursor = Some(cursor);
            }

            let ctx = ParserContext {
                source_id: source_id.clone(),
                source_material_id: material_id,
                record_anchor: materialized.record.anchor.clone(),
                operation_id,
                job_id,
                host: host.clone(),
                acquisition_time: Timestamp::now(),
            };

            let intents = match self
                .parser
                .parse_record_with_binding(materialized.record, &ctx, &self.binding_config)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "parse_record error — skipping"
                    );
                    continue;
                }
            };

            let anchor_payload_hash = materialized.anchor_payload_hash;
            for intent in intents {
                // Use the materialization anchor so events reference their real
                // material location, whether the record came from the default
                // append stream or from an adapter-staged content material.
                match intent_to_event_with_anchor(
                    intent,
                    material_id,
                    materialized.anchor_byte,
                    materialized.offset_start,
                    materialized.offset_end,
                    anchor_payload_hash,
                ) {
                    Ok(event) => {
                        if let Err(e) = event_emitter.emit(event).await {
                            warn!(
                                source = self.source_id,
                                error = %e,
                                "emit failed — event dropped"
                            );
                        } else {
                            emitted += 1;
                        }
                    }
                    Err(e) => {
                        warn!(
                            source = self.source_id,
                            error = %e,
                            "intent_to_event_with_anchor conversion failed — skipping"
                        );
                    }
                }
            }
        }

        // The stream material is NOT finalized here — it persists across drain
        // cycles. Finalization happens when run_continuous exits (shutdown signal)
        // or when the source is dropped.

        state.total_events_emitted += emitted;
        debug!(
            source = self.source_id,
            emitted,
            total = state.total_events_emitted,
            "drain_adapter complete"
        );
        Ok(emitted)
    }
}

impl<A, P> Drop for AdapterBackedSource<A, P>
where
    A: InputShapeAdapter + Default + InputShapeAdapterExt,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        // Best-effort: signal the snapshot lane to exit and abort if still
        // running. Drop runs on synchronous teardown paths (panic, scope
        // exit) so we cannot await; aborting is the only safe option.
        if let Some(tx) = self.snapshot_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = self.snapshot_task.take() {
            task.abort();
        }
        self.stop_private_mode_control_listener();
    }
}

impl<A, P> Default for AdapterBackedSource<A, P>
where
    A: InputShapeAdapter + Default + InputShapeAdapterExt,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        // Default::default() is required by SourceDriverRuntime<I>.
        // The source_id is a sentinel that the macro overrides via `new`.
        Self::new("__unset__")
    }
}

// =============================================================================
// SourceDriver impl
// =============================================================================

impl<A, P> SourceDriver for AdapterBackedSource<A, P>
where
    A: InputShapeAdapter + Default + Send + Sync + 'static + InputShapeAdapterExt,
    P: MaterialParser + Default + Send + Sync + 'static,
    A::Config: Clone + Serialize + DeserializeOwned + Send + Sync,
    A::Cursor: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    type Config = AdapterSourceConfig;
    type State = AdapterModuleState<A::Cursor>;

    fn name(&self) -> &str {
        self.source_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            supports_snapshot: true,
            supports_historical: true,
            // Continuous mode is poll-based for adapters that don't stream.
            supports_continuous: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
            manages_own_checkpoints: false,
        }
    }

    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &RuntimeContext,
        _state: &mut Self::State,
    ) -> RuntimeResult<()> {
        // Build the AcquisitionManager from the runtime's NATS handles.
        let acq = runtime
            .acquisition_manager(
                crate::runtime::acquisition_manager::RotationPolicy::default(),
                self.source_id,
            )
            .map_err(|e| {
                crate::runtime::SinexError::lifecycle(
                    "AdapterBackedSource: failed to build AcquisitionManager",
                )
                .with_context("source_id", self.source_id)
                .with_std_error(&e)
            })?;

        self.acquisition_manager = Some(Arc::new(acq));
        self.binding_config = config.to_binding_config_for_source(self.source_id)?;
        self.poll_interval = config.continuous_poll_interval()?;
        self.runtime_config = Some(config.clone());
        #[cfg(feature = "messaging")]
        if let Some(state_dir) = config.private_mode_state_dir.clone()
            && let Some(nats_client) = runtime.nats_client()
        {
            self.private_mode_control_task = Some(spawn_private_mode_control_listener(
                nats_client,
                state_dir,
                self.source_id,
            ));
        }

        // Merge user-supplied JSON over the parser's baseline. The parser
        // declares mandatory adapter fields (parser-specific SQL query,
        // static D-Bus bus name, ChainedAdapter primary leg) via
        // `MaterialParser::baseline_adapter_config`; the user's
        // `--runtime-config` JSON overlays it (user keys win on conflict).
        let adapter_json = merge_json_over(P::baseline_adapter_config(), config.adapter);
        let adapter_config: A::Config = serde_json::from_value(adapter_json).map_err(|e| {
            crate::runtime::SinexError::configuration(
                "AdapterBackedSource: failed to deserialize adapter config",
            )
            .with_context("source_id", self.source_id)
            .with_std_error(&e)
        })?;
        // Opt-in parallel snapshot lane.  The adapter declares whether it
        // wants one by returning `Some(spec)` from `snapshot_lane`; we spawn
        // an independent tokio task that captures the substrate on its own
        // timer.  Per-record drain (above) is untouched.
        if let Some(spec) = self.adapter.snapshot_lane(self.source_id, &adapter_config) {
            #[allow(clippy::expect_used)]
            let manager = Arc::clone(
                self.acquisition_manager
                    .as_ref()
                    .expect("acquisition_manager set above"),
            );
            let (tx, rx) = tokio::sync::watch::channel(false);
            let lane = SqliteSnapshotLane::new(spec, manager)
                .with_latest_evidence(self.sqlite_snapshot_evidence.clone());
            let unit_id = self.source_id;
            let handle = tokio::spawn(async move {
                let result = lane.run(rx).await;
                if let Err(ref e) = result {
                    warn!(
                        source = unit_id,
                        error = %e,
                        "snapshot lane exited with error",
                    );
                }
                result
            });
            self.snapshot_task = Some(handle);
            self.snapshot_shutdown = Some(tx);
        }

        self.config = Some(adapter_config);
        self.runtime = Some(runtime.clone());

        info!(
            source = self.source_id,
            adapter_kind = A::KIND.as_str(),
            snapshot_lane = self.snapshot_task.is_some(),
            "AdapterBackedSource initialized"
        );
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        let start = Instant::now();
        // Snapshot: drain from cursor (resume after last known position).
        let cursor = state.cursor.clone();
        let emitted = self.drain_adapter(cursor, state).await?;
        let checkpoint = cursor_to_checkpoint(state);

        Ok(ScanReport {
            events_processed: emitted,
            duration: start.elapsed(),
            final_checkpoint: checkpoint,
            time_range: None,
            runtime_stats: HashMap::from([("emitted".to_string(), emitted)]),
            successful_targets: vec![self.source_id.to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn scan_historical(
        &mut self,
        state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        let start = Instant::now();
        // Historical: re-open from persisted cursor (may be behind `from` if
        // the source was offline). The adapter's cursor is the authoritative
        // resume position.
        let cursor = state.cursor.clone();
        let emitted = self.drain_adapter(cursor, state).await?;
        let checkpoint = cursor_to_checkpoint(state);

        Ok(ScanReport {
            events_processed: emitted,
            duration: start.elapsed(),
            final_checkpoint: checkpoint,
            time_range: None,
            runtime_stats: HashMap::from([("emitted".to_string(), emitted)]),
            successful_targets: vec![self.source_id.to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        _start: ContinuousStart,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> RuntimeResult<ScanReport> {
        let wall_start = Instant::now();
        let mut total_emitted: u64 = 0;

        let poll_interval = self.poll_interval;

        info!(
            source = self.source_id,
            poll_interval_s = poll_interval.as_secs(),
            "AdapterBackedSource entering continuous poll loop"
        );

        loop {
            // Check for shutdown before polling.
            if *shutdown_rx.borrow() {
                info!(
                    source = self.source_id,
                    "Drain signal received; exiting continuous loop"
                );
                break;
            }

            let cursor = state.cursor.clone();
            match self.drain_adapter(cursor, state).await {
                Ok(n) => total_emitted += n,
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "drain_adapter error in continuous mode — retrying after interval"
                    );
                }
            }

            // Wait for the poll interval or a shutdown signal.
            tokio::select! {
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        info!(source = self.source_id, "Drain signal received; exiting continuous loop");
                        break;
                    }
                }
                () = tokio::time::sleep(poll_interval) => {}
            }
        }

        // Finalize the in-flight stream material on clean shutdown so event_engine
        // receives the END frame and commits the row count before the process
        // exits.  Best-effort: a failure here only affects the current open
        // material; already-finalized materials and persisted events are safe.
        if let Some(acquirer) = self.stream_acquirer.as_mut()
            && let Err(e) = acquirer.finalize("continuous-mode-shutdown").await
        {
            warn!(
                source = self.source_id,
                error = %e,
                "Failed to finalize stream material on shutdown — in-flight material may be incomplete"
            );
        }

        // Signal the snapshot lane (if any) to exit and wait briefly for it.
        if let Some(tx) = self.snapshot_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = self.snapshot_task.take() {
            // Bounded wait so a misbehaving snapshot capture cannot block
            // shutdown indefinitely.  The lane finalises its own in-flight
            // material on shutdown; if the join times out we abort.
            match tokio::time::timeout(Duration::from_secs(5), task).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => warn!(
                    source = self.source_id,
                    error = %e,
                    "snapshot lane task returned error on shutdown",
                ),
                Err(_) => warn!(
                    source = self.source_id,
                    "snapshot lane did not exit within timeout; aborting",
                ),
            }
        }
        self.stop_private_mode_control_listener();

        let checkpoint = cursor_to_checkpoint(state);
        Ok(ScanReport {
            events_processed: total_emitted,
            duration: wall_start.elapsed(),
            final_checkpoint: checkpoint,
            time_range: None,
            runtime_stats: HashMap::from([("emitted".to_string(), total_emitted)]),
            successful_targets: vec![self.source_id.to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn shutdown(&mut self, _state: &Self::State) -> RuntimeResult<()> {
        self.stop_private_mode_control_listener();
        if let Some(acquirer) = self.stream_acquirer.as_mut() {
            acquirer.finalize("adapter-source-shutdown").await?;
        }
        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

#[derive(Debug, Deserialize)]
struct PrivateModeControlUpdate {
    state: RuntimePrivateModeState,
}

#[cfg(feature = "messaging")]
fn spawn_private_mode_control_listener(
    client: async_nats::Client,
    state_dir: PathBuf,
    source_id: &'static str,
) -> tokio::task::JoinHandle<()> {
    let subject =
        sinex_primitives::environment::environment().nats_subject(PRIVATE_MODE_CONTROL_SUBJECT);

    tokio::spawn(async move {
        let mut subscription = match client.subscribe(subject.clone()).await {
            Ok(subscription) => subscription,
            Err(error) => {
                warn!(
                    source = source_id,
                    subject = %subject,
                    error = %error,
                    "failed to subscribe to private-mode control subject"
                );
                return;
            }
        };

        info!(
            source = source_id,
            subject = %subject,
            state_dir = %state_dir.display(),
            "private-mode control listener started"
        );

        while let Some(message) = subscription.next().await {
            match serde_json::from_slice::<PrivateModeControlUpdate>(&message.payload) {
                Ok(update) => {
                    if let Err(error) = save_private_mode_state(&state_dir, &update.state) {
                        warn!(
                            source = source_id,
                            subject = %subject,
                            error = %error,
                            "failed to persist private-mode control update"
                        );
                    } else {
                        debug!(
                            source = source_id,
                            subject = %subject,
                            enabled = update.state.enabled,
                            "persisted private-mode control update"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        source = source_id,
                        subject = %subject,
                        error = %error,
                        "failed to parse private-mode control update"
                    );
                }
            }
        }

        warn!(
            source = source_id,
            subject = %subject,
            "private-mode control subscription closed"
        );
    })
}

/// Convert a `ParsedEventIntent` to an `Event<JsonValue>` ready for emission.
///
/// The anchor from the intent maps to the material `anchor_byte`. For
/// anchors that carry a natural integer offset (`ByteRange::start`,
/// `SqliteRow::rowid`, `Line::byte_start`) we use that. All others map to 0.
/// Merge `over` JSON value on top of `base`, recursively for objects.
///
/// Object keys: if both sides have the key and both values are objects,
/// merge recursively. Otherwise `over` wins. Non-object values: `over`
/// wins unconditionally. Used to layer user-supplied runtime config over
/// the parser-declared baseline (`MaterialParser::baseline_adapter_config`).
fn merge_json_over(base: JsonValue, over: JsonValue) -> JsonValue {
    match (base, over) {
        (JsonValue::Object(mut base_map), JsonValue::Object(over_map)) => {
            for (k, v) in over_map {
                let merged = match base_map.remove(&k) {
                    Some(existing) => merge_json_over(existing, v),
                    None => v,
                };
                base_map.insert(k, merged);
            }
            JsonValue::Object(base_map)
        }
        (_, over) => over,
    }
}

fn anchor_offsets_for_materialized_record(
    anchor: &MaterialAnchor,
) -> (i64, Option<i64>, Option<i64>) {
    match anchor {
        MaterialAnchor::ByteRange { start, len } => {
            let start = (*start).min(i64::MAX as u64) as i64;
            let len = (*len).min(i64::MAX as u64) as i64;
            let end = start.saturating_add(len);
            (start, Some(start), Some(end))
        }
        MaterialAnchor::Line { byte_start, .. } => {
            let start = (*byte_start).min(i64::MAX as u64) as i64;
            (start, Some(start), None)
        }
        MaterialAnchor::StreamFrame {
            material_offset, ..
        } => {
            let start = (*material_offset).min(i64::MAX as u64) as i64;
            (start, Some(start), None)
        }
        MaterialAnchor::SqliteRow { rowid, .. } => (*rowid, None, None),
        MaterialAnchor::DirectoryEntry { .. } | MaterialAnchor::GitObject { .. } => (0, None, None),
    }
}

/// Convert a `ParsedEventIntent` to an `Event<JsonValue>`, overriding `anchor_byte`
/// with the stream-acquirer byte offset rather than the anchor embedded in the intent.
///
/// When events are emitted from an adapter-materialized source record, the
/// intent anchor may reflect a logical position within the source record. The
/// materialization step owns the real material anchor, either from
/// `AppendStreamAcquirer` or from an adapter-supplied staged material.
fn intent_to_event_with_anchor(
    intent: ParsedEventIntent,
    material_id: Id<SourceMaterial>,
    anchor_byte_override: i64,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    anchor_payload_hash: Option<[u8; 32]>,
) -> Result<Event<JsonValue>, String> {
    let builder: EventBuilder<JsonValue, NoProvenance> =
        EventBuilder::new_internal(intent.event_source, intent.event_type, intent.payload);

    // #1570 Prong B two-sided join: when the parser resolved the timing
    // evidence (intrinsic field, mtime, user-declared) it owns ts_orig and its
    // quality rung. Otherwise (wrapper-ledger or staged-at fallback) it leaves
    // ts_orig unresolved so the admission/persistence stage derives it from the
    // source-material timing tier.
    let mut builder = builder.from_material(material_id, anchor_byte_override);
    if let Some(quality) = intent.timing.resolved_quality() {
        builder = builder.at_time_with_quality(intent.ts_orig, quality);
    }
    if let (Some(start), Some(end)) = (offset_start, offset_end) {
        builder = builder
            .with_offset_start(start)
            .map_err(|e| format!("EventBuilder::with_offset_start failed: {e}"))?
            .with_offset_end(end)
            .map_err(|e| format!("EventBuilder::with_offset_end failed: {e}"))?;
    }
    if let Some(hash) = anchor_payload_hash {
        builder = builder.with_anchor_payload_hash(hash);
    }

    let mut built = builder
        .build()
        .map_err(|e| format!("EventBuilder::build failed: {e}"))?;

    // #1570 Prong C: carry the parser's occurrence (natural) key onto the event
    // as `equivalence_key` so it lands in a queryable column. This feeds the
    // curation duplicate-detection workbench (#1448) and also drives admission
    // suppression (#1637): the event_engine will suppress a new event if a live
    // row with the same equivalence_key already exists in core.events.
    built.equivalence_key =
        sinex_primitives::parser::maybe_occurrence_key_string(intent.occurrence_key.as_ref());

    Ok(built)
}

/// Produce a `Checkpoint` from the current module state.
///
/// Uses `External` with the serialized cursor, falling back to `None` if no
/// cursor has been advanced yet.
fn cursor_to_checkpoint<C>(state: &AdapterModuleState<C>) -> Checkpoint
where
    C: Clone + Serialize + DeserializeOwned,
{
    match &state.cursor {
        Some(c) => {
            let pos = serde_json::to_value(c).unwrap_or(JsonValue::Null);
            Checkpoint::External {
                position: pos,
                description: "adapter cursor".to_string(),
            }
        }
        None => Checkpoint::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::checkpoint::CheckpointManager;
    use crate::runtime::parser::{InputShapeKind, ParserError, ParserResult, SourceRecord};
    use crate::runtime::stream::{EventEmitter, RuntimeHandles, ServiceInfo};
    use crate::runtime::{EventTransport, NatsPublisher};
    use async_trait::async_trait;
    use camino::Utf8PathBuf;
    use futures::stream::{self, BoxStream};
    use sinex_db::DbPoolExt;
    use sinex_db::repositories::source_material_relation_types;
    use sinex_primitives::domain::{EventSource, EventType};
    use sinex_primitives::events::Event;
    use sinex_primitives::parser::{MaterialAnchor, ParserId, ParserManifest, SourceId};
    use sinex_primitives::privacy::ProcessingContext;
    use sinex_primitives::privacy::{
        RuntimePrivateModeState, load_private_mode_state, private_mode_state_path,
        save_private_mode_state,
    };
    use sinex_primitives::rpc::sources::{CaveatSeverity, caveat_codes};
    use sinex_primitives::{HostName, JsonValue, SinexError};
    use std::collections::HashMap;
    use tokio::sync::mpsc;
    use xtask::sandbox::prelude::{TestContext, TestResult, WaitHelpers, sinex_test};

    #[derive(Default)]
    struct TestAdapter;

    #[async_trait]
    impl InputShapeAdapter for TestAdapter {
        type Config = ();
        type Cursor = u64;

        const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

        async fn open(
            &self,
            _material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            _cursor: Option<Self::Cursor>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            Ok(Box::pin(stream::empty()))
        }

        fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
            Ok(0)
        }
    }

    impl InputShapeAdapterExt for TestAdapter {}

    #[derive(Default)]
    struct FingerprintAdapter {
        fingerprint: Option<SourceRecordFingerprint>,
    }

    #[async_trait]
    impl InputShapeAdapter for FingerprintAdapter {
        type Config = ();
        type Cursor = u64;

        const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

        async fn open(
            &self,
            _material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            _cursor: Option<Self::Cursor>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            Ok(Box::pin(stream::empty()))
        }

        fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
            Ok(0)
        }

        fn input_fingerprint(
            &self,
            _config: &Self::Config,
        ) -> ParserResult<Option<SourceRecordFingerprint>> {
            Ok(self.fingerprint.clone())
        }
    }

    impl InputShapeAdapterExt for FingerprintAdapter {}

    #[derive(Default)]
    struct TestParser;

    #[async_trait]
    impl MaterialParser for TestParser {
        type Config = ();

        fn manifest(&self) -> ParserManifest {
            ParserManifest {
                parser_id: ParserId::from_static("test-parser"),
                parser_version: "1.0.0".to_string(),
                accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
                source_id: SourceId::from_static("desktop.clipboard"),
                declared_event_types: vec![(
                    EventSource::from_static("test"),
                    EventType::from_static("test.event"),
                )],
                privacy_contexts: vec![ProcessingContext::Metadata],
                sensitivity_hints: Vec::new(),
                description: String::new(),
            }
        }

        fn required_input_keys(&self) -> Vec<String> {
            vec!["/message".to_string()]
        }

        async fn parse_record(
            &mut self,
            _record: SourceRecord,
            _ctx: &ParserContext,
        ) -> ParserResult<Vec<ParsedEventIntent>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct OversizedRecordAdapter;

    #[async_trait]
    impl InputShapeAdapter for OversizedRecordAdapter {
        type Config = ();
        type Cursor = u64;

        const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

        async fn open(
            &self,
            material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            _cursor: Option<Self::Cursor>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            let oversized = vec![b'x'; 512 * 1024 + 1];
            let record = SourceRecord {
                material_id,
                anchor: MaterialAnchor::ByteRange {
                    start: 0,
                    len: oversized.len() as u64,
                },
                bytes: oversized,
                logical_path: None,
                source_ts_hint: None,
                metadata: JsonValue::Null,
            };
            Ok(Box::pin(stream::iter(vec![Ok(record)])))
        }

        fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
            Ok(1)
        }
    }

    impl InputShapeAdapterExt for OversizedRecordAdapter {}

    #[derive(Default)]
    struct AlreadyMaterializedRecordAdapter;

    #[async_trait]
    impl InputShapeAdapter for AlreadyMaterializedRecordAdapter {
        type Config = ();
        type Cursor = u64;

        const KIND: InputShapeKind = InputShapeKind::AppendOnlyFile;

        async fn open(
            &self,
            _material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            _cursor: Option<Self::Cursor>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            Err(ParserError::Adapter(
                "open_with_acquisition should be used for materialized records".to_string(),
            ))
        }

        fn cursor_after(&self, _record: &SourceRecord) -> ParserResult<Self::Cursor> {
            Ok(1)
        }
    }

    #[async_trait]
    impl InputShapeAdapterExt for AlreadyMaterializedRecordAdapter {
        async fn open_with_acquisition(
            &self,
            _material_id: Id<SourceMaterial>,
            _config: &Self::Config,
            _cursor: Option<Self::Cursor>,
            acquisition: Option<Arc<AcquisitionManager>>,
        ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
            if acquisition.is_none() {
                return Err(ParserError::Adapter(
                    "adapter-backed source did not provide acquisition manager".to_string(),
                ));
            }
            let record = SourceRecord {
                material_id: Id::from_uuid(Uuid::from_u128(42)),
                anchor: MaterialAnchor::ByteRange { start: 17, len: 5 },
                bytes: b"hello".to_vec(),
                logical_path: Some(Utf8PathBuf::from("/tmp/materialized.txt")),
                source_ts_hint: None,
                metadata: JsonValue::Null,
            };
            Ok(Box::pin(stream::iter(vec![Ok(record)])))
        }
    }

    #[derive(Default)]
    struct EmittingParser;

    #[async_trait]
    impl MaterialParser for EmittingParser {
        type Config = ();

        fn manifest(&self) -> ParserManifest {
            ParserManifest {
                parser_id: ParserId::from_static("emitting-parser"),
                parser_version: "1.0.0".to_string(),
                accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
                source_id: SourceId::from_static("desktop.clipboard"),
                declared_event_types: vec![(
                    EventSource::from_static("test"),
                    EventType::from_static("test.event"),
                )],
                privacy_contexts: vec![ProcessingContext::Metadata],
                sensitivity_hints: Vec::new(),
                description: String::new(),
            }
        }

        async fn parse_record(
            &mut self,
            record: SourceRecord,
            ctx: &ParserContext,
        ) -> ParserResult<Vec<ParsedEventIntent>> {
            Ok(vec![
                ParsedEventIntent::builder()
                    .source_id(ctx.source_id.clone())
                    .parser_id(ParserId::from_static("emitting-parser"))
                    .parser_version("1.0.0")
                    .event_type(EventType::from_static("test.event"))
                    .event_source(EventSource::from_static("test"))
                    .payload(serde_json::json!({"parsed": true}))
                    .ts_orig(ctx.acquisition_time)
                    .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
                    .anchor(record.anchor)
                    .privacy_context(ProcessingContext::Metadata)
                    .build(),
            ])
        }
    }

    async fn make_adapter_runtime(
        ctx: &TestContext,
    ) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            "adapter-append-failure-test".to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = RuntimeHandles::new_edge(
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempfile::tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            SinexError::validation("temporary work dir should be UTF-8")
                .with_context("path", path.display().to_string())
        })?;
        Ok((
            RuntimeContext::new(
                ServiceInfo::new(
                    "adapter-append-failure-test".to_string(),
                    "adapter-append-failure-test".to_string(),
                    HostName::from_static("test-host"),
                    work_dir_path,
                    false,
                    format!("instance-{}", Uuid::now_v7().simple()),
                    env!("CARGO_PKG_VERSION").to_string(),
                    None,
                ),
                handles,
                HashMap::new(),
                work_dir_utf8,
            ),
            event_receiver,
        ))
    }

    async fn make_adapter_runtime_with_db(
        ctx: &TestContext,
    ) -> TestResult<(RuntimeContext, mpsc::Receiver<Event<JsonValue>>)> {
        let kv = ctx.checkpoint_kv().await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            "adapter-snapshot-link-test".to_string(),
            "test-group".to_string(),
            format!("test-consumer-{}", Uuid::now_v7().simple()),
        ));
        let (event_sender, event_receiver) = mpsc::channel::<Event<JsonValue>>(8);
        let emitter = EventEmitter::new(event_sender, false);
        let publisher = Arc::new(NatsPublisher::new(ctx.nats_client()));
        let handles = RuntimeHandles::new(
            ctx.pool().clone(),
            checkpoint_manager,
            emitter,
            EventTransport::Nats(publisher),
            None,
            None,
        );
        let work_dir = tempfile::tempdir()?;
        let work_dir_path = work_dir.keep();
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir_path.clone()).map_err(|path| {
            SinexError::validation("temporary work dir should be UTF-8")
                .with_context("path", path.display().to_string())
        })?;
        Ok((
            RuntimeContext::new(
                ServiceInfo::new(
                    "adapter-snapshot-link-test".to_string(),
                    "adapter-snapshot-link-test".to_string(),
                    HostName::from_static("test-host"),
                    work_dir_path,
                    false,
                    format!("instance-{}", Uuid::now_v7().simple()),
                    env!("CARGO_PKG_VERSION").to_string(),
                    None,
                ),
                handles,
                HashMap::new(),
                work_dir_utf8,
            ),
            event_receiver,
        ))
    }

    #[sinex_test]
    async fn adapter_source_config_derives_private_mode_binding_flag()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        save_private_mode_state(dir.path(), &state)?;
        let config = AdapterSourceConfig {
            private_mode_state_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let binding = config.to_binding_config_for_source("desktop.clipboard")?;

        assert!(binding.is_truthy("private_mode_active"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_config_validates_continuous_poll_interval()
    -> xtask::sandbox::TestResult<()> {
        let default_config = AdapterSourceConfig::default();
        assert_eq!(
            default_config.continuous_poll_interval()?,
            Duration::from_secs(30)
        );

        let custom_config = AdapterSourceConfig {
            continuous_poll_interval_secs: Some(5),
            ..Default::default()
        };
        assert_eq!(
            custom_config.continuous_poll_interval()?,
            Duration::from_secs(5)
        );

        let invalid_config = AdapterSourceConfig {
            continuous_poll_interval_secs: Some(0),
            ..Default::default()
        };
        let error = invalid_config
            .continuous_poll_interval()
            .expect_err("zero-second poll interval should fail configuration validation");
        assert!(format!("{error:#}").contains("continuous_poll_interval_secs"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_config_respects_private_mode_source_scope()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        save_private_mode_state(dir.path(), &state)?;
        let config = AdapterSourceConfig {
            private_mode_state_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let binding = config.to_binding_config_for_source("terminal.zsh-history")?;

        assert!(!binding.is_truthy("private_mode_active"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_config_ignores_expired_private_mode_state()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        )
        .with_expires_at(Timestamp::from_unix_timestamp(1));
        save_private_mode_state(dir.path(), &state)?;
        let config = AdapterSourceConfig {
            private_mode_state_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let binding = config.to_binding_config_for_source("desktop.clipboard")?;

        assert!(!binding.is_truthy("private_mode_active"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_config_fails_closed_when_private_mode_state_is_unavailable()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let path = private_mode_state_path(dir.path());
        let parent = path
            .parent()
            .ok_or_else(|| SinexError::validation("private-mode path must have parent"))?;
        tokio::fs::create_dir_all(parent).await?;
        tokio::fs::write(&path, b"{not-json").await?;
        let config = AdapterSourceConfig {
            private_mode_state_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let binding = config.to_binding_config_for_source("desktop.clipboard")?;

        assert!(binding.is_truthy("private_mode_active"));
        assert!(binding.is_truthy("private_mode_state_unavailable"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_config_fail_open_requires_explicit_low_sensitivity_choice()
    -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let path = private_mode_state_path(dir.path());
        let parent = path
            .parent()
            .ok_or_else(|| SinexError::validation("private-mode path must have parent"))?;
        tokio::fs::create_dir_all(parent).await?;
        tokio::fs::write(&path, b"{not-json").await?;
        let config = AdapterSourceConfig {
            private_mode_state_dir: Some(dir.path().to_path_buf()),
            private_mode_fail_closed: Some(false),
            ..Default::default()
        };

        let binding = config.to_binding_config_for_source("system.metrics")?;

        assert!(!binding.is_truthy("private_mode_active"));
        assert!(binding.is_truthy("private_mode_state_unavailable"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_backed_source_refreshes_private_mode_binding() -> xtask::sandbox::TestResult<()>
    {
        let dir = tempfile::tempdir()?;
        save_private_mode_state(dir.path(), &RuntimePrivateModeState::disabled())?;
        let mut source = AdapterBackedSource::<TestAdapter, TestParser>::new("desktop.clipboard");
        source.runtime_config = Some(AdapterSourceConfig {
            private_mode_state_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        });

        source.refresh_binding_config()?;
        assert!(!source.binding_config.is_truthy("private_mode_active"));

        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        save_private_mode_state(dir.path(), &state)?;

        source.refresh_binding_config()?;
        assert!(source.binding_config.is_truthy("private_mode_active"));
        Ok(())
    }

    #[sinex_test]
    async fn adapter_append_failure_does_not_emit_nil_material_event(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
        let mut source =
            AdapterBackedSource::<OversizedRecordAdapter, EmittingParser>::new("desktop.clipboard");
        let mut state = AdapterModuleState::default();

        source
            .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
            .await?;
        let emitted = source.drain_adapter(None, &mut state).await?;

        assert_eq!(emitted, 0);
        assert!(
            state.cursor.is_none(),
            "failed material append must not advance the adapter cursor"
        );
        assert!(
            matches!(
                event_receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "failed material append must not emit an event with degraded provenance"
        );
        Ok(())
    }

    #[sinex_test]
    async fn adapter_backed_source_preserves_already_materialized_record_provenance(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (runtime, mut event_receiver) = make_adapter_runtime(&ctx).await?;
        let mut source =
            AdapterBackedSource::<AlreadyMaterializedRecordAdapter, EmittingParser>::new(
                "desktop.clipboard",
            );
        let mut state = AdapterModuleState::default();

        source
            .initialize(AdapterSourceConfig::default(), &runtime, &mut state)
            .await?;
        let emitted = source.drain_adapter(None, &mut state).await?;
        let event = event_receiver
            .recv()
            .await
            .ok_or_else(|| SinexError::processing("expected emitted event"))?;

        assert_eq!(emitted, 1);
        assert_eq!(state.cursor, Some(1));
        assert_eq!(
            source.current_material_id(),
            None,
            "pre-materialized records must not open the append-stream materializer",
        );
        assert_eq!(event.get_anchor_byte(), Some(17));
        match event.provenance() {
            sinex_primitives::events::Provenance::Material {
                id,
                offset_start,
                offset_end,
                ..
            } => {
                assert_eq!(id.to_uuid(), Uuid::from_u128(42));
                assert_eq!(*offset_start, Some(17));
                assert_eq!(*offset_end, Some(22));
            }
            other => panic!("expected material provenance, got {other:?}"),
        }
        Ok(())
    }

    #[cfg(feature = "messaging")]
    #[sinex_test]
    async fn adapter_private_mode_control_listener_persists_broadcast(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let dir = tempfile::tempdir()?;
        save_private_mode_state(dir.path(), &RuntimePrivateModeState::disabled())?;
        let handle = spawn_private_mode_control_listener(
            ctx.nats_client(),
            dir.path().to_path_buf(),
            "desktop.clipboard",
        );

        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        let subject =
            sinex_primitives::environment::environment().nats_subject(PRIVATE_MODE_CONTROL_SUBJECT);
        ctx.nats_client()
            .publish(
                subject,
                serde_json::to_vec(&serde_json::json!({
                    "action": "enable",
                    "timestamp": Timestamp::now(),
                    "state": state,
                }))?
                .into(),
            )
            .await?;
        ctx.nats_client().flush().await?;

        let state_dir = dir.path().to_path_buf();
        WaitHelpers::wait_for_condition(
            || {
                let state_dir = state_dir.clone();
                async move {
                    let state = load_private_mode_state(&state_dir)?;
                    Ok::<_, crate::runtime::SinexError>(state.enabled)
                }
            },
            10,
        )
        .await?;

        let loaded = load_private_mode_state(dir.path())?;
        assert!(loaded.enabled);
        assert_eq!(loaded.actor, "sinity");
        assert_eq!(loaded.affected_source_classes, vec!["desktop"]);
        handle.abort();
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_state_defaults_missing_input_fingerprint()
    -> xtask::sandbox::TestResult<()> {
        let value = serde_json::json!({
            "cursor": 7,
            "total_events_emitted": 12
        });

        let state: AdapterModuleState<u64> = serde_json::from_value(value)?;

        assert_eq!(state.cursor, Some(7));
        assert_eq!(state.total_events_emitted, 12);
        assert!(state.last_input_fingerprint.is_none());
        assert!(state.recent_input_drifts.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_state_records_bounded_input_drift_history()
    -> xtask::sandbox::TestResult<()> {
        let source_id = SourceId::from_static("desktop.clipboard");
        let mut source =
            AdapterBackedSource::<FingerprintAdapter, TestParser>::new("desktop.clipboard");
        let mut state = AdapterModuleState::<u64>::default();

        source.adapter.fingerprint = Some(SourceRecordFingerprint::from_json(
            &serde_json::json!({"count": 1}),
        ));
        source.observe_input_fingerprint(&(), &mut state, &source_id);
        assert!(state.recent_input_drifts.is_empty());

        source.adapter.fingerprint = Some(SourceRecordFingerprint::from_json(
            &serde_json::json!({"count": "1", "enabled": true}),
        ));
        source.observe_input_fingerprint(&(), &mut state, &source_id);

        assert_eq!(state.recent_input_drifts.len(), 1);
        let drift = &state.recent_input_drifts[0];
        assert_eq!(drift.source_id, source_id);
        assert_eq!(drift.added_keys, vec!["/enabled".to_string()]);
        assert_eq!(drift.required_input_keys, vec!["/message".to_string()]);
        assert_eq!(
            drift.type_changes,
            vec![(
                "/count".to_string(),
                "integer".to_string(),
                "string".to_string()
            )]
        );

        for idx in 0..(MAX_RECENT_INPUT_DRIFTS + 3) {
            let drift = SourceRecordFingerprint::diff(
                source_id.clone(),
                &SourceRecordFingerprint::from_json(&serde_json::json!({ "idx": idx })),
                &SourceRecordFingerprint::from_json(&serde_json::json!({ "idx": idx, "x": true })),
            )
            .ok_or_else(|| SinexError::validation("different fingerprints should produce drift"))?;
            state.record_input_drift(drift);
        }

        assert_eq!(state.recent_input_drifts.len(), MAX_RECENT_INPUT_DRIFTS);
        Ok(())
    }

    #[sinex_test]
    async fn adapter_source_state_summarizes_latest_input_drift_caveats()
    -> xtask::sandbox::TestResult<()> {
        let source_id = SourceId::from_static("desktop.clipboard");
        let mut state = AdapterModuleState::<u64>::default();

        let additive = SourceRecordFingerprint::diff(
            source_id.clone(),
            &SourceRecordFingerprint::from_json(&serde_json::json!({ "message": "hello" })),
            &SourceRecordFingerprint::from_json(&serde_json::json!({
                "message": "hello",
                "window_title": "terminal"
            })),
        )
        .ok_or_else(|| SinexError::validation("additive drift should be detected"))?;
        state.record_input_drift(additive);

        let additive_caveats = state.latest_input_drift_caveats();
        assert_eq!(additive_caveats.len(), 1);
        assert_eq!(additive_caveats[0].code, caveat_codes::SOURCE_SHAPE_CHANGED);

        let mut degraded = SourceRecordFingerprint::diff(
            source_id,
            &SourceRecordFingerprint::from_json(&serde_json::json!({
                "message": "hello",
                "count": 1
            })),
            &SourceRecordFingerprint::from_json(&serde_json::json!({
                "count": "1"
            })),
        )
        .ok_or_else(|| SinexError::validation("degraded drift should be detected"))?;
        degraded.required_input_keys = vec!["/message".to_string()];
        state.record_input_drift(degraded);

        let degraded_caveats = state.latest_input_drift_caveats();
        let degraded_codes: Vec<&str> = degraded_caveats
            .iter()
            .map(|caveat| caveat.code.as_str())
            .collect();
        assert_eq!(
            degraded_codes,
            vec![
                caveat_codes::PARSER_FIELD_TYPE_CHANGED,
                caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            ]
        );
        assert!(
            degraded_caveats.iter().any(|caveat| {
                caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                    && caveat.severity == CaveatSeverity::Blocking
            }),
            "required input removal should be blocking: {degraded_caveats:?}"
        );
        Ok(())
    }

    // -------------------------------------------------------------------------
    // #1570 Prong C — occurrence_key lands on the event as equivalence_key
    // -------------------------------------------------------------------------

    /// A parser-supplied occurrence key is carried onto the event as
    /// `equivalence_key`, so it reaches the curation duplicate workbench.
    #[sinex_test]
    async fn occurrence_key_lands_as_equivalence_key() -> xtask::sandbox::TestResult<()> {
        use sinex_primitives::parser::{OccurrenceKey, occurrence_key_string};
        let key = OccurrenceKey {
            source_id: SourceId::from_static("test.unit"),
            fields: vec![
                ("track_uri".into(), "spotify:track:abc".into()),
                ("played_ms".into(), "1234".into()),
            ],
        };
        let intent = ParsedEventIntent::builder()
            .source_id(SourceId::from_static("test.unit"))
            .parser_id(ParserId::from_static("test-parser"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("test.event"))
            .event_source(EventSource::from_static("test"))
            .payload(serde_json::json!({"k": "v"}))
            .ts_orig(Timestamp::now())
            .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
            .anchor(MaterialAnchor::ByteRange { start: 0, len: 0 })
            .privacy_context(ProcessingContext::Metadata)
            .occurrence_key(key.clone())
            .build();
        let event = intent_to_event_with_anchor(
            intent,
            Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            0,
            None,
            None,
            None,
        )
        .expect("intent conversion");
        assert_eq!(event.equivalence_key, Some(occurrence_key_string(&key)));
        Ok(())
    }

    /// Intents without an occurrence key leave `equivalence_key` unset (the
    /// curation workbench simply has nothing to group on for that event).
    #[sinex_test]
    async fn absent_occurrence_key_leaves_equivalence_key_none() -> xtask::sandbox::TestResult<()> {
        let intent = ParsedEventIntent::builder()
            .source_id(SourceId::from_static("test.unit"))
            .parser_id(ParserId::from_static("test-parser"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("test.event"))
            .event_source(EventSource::from_static("test"))
            .payload(serde_json::json!({"k": "v"}))
            .ts_orig(Timestamp::now())
            .timing(sinex_primitives::parser::TimingEvidence::StagedAtFallback)
            .anchor(MaterialAnchor::ByteRange { start: 0, len: 0 })
            .privacy_context(ProcessingContext::Metadata)
            .build();
        let event = intent_to_event_with_anchor(
            intent,
            Id::<SourceMaterial>::from_uuid(Uuid::now_v7()),
            0,
            None,
            None,
            None,
        )
        .expect("intent conversion");
        assert_eq!(event.equivalence_key, None);
        Ok(())
    }

    #[sinex_test]
    async fn sqlite_snapshot_evidence_link_is_idempotent(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (runtime, _events) = make_adapter_runtime_with_db(&ctx).await?;
        let row_material_id = Uuid::now_v7();
        let snapshot_material_id = Uuid::now_v7();

        ctx.pool()
            .source_materials()
            .register_external_in_flight(
                row_material_id,
                "stream",
                Some("test://sqlite-row-stream"),
                json!({"test": "row"}),
                Timestamp::now(),
            )
            .await?;
        ctx.pool()
            .source_materials()
            .register_external_in_flight(
                snapshot_material_id,
                "file",
                Some("test://sqlite-snapshot"),
                json!({"test": "snapshot"}),
                Timestamp::now(),
            )
            .await?;

        let mut source = AdapterBackedSource::<TestAdapter, EmittingParser>::new("test.sqlite");
        source.runtime = Some(runtime);
        source.sqlite_snapshot_evidence.update(
            crate::runtime::parser::adapters::SqliteSnapshotEvidence {
                material_id: Id::<SourceMaterial>::from_uuid(snapshot_material_id),
                source_identifier: "test.sqlite.snapshot".to_string(),
                source_path: "/tmp/test.sqlite".to_string(),
                content_hash_blake3: "abc123".to_string(),
                size_bytes: 123,
            },
        );

        let row_material = Id::<SourceMaterial>::from_uuid(row_material_id);
        source
            .link_latest_sqlite_snapshot_backing_material(row_material)
            .await;
        source
            .link_latest_sqlite_snapshot_backing_material(row_material)
            .await;

        let links = ctx
            .pool()
            .source_materials()
            .links_from(row_material_id)
            .await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].to_material_id, snapshot_material_id);
        assert_eq!(
            links[0].relation_type,
            source_material_relation_types::BACKED_BY
        );
        assert_eq!(links[0].metadata["evidence_role"], "sqlite_snapshot");
        assert_eq!(
            links[0].metadata["source_identifier"],
            "test.sqlite.snapshot"
        );
        assert_eq!(links[0].metadata["content_hash_blake3"], "abc123");
        assert_eq!(links[0].metadata["size_bytes"], 123);
        Ok(())
    }
}
