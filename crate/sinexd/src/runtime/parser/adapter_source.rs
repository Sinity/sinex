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
//!   "binding_flags": { "private_mode_active": false },
//!   "continuous_start_position": "latest"
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
//! For row/log stream adapters, a single [`AppendStreamAcquirer`] is held across
//! drain cycles. Record bytes are appended to the growing material and
//! [`AppendStreamAcquirer`] handles size/time-based rotation transparently. This
//! ensures `raw.source_material_registry` grows at `O(rotation_count)`, not
//! `O(poll_count)`.
//!
//! Snapshot-style finite poll adapters (`StaticFile`, `DirectoryWalk`) finalize
//! their stream material after each non-empty finite drain. Empty polls do not
//! create a material, so this closes one-shot/static batches promptly without
//! generating `O(poll_count)` empty registry rows.
//!
//! If a streaming adapter goes idle while a material is open, the drain loop
//! periodically finalizes the material before event-engine's stale-slice
//! watchdog can classify it as failed. Busy streams still rotate on append.
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
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value as JsonValue, json};
use tracing::{debug, info, warn};

use sinex_primitives::events::Event;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::builder::{EventBuilder, NoProvenance};
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{
    MaterialAnchor, ParsedEventIntent, ParserContext, TimingEvidence,
};
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
use crate::runtime::checkpoint::{CheckpointManager, CheckpointState};
use crate::runtime::parser::adapters::{LatestSqliteSnapshotEvidence, SqliteSnapshotLane};
use crate::runtime::parser::{
    BindingConfig, DriftEvent, InitialStreamPosition, InputShapeAdapter, InputShapeAdapterExt,
    InputShapeKind, MaterialParser, ParserResult, SourceRecord, SourceRecordFingerprint,
};
use crate::runtime::source_driver::{SourceDriver, SourceDriverState};
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, MaterialReplayContext, RuntimeCapabilities, RuntimeContext,
    ScanArgs, ScanReport, TimeHorizon,
};
use camino::Utf8PathBuf;
use std::path::PathBuf;
use std::sync::Arc;

const MAX_RECENT_INPUT_DRIFTS: usize = 16;
const PRIVATE_MODE_CONTROL_SUBJECT: &str = "sinex.control.privacy.private_mode";
const STREAM_CHECKPOINT_PERSIST_INTERVAL: Duration = Duration::from_secs(5);
const ADAPTER_MATERIAL_BATCH_MAX_RECORDS: usize = 1024;
const ADAPTER_MATERIAL_BATCH_MAX_BYTES: usize = 256 * 1024;
const ADAPTER_BATCH_DRAIN_WINDOW: Duration = Duration::from_millis(1);
const STREAM_IDLE_FINALIZE_REASON: &str = "adapter-stream-idle";

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
///   "continuous_start_position": "latest",
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

    /// Continuous startup policy used only when the source has no checkpoint
    /// cursor yet. Explicit historical scans and checkpointed live restarts use
    /// the normal adapter cursor path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuous_start_position: Option<InitialStreamPosition>,

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

    /// Runtime checkpoint manager used to durably persist streaming cursors
    /// without waiting for continuous mode to exit.
    checkpoint_manager: Option<Arc<CheckpointManager>>,

    /// Last NATS KV revision observed from an adapter-owned checkpoint save.
    checkpoint_revision: u64,

    /// Wall-clock throttle for best-effort streaming checkpoint writes.
    last_stream_checkpoint_persist: Option<Instant>,

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

struct PendingAdapterRecord<C> {
    record: SourceRecord,
    next_cursor: Option<C>,
    materialization_bytes: Option<Vec<u8>>,
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
            checkpoint_manager: None,
            checkpoint_revision: 0,
            last_stream_checkpoint_persist: None,
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

    fn idle_stream_finalize_interval(&self) -> Duration {
        let max_age = Duration::from_secs(self.rotation_policy.max_age_seconds.as_secs().max(1));
        if let Some(acquirer) = self.stream_acquirer.as_ref()
            && let Some(remaining) = acquirer.current_material_remaining_open_duration(max_age)
        {
            return remaining;
        }
        max_age
    }

    fn finalize_after_finite_poll_drain(&self) -> bool {
        matches!(
            A::KIND,
            InputShapeKind::StaticFile | InputShapeKind::DirectoryWalk
        )
    }

    async fn finalize_idle_stream_material_if_due(&mut self) -> RuntimeResult<bool> {
        let interval = self.idle_stream_finalize_interval();
        let Some(acquirer) = self.stream_acquirer.as_mut() else {
            return Ok(false);
        };
        acquirer
            .finalize_if_age_exceeds(interval, STREAM_IDLE_FINALIZE_REASON)
            .await
    }

    async fn next_record_preserving_pending_yield(
        &mut self,
        stream: &mut BoxStream<'static, ParserResult<SourceRecord>>,
    ) -> RuntimeResult<Option<ParserResult<SourceRecord>>> {
        let next_record = stream.next();
        tokio::pin!(next_record);

        loop {
            let idle_tick = tokio::time::sleep(self.idle_stream_finalize_interval());
            tokio::pin!(idle_tick);

            tokio::select! {
                record = &mut next_record => return Ok(record),
                () = &mut idle_tick => {
                    match self.finalize_idle_stream_material_if_due().await {
                        Ok(true) => {
                            info!(
                                source = self.source_id,
                                "Finalized idle adapter stream material before stale-slice timeout"
                            );
                        }
                        Ok(false) => {
                            debug!(
                                source = self.source_id,
                                "Adapter stream idle tick found no material old enough to finalize"
                            );
                        }
                        Err(error) => {
                            warn!(
                                source = self.source_id,
                                error = %error,
                                "Failed to finalize idle adapter stream material"
                            );
                        }
                    }
                }
            }
        }
    }

    async fn finalize_finite_drain_material(&mut self, reason: &str) -> RuntimeResult<bool> {
        if !self.finalize_after_finite_poll_drain() {
            return Ok(false);
        }
        let Some(acquirer) = self.stream_acquirer.as_mut() else {
            return Ok(false);
        };
        if acquirer.current_material_id().is_none() {
            return Ok(false);
        }
        acquirer.finalize(reason).await?;
        Ok(true)
    }

    async fn replay_file_drop_materials(
        &mut self,
        replay: MaterialReplayContext,
    ) -> RuntimeResult<ScanReport> {
        let start = Instant::now();
        let mut emitted: u64 = 0;
        let mut skipped: u64 = 0;

        for material in &replay.materials {
            let metadata = material.material_metadata.clone();
            if !self.replay_material_matches_event_type_filter(&metadata, &replay) {
                skipped = skipped.saturating_add(1);
                continue;
            }

            let logical_path = metadata
                .get("path")
                .and_then(JsonValue::as_str)
                .map(Utf8PathBuf::from);
            let bytes = logical_path
                .as_ref()
                .map_or_else(Vec::new, |path| path.as_str().as_bytes().to_vec());
            let content_len = metadata
                .get("content_size_bytes")
                .and_then(JsonValue::as_u64)
                .unwrap_or(bytes.len() as u64);
            let record = SourceRecord {
                material_id: Id::from_uuid(material.source_material_id),
                anchor: MaterialAnchor::ByteRange {
                    start: 0,
                    len: content_len,
                },
                bytes,
                logical_path,
                source_ts_hint: None,
                metadata,
            };

            emitted = emitted.saturating_add(
                self.process_materialized_record(record, replay.operation_id)
                    .await?,
            );
        }

        Ok(ScanReport {
            events_processed: emitted,
            duration: start.elapsed(),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            runtime_stats: HashMap::from([
                ("emitted".to_string(), emitted),
                ("replay_materials_skipped".to_string(), skipped),
            ]),
            successful_targets: vec![self.source_id.to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn replay_material_matches_event_type_filter(
        &self,
        metadata: &JsonValue,
        replay: &MaterialReplayContext,
    ) -> bool {
        let Some(event_types) = replay.replay_scope.event_types.as_ref() else {
            return true;
        };
        let Some(event_kind) = metadata.get("event_kind").and_then(JsonValue::as_str) else {
            return true;
        };
        let event_type = match event_kind {
            "Created" | "created" | "create" => "file.created",
            "Modified" | "modified" | "modify" => "file.modified",
            "Deleted" | "deleted" | "delete" => "file.deleted",
            "Moved" | "moved" | "move" => "file.moved",
            _ => return true,
        };
        event_types.iter().any(|expected| expected == event_type)
    }

    async fn process_materialized_record(
        &mut self,
        record: SourceRecord,
        operation_id: Uuid,
    ) -> RuntimeResult<u64> {
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

        let material_id = record.material_id;
        let (anchor_byte, offset_start, offset_end) =
            anchor_offsets_for_materialized_record(&record.anchor);
        let anchor_payload_hash = blake3::hash(record.bytes.as_slice()).as_bytes().to_owned();
        self.link_latest_sqlite_snapshot_backing_material(material_id)
            .await;

        let job_id = Uuid::now_v7();
        let host = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown".to_string());
        let ctx = ParserContext {
            source_id,
            source_material_id: material_id,
            record_anchor: record.anchor.clone(),
            operation_id,
            job_id,
            host,
            acquisition_time: Timestamp::now(),
        };

        let record_timing_hint = record.source_ts_hint.clone();
        let intents = match self
            .parser
            .parse_record_with_binding(record, &ctx, &self.binding_config)
            .await
        {
            Ok(v) => apply_record_timing_hint_to_intents(v, record_timing_hint.as_ref()),
            Err(e) => {
                warn!(
                    source = self.source_id,
                    error = %e,
                    "parse_record error during replay material processing — skipping"
                );
                return Ok(0);
            }
        };

        let mut emitted = 0u64;
        for intent in intents {
            match intent_to_event_with_anchor(
                intent,
                material_id,
                anchor_byte,
                offset_start,
                offset_end,
                Some(anchor_payload_hash),
            ) {
                Ok(event) => {
                    if let Err(e) = event_emitter.emit(event).await {
                        warn!(
                            source = self.source_id,
                            error = %e,
                            "emit failed during replay material processing — event dropped"
                        );
                    } else {
                        emitted = emitted.saturating_add(1);
                    }
                }
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "intent_to_event_with_anchor conversion failed during replay material processing — skipping"
                    );
                }
            }
        }
        Ok(emitted)
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
        let record_bytes = materialization_bytes_for_adapter_record(&record)?;
        let anchor_payload_hash = blake3::hash(record_bytes.as_slice()).as_bytes().to_owned();
        let source_id_for_anchor = self.source_id;
        let anchor = self
            .ensure_stream_acquirer()?
            .append_with_anchor(record_bytes.as_slice(), source_id_for_anchor)
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

    fn prepare_pending_adapter_record(
        &self,
        record: SourceRecord,
    ) -> RuntimeResult<PendingAdapterRecord<A::Cursor>> {
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

        if record.material_id.to_uuid() != Uuid::nil() {
            return Ok(PendingAdapterRecord {
                record,
                next_cursor,
                materialization_bytes: None,
                anchor_payload_hash: None,
            });
        }

        let record_bytes = materialization_bytes_for_adapter_record(&record)?;
        let anchor_payload_hash = blake3::hash(record_bytes.as_slice()).as_bytes().to_owned();

        Ok(PendingAdapterRecord {
            record,
            next_cursor,
            materialization_bytes: Some(record_bytes),
            anchor_payload_hash: Some(anchor_payload_hash),
        })
    }

    async fn materialize_adapter_batch(
        &mut self,
        pending_records: Vec<PendingAdapterRecord<A::Cursor>>,
    ) -> RuntimeResult<Vec<(MaterializedAdapterRecord, Option<A::Cursor>)>> {
        if pending_records.is_empty() {
            return Ok(Vec::new());
        }

        if pending_records
            .iter()
            .any(|pending| pending.record.material_id.to_uuid() != Uuid::nil())
        {
            let mut materialized = Vec::with_capacity(pending_records.len());
            for pending in pending_records {
                materialized.push((
                    self.materialize_adapter_record(pending.record).await?,
                    pending.next_cursor,
                ));
            }
            return Ok(materialized);
        }

        let records: Vec<Vec<u8>> = pending_records
            .iter()
            .map(|pending| {
                pending.materialization_bytes.clone().ok_or_else(|| {
                    crate::runtime::SinexError::invalid_state(
                        "missing materialization bytes for adapter batch record",
                    )
                })
            })
            .collect::<RuntimeResult<_>>()?;

        let source_id_for_anchor = self.source_id;
        let anchors = self
            .ensure_stream_acquirer()?
            .append_many_with_anchors(&records, source_id_for_anchor)
            .await
            .map_err(|error| {
                crate::runtime::SinexError::processing("append_many_with_anchors failed")
                    .with_context("source_id", self.source_id)
                    .with_context("records", records.len().to_string())
                    .with_std_error(&error)
            })?;

        let materialized = pending_records
            .into_iter()
            .zip(anchors)
            .map(|(pending, anchor)| {
                (
                    MaterializedAdapterRecord {
                        record: pending.record,
                        material_id: Id::<SourceMaterial>::from_uuid(anchor.material_id),
                        anchor_byte: anchor.offset_start,
                        offset_start: Some(anchor.offset_start),
                        offset_end: Some(anchor.offset_end),
                        anchor_payload_hash: pending.anchor_payload_hash,
                    },
                    pending.next_cursor,
                )
            })
            .collect();

        Ok(materialized)
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
        initial_position: Option<InitialStreamPosition>,
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

        let effective_config;
        let config = if cursor.is_none()
            && let Some(position) = initial_position
        {
            effective_config = self
                .adapter
                .configure_initial_stream_position(config, position)
                .map_err(|e| {
                    crate::runtime::SinexError::configuration(
                        "adapter rejected requested initial stream position",
                    )
                    .with_context("source_id", self.source_id)
                    .with_context("adapter_kind", A::KIND.as_str())
                    .with_context("initial_position", format!("{position:?}"))
                    .with_context("error", e.to_string())
                })?;
            &effective_config
        } else {
            config
        };

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
        let mut deferred_pending_record: Option<PendingAdapterRecord<A::Cursor>> = None;

        loop {
            let first_pending = if let Some(pending) = deferred_pending_record.take() {
                pending
            } else {
                let record_result = match self
                    .next_record_preserving_pending_yield(&mut stream)
                    .await?
                {
                    Some(record_result) => record_result,
                    None => break,
                };
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

                match self.prepare_pending_adapter_record(record) {
                    Ok(pending) => pending,
                    Err(e) => {
                        warn!(
                            source = self.source_id,
                            error = %e,
                            "record materialization preparation failed — skipping record"
                        );
                        continue;
                    }
                }
            };
            let mut pending_batch = vec![first_pending];
            let batch_unmaterialized = pending_batch[0].record.material_id.to_uuid() == Uuid::nil();
            let mut batch_bytes = pending_batch[0]
                .materialization_bytes
                .as_ref()
                .map_or(0, Vec::len);
            let mut stream_exhausted = false;

            while batch_unmaterialized
                && A::KIND != InputShapeKind::FileDrop
                && pending_batch.len() < ADAPTER_MATERIAL_BATCH_MAX_RECORDS
                && batch_bytes < ADAPTER_MATERIAL_BATCH_MAX_BYTES
            {
                self.refresh_binding_config()?;
                if self.binding_config.is_truthy("private_mode_active") {
                    info!(
                        source = self.source_id,
                        adapter_kind = A::KIND.as_str(),
                        pending_records = pending_batch.len(),
                        "private mode became active while batching; materializing only records already accepted"
                    );
                    break;
                }

                let next_record_result =
                    match tokio::time::timeout(ADAPTER_BATCH_DRAIN_WINDOW, stream.next()).await {
                        Ok(Some(next_record_result)) => next_record_result,
                        Ok(None) => {
                            stream_exhausted = true;
                            break;
                        }
                        Err(_) => break,
                    };

                let next_record = match next_record_result {
                    Ok(record) => record,
                    Err(e) => {
                        warn!(
                            source = self.source_id,
                            error = %e,
                            "Adapter stream error while batching — skipping record"
                        );
                        continue;
                    }
                };

                let next_pending = match self.prepare_pending_adapter_record(next_record) {
                    Ok(pending) => pending,
                    Err(e) => {
                        warn!(
                            source = self.source_id,
                            error = %e,
                            "record materialization preparation failed while batching — skipping record"
                        );
                        continue;
                    }
                };

                if next_pending.record.material_id.to_uuid() != Uuid::nil() {
                    deferred_pending_record = Some(next_pending);
                    break;
                }

                let next_bytes = next_pending
                    .materialization_bytes
                    .as_ref()
                    .map_or(0, Vec::len);
                if !pending_batch.is_empty()
                    && batch_bytes.saturating_add(next_bytes) > ADAPTER_MATERIAL_BATCH_MAX_BYTES
                {
                    deferred_pending_record = Some(next_pending);
                    break;
                }
                batch_bytes = batch_bytes.saturating_add(next_bytes);
                pending_batch.push(next_pending);
            }

            let materialized_batch = match self.materialize_adapter_batch(pending_batch).await {
                Ok(materialized) => materialized,
                Err(e) => {
                    warn!(
                        source = self.source_id,
                        error = %e,
                        "record materialization batch failed — skipping batch so material provenance can be retried"
                    );
                    continue;
                }
            };

            for (materialized, next_cursor) in materialized_batch {
                let material_id = materialized.material_id;
                self.link_latest_sqlite_snapshot_backing_material(material_id)
                    .await;

                let ctx = ParserContext {
                    source_id: source_id.clone(),
                    source_material_id: material_id,
                    record_anchor: materialized.record.anchor.clone(),
                    operation_id,
                    job_id,
                    host: host.clone(),
                    acquisition_time: Timestamp::now(),
                };

                let record_timing_hint = materialized.record.source_ts_hint.clone();
                let intents = match self
                    .parser
                    .parse_record_with_binding(materialized.record, &ctx, &self.binding_config)
                    .await
                {
                    Ok(v) => apply_record_timing_hint_to_intents(v, record_timing_hint.as_ref()),
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
                let mut record_processed = true;
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
                                record_processed = false;
                            } else {
                                emitted += 1;
                                state.total_events_emitted =
                                    state.total_events_emitted.saturating_add(1);
                            }
                        }
                        Err(e) => {
                            warn!(
                                source = self.source_id,
                                error = %e,
                                "intent_to_event_with_anchor conversion failed — skipping"
                            );
                            record_processed = false;
                        }
                    }
                }

                if record_processed {
                    if let Some(cursor) = next_cursor {
                        state.cursor = Some(merge_cursor_update(state.cursor.clone(), cursor));
                        self.persist_stream_checkpoint_if_due(state, false).await;
                    }
                } else {
                    warn!(
                        source = self.source_id,
                        "record processing failed — cursor not advanced so the record can be retried"
                    );
                }
            }

            if stream_exhausted {
                break;
            }
        }

        // The stream material is not finalized merely because one drain cycle
        // returned. It persists across drain cycles and is finalized by age/size
        // rotation, idle-stream finalization, or clean shutdown.

        self.persist_stream_checkpoint_if_due(state, true).await;
        debug!(
            source = self.source_id,
            emitted,
            total = state.total_events_emitted,
            "drain_adapter complete"
        );
        Ok(emitted)
    }

    async fn persist_stream_checkpoint_if_due(
        &mut self,
        state: &AdapterModuleState<A::Cursor>,
        force: bool,
    ) {
        if state.cursor.is_none() {
            return;
        }

        let now = Instant::now();
        if !force
            && self
                .last_stream_checkpoint_persist
                .is_some_and(|last| now.duration_since(last) < STREAM_CHECKPOINT_PERSIST_INTERVAL)
        {
            return;
        }

        let Some(checkpoint_manager) = self.checkpoint_manager.clone() else {
            return;
        };

        let checkpoint = cursor_to_checkpoint(state);
        let timestamp = Timestamp::now();
        let source_state = SourceDriverState {
            user_state: state.clone(),
            last_checkpoint: timestamp,
            revision: self.checkpoint_revision,
            checkpoint: checkpoint.clone(),
        };
        let data = match serde_json::to_value(&source_state) {
            Ok(data) => data,
            Err(error) => {
                warn!(
                    source = self.source_id,
                    error = %error,
                    "failed to encode adapter stream checkpoint"
                );
                return;
            }
        };

        let mut checkpoint_state = CheckpointState {
            checkpoint,
            processed_count: state.total_events_emitted,
            last_activity: timestamp,
            data: Some(data),
            version: 2,
            revision: self.checkpoint_revision,
        };

        if checkpoint_state.revision == 0 {
            match checkpoint_manager.load_checkpoint().await {
                Ok(existing) => {
                    checkpoint_state.revision = existing.revision;
                }
                Err(error) => {
                    warn!(
                        source = self.source_id,
                        error = %error,
                        "failed to inspect current checkpoint revision before streaming save"
                    );
                }
            }
        }

        match checkpoint_manager.save_checkpoint(&checkpoint_state).await {
            Ok(revision) => {
                self.checkpoint_revision = revision;
                self.last_stream_checkpoint_persist = Some(now);
            }
            Err(error) => {
                warn!(
                    source = self.source_id,
                    error = %error,
                    "failed to persist adapter stream checkpoint"
                );
            }
        }
    }
}

fn materialization_bytes_for_adapter_record(record: &SourceRecord) -> RuntimeResult<Vec<u8>> {
    if !record.bytes.is_empty() {
        return Ok(record.bytes.clone());
    }

    let Some(logical_path) = record.logical_path.as_ref() else {
        return Ok(Vec::new());
    };

    let descriptor = json!({
        "kind": "logical_adapter_record",
        "logical_path": logical_path.as_str(),
        "anchor": record.anchor,
        "metadata": record.metadata,
    });
    let mut bytes = serde_json::to_vec(&descriptor).map_err(|error| {
        crate::runtime::SinexError::serialization(
            "failed to serialize logical adapter record descriptor",
        )
        .with_std_error(&error)
    })?;
    bytes.push(b'\n');
    Ok(bytes)
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
            .acquisition_manager(self.rotation_policy.clone(), self.source_id)
            .map_err(|e| {
                crate::runtime::SinexError::lifecycle(
                    "AdapterBackedSource: failed to build AcquisitionManager",
                )
                .with_context("source_id", self.source_id)
                .with_std_error(&e)
            })?;

        self.acquisition_manager = Some(Arc::new(acq));
        self.checkpoint_manager = Some(runtime.checkpoint_manager());
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
        let emitted = self.drain_adapter(cursor, state, None).await?;
        self.finalize_finite_drain_material("adapter-snapshot-complete")
            .await?;
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
        args: ScanArgs,
    ) -> RuntimeResult<ScanReport> {
        let start = Instant::now();
        if A::KIND == InputShapeKind::FileDrop
            && let Some(replay) = args.replay
        {
            return self.replay_file_drop_materials(replay).await;
        }

        // Historical: re-open from persisted cursor (may be behind `from` if
        // the source was offline). The adapter's cursor is the authoritative
        // resume position.
        let cursor = state.cursor.clone();
        let emitted = self.drain_adapter(cursor, state, None).await?;
        self.finalize_finite_drain_material("adapter-historical-complete")
            .await?;
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
            let initial_position = cursor
                .is_none()
                .then(|| {
                    self.runtime_config
                        .as_ref()
                        .and_then(|config| config.continuous_start_position)
                })
                .flatten();
            // Drain the adapter, but stay responsive to shutdown *while draining*.
            // Event-driven adapters (e.g. file-drop / notify) legitimately block
            // in `drain_adapter` indefinitely — the stream never ends because the
            // watcher stays alive. Selecting against the shutdown signal lets such
            // a blocking drain be cancelled cleanly (dropping the drain future
            // tears down the watcher) so the loop reaches the material-finalize
            // path below instead of being force-aborted. Poll-drain adapters that
            // return normally fall through to the poll-interval wait as before.
            let source_id = self.source_id;
            tokio::select! {
                drained = self.drain_adapter(cursor, state, initial_position) => {
                    match drained {
                        Ok(n) => {
                            total_emitted += n;
                            match self
                                .finalize_finite_drain_material("adapter-poll-drain-complete")
                                .await
                            {
                                Ok(true) => {
                                    info!(
                                        source = source_id,
                                        emitted = n,
                                        "Finalized adapter stream material after finite poll drain"
                                    );
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    warn!(
                                        source = source_id,
                                        emitted = n,
                                        error = %error,
                                        "Failed to finalize adapter stream material after finite poll drain"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                source = source_id,
                                error = %e,
                                "drain_adapter error in continuous mode — retrying after interval"
                            );
                        }
                    }
                }
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        info!(source = source_id, "Drain signal received; exiting continuous loop");
                        break;
                    }
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

fn merge_cursor_json_update(base: JsonValue, over: JsonValue) -> JsonValue {
    match (base, over) {
        (JsonValue::Object(mut base_map), JsonValue::Object(over_map)) => {
            for (key, value) in over_map {
                let merged = match base_map.remove(&key) {
                    Some(existing) => merge_cursor_json_update(existing, value),
                    None => value,
                };
                base_map.insert(key, merged);
            }
            JsonValue::Object(base_map)
        }
        (_, over) => over,
    }
}

fn merge_cursor_update<C>(current: Option<C>, update: C) -> C
where
    C: Clone + Serialize + DeserializeOwned,
{
    let Some(current) = current else {
        return update;
    };

    let Ok(current_json) = serde_json::to_value(&current) else {
        return update;
    };
    let Ok(update_json) = serde_json::to_value(&update) else {
        return update;
    };
    let merged = merge_cursor_json_update(current_json, update_json);
    serde_json::from_value(merged).unwrap_or(update)
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

fn apply_record_timing_hint_to_intents(
    intents: Vec<ParsedEventIntent>,
    hint: Option<&TimingEvidence>,
) -> Vec<ParsedEventIntent> {
    let Some(hint) = hint else {
        return intents;
    };
    let Some(ts_orig) = hint.timestamp_value() else {
        return intents;
    };

    intents
        .into_iter()
        .map(|mut intent| {
            if matches!(
                intent.timing,
                TimingEvidence::StagedAtFallback | TimingEvidence::Atemporal
            ) {
                intent.ts_orig = ts_orig;
                intent.timing = hint.clone();
            }
            intent
        })
        .collect()
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
#[path = "adapter_source_test.rs"]
mod tests;
