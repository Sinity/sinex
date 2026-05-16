//! Generic [`AdapterBackedIngestor`] — wires an [`InputShapeAdapter`] to a
//! [`MaterialParser`] as a full [`IngestorNode`].
//!
//! # Purpose
//!
//! Wave-B ingestor folds need one line per source unit:
//!
//! ```rust,ignore
//! register_adapter_ingestor!(
//!     source_unit_id: "terminal.atuin-history",
//!     adapter:        SqliteRowAdapter,
//!     parser:         AtuinHistoryRecord,
//! );
//! ```
//!
//! `AdapterBackedIngestor<A, P>` is the `IngestorNode` implementation that
//! backs every such registration. It handles:
//!
//! - Snapshot and historical scans (drive adapter stream → parse → emit).
//! - Continuous mode for append-only adapters (tail loop with shutdown signal).
//! - Cursor persistence via the standard `IngestorNode` state mechanism.
//! - Conversion of `ParsedEventIntent` → `Event<JsonValue>` → `emit()`.
//! - Long-lived source-material lifecycle: records from many drain cycles are
//!   appended to the same [`AppendStreamAcquirer`], which auto-rotates at 100
//!   MB or 1 hour (configurable). This prevents O(poll_count) material rows.
//!
//! # Config shape
//!
//! The node JSON config is deserialized into [`AdapterNodeConfig<A::Config>`]:
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
//! node configs use. The optional `binding_flags` map carries runtime flags
//! for `#[suppress_if]` predicates (the `BindingConfig` concern), which is
//! separate from the adapter's typed config.
//!
//! # Design constraints
//!
//! - `A::Cursor` must be serialisable so the SDK checkpoint machinery can
//!   persist and restore it.
//! - `P` must be `Default + MaterialParser`. Both hold for every
//!   `#[derive(SourceRecord)]` struct and for imperative parsers that `impl
//!   Default`.
//! - This struct does NOT own transport or admission — it calls
//!   `runtime.event_emitter().emit()` exactly as every other ingestor does.
//!
//! # Material lifecycle
//!
//! A single [`AppendStreamAcquirer`] is held across all drain cycles (snapshot,
//! historical, and every continuous poll). Record bytes are appended to the
//! growing material; [`AppendStreamAcquirer`] handles size/time-based rotation
//! transparently. This ensures `raw.source_material_registry` grows at
//! O(rotation_count), not O(poll_count).
//!
//! When `run_continuous` exits cleanly (shutdown signal), the current material
//! is finalized. On ingestor drop the [`AppendStreamAcquirer`] finalizes via its
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
//! `IngestorNode` instead.

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use tracing::{debug, info, warn};

use sinex_primitives::events::Event;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::builder::{EventBuilder, NoProvenance};
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, ParsedEventIntent, ParserContext};
use sinex_primitives::primitives::Uuid;
use sinex_primitives::temporal::Timestamp;

use crate::NodeResult;
use crate::acquisition_manager::{AcquisitionManager, AppendStreamAcquirer, RotationPolicy};
use crate::ingestor_node::IngestorNode;
use crate::parser::adapters::SqliteSnapshotLane;
use crate::parser::{BindingConfig, InputShapeAdapter, MaterialParser};
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport,
    TimeHorizon,
};
use std::sync::Arc;

// =============================================================================
// Typed node config — wraps adapter config + optional binding flags
// =============================================================================

/// Node-level config for [`AdapterBackedIngestor`].
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
/// an optional nested map. Existing node configs (e.g. `{ "path": "..." }`)
/// continue to work without modification.
///
/// ```json
/// {
///   "path": "/home/user/.weechat/logs/irc.log",
///   "binding_flags": { "private_mode_active": false }
/// }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdapterNodeConfig {
    /// Adapter-specific config fields. Flattened so they live at the top
    /// level of the JSON object. Deserialized into `A::Config` at
    /// `initialize` time.
    #[serde(flatten)]
    pub adapter: JsonValue,

    /// Optional runtime flags for `BindingConfig`-aware parsers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub binding_flags: BTreeMap<String, bool>,
}

impl AdapterNodeConfig {
    /// Convert the `binding_flags` map into a [`BindingConfig`] for use with
    /// `DeclarativeParser::evaluate`.
    pub fn to_binding_config(&self) -> BindingConfig {
        let mut bc = BindingConfig::new();
        for (name, &value) in &self.binding_flags {
            bc = bc.with_flag(name, value);
        }
        bc
    }

    /// Deserialize the flattened adapter JSON into the typed adapter config.
    pub fn into_adapter_config<C: DeserializeOwned>(self) -> Result<C, serde_json::Error> {
        serde_json::from_value(self.adapter)
    }
}

// =============================================================================
// Adapter-node state (checkpoint-persisted)
// =============================================================================

/// Checkpoint state for [`AdapterBackedIngestor`].
///
/// Contains the adapter cursor (opaque to the SDK) and event counters.
/// Serialized as the `IngestorState<S>::user_state` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "C: Clone + Serialize + DeserializeOwned")]
pub struct AdapterNodeState<C>
where
    C: Clone + Serialize + DeserializeOwned,
{
    /// Last cursor returned by `adapter.cursor_after(record)`.
    pub cursor: Option<C>,

    /// Total events emitted across all scans.
    pub total_events_emitted: u64,
}

impl<C> Default for AdapterNodeState<C>
where
    C: Clone + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            cursor: None,
            total_events_emitted: 0,
        }
    }
}

// =============================================================================
// AdapterBackedIngestor
// =============================================================================

/// A generic ingestor that wraps `(A: InputShapeAdapter, P: MaterialParser)`.
///
/// Type parameters:
/// - `A` — the input-shape adapter (e.g. `SqliteRowAdapter`,
///   `AppendOnlyFileAdapter`).
/// - `P` — the material parser (any type implementing `MaterialParser`, including
///   `#[derive(SourceRecord)]` structs and imperative parsers).
///
/// The adapter and parser are constructed via `Default`, then configured during
/// `initialize`. The node config is deserialized into
/// `AdapterNodeConfig<A::Config>`; the source-unit id is hard-coded at
/// registration time via the `register_adapter_ingestor!` macro.
pub struct AdapterBackedIngestor<A, P>
where
    A: InputShapeAdapter + Default,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    /// Human-readable source-unit id, baked in at registration time.
    source_unit_id: &'static str,

    /// The adapter instance. Constructed in `Default`, configured in
    /// `initialize`.
    adapter: A,

    /// The parser instance. Constructed in `Default`.
    parser: P,

    /// Adapter config deserialized from the node config at `initialize`.
    config: Option<A::Config>,

    /// BindingConfig derived from `binding_flags` in the node config.
    /// Held for the lifetime of the ingestor; passed to any `BindingConfig`-
    /// aware parsers (currently `DeclarativeParser`).
    binding_config: BindingConfig,

    /// Runtime handles captured during `initialize`.
    runtime: Option<NodeRuntimeState>,

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
    /// SQLite DB file) into a separate source-material lineage. Per-row
    /// drain is unaffected.
    snapshot_task: Option<tokio::task::JoinHandle<NodeResult<()>>>,

    /// Sender that shuts down the snapshot-lane task. Held alongside
    /// `snapshot_task`; both are `Some` together or both are `None`.
    snapshot_shutdown: Option<tokio::sync::watch::Sender<bool>>,

    _phantom: PhantomData<()>,
}

impl<A, P> AdapterBackedIngestor<A, P>
where
    A: InputShapeAdapter + Default,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    /// Create a new adapter-backed ingestor for the given source-unit id.
    ///
    /// Called by `register_adapter_ingestor!` via `Default::default()` and the
    /// `new` constructor. Callers should normally use the macro, not this
    /// constructor directly.
    #[must_use]
    pub fn new(source_unit_id: &'static str) -> Self {
        Self {
            source_unit_id,
            adapter: A::default(),
            parser: P::default(),
            config: None,
            binding_config: BindingConfig::default(),
            runtime: None,
            stream_acquirer: None,
            acquisition_manager: None,
            rotation_policy: RotationPolicy::default(),
            snapshot_task: None,
            snapshot_shutdown: None,
            _phantom: PhantomData,
        }
    }

    /// Create a new adapter-backed ingestor with a custom rotation policy.
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
    pub async fn rotate_for_test(&mut self) -> NodeResult<()> {
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
            .and_then(|a| a.current_material_id())
    }

    /// Ensure the `AppendStreamAcquirer` is initialized, creating it from the
    /// acquisition manager if necessary.
    ///
    /// Returns a mutable reference to the acquirer, or an error if the ingestor
    /// has not been initialized yet.
    #[allow(clippy::expect_used)]
    async fn ensure_stream_acquirer(&mut self) -> NodeResult<&mut AppendStreamAcquirer> {
        if self.stream_acquirer.is_none() {
            let manager = self.acquisition_manager.as_ref().ok_or_else(|| {
                crate::SinexError::lifecycle(
                    "AdapterBackedIngestor: acquisition_manager not set (initialize not called)",
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

    /// Open the adapter, drain all records through the parser, emit each
    /// `ParsedEventIntent` via the runtime, and append record bytes to the
    /// long-lived stream material.
    ///
    /// The stream acquirer is reused across drain calls; it rotates the
    /// underlying source material automatically at the configured size/time
    /// thresholds. This ensures `raw.source_material_registry` grows at
    /// O(rotation_count) rather than O(poll_count).
    /// On adapter-open failure the material is cancelled before returning the
    /// error.
    ///
    /// Returns total events emitted.
    async fn drain_adapter(
        &mut self,
        cursor: Option<A::Cursor>,
        state: &mut AdapterNodeState<A::Cursor>,
    ) -> NodeResult<u64> {
        let config = self.config.as_ref().ok_or_else(|| {
            crate::SinexError::lifecycle(
                "AdapterBackedIngestor: adapter config not set (initialize not called)",
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
                crate::SinexError::lifecycle(
                    "AdapterBackedIngestor: runtime not available (initialize not called)",
                )
            })?
            .event_emitter()
            .clone();

        let source_unit_id = sinex_primitives::parser::SourceUnitId::new(self.source_unit_id)
            .map_err(|e| {
                crate::SinexError::validation("invalid source_unit_id in AdapterBackedIngestor")
                    .with_std_error(&e)
            })?;

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

        // Open the adapter stream.
        let mut stream = match self
            .adapter
            .open(placeholder_material_id, config, cursor)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return Err(crate::SinexError::processing("adapter open failed")
                    .with_context("source_unit_id", self.source_unit_id)
                    .with_context("adapter_kind", A::KIND.as_str())
                    .with_context("error", e.to_string()));
            }
        };

        let mut emitted: u64 = 0;

        while let Some(record_result) = stream.next().await {
            let record = match record_result {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        source_unit = self.source_unit_id,
                        error = %e,
                        "Adapter stream error — skipping record"
                    );
                    continue;
                }
            };

            // Advance cursor before parsing (cursor tracks adapter position,
            // not parser success). Best effort — log and continue on failure.
            match self.adapter.cursor_after(&record) {
                Ok(c) => state.cursor = Some(c),
                Err(e) => {
                    warn!(
                        source_unit = self.source_unit_id,
                        error = %e,
                        "cursor_after failed — checkpoint may regress"
                    );
                }
            }

            // Append record bytes to the long-lived stream material. The acquirer
            // returns a SourceRecordAnchor with (material_id, offset_start,
            // offset_end) that precisely locates this record within the growing
            // material blob.  The acquirer handles size/time-based rotation
            // transparently — `raw.source_material_registry` grows at
            // O(rotation_count) across all drain cycles rather than O(poll_count).
            let record_bytes = record.bytes.as_slice();
            // Pre-load source_unit_id into a local: ensure_stream_acquirer
            // takes &mut self, so we can't simultaneously hold &self.source_unit_id
            // as an argument to append_with_anchor. Copy now (it's a &'static str
            // so Copy semantics apply).
            let source_unit_id_for_anchor = self.source_unit_id;
            let anchor = match self
                .ensure_stream_acquirer()
                .await?
                .append_with_anchor(record_bytes, source_unit_id_for_anchor)
                .await
            {
                Ok(a) => a,
                Err(e) => {
                    warn!(
                        source_unit = self.source_unit_id,
                        error = %e,
                        "append_with_anchor failed — material content may be incomplete"
                    );
                    // Best-effort: emit the event with a zeroed anchor rather than
                    // dropping it entirely. The provenance will be degraded but the
                    // event is not silently lost.
                    crate::acquisition_manager::SourceRecordAnchor {
                        material_id: Uuid::nil(),
                        offset_start: 0,
                        offset_end: 0,
                    }
                }
            };

            let material_id = Id::<SourceMaterial>::from_uuid(anchor.material_id);

            let ctx = ParserContext {
                source_unit_id: source_unit_id.clone(),
                source_material_id: material_id,
                record_anchor: record.anchor.clone(),
                operation_id,
                job_id,
                host: host.clone(),
                acquisition_time: Timestamp::now(),
            };

            let intents = match self.parser.parse_record(record, &ctx).await {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        source_unit = self.source_unit_id,
                        error = %e,
                        "parse_record error — skipping"
                    );
                    continue;
                }
            };

            for intent in intents {
                // Use the byte offset from the stream acquirer anchor so the event
                // correctly references its position in the long-lived material.
                match intent_to_event_with_anchor(intent, material_id, anchor.offset_start) {
                    Ok(event) => {
                        if let Err(e) = event_emitter.emit(event).await {
                            warn!(
                                source_unit = self.source_unit_id,
                                error = %e,
                                "emit failed — event dropped"
                            );
                        } else {
                            emitted += 1;
                        }
                    }
                    Err(e) => {
                        warn!(
                            source_unit = self.source_unit_id,
                            error = %e,
                            "intent_to_event_with_anchor conversion failed — skipping"
                        );
                    }
                }
            }
        }

        // The stream material is NOT finalized here — it persists across drain
        // cycles. Finalization happens when run_continuous exits (shutdown signal)
        // or when the ingestor is dropped.

        state.total_events_emitted += emitted;
        debug!(
            source_unit = self.source_unit_id,
            emitted,
            total = state.total_events_emitted,
            "drain_adapter complete"
        );
        Ok(emitted)
    }
}

impl<A, P> Drop for AdapterBackedIngestor<A, P>
where
    A: InputShapeAdapter + Default,
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
    }
}

impl<A, P> Default for AdapterBackedIngestor<A, P>
where
    A: InputShapeAdapter + Default,
    P: MaterialParser + Default,
    A::Config: Clone + Serialize + DeserializeOwned,
    A::Cursor: Clone + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        // Default::default() is required by IngestorNodeAdapter<I>.
        // The source_unit_id is a sentinel that the macro overrides via `new`.
        Self::new("__unset__")
    }
}

// =============================================================================
// IngestorNode impl
// =============================================================================

impl<A, P> IngestorNode for AdapterBackedIngestor<A, P>
where
    A: InputShapeAdapter + Default + Send + Sync + 'static,
    P: MaterialParser + Default + Send + Sync + 'static,
    A::Config: Clone + Serialize + DeserializeOwned + Send + Sync,
    A::Cursor: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    type Config = AdapterNodeConfig;
    type State = AdapterNodeState<A::Cursor>;

    fn name(&self) -> &str {
        self.source_unit_id
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
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
        runtime: &NodeRuntimeState,
        _state: &mut Self::State,
    ) -> NodeResult<()> {
        // Build the AcquisitionManager from the runtime's NATS handles.
        let acq = runtime
            .acquisition_manager(
                crate::acquisition_manager::RotationPolicy::default(),
                self.source_unit_id,
            )
            .map_err(|e| {
                crate::SinexError::lifecycle(
                    "AdapterBackedIngestor: failed to build AcquisitionManager",
                )
                .with_context("source_unit_id", self.source_unit_id)
                .with_std_error(&e)
            })?;

        self.acquisition_manager = Some(Arc::new(acq));
        self.binding_config = config.to_binding_config();

        // Merge user-supplied JSON over the parser's baseline. The parser
        // declares mandatory adapter fields (parser-specific SQL query,
        // static D-Bus bus name, ChainedAdapter primary leg) via
        // `MaterialParser::baseline_adapter_config`; the user's
        // `--node-config` JSON overlays it (user keys win on conflict).
        let adapter_json = merge_json_over(P::baseline_adapter_config(), config.adapter);
        let adapter_config: A::Config = serde_json::from_value(adapter_json).map_err(|e| {
            crate::SinexError::configuration(
                "AdapterBackedIngestor: failed to deserialize adapter config",
            )
            .with_context("source_unit_id", self.source_unit_id)
            .with_std_error(&e)
        })?;
        // Opt-in parallel snapshot lane.  The adapter declares whether it
        // wants one by returning `Some(spec)` from `snapshot_lane`; we spawn
        // an independent tokio task that captures the substrate on its own
        // timer.  Per-record drain (above) is untouched.
        if let Some(spec) = self
            .adapter
            .snapshot_lane(self.source_unit_id, &adapter_config)
        {
            #[allow(clippy::expect_used)]
            let manager = Arc::clone(
                self.acquisition_manager
                    .as_ref()
                    .expect("acquisition_manager set above"),
            );
            let (tx, rx) = tokio::sync::watch::channel(false);
            let lane = SqliteSnapshotLane::new(spec, manager);
            let unit_id = self.source_unit_id;
            let handle = tokio::spawn(async move {
                let result = lane.run(rx).await;
                if let Err(ref e) = result {
                    warn!(
                        source_unit = unit_id,
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
            source_unit = self.source_unit_id,
            adapter_kind = A::KIND.as_str(),
            snapshot_lane = self.snapshot_task.is_some(),
            "AdapterBackedIngestor initialized"
        );
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
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
            node_stats: HashMap::from([("emitted".to_string(), emitted)]),
            successful_targets: vec![self.source_unit_id.to_string()],
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
    ) -> NodeResult<ScanReport> {
        let start = Instant::now();
        // Historical: re-open from persisted cursor (may be behind `from` if
        // the node was offline). The adapter's cursor is the authoritative
        // resume position.
        let cursor = state.cursor.clone();
        let emitted = self.drain_adapter(cursor, state).await?;
        let checkpoint = cursor_to_checkpoint(state);

        Ok(ScanReport {
            events_processed: emitted,
            duration: start.elapsed(),
            final_checkpoint: checkpoint,
            time_range: None,
            node_stats: HashMap::from([("emitted".to_string(), emitted)]),
            successful_targets: vec![self.source_unit_id.to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        _start: ContinuousStart,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let wall_start = Instant::now();
        let mut total_emitted: u64 = 0;

        // Poll interval for adapters without native streaming.
        // TODO: make configurable via binding_flags or a dedicated config field.
        let poll_interval = Duration::from_secs(30);

        info!(
            source_unit = self.source_unit_id,
            poll_interval_s = poll_interval.as_secs(),
            "AdapterBackedIngestor entering continuous poll loop"
        );

        loop {
            // Check for shutdown before polling.
            if *shutdown_rx.borrow() {
                info!(
                    source_unit = self.source_unit_id,
                    "Drain signal received; exiting continuous loop"
                );
                break;
            }

            let cursor = state.cursor.clone();
            match self.drain_adapter(cursor, state).await {
                Ok(n) => total_emitted += n,
                Err(e) => {
                    warn!(
                        source_unit = self.source_unit_id,
                        error = %e,
                        "drain_adapter error in continuous mode — retrying after interval"
                    );
                }
            }

            // Wait for the poll interval or a shutdown signal.
            tokio::select! {
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        info!(source_unit = self.source_unit_id, "Drain signal received; exiting continuous loop");
                        break;
                    }
                }
                () = tokio::time::sleep(poll_interval) => {}
            }
        }

        // Finalize the in-flight stream material on clean shutdown so ingestd
        // receives the END frame and commits the row count before the process
        // exits.  Best-effort: a failure here only affects the current open
        // material; already-finalized materials and persisted events are safe.
        if let Some(acquirer) = self.stream_acquirer.as_mut() {
            if let Err(e) = acquirer.finalize("continuous-mode-shutdown").await {
                warn!(
                    source_unit = self.source_unit_id,
                    error = %e,
                    "Failed to finalize stream material on shutdown — in-flight material may be incomplete"
                );
            }
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
                    source_unit = self.source_unit_id,
                    error = %e,
                    "snapshot lane task returned error on shutdown",
                ),
                Err(_) => warn!(
                    source_unit = self.source_unit_id,
                    "snapshot lane did not exit within timeout; aborting",
                ),
            }
        }

        let checkpoint = cursor_to_checkpoint(state);
        Ok(ScanReport {
            events_processed: total_emitted,
            duration: wall_start.elapsed(),
            final_checkpoint: checkpoint,
            time_range: None,
            node_stats: HashMap::from([("emitted".to_string(), total_emitted)]),
            successful_targets: vec![self.source_unit_id.to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Convert a `ParsedEventIntent` to an `Event<JsonValue>` ready for emission.
///
/// The anchor from the intent maps to the material `anchor_byte`. For
/// anchors that carry a natural integer offset (`ByteRange::start`,
/// `SqliteRow::rowid`, `Line::byte_start`) we use that. All others map to 0.
/// Merge `over` JSON value on top of `base`, recursively for objects.
///
/// Object keys: if both sides have the key and both values are objects,
/// merge recursively. Otherwise `over` wins. Non-object values: `over`
/// wins unconditionally. Used to layer user-supplied node config over
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

fn intent_to_event(
    intent: ParsedEventIntent,
    material_id: Id<SourceMaterial>,
) -> Result<Event<JsonValue>, String> {
    let anchor_byte: i64 = match &intent.anchor {
        MaterialAnchor::ByteRange { start, .. } => *start as i64,
        MaterialAnchor::Line { byte_start, .. } => *byte_start as i64,
        MaterialAnchor::SqliteRow { rowid, .. } => *rowid,
        MaterialAnchor::StreamFrame {
            material_offset, ..
        } => *material_offset as i64,
        MaterialAnchor::DirectoryEntry { .. } | MaterialAnchor::GitObject { .. } => 0,
    };

    let builder: EventBuilder<JsonValue, NoProvenance> =
        EventBuilder::new_internal(intent.event_source, intent.event_type, intent.payload);

    let built = builder
        .from_material(material_id, anchor_byte)
        .at_time(intent.ts_orig)
        .build()
        .map_err(|e| format!("EventBuilder::build failed: {e}"))?;

    Ok(built)
}

/// Convert a `ParsedEventIntent` to an `Event<JsonValue>`, overriding `anchor_byte`
/// with the stream-acquirer byte offset rather than the anchor embedded in the intent.
///
/// When events are emitted from a long-lived source material managed by
/// [`AppendStreamAcquirer`], the "natural" anchor inside `ParsedEventIntent` reflects
/// a logical position within the *source record* (e.g. a SQLite rowid).  The real
/// byte position in the material is the offset returned by `append_with_anchor`, which
/// is what downstream queries need to replay or seek into the material blob.
fn intent_to_event_with_anchor(
    intent: ParsedEventIntent,
    material_id: Id<SourceMaterial>,
    anchor_byte_override: i64,
) -> Result<Event<JsonValue>, String> {
    let builder: EventBuilder<JsonValue, NoProvenance> =
        EventBuilder::new_internal(intent.event_source, intent.event_type, intent.payload);

    let built = builder
        .from_material(material_id, anchor_byte_override)
        .at_time(intent.ts_orig)
        .build()
        .map_err(|e| format!("EventBuilder::build failed: {e}"))?;

    Ok(built)
}

/// Produce a `Checkpoint` from the current node state.
///
/// Uses `External` with the serialized cursor, falling back to `None` if no
/// cursor has been advanced yet.
fn cursor_to_checkpoint<C>(state: &AdapterNodeState<C>) -> Checkpoint
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
