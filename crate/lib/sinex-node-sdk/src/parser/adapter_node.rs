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
//! - Real source-material lifecycle: each drain opens a material via
//!   `AcquisitionManager`, appends record bytes, and finalizes on completion.
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
//! One source material is opened per drain invocation (snapshot, historical,
//! or each continuous poll). The material receives the raw bytes of every
//! source record processed. On a clean drain, the material is finalized with
//! `"drain-complete"`. On adapter error, it is cancelled before returning.
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
use sinex_primitives::events::builder::{EventBuilder, NoProvenance};
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, ParsedEventIntent, ParserContext};
use sinex_primitives::primitives::Uuid;
use sinex_primitives::temporal::Timestamp;

use crate::acquisition_manager::AcquisitionManager;
use crate::ingestor_node::IngestorNode;
use crate::parser::{BindingConfig, InputShapeAdapter, MaterialParser};
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport,
    TimeHorizon,
};
use crate::NodeResult;

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

    /// AcquisitionManager built during `initialize` from the runtime handles.
    /// Used to open/finalize a source material for each drain invocation.
    acquisition_manager: Option<AcquisitionManager>,

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
            acquisition_manager: None,
            _phantom: PhantomData,
        }
    }

    /// Open the adapter, drain all records through the parser, emit each
    /// `ParsedEventIntent` via the runtime, and finalize the source material.
    ///
    /// One source material is opened per call. Record bytes are appended to the
    /// material before parsing, providing a content-addressable provenance trail.
    /// On a clean drain the material is finalized with `"drain-complete"`.
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

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            crate::SinexError::lifecycle(
                "AdapterBackedIngestor: runtime not available (initialize not called)",
            )
        })?;

        let acquisition_manager = self.acquisition_manager.as_ref().ok_or_else(|| {
            crate::SinexError::lifecycle(
                "AdapterBackedIngestor: acquisition_manager not set (initialize not called)",
            )
        })?;

        let source_unit_id = sinex_primitives::parser::SourceUnitId::new(self.source_unit_id)
            .map_err(|e| {
                crate::SinexError::validation("invalid source_unit_id in AdapterBackedIngestor")
                    .with_std_error(&e)
            })?;

        // Open a real source material for this drain invocation. This registers
        // the material in raw.source_material_registry via ingestd, satisfying
        // the FK constraint on core.events.source_material_id.
        let mut material_handle = acquisition_manager
            .begin_material(self.source_unit_id)
            .await
            .map_err(|e| {
                crate::SinexError::processing("AdapterBackedIngestor: begin_material failed")
                    .with_context("source_unit_id", self.source_unit_id)
                    .with_std_error(&e)
            })?;

        let material_id = Id::<SourceMaterial>::from_uuid(material_handle.material_id);

        let operation_id = Uuid::now_v7();
        let job_id = Uuid::now_v7();
        let host = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown".to_string());

        // Open the adapter stream. On failure, cancel the material before returning.
        let mut stream = match self
            .adapter
            .open(material_id, config, cursor)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                if let Err(cancel_err) = acquisition_manager
                    .cancel(&mut material_handle, "adapter-open-failure")
                    .await
                {
                    warn!(
                        source_unit = self.source_unit_id,
                        error = %cancel_err,
                        "Failed to cancel material after adapter open failure"
                    );
                }
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

            // Append the record bytes to the source material for provenance.
            // For structured adapters (e.g. SqliteRowAdapter) the bytes are
            // the adapter's wire representation of the record; for file adapters
            // they are the raw source bytes. Errors here are non-fatal —
            // the event is still emitted, but the material content may be
            // incomplete. We log and continue to avoid dropping events over I/O
            // transients.
            let record_bytes_for_material = record.bytes.as_slice();
            if let Err(e) = acquisition_manager
                .append_slice(&mut material_handle, record_bytes_for_material)
                .await
            {
                warn!(
                    source_unit = self.source_unit_id,
                    material_id = %material_handle.material_id,
                    error = %e,
                    "append_slice failed — material content may be incomplete"
                );
            }

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
                match intent_to_event(intent, material_id) {
                    Ok(event) => {
                        if let Err(e) = runtime.event_emitter().emit(event).await {
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
                            "intent_to_event conversion failed — skipping"
                        );
                    }
                }
            }
        }

        // Finalize the material now that all records have been processed.
        if let Err(e) = acquisition_manager
            .finalize(material_handle, "drain-complete")
            .await
        {
            warn!(
                source_unit = self.source_unit_id,
                error = %e,
                "finalize material failed — material registry may be incomplete"
            );
        }

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
            .acquisition_manager(crate::acquisition_manager::RotationPolicy::default(), self.source_unit_id)
            .map_err(|e| {
                crate::SinexError::lifecycle(
                    "AdapterBackedIngestor: failed to build AcquisitionManager",
                )
                .with_context("source_unit_id", self.source_unit_id)
                .with_std_error(&e)
            })?;

        self.acquisition_manager = Some(acq);
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
        self.config = Some(adapter_config);
        self.runtime = Some(runtime.clone());

        info!(
            source_unit = self.source_unit_id,
            adapter_kind = A::KIND.as_str(),
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
                info!(source_unit = self.source_unit_id, "Drain signal received; exiting continuous loop");
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
        MaterialAnchor::StreamFrame { material_offset, .. } => *material_offset as i64,
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
