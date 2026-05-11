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
//! # Blocker note
//!
//! Continuous mode for adapters that do not support streaming (e.g.
//! `SqliteRowAdapter`, `StaticFileAdapter`) parks the node in a sleep loop
//! after the initial drain, then re-polls on a configurable interval (default
//! 30 s). Adapters that natively support streaming (e.g. future
//! `UnixSocketStreamAdapter`-backed ingestors) should override this by
//! implementing their own `IngestorNode`.
//!
//! The binding config (`BindingConfig`) available in `declarative.rs` is NOT
//! yet threaded into `AdapterBackedIngestor` — callers supply the adapter
//! config via `serde_json::Value` config (deserialized to `A::Config` at
//! `initialize`). If adapter configs need per-binding overrides, the config
//! JSON should carry them; no SDK extension is required.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use async_trait::async_trait;
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

use crate::ingestor_node::IngestorNode;
use crate::parser::{InputShapeAdapter, MaterialParser, ParserError, ParserResult};
use crate::runtime::stream::{
    Checkpoint, ContinuousStart, NodeCapabilities, NodeRuntimeState, ScanArgs, ScanReport,
    TimeHorizon,
};
use crate::NodeResult;

// =============================================================================
// Adapter-node state (checkpoint-persisted)
// =============================================================================

/// Checkpoint state for [`AdapterBackedIngestor`].
///
/// Contains the adapter cursor (opaque to the SDK) and event counters.
/// Serialized as the `IngestorState<S>::user_state` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
/// `initialize`. `A::Config` is deserialized from the node JSON config; the
/// source-unit id used for `ParserContext` is hard-coded at registration time
/// via the `register_adapter_ingestor!` macro.
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

    /// Adapter config deserialized from the node JSON config at `initialize`.
    config: Option<A::Config>,

    /// Runtime handles captured during `initialize`.
    runtime: Option<NodeRuntimeState>,

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
            runtime: None,
            _phantom: PhantomData,
        }
    }

    /// Open the adapter against a synthetic material id and drain all records
    /// through the parser, emitting each `ParsedEventIntent` via the runtime.
    ///
    /// Returns (events_emitted, final_cursor).
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

        // Synthesize a stable material id from the source-unit id.
        // This is a design placeholder: in a full fold, the material id comes
        // from the source-material registry. Using a deterministic v5 UUID
        // keeps the anchor meaningful across restarts without requiring a DB
        // round-trip here.
        let ns = Uuid::NAMESPACE_OID;
        let mat_uuid = Uuid::new_v5(&ns, self.source_unit_id.as_bytes());
        let material_id = Id::<SourceMaterial>::from_uuid(mat_uuid);

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

        // Open the adapter stream.
        let mut stream = self
            .adapter
            .open(material_id, config, cursor)
            .await
            .map_err(|e| {
                crate::SinexError::processing("adapter open failed")
                    .with_context("source_unit_id", self.source_unit_id)
                    .with_context("adapter_kind", A::KIND.as_str())
                    .with_context("error", e.to_string())
            })?;

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
    type Config = serde_json::Value;
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
        let adapter_config: A::Config = serde_json::from_value(config).map_err(|e| {
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
        start: ContinuousStart,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> NodeResult<ScanReport> {
        let wall_start = Instant::now();
        let mut total_emitted: u64 = 0;

        // Poll interval for adapters without native streaming.
        // TODO: make configurable via config JSON field "poll_interval_secs".
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
