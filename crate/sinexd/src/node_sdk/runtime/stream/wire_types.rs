//! Wire-format and configuration data types for stream nodes.
//!
//! Pure data structures plus their `Default`/`Display` impls. No runtime logic.

use super::{Checkpoint, TimeHorizon};
use serde::{Deserialize, Serialize};
use sinex_db::SourceMaterialRecord;
use sinex_primitives::{Timestamp, Uuid};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Default)]
pub struct SchemaBroadcastCache {
    schemas: Arc<RwLock<Vec<SchemaBroadcastEntry>>>,
}

impl SchemaBroadcastCache {
    pub async fn update(&self, entries: Vec<SchemaBroadcastEntry>) {
        let mut guard = self.schemas.write().await;
        *guard = entries;
    }

    pub async fn get(&self) -> Vec<SchemaBroadcastEntry> {
        self.schemas.read().await.clone()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaBroadcastEntry {
    pub name: String,
    pub version: String,
    pub schema_id: String,
}

/// Coordinator-resolved replay metadata passed into node scans.
///
/// When a replay operation triggers a historical scan, the coordinator resolves the
/// source material record and scope filters once, then passes them typed into the node.
/// This prevents nodes from re-querying `source_material_registry` as a second authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedReplayMaterial {
    /// Stable registry identity of the source material.
    pub source_material_id: Uuid,

    /// Stored source-material class. Current persisted values still include
    /// legacy storage-shaped names such as `annex`.
    pub material_kind: String,

    /// Source identifier (for example file path or upstream URI).
    pub source_identifier: String,

    /// Registry metadata for the material.
    pub material_metadata: serde_json::Value,

    /// Material start bound, if known.
    pub material_start_time: Option<Timestamp>,

    /// Material end bound, if known.
    pub material_end_time: Option<Timestamp>,
}

impl From<SourceMaterialRecord> for ResolvedReplayMaterial {
    fn from(record: SourceMaterialRecord) -> Self {
        Self {
            source_material_id: record.id,
            material_kind: record.material_kind,
            source_identifier: record.source_identifier,
            material_metadata: record.metadata,
            material_start_time: record.start_time,
            material_end_time: record.end_time,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterialReplayContext {
    /// Unique ID for this replay operation (for correlation and idempotency).
    pub operation_id: Uuid,

    /// Fully resolved source materials covered by this replay scope.
    pub materials: Vec<ResolvedReplayMaterial>,

    /// Scope filters narrowing what to replay within the material.
    pub replay_scope: ReplayScopeFilters,
}

/// Scope filters for replay operations, narrowing what to replay within a material.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayScopeFilters {
    /// Restrict replay to specific source materials.
    pub material_ids: Option<Vec<Uuid>>,

    /// Restrict replay to specific event types.
    pub event_types: Option<Vec<String>>,
}

/// Scan operation arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanArgs {
    /// Paths to scan (for ingestors) or filters (for automata)
    pub targets: Vec<String>,

    /// Dry run mode - analyze but don't emit events
    pub dry_run: bool,

    /// Interactive mode - prompt user for decisions
    pub interactive: bool,

    /// Maximum events to process (0 = unlimited)
    pub max_events: u64,

    /// Skip duplicate detection
    pub skip_duplicates: bool,

    /// Node-specific configuration
    pub config: HashMap<String, serde_json::Value>,

    /// Replay context when this scan was triggered by a material replay operation.
    /// `None` for normal (non-replay) scans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<MaterialReplayContext>,
}

impl Default for ScanArgs {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            dry_run: false,
            interactive: false,
            max_events: 0,
            skip_duplicates: true,
            config: HashMap::new(),
            replay: None,
        }
    }
}

/// Start context for a continuous ingestion loop.
///
/// The SDK startup runner performs snapshot and bounded gap-fill before it
/// constructs this value. The embedded checkpoint is a live-tail resume cursor,
/// not permission for a node to widen continuous startup into a historical scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousStart {
    checkpoint: Checkpoint,
}

impl ContinuousStart {
    #[must_use]
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Self {
        Self { checkpoint }
    }

    #[must_use]
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}

// ── Node-Dispatch Replay Wire Types ──────────────────────────────────────────
//
// These types implement the node-dispatch replay protocol. Instead of the
// gateway republishing stored event rows to NATS (reinjection), it dispatches
// a scan command to the running ingestor node. The node re-reads source material
// through its normal scan_historical() path and emits fresh events.
//
// Protocol:
//   gateway → NATS request `sinex.control.nodes.<name>.scan` (NodeScanCommand)
//   node    → NATS reply (NodeScanAck)
//   node    → NATS publish `sinex.control.replay.progress.<operation_id>` (NodeScanProgress)

/// Command dispatched to a running node to trigger a scan.
/// Published to `sinex.control.nodes.<name>.scan` via NATS request-reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeScanCommand {
    /// Unique identifier for this replay operation (correlation + idempotency).
    pub operation_id: Uuid,
    /// Resume from this checkpoint (usually `Checkpoint::None` for full replay).
    pub from: Checkpoint,
    /// Scan horizon — `Historical` with an `end_time` for replay.
    pub until: TimeHorizon,
    /// Scan arguments including `MaterialReplayContext` in `args.replay`.
    pub args: ScanArgs,
}

/// Acknowledgement from node after receiving scan command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeScanAck {
    /// Correlates with the `NodeScanCommand.operation_id`.
    pub operation_id: Uuid,
    /// Node that received the command.
    pub node_name: String,
    /// Whether the command was accepted.
    pub accepted: bool,
    /// Error message if rejected (e.g., scan already in progress, not an ingestor).
    pub error: Option<String>,
}

/// Progress update published by node during dispatched scan.
/// Published to `sinex.control.replay.progress.<operation_id>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeScanProgress {
    /// Correlates with the `NodeScanCommand.operation_id`.
    pub operation_id: Uuid,
    /// Node executing the scan.
    pub node_name: String,
    /// Events processed so far.
    pub events_processed: u64,
    /// Events emitted (may be fewer than processed if filtering).
    pub events_emitted: u64,
    /// Final report when scan completes (None while in progress).
    pub final_report: Option<ScanReport>,
    /// Terminal error when the scan could not complete.
    pub error: Option<String>,
}

/// Report from a completed scan operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanReport {
    /// Total events processed/generated
    pub events_processed: u64,

    /// Duration of the scan operation
    pub duration: std::time::Duration,

    /// Final checkpoint after scan
    pub final_checkpoint: Checkpoint,

    /// Time range covered by the scan
    pub time_range: Option<(
        sinex_primitives::temporal::Timestamp,
        sinex_primitives::temporal::Timestamp,
    )>,

    /// Node-specific statistics
    pub node_stats: HashMap<String, u64>,

    /// Targets that were successfully processed
    pub successful_targets: Vec<String>,

    /// Targets that failed processing with error messages
    pub failed_targets: Vec<(String, String)>,

    /// Warnings encountered during processing
    pub warnings: Vec<String>,
}

/// Re-export from sinex-primitives so SDK consumers see the canonical three-variant enum
/// (`Ingestor | Automaton | Service`). The former two-variant local copy dropped `Service`
/// silently during RPC round-trips — see issue #746 (A5).
pub use sinex_primitives::domain::NodeType;

/// Node capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Supports continuous scanning (sensor mode)
    pub supports_continuous: bool,

    /// Supports historical scanning
    pub supports_historical: bool,

    /// Supports snapshot scanning
    pub supports_snapshot: bool,

    /// Supports interactive mode
    pub supports_interactive: bool,

    /// Maximum recommended scan size
    pub max_scan_size: Option<u64>,

    /// Supports concurrent processing
    pub supports_concurrent: bool,

    /// Node manages its own continuous loop (runner skips `JetStream` bridge)
    pub manages_own_continuous_loop: bool,

    /// Node persists its own event-processing checkpoint/state.
    ///
    /// When true, the generic automaton bridge must not create or advance a
    /// second checkpoint entry for the same runtime, because that would race
    /// with the node-owned state snapshot and can clobber its payload.
    pub manages_own_checkpoints: bool,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            supports_continuous: true,
            supports_historical: true,
            supports_snapshot: false,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: false,
            manages_own_checkpoints: false,
        }
    }
}

/// Scan operation estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanEstimate {
    /// Estimated number of events to be processed
    pub estimated_events: u64,

    /// Estimated processing duration
    pub estimated_duration: std::time::Duration,

    /// Estimated data size to be processed
    pub estimated_data_size: u64,

    /// Number of targets that will be processed
    pub estimated_targets: u64,

    /// Warnings about potential issues
    pub warnings: Vec<String>,

    /// Confidence level of estimate (0.0 to 1.0)
    pub confidence: f32,
}

impl Default for ScanEstimate {
    fn default() -> Self {
        Self {
            estimated_events: 0,
            estimated_duration: std::time::Duration::from_secs(0),
            estimated_data_size: 0,
            estimated_targets: 0,
            warnings: Vec::new(),
            confidence: 0.0,
        }
    }
}

/// Lifecycle state of a [`NodeRunner`].
///
/// Guards against re-entrant calls to `initialize`, `run_service`/`run_scan`,
/// and `shutdown`. State transitions are strictly forward-only:
///
/// ```text
/// Created ──► Initializing ──► Initialized ──► Running ──► ShutDown
///                                                  │
///                                                  └──► ShutdownFailed
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerLifecycle {
    /// Freshly constructed, not yet initialized.
    Created,
    /// `initialize_with_transport` is executing.
    Initializing,
    /// Initialization complete; ready for `run_service` / `run_scan`.
    Initialized,
    /// `run_service` or `run_scan` is executing.
    Running,
    /// `shutdown` failed and the runner is in a partially torn-down state.
    ShutdownFailed,
    /// `shutdown` has completed (or was never initialized).
    ShutDown,
}

impl std::fmt::Display for RunnerLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initializing => write!(f, "Initializing"),
            Self::Initialized => write!(f, "Initialized"),
            Self::Running => write!(f, "Running"),
            Self::ShutdownFailed => write!(f, "ShutdownFailed"),
            Self::ShutDown => write!(f, "ShutDown"),
        }
    }
}
