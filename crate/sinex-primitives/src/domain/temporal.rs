//! Temporal evidence and automaton timing vocabulary.

/// How the capture timestamp was determined for a material slice.
///
/// Stored as `source_type` in `raw.temporal_ledger`. Shared between schema
/// CHECK constraints, DB repositories, and runtime-side `LedgerReader`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalSourceType {
    /// Timestamp recorded at the moment of live data capture
    RealtimeCapture,
    /// Timestamp parsed from the content itself (e.g., log line timestamp)
    IntrinsicContent,
    /// Inferred from file modification time
    InferredMtime,
    /// Inferred from file creation time
    InferredCtime,
    /// User-provided timestamp
    InferredUser,
    /// Fallback: timestamp recorded when the slice was staged for ingestion
    StagedAt,
}

impl std::fmt::Display for TemporalSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RealtimeCapture => write!(f, "realtime_capture"),
            Self::IntrinsicContent => write!(f, "intrinsic_content"),
            Self::InferredMtime => write!(f, "inferred_mtime"),
            Self::InferredCtime => write!(f, "inferred_ctime"),
            Self::InferredUser => write!(f, "inferred_user"),
            Self::StagedAt => write!(f, "staged_at"),
        }
    }
}

impl std::str::FromStr for TemporalSourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "realtime_capture" => Ok(Self::RealtimeCapture),
            "intrinsic_content" => Ok(Self::IntrinsicContent),
            "inferred_mtime" => Ok(Self::InferredMtime),
            "inferred_ctime" => Ok(Self::InferredCtime),
            "inferred_user" => Ok(Self::InferredUser),
            "staged_at" => Ok(Self::StagedAt),
            _ => Err(format!("unknown temporal source type: {s}")),
        }
    }
}

/// Precision of a temporal ledger entry.
///
/// Stored as `precision` in `raw.temporal_ledger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalPrecision {
    /// Exact timestamp with no meaningful uncertainty
    Exact,
    /// Bounded timestamp with known or estimated uncertainty
    Bounded,
}

impl std::fmt::Display for TemporalPrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact => write!(f, "exact"),
            Self::Bounded => write!(f, "bounded"),
        }
    }
}

impl std::str::FromStr for TemporalPrecision {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "exact" => Ok(Self::Exact),
            "bounded" => Ok(Self::Bounded),
            _ => Err(format!("unknown temporal precision: {s}")),
        }
    }
}

/// Clock source used for a temporal ledger entry.
///
/// Stored as `clock` in `raw.temporal_ledger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalClock {
    /// Monotonic clock (guarantees ordering, not absolute time)
    Monotonic,
    /// Wall clock (real-time, subject to NTP adjustments)
    Wall,
}

impl std::fmt::Display for TemporalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Monotonic => write!(f, "monotonic"),
            Self::Wall => write!(f, "wall"),
        }
    }
}

impl std::str::FromStr for TemporalClock {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "monotonic" => Ok(Self::Monotonic),
            "wall" => Ok(Self::Wall),
            _ => Err(format!("unknown temporal clock: {s}")),
        }
    }
}

/// How a synthetic event's `ts_orig` was determined.
///
/// Declared per-output by automatons. Persisted as `temporal_policy`
/// on `core.events` for synthetic rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticTemporalPolicy {
    /// Inherit `ts_orig` from the single parent event (1:1 transforms)
    InheritParent,
    /// Use the latest contributing input's `ts_orig`
    LatestInput,
    /// Use the window boundary timestamp (e.g., window end)
    WindowBoundary,
    /// Use an explicitly declared effective timestamp from domain logic
    DeclaredEffective,
}

impl std::fmt::Display for SyntheticTemporalPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InheritParent => write!(f, "inherit_parent"),
            Self::LatestInput => write!(f, "latest_input"),
            Self::WindowBoundary => write!(f, "window_boundary"),
            Self::DeclaredEffective => write!(f, "declared_effective"),
        }
    }
}

impl std::str::FromStr for SyntheticTemporalPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "inherit_parent" => Ok(Self::InheritParent),
            "latest_input" => Ok(Self::LatestInput),
            "window_boundary" => Ok(Self::WindowBoundary),
            "declared_effective" => Ok(Self::DeclaredEffective),
            _ => Err(format!("unknown synthetic temporal policy: {s}")),
        }
    }
}

/// Classification of an automaton's computation model.
///
/// Each automaton must declare which model it uses, which determines
/// how the runtime prepares inputs and manages scope/window state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomatonModel {
    /// Processes one triggering event at a time; deterministic fallback order is `id ASC`
    Transducer,
    /// Declares window identity and completion logic; runtime prepares completed windows
    Windowed,
    /// Declares `trigger→scope_key` mapping; loads persisted working set for
    /// deterministic recomputation
    ScopeReconciler,
}

impl std::fmt::Display for AutomatonModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transducer => write!(f, "transducer"),
            Self::Windowed => write!(f, "windowed"),
            Self::ScopeReconciler => write!(f, "scope_reconciler"),
        }
    }
}

impl std::str::FromStr for AutomatonModel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "transducer" => Ok(Self::Transducer),
            "windowed" => Ok(Self::Windowed),
            "scope_reconciler" => Ok(Self::ScopeReconciler),
            _ => Err(format!("unknown automaton model: {s}")),
        }
    }
}

/// The mode in which a runtime module is currently processing events.
///
/// Provided via trigger context so module logic can distinguish live arrival
/// from historical scan, replay recomputation, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingMode {
    /// Normal live event arrival
    Live,
    /// Historical scan of existing material
    HistoricalScan,
    /// Replay-driven recomputation
    Replay,
    /// Late backfill of previously unseen material
    Backfill,
}

impl std::fmt::Display for ProcessingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::HistoricalScan => write!(f, "historical_scan"),
            Self::Replay => write!(f, "replay"),
            Self::Backfill => write!(f, "backfill"),
        }
    }
}

impl std::str::FromStr for ProcessingMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "historical_scan" => Ok(Self::HistoricalScan),
            "replay" => Ok(Self::Replay),
            "backfill" => Ok(Self::Backfill),
            _ => Err(format!("unknown processing mode: {s}")),
        }
    }
}

/// What caused an automaton to be triggered.
///
/// Provided in the trigger context so modules can distinguish between
/// new evidence, late backfill, scope invalidation, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    /// A new event arrived in the subscribed stream
    NewEvent,
    /// Late historical data was backfilled
    LateBackfill,
    /// An existing scope was invalidated (e.g., by archival)
    ScopeInvalidation,
    /// A replay operation triggered recomputation
    ReplayRecompute,
}

impl std::fmt::Display for TriggerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewEvent => write!(f, "new_event"),
            Self::LateBackfill => write!(f, "late_backfill"),
            Self::ScopeInvalidation => write!(f, "scope_invalidation"),
            Self::ReplayRecompute => write!(f, "replay_recompute"),
        }
    }
}

impl std::str::FromStr for TriggerKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "new_event" => Ok(Self::NewEvent),
            "late_backfill" => Ok(Self::LateBackfill),
            "scope_invalidation" => Ok(Self::ScopeInvalidation),
            "replay_recompute" => Ok(Self::ReplayRecompute),
            _ => Err(format!("unknown trigger kind: {s}")),
        }
    }
}

/// What happened to a persisted fact that triggered scope invalidation.
///
/// Carried by `DerivedScopeInvalidation` so automatons know whether
/// to recompute, archive their outputs, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationAction {
    /// A new event was inserted (live arrival or late backfill)
    Inserted,
    /// An existing event was archived (e.g., by replay)
    Archived,
    /// An event was replaced by a new version (archive + re-insert)
    Replaced,
}

impl std::fmt::Display for InvalidationAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inserted => write!(f, "inserted"),
            Self::Archived => write!(f, "archived"),
            Self::Replaced => write!(f, "replaced"),
        }
    }
}

impl std::str::FromStr for InvalidationAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "inserted" => Ok(Self::Inserted),
            "archived" => Ok(Self::Archived),
            "replaced" => Ok(Self::Replaced),
            _ => Err(format!("unknown invalidation action: {s}")),
        }
    }
}
