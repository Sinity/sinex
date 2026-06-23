use serde::Serialize;

/// Concrete resource ceiling a binding declares, used to derive the systemd
/// unit's `MemoryMax`/`CPUWeight` when the deployment unit is generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResourceLimits {
    /// Hard memory ceiling in MiB.
    pub memory_max_mib: u32,
    /// systemd `CPUWeight` (1–10000; 100 = default share).
    pub cpu_weight: u16,
}

/// Runtime work class used to group package budget expectations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkClass {
    Interactive,
    AdmissionHot,
    CaptureLive,
    ProjectionHot,
    ProjectionCold,
    BulkImport,
    Maintenance,
}

/// Operator-visible actions a runtime can take when a package is under pressure.
///
/// These are operational pressure responses only. They do not authorize schema
/// changes, hidden disclosure changes, silent material deletion, or bypassing
/// admission policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetPressureAction {
    Throttle,
    Defer,
    Pause,
    Drain,
    Inspect,
    Retry,
}

/// Package-level resource budget derived from a [`ResourceProfile`].
///
/// [`ResourceLimits`] remains the deployment ceiling consumed by the Nix catalog.
/// This richer budget is the Sinex-side contract for package completeness,
/// pressure visibility, and future runtime controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ResourceBudgetSpec {
    pub work_class: WorkClass,
    pub steady_memory_mib: u32,
    pub burst_memory_mib: u32,
    pub cpu_weight: u16,
    pub max_input_bytes_per_sec: Option<u64>,
    pub max_input_events_per_sec: Option<u32>,
    pub max_pending_material_bytes: u64,
    pub max_pending_candidates: u32,
    pub max_unacked_transport_messages: Option<u32>,
    pub batch_size: Option<u32>,
    pub flush_interval_ms: Option<u64>,
    pub checkpoint_interval_ms: Option<u64>,
    pub expected_disk_write_bytes_per_min: Option<u64>,
    pub expected_wal_write_bytes_per_min: Option<u64>,
    pub pressure_actions: &'static [BudgetPressureAction],
}

/// Resource profile of a source binding.
///
/// Replaces the former free-form `resource_shape` string. Each variant maps to a
/// concrete [`ResourceLimits`] ceiling so the deployment unit's limits are a
/// typed function of the declared profile rather than a hand-set number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceProfile {
    /// Reads a bounded file/scan (history files, export files, watched files).
    BoundedFile,
    /// Streams rows with bounded working memory (sqlite/db row cursors).
    BoundedStream,
    /// Long-lived watcher with low steady-state memory (sockets, signals, polls).
    LiveWatcher,
    /// Walks a directory tree; memory bounded by the walk, not the tree size.
    DirectoryScan,
    /// Runs once over a bounded input then exits (on-demand batch).
    Oneshot,
    /// Consumes the derived-event stream (automata).
    EventStreamConsumer,
    /// Emits telemetry from within a running binary (embedded emitters).
    EmbeddedEmitter,
}

const THROTTLE_DEFER_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Throttle,
    BudgetPressureAction::Defer,
    BudgetPressureAction::Inspect,
];

const PAUSE_DRAIN_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Pause,
    BudgetPressureAction::Drain,
    BudgetPressureAction::Inspect,
];

const THROTTLE_PAUSE_DRAIN_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Throttle,
    BudgetPressureAction::Pause,
    BudgetPressureAction::Drain,
    BudgetPressureAction::Inspect,
];

const THROTTLE_DEFER_RETRY_INSPECT: &[BudgetPressureAction] = &[
    BudgetPressureAction::Throttle,
    BudgetPressureAction::Defer,
    BudgetPressureAction::Retry,
    BudgetPressureAction::Inspect,
];

impl ResourceProfile {
    /// Concrete systemd resource ceiling for this profile.
    #[must_use]
    pub const fn limits(self) -> ResourceLimits {
        match self {
            Self::BoundedFile | Self::Oneshot => ResourceLimits {
                memory_max_mib: 256,
                cpu_weight: 100,
            },
            Self::BoundedStream => ResourceLimits {
                memory_max_mib: 512,
                cpu_weight: 100,
            },
            Self::LiveWatcher | Self::EmbeddedEmitter => ResourceLimits {
                memory_max_mib: 128,
                cpu_weight: 80,
            },
            Self::DirectoryScan => ResourceLimits {
                memory_max_mib: 1024,
                cpu_weight: 120,
            },
            Self::EventStreamConsumer => ResourceLimits {
                memory_max_mib: 512,
                cpu_weight: 120,
            },
        }
    }

    /// Package budget contract derived from this profile.
    #[must_use]
    pub const fn budget_spec(self) -> ResourceBudgetSpec {
        let limits = self.limits();
        match self {
            Self::BoundedFile | Self::Oneshot => ResourceBudgetSpec {
                work_class: WorkClass::BulkImport,
                steady_memory_mib: 128,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(16 * 1024 * 1024),
                max_input_events_per_sec: None,
                max_pending_material_bytes: 64 * 1024 * 1024,
                max_pending_candidates: 10_000,
                max_unacked_transport_messages: None,
                batch_size: Some(1_000),
                flush_interval_ms: Some(1_000),
                checkpoint_interval_ms: Some(5_000),
                expected_disk_write_bytes_per_min: Some(512 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(256 * 1024 * 1024),
                pressure_actions: THROTTLE_DEFER_INSPECT,
            },
            Self::BoundedStream => ResourceBudgetSpec {
                work_class: WorkClass::AdmissionHot,
                steady_memory_mib: 256,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(32 * 1024 * 1024),
                max_input_events_per_sec: Some(10_000),
                max_pending_material_bytes: 128 * 1024 * 1024,
                max_pending_candidates: 25_000,
                max_unacked_transport_messages: Some(1_000),
                batch_size: Some(2_000),
                flush_interval_ms: Some(500),
                checkpoint_interval_ms: Some(2_000),
                expected_disk_write_bytes_per_min: Some(1024 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(512 * 1024 * 1024),
                pressure_actions: THROTTLE_DEFER_RETRY_INSPECT,
            },
            Self::LiveWatcher | Self::EmbeddedEmitter => ResourceBudgetSpec {
                work_class: WorkClass::CaptureLive,
                steady_memory_mib: 64,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(1024 * 1024),
                max_input_events_per_sec: Some(1_000),
                max_pending_material_bytes: 8 * 1024 * 1024,
                max_pending_candidates: 1_000,
                max_unacked_transport_messages: Some(256),
                batch_size: Some(128),
                flush_interval_ms: Some(250),
                checkpoint_interval_ms: Some(1_000),
                expected_disk_write_bytes_per_min: Some(64 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(64 * 1024 * 1024),
                pressure_actions: THROTTLE_PAUSE_DRAIN_INSPECT,
            },
            Self::DirectoryScan => ResourceBudgetSpec {
                work_class: WorkClass::BulkImport,
                steady_memory_mib: 512,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(64 * 1024 * 1024),
                max_input_events_per_sec: None,
                max_pending_material_bytes: 256 * 1024 * 1024,
                max_pending_candidates: 50_000,
                max_unacked_transport_messages: None,
                batch_size: Some(5_000),
                flush_interval_ms: Some(1_000),
                checkpoint_interval_ms: Some(10_000),
                expected_disk_write_bytes_per_min: Some(2048 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(1024 * 1024 * 1024),
                pressure_actions: PAUSE_DRAIN_INSPECT,
            },
            Self::EventStreamConsumer => ResourceBudgetSpec {
                work_class: WorkClass::ProjectionHot,
                steady_memory_mib: 256,
                burst_memory_mib: limits.memory_max_mib,
                cpu_weight: limits.cpu_weight,
                max_input_bytes_per_sec: Some(16 * 1024 * 1024),
                max_input_events_per_sec: Some(20_000),
                max_pending_material_bytes: 32 * 1024 * 1024,
                max_pending_candidates: 20_000,
                max_unacked_transport_messages: Some(2_000),
                batch_size: Some(2_000),
                flush_interval_ms: Some(500),
                checkpoint_interval_ms: Some(1_000),
                expected_disk_write_bytes_per_min: Some(512 * 1024 * 1024),
                expected_wal_write_bytes_per_min: Some(512 * 1024 * 1024),
                pressure_actions: THROTTLE_DEFER_RETRY_INSPECT,
            },
        }
    }
}
