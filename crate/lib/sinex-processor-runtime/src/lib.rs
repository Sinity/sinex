//! Processor-facing CLI and runner utilities shared by all satellites.

pub mod cli;

pub use cli::{
    parse_checkpoint, parse_time_horizon, ActivityEntry, CoverageAnalysis, ExplorationProvider,
    ExportFormat, IngestionHistoryEntry, MissingItem, ProcessorCli, ProcessorCliRunner,
    ProcessorCommand, SourceState,
};

pub mod replay {
    pub use sinex_node_sdk::replay::{
        MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters, ReplayMetrics,
        ReplayMode, ReplayProgress, ReplayResult, ReplayService, ReplayStats,
    };
    pub use sinex_node_sdk::NodeError;

    use sinex_node_sdk::stream_processor::ProcessorRuntimeState;

    /// Extension helpers for building replay services from runtime state.
    pub trait ReplayRuntimeExt {
        fn replay_service(&self, mode: ReplayMode) -> ReplayService;
    }

    impl ReplayRuntimeExt for ProcessorRuntimeState {
        fn replay_service(&self, mode: ReplayMode) -> ReplayService {
            ReplayService::from_runtime(self, mode)
        }
    }
}
