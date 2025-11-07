//! Processor-facing CLI and runner utilities shared by all satellites.

pub mod cli;
pub mod runner;

pub use cli::{
    parse_checkpoint, parse_time_horizon, ActivityEntry, CoverageAnalysis, ExplorationProvider,
    ExportFormat, IngestionHistoryEntry, MissingItem, ProcessorCli, ProcessorCliRunner,
    ProcessorCommand, SourceState,
};
pub use runner::{ProcessorMode, ProcessorRunner, ProcessorRunnerConfig};

pub mod replay {
    pub use sinex_satellite_sdk::replay::{
        MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters, ReplayMetrics,
        ReplayMode, ReplayProgress, ReplayResult, ReplayService, ReplayStats,
    };
    pub use sinex_satellite_sdk::SatelliteError;

    use sinex_satellite_sdk::stream_processor::ProcessorRuntimeState;

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
