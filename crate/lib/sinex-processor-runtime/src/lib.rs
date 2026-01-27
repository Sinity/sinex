#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/cli_framework.md")]

//! Processor-facing CLI and runner utilities shared by all nodes.

pub mod cli;

pub use cli::{
    parse_checkpoint, parse_time_horizon, ActivityEntry, CoverageAnalysis, ExplorationProvider,
    ExportFormat, IngestionHistoryEntry, MissingItem, NodeCli, NodeCliRunner, NodeCommand,
    SourceState,
};

pub mod replay {
    pub use sinex_node_sdk::replay::{
        MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters, ReplayMetrics,
        ReplayMode, ReplayProgress, ReplayResult, ReplayService, ReplayStats,
    };
    pub use sinex_node_sdk::NodeError;

    use sinex_node_sdk::stream_processor::NodeRuntimeState;

    /// Extension helpers for building replay services from runtime state.
    pub trait ReplayRuntimeExt {
        fn replay_service(&self, mode: ReplayMode) -> ReplayService;
    }

    impl ReplayRuntimeExt for NodeRuntimeState {
        fn replay_service(&self, mode: ReplayMode) -> ReplayService {
            ReplayService::from_runtime(self, mode)
        }
    }
}
