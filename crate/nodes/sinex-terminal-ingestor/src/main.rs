//! Main binary for the unified terminal node
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_node_sdk::{
    runtime::stream::{
        Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeType, ScanArgs, ScanEstimate,
        ScanReport, TimeHorizon,
    },
    NodeResult, IngestorNodeAdapter,
};
use sinex_primitives::domain::SanitizedPath;
use sinex_primitives::temporal::Timestamp;
use sinex_node_sdk::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_terminal_ingestor::{TerminalConfig, TerminalNode};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Default)]
struct UnifiedTerminalNode(IngestorNodeAdapter<TerminalNode>);

impl UnifiedTerminalNode {
    #[allow(dead_code)] // Convenience constructor
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

// Implement Node by delegrating to IngestorNodeAdapter
impl Node for UnifiedTerminalNode {
    type Config = TerminalConfig;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        self.0.initialize(init).await
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        self.0.scan(from, until, args).await
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        self.0.shutdown().await
    }

    fn node_name(&self) -> &str {
        self.0.node_name()
    }

    fn node_type(&self) -> NodeType {
        self.0.node_type()
    }

    fn capabilities(&self) -> NodeCapabilities {
        self.0.capabilities()
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        self.0.current_checkpoint().await
    }

    async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        self.0.estimate_scan_scope(from, until, args).await
    }
}

// Implement ExplorationProvider by delegating to inner ingestor
impl ExplorationProvider for UnifiedTerminalNode {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        self.0.ingestor().get_source_state()
    }

    fn get_ingestion_history(&self, limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        self.0.ingestor().get_ingestion_history(limit)
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(Timestamp, Timestamp)>,
    ) -> NodeResult<CoverageAnalysis> {
        self.0.ingestor().get_coverage_analysis(time_range)
    }

    fn export_data(&self, path: &SanitizedPath, format: ExportFormat) -> NodeResult<()> {
        self.0.ingestor().export_data(path, format)
    }
}

// Use the new unified architecture with macro
sinex_node_sdk::node_entrypoint!(UnifiedTerminalNode);
