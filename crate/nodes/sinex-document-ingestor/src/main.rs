//! Main binary for the unified document ingestor
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_core::types::domain::SanitizedPath;
use sinex_document_ingestor::{DocumentIngestorConfig, DocumentProcessor};
use sinex_node_sdk::{
    stream_processor::{
        Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeType, ScanArgs, ScanEstimate,
        ScanReport, TimeHorizon,
    },
    NodeResult, SimpleIngestorWrapper,
};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct UnifiedDocumentNode(SimpleIngestorWrapper<DocumentProcessor>);

impl UnifiedDocumentNode {
    #[allow(dead_code)] // Convenience constructor
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for UnifiedDocumentNode {
    fn default() -> Self {
        Self(SimpleIngestorWrapper::default())
    }
}

#[async_trait]
impl Node for UnifiedDocumentNode {
    type Config = DocumentIngestorConfig;

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

impl ExplorationProvider for UnifiedDocumentNode {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        self.0.ingestor().get_source_state()
    }

    fn get_ingestion_history(
        &self,
        limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        self.0.ingestor().get_ingestion_history(limit)
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        self.0.ingestor().get_coverage_analysis(time_range)
    }

    fn export_data(
        &self,
        path: &SanitizedPath,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        self.0.ingestor().export_data(path, format)
    }
}

// Use the new unified architecture with macro
sinex_processor_runtime::processor_main!(UnifiedDocumentNode);
