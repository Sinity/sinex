//! SimpleIngestor trait for reducing boilerplate in ingestor nodes.
//!
//! This module provides a high-level abstraction (similar to `SimpleNode`) but tailored
//! for Ingestors, which typically produce events from external sources rather than
//! transforming input events.
//!
//! Key features:
//! - Automated lifecycle management (initialize, shutdown)
//! - State persistence (Checkpoints)
//! - Standardized `scan` dispatching (Snapshot, Historical, Continuous)

use async_trait::async_trait;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::checkpoint::{CheckpointManager, CheckpointState};
use crate::shutdown::ShutdownConfig;
use crate::stream_processor::{
    Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType, ScanArgs,
    ScanReport, TimeHorizon,
};
use crate::{
    exploration::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    },
    NodeResult, SimpleNodeConfig, SinexError,
};
use sinex_primitives::SanitizedPath;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::info;

// Re-use PersistedState from simple_node or define a new one?
// For now, let's redefine similar structure to decouple or use the one from simple_node if public.
// It is public in simple_node.rs, but that module might be gated or the struct specific.
// Let's define our own here for clarity, or import if we can make it shared.
// simple_node::PersistedState is generic. Let's try to use it if accessible, otherwise duplicate.
// It is `pub struct PersistedState<S>`.

/// Wrapper around user state to include metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorState<S> {
    pub user_state: S,
    pub last_checkpoint: sinex_primitives::temporal::OffsetDateTime,
    pub revision: u64,
}

impl<S: Default> Default for IngestorState<S> {
    fn default() -> Self {
        Self {
            user_state: S::default(),
            last_checkpoint: sinex_primitives::temporal::OffsetDateTime::now_utc(),
            revision: 0,
        }
    }
}

/// Trait for simplified Ingestor implementation.
#[async_trait]
pub trait SimpleIngestor: Send + Sync + 'static {
    /// Configuration type (from config file/env)
    type Config: Clone + Send + Sync + Serialize + DeserializeOwned + Default;

    /// Persistent state type
    type State: Clone + Send + Sync + Default + Serialize + DeserializeOwned;

    /// Name of the ingestor
    fn name(&self) -> &str;

    /// Capabilities description
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_snapshot: true,
            supports_historical: true,
            supports_continuous: true,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: true,
        }
    }

    /// Initialize the ingestor logic.
    /// Called after state is loaded and runtime is set up.
    async fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &NodeRuntimeState,
        state: &mut Self::State,
    ) -> NodeResult<()>;

    /// Perform a snapshot scan.
    async fn scan_snapshot(&self, state: &Self::State, args: ScanArgs) -> NodeResult<ScanReport>;

    /// Perform a historical scan.
    async fn scan_historical(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport>;

    /// Run continuous ingestion loop.
    async fn run_continuous(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        shutdown_rx: watch::Receiver<bool>,
    ) -> NodeResult<ScanReport>;

    /// Optional shutdown hook
    async fn shutdown(&mut self, _state: &Self::State) -> NodeResult<()> {
        Ok(())
    }

    // Exploration provider methods
    fn get_source_state(&self, _state: &Self::State) -> NodeResult<SourceState> {
        Err(SinexError::processing(
            "Source state exploration not implemented",
        ))
    }

    fn get_ingestion_history(
        &self,
        _state: &Self::State,
        _limit: u64,
    ) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Err(SinexError::processing("Ingestion history not implemented"))
    }

    fn get_coverage_analysis(
        &self,
        _state: &Self::State,
        _time_range: Option<(
            sinex_primitives::temporal::OffsetDateTime,
            sinex_primitives::temporal::OffsetDateTime,
        )>,
    ) -> NodeResult<CoverageAnalysis> {
        Err(SinexError::processing("Coverage analysis not implemented"))
    }

    fn export_data(
        &self,
        _state: &Self::State,
        _path: &SanitizedPath,
        _format: ExportFormat,
    ) -> NodeResult<()> {
        Err(SinexError::processing("Data export not implemented"))
    }
}

/// Wrapper implementing `Node` for `SimpleIngestor`.
pub struct SimpleIngestorWrapper<I: SimpleIngestor> {
    ingestor: I,
    state: IngestorState<I::State>,
    config: SimpleNodeConfig,
    shutdown_config: ShutdownConfig,
    runtime: Option<NodeRuntimeState>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl<I: SimpleIngestor> SimpleIngestorWrapper<I> {
    pub fn new(ingestor: I) -> Self {
        Self {
            ingestor,
            state: IngestorState::default(),
            config: SimpleNodeConfig::default(),
            shutdown_config: ShutdownConfig::default(),
            runtime: None,
            checkpoint_manager: None,
            shutdown_tx: None,
        }
    }

    pub fn with_config(mut self, config: SimpleNodeConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_shutdown_config(mut self, config: ShutdownConfig) -> Self {
        self.shutdown_config = config;
        self
    }

    pub fn ingestor(&self) -> &I {
        &self.ingestor
    }
}

impl<I: SimpleIngestor + Default> Default for SimpleIngestorWrapper<I> {
    fn default() -> Self {
        Self::new(I::default())
    }
}

impl<I: SimpleIngestor> SimpleIngestorWrapper<I> {
    async fn load_state(&mut self) -> NodeResult<()> {
        // 1. Try file (hot reload)
        if self.shutdown_config.restore_state_on_startup {
            let path = self.shutdown_config.checkpoint_path(self.ingestor.name());
            if let Some(ckpt) = CheckpointState::load_from_file(&path).await {
                if let Some(data) = ckpt.data {
                    if let Ok(s) = serde_json::from_value(data) {
                        self.state = s;
                        let _ = CheckpointState::delete_file(&path).await;
                        return Ok(());
                    }
                }
            }
        }

        // 2. Try NATS KV
        if let Some(cm) = &self.checkpoint_manager {
            let ckpt = cm.load_checkpoint().await?;
            if let Some(data) = ckpt.data {
                if let Ok(s) = serde_json::from_value(data) {
                    self.state = s;
                    self.state.revision = ckpt.revision;
                }
            } else {
                self.state = IngestorState::default();
            }
        }

        Ok(())
    }

    async fn save_state(&mut self, is_shutdown: bool) -> NodeResult<()> {
        self.state.last_checkpoint = sinex_primitives::temporal::OffsetDateTime::now_utc();
        let json_state =
            serde_json::to_value(&self.state).map_err(|e| SinexError::serialization(e))?;

        let ckpt_state = CheckpointState {
            checkpoint: Checkpoint::external(
                serde_json::json!({"v": 1}), // opaque
                format!("ingestor_{}", self.ingestor.name()),
            ),
            processed_count: 0, // Ingestors might track this in user state if needed
            last_activity: sinex_primitives::temporal::OffsetDateTime::now_utc(),
            data: Some(json_state),
            version: 1,
            revision: self.state.revision,
        };

        if is_shutdown && self.shutdown_config.save_state_on_shutdown {
            let path = self.shutdown_config.checkpoint_path(self.ingestor.name());
            ckpt_state
                .save_to_file(&path)
                .await
                .map_err(|e| SinexError::io(e))?;
        }

        if let Some(cm) = &self.checkpoint_manager {
            self.state.revision = cm.save_checkpoint(&ckpt_state).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<I: SimpleIngestor> Node for SimpleIngestorWrapper<I> {
    type Config = I::Config;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.checkpoint_manager = Some(runtime.checkpoint_manager().clone());
        self.runtime = Some(runtime.clone());

        self.load_state().await?;

        self.ingestor
            .initialize(config, &runtime, &mut self.state.user_state)
            .await?;

        info!("SimpleIngestor {} initialized", self.ingestor.name());
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let report = match until {
            TimeHorizon::Snapshot => {
                self.ingestor
                    .scan_snapshot(&self.state.user_state, args)
                    .await?
            }
            TimeHorizon::Historical { .. } => {
                self.ingestor
                    .scan_historical(&mut self.state.user_state, from, until, args)
                    .await?
            }
            TimeHorizon::Continuous => {
                let (tx, rx) = watch::channel(false);
                self.shutdown_tx = Some(tx);
                self.ingestor
                    .run_continuous(&mut self.state.user_state, from, rx)
                    .await?
            }
        };

        self.save_state(false).await?;
        Ok(report)
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        self.ingestor.shutdown(&self.state.user_state).await?;
        self.save_state(true).await?;
        Ok(())
    }

    fn node_name(&self) -> &str {
        self.ingestor.name()
    }

    fn node_type(&self) -> NodeType {
        NodeType::Ingestor
    }

    fn capabilities(&self) -> NodeCapabilities {
        self.ingestor.capabilities()
    }

    // Default current_checkpoint impl which returns None or we could implement it
    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::None) // Ingestors often manage checkpointing internally or via the state saving
    }
}

impl<I: SimpleIngestor> ExplorationProvider for SimpleIngestorWrapper<I> {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        self.ingestor.get_source_state(&self.state.user_state)
    }

    fn get_ingestion_history(&self, limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        self.ingestor
            .get_ingestion_history(&self.state.user_state, limit)
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(
            sinex_primitives::temporal::OffsetDateTime,
            sinex_primitives::temporal::OffsetDateTime,
        )>,
    ) -> NodeResult<CoverageAnalysis> {
        self.ingestor
            .get_coverage_analysis(&self.state.user_state, time_range)
    }

    fn export_data(&self, path: &SanitizedPath, format: ExportFormat) -> NodeResult<()> {
        self.ingestor
            .export_data(&self.state.user_state, path, format)
    }
}
