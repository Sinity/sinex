//! `IngestorNode` trait for reducing boilerplate in ingestor nodes.
//!
//! This module provides a high-level abstraction (similar to `AutomatonNode`) but tailored
//! for Ingestors, which typically produce events from external sources rather than
//! transforming input events.
//!
//! Key features:
//! - Automated lifecycle management (initialize, shutdown)
//! - State persistence (Checkpoints)
//! - Standardized `scan` dispatching (Snapshot, Historical, Continuous)

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::checkpoint::{CheckpointManager, CheckpointState, decode_checkpoint_data};
use crate::runtime::stream::{
    Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType, ScanArgs,
    ScanReport, TimeHorizon,
};
use crate::shutdown::ShutdownConfig;
use crate::{
    NodeResult, SinexError,
    exploration::{
        CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
    },
};
use sinex_primitives::SanitizedPath;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

fn signal_shutdown_channel(tx: watch::Sender<bool>, node_name: &str) -> bool {
    if tx.send(true).is_err() {
        warn!(
            node = node_name,
            "Ingestor shutdown receiver was already dropped before graceful shutdown"
        );
        return false;
    }
    true
}

/// Adapter state around user state with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorState<S> {
    pub user_state: S,
    pub last_checkpoint: sinex_primitives::temporal::Timestamp,
    pub revision: u64,
    #[serde(default)]
    pub checkpoint: Checkpoint,
}

impl<S: Default> Default for IngestorState<S> {
    fn default() -> Self {
        Self {
            user_state: S::default(),
            last_checkpoint: sinex_primitives::temporal::Timestamp::now(),
            revision: 0,
            checkpoint: Checkpoint::None,
        }
    }
}

/// Trait for simplified Ingestor implementation.
pub trait IngestorNode: Send + Sync + 'static {
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
    fn initialize(
        &mut self,
        config: Self::Config,
        runtime: &NodeRuntimeState,
        state: &mut Self::State,
    ) -> impl std::future::Future<Output = NodeResult<()>> + Send;

    /// Perform a snapshot scan.
    fn scan_snapshot(
        &mut self,
        state: &mut Self::State,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = NodeResult<ScanReport>> + Send;

    /// Perform a historical scan.
    fn scan_historical(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = NodeResult<ScanReport>> + Send;

    /// Run continuous ingestion loop.
    fn run_continuous(
        &mut self,
        state: &mut Self::State,
        from: Checkpoint,
        shutdown_rx: watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = NodeResult<ScanReport>> + Send;

    /// Optional shutdown hook
    fn shutdown(
        &mut self,
        _state: &Self::State,
    ) -> impl std::future::Future<Output = NodeResult<()>> + Send {
        async { Ok(()) }
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
            sinex_primitives::temporal::Timestamp,
            sinex_primitives::temporal::Timestamp,
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

/// Adapter implementing `Node` for `IngestorNode`.
pub struct IngestorNodeAdapter<I: IngestorNode> {
    ingestor: I,
    state: IngestorState<I::State>,
    shutdown_config: ShutdownConfig,
    runtime: Option<NodeRuntimeState>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl<I: IngestorNode> IngestorNodeAdapter<I> {
    pub fn new(ingestor: I) -> Self {
        Self {
            ingestor,
            state: IngestorState::default(),
            shutdown_config: ShutdownConfig::default(),
            runtime: None,
            checkpoint_manager: None,
            shutdown_tx: None,
        }
    }

    pub fn with_shutdown_config(mut self, config: ShutdownConfig) -> Self {
        self.shutdown_config = config;
        self
    }

    pub fn ingestor(&self) -> &I {
        &self.ingestor
    }
}

impl<I: IngestorNode + Default> Default for IngestorNodeAdapter<I> {
    fn default() -> Self {
        Self::new(I::default())
    }
}

impl<I: IngestorNode> IngestorNodeAdapter<I> {
    fn effective_final_checkpoint(
        until: &TimeHorizon,
        previous_checkpoint: &Checkpoint,
        reported_checkpoint: Checkpoint,
    ) -> Checkpoint {
        if matches!(until, TimeHorizon::Snapshot)
            && matches!(reported_checkpoint, Checkpoint::None)
            && !matches!(previous_checkpoint, Checkpoint::None)
        {
            return previous_checkpoint.clone();
        }

        reported_checkpoint
    }

    async fn load_state(&mut self) -> NodeResult<()> {
        // 1. Try file (hot reload)
        if self.shutdown_config.restore_state_on_startup {
            let path = self.shutdown_config.checkpoint_path(self.ingestor.name());
            if let Some(ckpt) = CheckpointState::load_from_file(&path).await? {
                let data = ckpt.data.ok_or_else(|| {
                    SinexError::checkpoint("Hot reload ingestor checkpoint file is missing state data")
                        .with_context("node", self.ingestor.name())
                        .with_context("path", path.display().to_string())
                })?;
                self.state =
                    decode_checkpoint_data(data, "hot reload ingestor state", self.ingestor.name())?;
                if matches!(self.state.checkpoint, Checkpoint::None)
                    && !matches!(ckpt.checkpoint, Checkpoint::None)
                {
                    self.state.checkpoint = ckpt.checkpoint;
                }
                CheckpointState::delete_file(&path).await.map_err(|error| {
                    SinexError::io("Failed to delete restored checkpoint file")
                        .with_context("node", self.ingestor.name())
                        .with_context("path", path.display().to_string())
                        .with_std_error(&error)
                })?;
                return Ok(());
            }
        }

        // 2. Try NATS KV
        if let Some(cm) = &self.checkpoint_manager {
            let ckpt = cm.load_checkpoint().await?;
            let data = ckpt.data.ok_or_else(|| {
                SinexError::checkpoint("Ingestor checkpoint KV entry is missing state data")
                    .with_context("node", self.ingestor.name())
            })?;
            self.state = decode_checkpoint_data(data, "ingestor checkpoint state", self.ingestor.name())?;
            self.state.revision = ckpt.revision;
            if matches!(self.state.checkpoint, Checkpoint::None)
                && !matches!(ckpt.checkpoint, Checkpoint::None)
            {
                self.state.checkpoint = ckpt.checkpoint;
            }
        }

        Ok(())
    }

    async fn save_state(&mut self, is_shutdown: bool) -> NodeResult<()> {
        self.state.last_checkpoint = sinex_primitives::temporal::Timestamp::now();
        let json_state = serde_json::to_value(&self.state).map_err(SinexError::serialization)?;

        let ckpt_state = CheckpointState {
            checkpoint: self.state.checkpoint.clone(),
            processed_count: 0, // Ingestors might track this in user state if needed
            last_activity: sinex_primitives::temporal::Timestamp::now(),
            data: Some(json_state),
            version: 1,
            revision: self.state.revision,
        };

        if is_shutdown && self.shutdown_config.save_state_on_shutdown {
            let path = self.shutdown_config.checkpoint_path(self.ingestor.name());
            ckpt_state
                .save_to_file(&path)
                .await
                .map_err(SinexError::io)?;
        }

        if let Some(cm) = &self.checkpoint_manager {
            self.state.revision = cm.save_checkpoint(&ckpt_state).await?;
        }

        Ok(())
    }
}

impl<I: IngestorNode> Node for IngestorNodeAdapter<I> {
    type Config = I::Config;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (config, runtime) = init.into_runtime();
        self.checkpoint_manager = Some(runtime.checkpoint_manager().clone());
        self.runtime = Some(runtime.clone());

        self.load_state().await?;

        self.ingestor
            .initialize(config, &runtime, &mut self.state.user_state)
            .await?;

        info!("IngestorNode {} initialized", self.ingestor.name());
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        let previous_checkpoint = self.state.checkpoint.clone();
        let mut report = match &until {
            TimeHorizon::Snapshot => {
                self.ingestor
                    .scan_snapshot(&mut self.state.user_state, args)
                    .await?
            }
            TimeHorizon::Historical { .. } => {
                self.ingestor
                    .scan_historical(&mut self.state.user_state, from, until.clone(), args)
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

        let effective_checkpoint =
            Self::effective_final_checkpoint(&until, &previous_checkpoint, report.final_checkpoint);
        report.final_checkpoint = effective_checkpoint.clone();
        self.state.checkpoint = effective_checkpoint;
        self.save_state(false).await?;
        Ok(report)
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            signal_shutdown_channel(tx, self.ingestor.name());
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

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(self.state.checkpoint.clone())
    }
}

impl<I: IngestorNode> ExplorationProvider for IngestorNodeAdapter<I> {
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
            sinex_primitives::temporal::Timestamp,
            sinex_primitives::temporal::Timestamp,
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

#[cfg(test)]
mod tests {
    // Inline because these cover a private shutdown-signaling helper.
    use super::{IngestorNodeAdapter, signal_shutdown_channel};
    use crate::checkpoint::{CheckpointManager, CheckpointState};
    use crate::runtime::stream::{
        Checkpoint, NodeCapabilities, ScanArgs, ScanReport, TimeHorizon,
    };
    use crate::shutdown::ShutdownConfig;
    use crate::{IngestorNode, NodeResult};
    use futures::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use sinex_primitives::Timestamp;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::watch;
    use xtask::sandbox::prelude::*;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestState;

    #[derive(Default)]
    struct TestIngestor;

    impl IngestorNode for TestIngestor {
        type Config = ();
        type State = TestState;

        fn name(&self) -> &str {
            "ingestor-adapter-test"
        }

        fn capabilities(&self) -> NodeCapabilities {
            NodeCapabilities::default()
        }

        async fn initialize(
            &mut self,
            _config: Self::Config,
            _runtime: &crate::runtime::stream::NodeRuntimeState,
            _state: &mut Self::State,
        ) -> NodeResult<()> {
            Ok(())
        }

        async fn scan_snapshot(
            &mut self,
            _state: &mut Self::State,
            _args: ScanArgs,
        ) -> NodeResult<ScanReport> {
            Ok(ScanReport {
                events_processed: 0,
                duration: std::time::Duration::ZERO,
                final_checkpoint: Checkpoint::None,
                time_range: None,
                node_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            })
        }

        async fn scan_historical(
            &mut self,
            _state: &mut Self::State,
            _from: Checkpoint,
            _until: TimeHorizon,
            _args: ScanArgs,
        ) -> NodeResult<ScanReport> {
            Ok(ScanReport {
                events_processed: 0,
                duration: std::time::Duration::ZERO,
                final_checkpoint: Checkpoint::None,
                time_range: None,
                node_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            })
        }

        async fn run_continuous(
            &mut self,
            _state: &mut Self::State,
            from: Checkpoint,
            _shutdown_rx: watch::Receiver<bool>,
        ) -> NodeResult<ScanReport> {
            Ok(ScanReport {
                events_processed: 0,
                duration: std::time::Duration::ZERO,
                final_checkpoint: from,
                time_range: None,
                node_stats: HashMap::new(),
                successful_targets: Vec::new(),
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            })
        }
    }

    #[sinex_test]
    async fn signal_shutdown_channel_reports_dropped_receiver() -> TestResult<()> {
        let (tx, rx) = watch::channel(false);
        drop(rx);

        assert!(!signal_shutdown_channel(tx, "test-ingestor"));
        Ok(())
    }

    #[sinex_test]
    async fn signal_shutdown_channel_delivers_to_receiver() -> TestResult<()> {
        let (tx, mut rx) = watch::channel(false);

        assert!(signal_shutdown_channel(tx, "test-ingestor"));
        rx.changed().await?;
        assert!(*rx.borrow());
        Ok(())
    }

    #[sinex_test]
    async fn load_state_rejects_hot_reload_file_without_state_payload() -> TestResult<()> {
        let temp_dir = tempdir()?;
        let checkpoint_path = temp_dir.path().join("ingestor-empty-state.checkpoint.json");
        CheckpointState {
            checkpoint: Checkpoint::stream("restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        }
        .save_to_file(&checkpoint_path)
        .await?;

        let mut adapter = IngestorNodeAdapter::new(TestIngestor);
        adapter.shutdown_config = ShutdownConfig {
            checkpoint_path: Some(checkpoint_path.clone()),
            ..ShutdownConfig::default()
        };

        let error = adapter
            .load_state()
            .await
            .expect_err("empty hot reload ingestor state must not be treated as absent");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("ingestor-adapter-test"));
        assert!(message.contains(&checkpoint_path.display().to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn load_state_rejects_kv_checkpoint_without_state_payload(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv.clone(),
            "ingestor-adapter-test".to_string(),
            "test-group".to_string(),
            "test-consumer".to_string(),
        );
        manager.save_checkpoint(&CheckpointState::default()).await?;

        let mut keys = kv.keys().await?;
        let key = keys
            .try_next()
            .await?
            .expect("checkpoint key should exist");
        let corrupt = serde_json::to_vec(&CheckpointState {
            checkpoint: Checkpoint::stream("restored", None),
            processed_count: 0,
            last_activity: Timestamp::now(),
            data: None,
            version: 2,
            revision: 0,
        })?;
        kv.put(&key, corrupt.into()).await?;

        let mut adapter = IngestorNodeAdapter::new(TestIngestor);
        adapter.checkpoint_manager = Some(Arc::new(manager));

        let error = adapter
            .load_state()
            .await
            .expect_err("empty ingestor checkpoint KV state must not be treated as fresh");
        let message = format!("{error:#}");
        assert!(message.contains("missing state data"));
        assert!(message.contains("ingestor-adapter-test"));
        Ok(())
    }
}
