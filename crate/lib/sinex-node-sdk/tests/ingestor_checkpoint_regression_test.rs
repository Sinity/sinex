#[path = "support/mod.rs"]
mod support;

use serde::{Deserialize, Serialize};
use sinex_node_sdk::runtime::stream::{
    Checkpoint, Node, NodeInitContext, ScanArgs, ScanReport, TimeHorizon,
};
use sinex_node_sdk::{IngestorNode, IngestorNodeAdapter, NodeResult};
use sinex_primitives::Timestamp;
use std::collections::HashMap;
use tokio::sync::watch;
use support::runtime::TestRuntimeBuilder;
use xtask::sandbox::prelude::*;

#[derive(Clone, Default, Serialize, Deserialize)]
struct TestState;

#[derive(Default)]
struct SnapshotCheckpointIngestor;

impl IngestorNode for SnapshotCheckpointIngestor {
    type Config = ();
    type State = TestState;

    fn name(&self) -> &str {
        "snapshot-checkpoint-ingestor"
    }

    async fn initialize(
        &mut self,
        _config: Self::Config,
        _runtime: &sinex_node_sdk::runtime::stream::NodeRuntimeState,
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
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: vec!["snapshot".to_string()],
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
            events_processed: 1,
            duration: std::time::Duration::from_millis(1),
            final_checkpoint: Checkpoint::stream("historical", None),
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: vec!["historical".to_string()],
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
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: from,
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: vec!["continuous".to_string()],
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

#[sinex_test]
async fn snapshot_scan_preserves_existing_checkpoint(ctx: TestContext) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "snapshot-checkpoint-ingestor")
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new((), raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::new(SnapshotCheckpointIngestor);
    node.initialize(init_ctx).await?;

    let historical_report = node
        .scan(
            Checkpoint::None,
            TimeHorizon::Historical {
                end_time: Timestamp::now(),
            },
            ScanArgs::default(),
        )
        .await?;
    assert_eq!(
        historical_report.final_checkpoint,
        Checkpoint::stream("historical", None)
    );

    let snapshot_report = node
        .scan(
            historical_report.final_checkpoint.clone(),
            TimeHorizon::Snapshot,
            ScanArgs::default(),
        )
        .await?;

    assert_eq!(
        snapshot_report.final_checkpoint,
        historical_report.final_checkpoint,
        "snapshot scans must preserve the last real checkpoint when they do not advance it"
    );
    assert_eq!(
        node.current_checkpoint().await?,
        historical_report.final_checkpoint,
        "snapshot scans must not erase persisted adapter checkpoint state"
    );

    Ok(())
}

#[sinex_test]
async fn fresh_snapshot_scan_keeps_empty_checkpoint(ctx: TestContext) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "fresh-snapshot-checkpoint-ingestor")
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new((), raw_config, service_info, handles, work_dir);

    let mut node = IngestorNodeAdapter::new(SnapshotCheckpointIngestor);
    node.initialize(init_ctx).await?;

    let snapshot_report = node
        .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
        .await?;

    assert_eq!(snapshot_report.final_checkpoint, Checkpoint::None);
    assert_eq!(node.current_checkpoint().await?, Checkpoint::None);

    Ok(())
}
