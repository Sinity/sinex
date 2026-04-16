#[path = "support/mod.rs"]
mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sinex_node_sdk::NodeResult;
use sinex_node_sdk::runtime::stream::{
    Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeType, ScanArgs, ScanReport,
    TimeHorizon,
};
use support::runtime::TestRuntimeBuilder;
use xtask::sandbox::sinex_test;

#[derive(Default)]
struct HangingNode {
    running: Arc<AtomicBool>,
}

impl Node for HangingNode {
    type Config = ();

    async fn initialize(&mut self, _init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        if matches!(until, TimeHorizon::Continuous) {
            self.running.store(true, Ordering::SeqCst);
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::stream("hanging", None),
            time_range: None,
            node_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "hanging-node"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Ingestor
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            ..NodeCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        Ok(Checkpoint::stream("hanging", None))
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[sinex_test]
async fn nodes_should_stop_background_tasks_on_shutdown(ctx: TestContext) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "node-shutdown-leak")
        .with_dry_run(true)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new((), raw_config, service_info, handles, work_dir);

    let mut node = HangingNode::default();
    node.initialize(init_ctx).await?;

    node.scan(
        Checkpoint::stream("hanging", None),
        TimeHorizon::Continuous,
        ScanArgs::default(),
    )
    .await?;

    assert!(
        node.running.load(Ordering::SeqCst),
        "Test setup failed: continuous scan never marked the watcher as running"
    );

    node.shutdown().await?;

    assert!(
        !node.running.load(Ordering::SeqCst),
        "Continuous scanners should clear their running state during shutdown"
    );

    Ok(())
}
