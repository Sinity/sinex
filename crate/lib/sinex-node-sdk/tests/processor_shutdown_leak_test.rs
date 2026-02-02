#[path = "support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sinex_node_sdk::stream_processor::{
    Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeType, ScanArgs, ScanReport,
    TimeHorizon,
};
use sinex_node_sdk::NodeResult;
use support::runtime::TestRuntimeBuilder;
use xtask::sandbox::sinex_test;

#[derive(Default)]
struct HangingProcessor {
    running: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Node for HangingProcessor {
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
            processor_stats: Default::default(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn node_name(&self) -> &'static str {
        "hanging-processor"
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
async fn processors_should_stop_background_tasks_on_shutdown(ctx: TestContext) -> TestResult<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "processor-shutdown-leak")
        .with_dry_run(true)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = NodeInitContext::new((), raw_config, service_info, handles, work_dir);

    let mut processor = HangingProcessor::default();
    processor.initialize(init_ctx).await?;

    processor
        .scan(
            Checkpoint::stream("hanging", None),
            TimeHorizon::Continuous,
            ScanArgs::default(),
        )
        .await?;

    assert!(
        processor.running.load(Ordering::SeqCst),
        "Test setup failed: continuous scan never marked the watcher as running"
    );

    processor.shutdown().await?;

    assert!(
        !processor.running.load(Ordering::SeqCst),
        "Continuous scanners should clear their running state during shutdown"
    );

    Ok(())
}
