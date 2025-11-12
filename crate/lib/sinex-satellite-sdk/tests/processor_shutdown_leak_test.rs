use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sinex_satellite_sdk::stream_processor::{
    Checkpoint, ProcessorCapabilities, ProcessorInitContext, ProcessorType, ScanArgs, ScanReport,
    StatefulStreamProcessor, TimeHorizon,
};
use sinex_satellite_sdk::SatelliteResult;
use sinex_test_utils::{satellite_runtime::TestRuntimeBuilder, sinex_test, TestContext};

#[derive(Default)]
struct HangingProcessor {
    running: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl StatefulStreamProcessor for HangingProcessor {
    type Config = ();

    async fn initialize(
        &mut self,
        _init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        Ok(())
    }

    async fn scan(
        &mut self,
        _from: Checkpoint,
        until: TimeHorizon,
        _args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
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

    fn processor_name(&self) -> &str {
        "hanging-processor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            ..ProcessorCapabilities::default()
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::stream("hanging", None))
    }
}

#[sinex_test]
async fn processors_should_stop_background_tasks_on_shutdown(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let runtime = TestRuntimeBuilder::new(&ctx, "processor-shutdown-leak")
        .with_dry_run(true)
        .build()
        .await?;
    let (service_info, handles, raw_config, work_dir) = runtime.runtime.clone().into_parts();
    let init_ctx = ProcessorInitContext::new((), raw_config, service_info, handles, work_dir);

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
