use sinex_processor_runtime::{ProcessorMode, ProcessorRunner, ProcessorRunnerConfig};
use sinex_satellite_sdk::prelude::*;
use sinex_satellite_sdk::stream_processor::ProcessorInitContext;
use sinex_test_utils::{sinex_test, TestContext};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Duration;

struct MockProcessor {
    name: String,
    processor_type: ProcessorType,
    events_to_process: usize,
}

#[async_trait::async_trait]
impl StatefulStreamProcessor for MockProcessor {
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
        let events_processed = match until {
            TimeHorizon::Snapshot => 10,
            TimeHorizon::Historical { .. } => 50,
            TimeHorizon::Continuous => self.events_to_process,
        };

        Ok(ScanReport {
            events_processed: events_processed as u64,
            duration: std::time::Duration::from_secs(1),
            final_checkpoint: Checkpoint::Internal {
                event_id: Ulid::new(),
                message_count: events_processed as u64,
            },
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        &self.name
    }

    fn processor_type(&self) -> ProcessorType {
        self.processor_type
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

struct ShutdownAwareProcessor {
    shutdown_called: Arc<AtomicBool>,
}

impl ShutdownAwareProcessor {
    fn new(flag: Arc<AtomicBool>) -> Self {
        Self {
            shutdown_called: flag,
        }
    }
}

#[async_trait::async_trait]
impl StatefulStreamProcessor for ShutdownAwareProcessor {
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
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        Ok(ScanReport {
            events_processed: 0,
            duration: Duration::from_millis(5),
            final_checkpoint: Checkpoint::Internal {
                event_id: Ulid::new(),
                message_count: 0,
            },
            time_range: None,
            processor_stats: HashMap::new(),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }

    fn processor_name(&self) -> &str {
        "shutdown-aware"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::Internal {
            event_id: Ulid::new(),
            message_count: 0,
        })
    }

    async fn shutdown(&mut self) -> SatelliteResult<()> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[sinex_test]
async fn processor_runner_executes_scan(ctx: TestContext) -> color_eyre::Result<()> {
    let db_pool = ctx.pool.clone();

    let processor = MockProcessor {
        name: "test-processor".to_string(),
        processor_type: ProcessorType::Automaton,
        events_to_process: 0,
    };

    let checkpoint_manager = CheckpointManager::new(
        db_pool.clone(),
        "test-processor".to_string(),
        "default".to_string(),
        "test-host".to_string(),
    );

    let config = ProcessorRunnerConfig {
        mode: ProcessorMode::Scan,
        scan_args: ScanArgs::default(),
        enable_shutdown_handler: false,
    };
    let mut runner = ProcessorRunner::new(processor, checkpoint_manager, config);
    runner.run().await?;
    Ok(())
}

#[sinex_test]
async fn processor_runner_triggers_processor_shutdown(ctx: TestContext) -> color_eyre::Result<()> {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let processor = ShutdownAwareProcessor::new(shutdown_flag.clone());

    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        "shutdown-aware".to_string(),
        "default".to_string(),
        "test-host".to_string(),
    );

    let config = ProcessorRunnerConfig {
        mode: ProcessorMode::Service,
        scan_args: ScanArgs::default(),
        enable_shutdown_handler: true,
    };

    let (signal_tx, signal_rx) = oneshot::channel();
    let mut runner =
        ProcessorRunner::new(processor, checkpoint_manager, config).with_shutdown_signal(signal_rx);

    let handle = tokio::spawn(async move { runner.run().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = signal_tx.send(());

    handle.await??;

    assert!(
        shutdown_flag.load(Ordering::SeqCst),
        "ProcessorRunner should invoke StatefulStreamProcessor::shutdown before exiting service mode"
    );

    Ok(())
}

#[sinex_test]
async fn processor_runner_handles_checkpoints(ctx: TestContext) -> color_eyre::Result<()> {
    let db_pool = ctx.pool.clone();

    let processor = MockProcessor {
        name: "checkpoint-processor".to_string(),
        processor_type: ProcessorType::Automaton,
        events_to_process: 10,
    };

    let checkpoint_manager = CheckpointManager::new(
        db_pool.clone(),
        "checkpoint-processor".to_string(),
        "default".to_string(),
        "test-host".to_string(),
    );

    let config = ProcessorRunnerConfig {
        mode: ProcessorMode::Scan,
        scan_args: ScanArgs::default(),
        enable_shutdown_handler: false,
    };
    let mut runner = ProcessorRunner::new(processor, checkpoint_manager, config);
    runner.run().await?;
    Ok(())
}
