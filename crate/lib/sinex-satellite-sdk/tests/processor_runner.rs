use async_trait::async_trait;
use sinex_satellite_sdk::prelude::*;
use sinex_satellite_sdk::{
    MaterialConsumer, ProcessorMode, ProcessorRunner, ProcessorRunnerConfig,
};
use sinex_test_utils::TestContext;
use std::collections::HashMap;

struct MockProcessor {
    name: String,
    processor_type: ProcessorType,
    events_to_process: usize,
}

#[async_trait]
impl StatefulStreamProcessor for MockProcessor {
    type Config = ();

    async fn initialize(
        &mut self,
        _ctx: StreamProcessorContext,
        _config: Self::Config,
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

#[async_trait]
impl MaterialConsumer for MockProcessor {
    async fn process_material_slice(
        &self,
        _material_id: Ulid,
        _slice_data: &[u8],
    ) -> Result<Vec<Event<JsonValue>>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn processor_runner_executes_scan() {
    let test_ctx = TestContext::with_name("processor_runner_test")
        .await
        .expect("ctx");
    let db_pool = test_ctx.pool.clone();

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
    assert!(runner.run().await.is_ok());
}

#[tokio::test]
async fn processor_runner_handles_checkpoints() {
    let test_ctx = TestContext::with_name("processor_checkpoint_test")
        .await
        .expect("ctx");
    let db_pool = test_ctx.pool.clone();

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
    assert!(runner.run().await.is_ok());
}
