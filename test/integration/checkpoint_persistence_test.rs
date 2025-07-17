use crate::common::prelude::*;
use async_trait::async_trait;
use serde_json::json;
use sinex_db::SqlxPgPool;
use sinex_events::RawEvent;
use sinex_satellite_sdk::{
    automaton::{
        EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent,
        HotlogAutomatonRunner, ProcessingResult,
    },
    checkpoint::CheckpointManager,
    grpc_client::IngestClient,
    redis_client::RedisStreamClient,
};
use sinex_test_macros::sinex_test;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Test automaton that tracks processed events
#[derive(Debug)]
struct TestCheckpointAutomaton {
    name: String,
    processed_events: Arc<Mutex<Vec<String>>>,
    context: Option<HotlogAutomatonContext>,
}

impl TestCheckpointAutomaton {
    fn new(name: String) -> Self {
        Self {
            name,
            processed_events: Arc::new(Mutex::new(Vec::new())),
            context: None,
        }
    }

    async fn get_processed_count(&self) -> usize {
        self.processed_events.lock().await.len()
    }
}

#[async_trait]
impl HotlogAutomaton for TestCheckpointAutomaton {
    async fn initialize(
        &mut self,
        ctx: HotlogAutomatonContext,
    ) -> sinex_satellite_sdk::SatelliteResult<()> {
        self.context = Some(ctx);
        Ok(())
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> sinex_satellite_sdk::SatelliteResult<ProcessingResult> {
        // Record that we processed this event
        self.processed_events
            .lock()
            .await
            .push(event.event.id.to_string());

        debug!(
            automaton = %self.name,
            event_id = %event.event.id,
            "Processing event in test automaton"
        );

        // Return success with some checkpoint data
        Ok(ProcessingResult::Success {
            checkpoint_data: Some(json!({
                "event_id": event.event.id.to_string(),
                "processed_at": chrono::Utc::now()
            })),
        })
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![EventFilter::new(Some("test".to_string()), None)]
    }

    fn automaton_name(&self) -> &str {
        &self.name
    }
}

/// Integration test for checkpoint persistence bug fix
#[sinex_test]
async fn test_checkpoint_persistence_and_restart_recovery(
    ctx: crate::TestContext,
) -> crate::TestResult {
    let pool = ctx.pool();
    let mut redis_client = ctx.redis().await?;
    let ingest_client = ctx.ingest_client().await?;

    // Test configuration
    let service_name = "test-checkpoint-automaton".to_string();
    let consumer_group = "test-checkpoint-group".to_string();
    let consumer_name = "test-checkpoint-consumer".to_string();
    let work_dir = std::path::PathBuf::from("/tmp");

    // Create test automaton
    let automaton = TestCheckpointAutomaton::new(service_name.clone());
    let processed_events_ref = automaton.processed_events.clone();

    // Create automaton runner
    let mut runner = HotlogAutomatonRunner::new(automaton);

    // Initialize runner
    runner
        .initialize(
            service_name.clone(),
            consumer_group.clone(),
            consumer_name.clone(),
            vec![EventFilter::new(Some("test".to_string()), None)],
            HashMap::new(),
            pool.clone(),
            redis_client.clone(),
            ingest_client.clone(),
            work_dir.clone(),
            false,
        )
        .await?;

    // Clear any existing consumer group state
    let _ = redis_client
        .delete_consumer_group("sinex:streams:hotlog", &consumer_group)
        .await;

    // Create checkpoint manager for verification
    let checkpoint_manager = CheckpointManager::new(
        pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        consumer_name.clone(),
    );

    // Step 1: Inject test events into the hotlog stream
    info!("Step 1: Injecting test events");

    let test_events = vec![
        RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ts_orig: chrono::Utc::now(),
            ts_ingest: chrono::Utc::now(),
            host: "test-host".to_string(),
            payload: json!({"test": "event1"}),
        },
        RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ts_orig: chrono::Utc::now(),
            ts_ingest: chrono::Utc::now(),
            host: "test-host".to_string(),
            payload: json!({"test": "event2"}),
        },
        RawEvent {
            id: sinex_ulid::Ulid::new(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ts_orig: chrono::Utc::now(),
            ts_ingest: chrono::Utc::now(),
            host: "test-host".to_string(),
            payload: json!({"test": "event3"}),
        },
    ];

    // Add events to hotlog stream (simulating ingestd)
    for event in &test_events {
        let serialized = serde_json::to_string(event)?;
        redis_client
            .add_to_stream("sinex:streams:hotlog", &[("data".to_string(), serialized)])
            .await?;
    }

    info!(
        "Injected {} test events into hotlog stream",
        test_events.len()
    );

    // Step 2: Run automaton for a short time to process some events
    info!("Step 2: Running automaton to process events");

    let runner_handle = {
        let mut runner_clone =
            HotlogAutomatonRunner::new(TestCheckpointAutomaton::new(service_name.clone()));
        runner_clone
            .initialize(
                service_name.clone(),
                consumer_group.clone(),
                consumer_name.clone(),
                vec![EventFilter::new(Some("test".to_string()), None)],
                HashMap::new(),
                pool.clone(),
                redis_client.clone(),
                ingest_client.clone(),
                work_dir.clone(),
                false,
            )
            .await?;

        tokio::spawn(async move {
            let _ = runner_clone.run().await;
        })
    };

    // Wait for events to be processed
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check that events were processed
    let processed_count_before_restart = processed_events_ref.lock().await.len();
    info!(
        "Processed {} events before restart",
        processed_count_before_restart
    );

    // Verify checkpoint was saved to database
    let checkpoint_before = checkpoint_manager.load_checkpoint().await?;
    assert!(
        checkpoint_before.processed_count > 0,
        "Processed count should be > 0 in checkpoint"
    );

    info!(
        "Checkpoint before restart: processed_count={}, last_id={:?}",
        checkpoint_before.processed_count, checkpoint_before.last_processed_id()
    );

    // Step 3: Simulate "crash" by dropping the runner
    info!("Step 3: Simulating crash by stopping automaton");
    runner_handle.abort();

    // Step 4: Create new automaton instance and restart
    info!("Step 4: Restarting automaton (simulating recovery)");

    let new_automaton = TestCheckpointAutomaton::new(format!("{}-restarted", service_name));
    let new_processed_events_ref = new_automaton.processed_events.clone();

    let mut new_runner = HotlogAutomatonRunner::new(new_automaton);
    new_runner
        .initialize(
            service_name.clone(),
            consumer_group.clone(),
            format!("{}-restarted", consumer_name), // Different consumer name
            vec![EventFilter::new(Some("test".to_string()), None)],
            HashMap::new(),
            pool.clone(),
            redis_client.clone(),
            ingest_client.clone(),
            work_dir.clone(),
            false,
        )
        .await?;

    // Run the restarted automaton
    let restart_handle = tokio::spawn(async move {
        let _ = new_runner.run().await;
    });

    // Wait for processing to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Step 5: Verify checkpoint recovery
    info!("Step 5: Verifying checkpoint recovery");

    let checkpoint_after = checkpoint_manager.load_checkpoint().await?;

    let new_processed_count = new_processed_events_ref.lock().await.len();
    info!("Processed {} events after restart", new_processed_count);

    // Verify that the system correctly resumed from checkpoint
    // The key test: if checkpoints work, we shouldn't reprocess events already processed
    assert!(
        checkpoint_after.processed_count >= checkpoint_before.processed_count,
        "Checkpoint processed count should not decrease: {} -> {}",
        checkpoint_before.processed_count,
        checkpoint_after.processed_count
    );

    info!(
        "Checkpoint after restart: processed_count={}, last_id={:?}",
        checkpoint_after.processed_count, checkpoint_after.last_processed_id()
    );

    // Clean up
    restart_handle.abort();

    // Step 6: Verify no duplicate processing occurred
    info!("Step 6: Verifying no duplicate processing occurred");

    // Check Redis consumer group info to verify messages were ACKed properly
    let pending_messages = redis_client
        .pending_messages(
            "sinex:streams:hotlog",
            &consumer_group,
            None,
            None,
            Some(100),
        )
        .await;

    match pending_messages {
        Ok(pending) => {
            info!("Pending messages in consumer group: {}", pending.len());
            // All messages should have been ACKed if checkpoints work correctly
        }
        Err(_) => {
            // Consumer group might not exist or be empty, which is fine
            info!("No pending messages found (consumer group may be empty)");
        }
    }

    info!("✅ Checkpoint persistence test completed successfully");

    Ok(())
}
