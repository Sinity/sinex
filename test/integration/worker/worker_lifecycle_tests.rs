use crate::common::prelude::*;
use sinex_db::models::WorkQueueItem;
use sinex_worker::{calculate_backoff_secs, EventProcessor, WorkerMetrics};
// Import test setup macros and utilities
use crate::common::worker_test_utils::{self, insert_test_work_item};

struct TestEventProcessor {
    agent_name: String,
    process_count: Arc<AtomicU32>,
    should_fail: Arc<AtomicBool>,
    processing_delay: Duration,
}

impl TestEventProcessor {
    fn new(agent_name: String) -> Self {
        Self {
            agent_name,
            process_count: Arc::new(AtomicU32::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            processing_delay: Duration::from_millis(10),
        }
    }
}

#[async_trait]
impl EventProcessor for TestEventProcessor {
    async fn process_event(
        &self,
        _pool: &DbPool,
        _item: &WorkQueueItem,
    ) -> Result<(), anyhow::Error> {
        self.process_count.fetch_add(1, Ordering::SeqCst);

        tokio::time::sleep(self.processing_delay).await;

        if self.should_fail.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Test processor failure"));
        }

        Ok(())
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    fn batch_size(&self) -> i32 {
        1
    }

    fn poll_interval_secs(&self) -> u64 {
        1
    }
}

#[sinex_test]
async fn test_event_processor_basic_processing(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let processor = TestEventProcessor::new("test_agent".to_string());
    let process_count = processor.process_count.clone();

    // Insert a test item using the utility
    let queue_ids = worker_test_utils::setup_test_worker(pool, "test", 1).await?;
    let _queue_id = queue_ids[0];

    // Process the item using the proper query function (setup_test_worker creates agent with _agent suffix)
    let item = sinex_db::queries::get_next_work_item(pool, "test_agent")
        .await?
        .expect("Should have work item for test_agent");

    processor.process_event(pool, &item).await?;

    pretty_assertions::assert_eq!(process_count.load(Ordering::SeqCst), 1);

    Ok(())
}

#[sinex_test]
async fn test_event_processor_failure_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let processor = TestEventProcessor::new("test_agent".to_string());

    processor.should_fail.store(true, Ordering::SeqCst);

    let _queue_id = insert_test_work_item(pool, "test_agent").await?;

    let item = sinex_db::queries::get_next_work_item(pool, "test_agent")
        .await?
        .expect("Should have work item for test_agent");

    let result = processor.process_event(pool, &item).await;
    assert!(result.is_err());

    Ok(())
}

#[sinex_test]
async fn test_backoff_calculation(_ctx: TestContext) -> TestResult {
    // Test backoff calculation function
    let backoff_0 = calculate_backoff_secs(0);
    let backoff_1 = calculate_backoff_secs(1);
    let backoff_5 = calculate_backoff_secs(5);
    let backoff_10 = calculate_backoff_secs(10);

    // Backoff should increase with attempts
    assert!(backoff_1 > backoff_0);
    assert!(backoff_5 > backoff_1);

    // Should not exceed maximum (24 hours)
    assert!(backoff_10 <= 24.0 * 3600.0);

    println!(
        "Backoff progression: 0:{:.1}s, 1:{:.1}s, 5:{:.1}s, 10:{:.1}s",
        backoff_0, backoff_1, backoff_5, backoff_10
    );

    Ok(())
}

#[sinex_test]
async fn test_worker_metrics_creation(_ctx: TestContext) -> TestResult {
    let metrics = WorkerMetrics::new("test_agent");

    // Test that metrics can be incremented
    metrics.items_claimed.inc();
    metrics.items_processed.inc();
    metrics.items_failed.inc();

    // Observe processing duration
    metrics.processing_duration.observe(0.5);

    // Just ensure metrics don't panic
    pretty_assertions::assert_eq!(metrics.items_claimed.get(), 1.0);
    pretty_assertions::assert_eq!(metrics.items_processed.get(), 1.0);
    pretty_assertions::assert_eq!(metrics.items_failed.get(), 1.0);

    Ok(())
}

#[sinex_test]
async fn test_multiple_processors_different_agents(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let processor_a = TestEventProcessor::new("agent_a".to_string());
    let processor_b = TestEventProcessor::new("agent_b".to_string());

    let count_a = processor_a.process_count.clone();
    let count_b = processor_b.process_count.clone();

    // Insert items for each agent
    eprintln!("DEBUG: About to insert test work items...");
    let _queue_id_a = insert_test_work_item(pool, "agent_a").await?;
    eprintln!("DEBUG: Inserted item for agent_a");
    let _queue_id_b = insert_test_work_item(pool, "agent_b").await?;
    eprintln!("DEBUG: Inserted item for agent_b");

    // Get and process items for each agent using the proper query function
    let item_a = sinex_db::queries::get_next_work_item(pool, "agent_a")
        .await?
        .expect("Should have work item for agent_a");

    let item_b = sinex_db::queries::get_next_work_item(pool, "agent_b")
        .await?
        .expect("Should have work item for agent_b");

    processor_a.process_event(pool, &item_a).await?;
    processor_b.process_event(pool, &item_b).await?;

    pretty_assertions::assert_eq!(count_a.load(Ordering::SeqCst), 1);
    pretty_assertions::assert_eq!(count_b.load(Ordering::SeqCst), 1);

    Ok(())
}

#[sinex_test]
async fn test_processor_configuration(_ctx: TestContext) -> TestResult {
    let processor = TestEventProcessor::new("test_agent".to_string());

    pretty_assertions::assert_eq!(processor.agent_name(), "test_agent");
    pretty_assertions::assert_eq!(processor.batch_size(), 1);
    pretty_assertions::assert_eq!(processor.poll_interval_secs(), 1);

    Ok(())
}

struct SlowProcessor {
    agent_name: String,
}

#[async_trait]
impl EventProcessor for SlowProcessor {
    async fn process_event(
        &self,
        _pool: &DbPool,
        _item: &WorkQueueItem,
    ) -> Result<(), anyhow::Error> {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    fn batch_size(&self) -> i32 {
        5
    }

    fn poll_interval_secs(&self) -> u64 {
        2
    }
}

#[sinex_test]
async fn test_processor_custom_configuration(_ctx: TestContext) -> TestResult {
    let processor = SlowProcessor {
        agent_name: "slow_agent".to_string(),
    };

    pretty_assertions::assert_eq!(processor.agent_name(), "slow_agent");
    pretty_assertions::assert_eq!(processor.batch_size(), 5);
    pretty_assertions::assert_eq!(processor.poll_interval_secs(), 2);

    Ok(())
}
