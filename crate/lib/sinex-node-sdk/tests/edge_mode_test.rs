//! Optional Database Dependency Test
//!
//! Verifies that nodes can run without `DATABASE_URL` (ingestors) while
//! automata that need it get clear error messages. Checkpoints always use NATS KV.


use sinex_db::models::Event;
use sinex_node_sdk::{
    EventTransport, NodeResult,
    checkpoint::CheckpointManager,
    nats_publisher::NatsPublisher,
    runtime::stream::{
        EventEmitter, Node, NodeCapabilities, NodeHandles, NodeInitContext, NodeRunner, NodeType,
        SchemaBroadcastEntry,
    },
};
// Channel size constant - not available in sinex_primitives::constants, use local
const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1000;
use sinex_primitives::{JsonValue, error::SinexError};
use std::sync::Arc;
use tokio::sync::mpsc;
use xtask::sandbox::sinex_serial_test;
use xtask::sandbox::timing::{DEFAULT_WAIT_SECS, WaitHelpers};

/// Minimal test node that doesn't require database access
struct EdgeTestNode {
    name: String,
}

impl EdgeTestNode {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Node for EdgeTestNode {
    type Config = serde_json::Value;

    async fn initialize(&mut self, _ctx: NodeInitContext<Self::Config>) -> NodeResult<()> {
        Ok(())
    }

    fn node_type(&self) -> NodeType {
        NodeType::Ingestor
    }

    fn node_name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true,
            supports_historical: false,
            supports_snapshot: false,
            supports_interactive: false,
            supports_concurrent: false,
            manages_own_continuous_loop: false,
            max_scan_size: None,
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<sinex_node_sdk::runtime::stream::Checkpoint> {
        Ok(sinex_node_sdk::runtime::stream::Checkpoint::stream(
            "0", None,
        ))
    }

    async fn scan(
        &mut self,
        _from: sinex_node_sdk::runtime::stream::Checkpoint,
        _until: sinex_node_sdk::runtime::stream::TimeHorizon,
        _args: sinex_node_sdk::runtime::stream::ScanArgs,
    ) -> NodeResult<sinex_node_sdk::runtime::stream::ScanReport> {
        Ok(sinex_node_sdk::runtime::stream::ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_secs(0),
            final_checkpoint: sinex_node_sdk::runtime::stream::Checkpoint::stream("0", None),
            time_range: None,
            node_stats: std::collections::HashMap::new(),
            successful_targets: vec![],
            failed_targets: vec![],
            warnings: vec![],
        })
    }

    async fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> NodeResult<sinex_node_sdk::runtime::stream::ProcessingStats> {
        Ok(sinex_node_sdk::runtime::stream::ProcessingStats::default())
    }
}

#[sinex_serial_test(timeout = 30)]
async fn test_ingestor_without_database(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // No DATABASE_URL - ingestors don't need it
    unsafe { std::env::remove_var("DATABASE_URL") };

    let node = EdgeTestNode::new("test_ingestor");
    let mut runner = NodeRunner::new(node);

    // Create NATS transport
    let nats = ctx.nats_handle()?;
    let nats_client = nats.connect().await?;
    let publisher = Arc::new(NatsPublisher::new(nats_client));
    let transport = EventTransport::Nats(publisher);

    // Should work fine without DATABASE_URL
    runner
        .initialize_with_transport(
            "test_ingestor".to_string(),
            std::collections::HashMap::new(),
            None, // No database pool - that's OK
            transport,
            std::path::PathBuf::from("/tmp/sinex/test_ingestor"),
            false,
        )
        .await?;

    // Verify initialization succeeded
    assert!(runner.runtime_state().is_some());

    Ok(())
}

#[sinex_serial_test(timeout = 30)]
async fn test_automaton_requires_db_pool(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let (event_sender, _event_receiver) =
        mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
    let emitter = EventEmitter::new(event_sender, false);

    let nats = ctx.nats_handle()?;
    let nats_client = nats.connect().await?;
    let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));
    let transport = EventTransport::Nats(publisher);

    let js = async_nats::jetstream::new(nats_client);
    let kv_store = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "KV_test_automaton".to_string(),
            ..Default::default()
        })
        .await?;

    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv_store,
        "test_automaton".to_string(),
        "default".to_string(),
        "test_consumer".to_string(),
    ));

    let handles = NodeHandles::new_edge(checkpoint_manager, emitter, transport, None, None);

    // Attempting to get DB pool for automaton should panic
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handles.require_db_pool()));
    assert!(
        result.is_err(),
        "require_db_pool should panic when database is unavailable"
    );

    Ok(())
}

#[sinex_serial_test(timeout = 30)]
async fn test_schema_broadcast_cache_updates(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // No DATABASE_URL needed - schema cache works without it
    unsafe { std::env::remove_var("DATABASE_URL") };

    let node = EdgeTestNode::new("edge_schema_cache");
    let mut runner = NodeRunner::new(node);

    let nats = ctx.nats_handle()?;
    let nats_client = nats.connect().await?;
    let js = async_nats::jetstream::new(nats_client.clone());

    // Create the schema KV bucket that the runner expects
    let env = sinex_primitives::environment::environment();
    let schema_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_schemas"));
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: schema_bucket,
        ..Default::default()
    })
    .await?;

    let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));
    let transport = EventTransport::Nats(publisher);

    runner
        .initialize_with_transport(
            "edge_schema_cache".to_string(),
            std::collections::HashMap::new(),
            None,
            transport,
            std::path::PathBuf::from("/tmp/sinex/edge_schema_cache"),
            false,
        )
        .await?;

    let runtime = runner.runtime_state().expect("runtime state should exist");
    let cache = runtime
        .handles()
        .schema_cache()
        .expect("schema cache should be initialized automatically");

    let subject =
        sinex_primitives::environment::environment().nats_subject("system.schemas.active");
    let entries = vec![SchemaBroadcastEntry {
        name: "schema.test".to_string(),
        version: "1.0.0".to_string(),
        schema_id: sinex_node_sdk::Uuid::now_v7().to_string(),
    }];

    nats_client
        .publish(subject, serde_json::to_vec(&entries)?.into())
        .await?;

    WaitHelpers::wait_for_condition(
        || {
            let cache = cache.clone();
            async move { Ok::<bool, SinexError>(!cache.get().await.is_empty()) }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    let cached = cache.get().await;
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].name, "schema.test");

    Ok(())
}
