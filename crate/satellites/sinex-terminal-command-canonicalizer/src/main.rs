//! Simple NATS-based Terminal Command Canonicalizer
//!
//! This bypasses the complex processor_main! macro and directly uses NATS consumer

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use async_trait::async_trait;
use sinex_satellite_sdk::{
    nats_stream_consumer::{
        BatchProcessingResult as NatsBatchProcessingResult,
        EventBatchProcessor as NatsEventBatchProcessor, EventFilter, NatsConsumerConfig,
        NatsStreamConsumer,
    },
    SatelliteResult,
};
use tracing::info;

/// Simple terminal command canonicalizer that just logs received events
struct SimpleTerminalCanonicalizer;

impl SimpleTerminalCanonicalizer {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl NatsEventBatchProcessor for SimpleTerminalCanonicalizer {
    async fn initialize(&mut self) -> SatelliteResult<()> {
        info!("Simple terminal command canonicalizer initialized");
        Ok(())
    }

    async fn process_batch(
        &mut self,
        events: Vec<sinex_db::models::Event>,
    ) -> SatelliteResult<NatsBatchProcessingResult> {
        info!(
            "Terminal command canonicalizer processed {} events",
            events.len()
        );

        // Simple implementation: just log and acknowledge
        for event in &events {
            info!(
                "Processing event: {} from {}",
                event.event_type, event.source
            );
        }

        Ok(NatsBatchProcessingResult {
            processed: events.len(),
            skipped: 0,
            failed: 0,
            duration: std::time::Duration::from_millis(1),
            errors: vec![],
        })
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![EventFilter::new()
            .with_source("terminal")
            .with_event_type("command_executed")
            .with_event_type("terminal_session_started")
            .with_event_type("terminal_session_ended")]
    }
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;

    // Initialize logging
    tracing_subscriber::fmt().with_env_filter("info").init();

    info!("Starting simple NATS-based terminal command canonicalizer");

    // Create consumer configuration
    let config = NatsConsumerConfig {
        group_name: "automata".to_string(),
        consumer_name: "terminal-command-canonicalizer".to_string(),
        stream_name: "events".to_string(),
        nats_servers: vec!["nats://localhost:4222".to_string()],
        filters: vec![],
        batch_size: 10,
        block_timeout: std::time::Duration::from_millis(5000),
    };

    // Create consumer and processor
    let mut consumer = NatsStreamConsumer::new(config);
    consumer.initialize(None).await?;

    let processor = SimpleTerminalCanonicalizer::new();

    // Run consumer
    consumer.run(processor).await?;

    Ok(())
}
