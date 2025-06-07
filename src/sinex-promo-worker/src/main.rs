use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use sinex_db::{
    models::{AgentHeartbeat, PromotionQueueItem, RawEvent},
    queries::{insert_raw_event, update_agent_heartbeat, upsert_agent_manifest},
};
use sinex_worker::{metrics::start_metrics_server, worker::Worker, EventProcessor};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::{signal, task};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Agent name to process events for
    #[arg(long, env = "AGENT_NAME")]
    agent_name: String,

    /// Worker ID (defaults to hostname-pid)
    #[arg(long, env = "WORKER_ID")]
    worker_id: Option<String>,

    /// Metrics port
    #[arg(long, env = "METRICS_PORT", default_value = "9090")]
    metrics_port: u16,

    /// Batch size for processing
    #[arg(long, env = "BATCH_SIZE", default_value = "10")]
    batch_size: i32,

    /// Poll interval in seconds
    #[arg(long, env = "POLL_INTERVAL", default_value = "1")]
    poll_interval: u64,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

/// Example processor that logs events
struct ExampleProcessor {
    agent_name: String,
    batch_size: i32,
    poll_interval: u64,
}

#[async_trait]
impl EventProcessor for ExampleProcessor {
    async fn process_event(&self, pool: &PgPool, item: &PromotionQueueItem) -> Result<()> {
        // Fetch the raw event
        let event: RawEvent = sqlx::query_as!(
            RawEvent,
            r#"
            SELECT 
                id::uuid as "id!", 
                source as "source!", 
                event_type as "event_type!", 
                ts_ingest as "ts_ingest!",
                ts_orig as "ts_orig?",
                host as "host!", 
                ingestor_version as "ingestor_version?", 
                payload_schema_id::uuid as "payload_schema_id?", 
                payload as "payload!"
            FROM raw.events 
            WHERE id = $1::uuid::ulid
            "#,
            item.raw_event_id
        )
        .fetch_one(pool)
        .await?;

        info!(
            agent = %self.agent_name,
            event_id = %event.id,
            source = %event.source,
            event_type = %event.event_type,
            "Processing event"
        );

        // Example: Just log the event payload
        info!(
            agent = %self.agent_name,
            payload = %event.payload,
            "Event payload"
        );

        // In a real implementation, you would:
        // 1. Parse the payload according to its schema
        // 2. Transform/enrich the data
        // 3. Insert into domain-specific tables
        // 4. Generate derived events if needed

        Ok(())
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    fn batch_size(&self) -> i32 {
        self.batch_size
    }

    fn poll_interval_secs(&self) -> u64 {
        self.poll_interval
    }
}

async fn register_agent(pool: &PgPool, agent_name: &str) -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    
    // Register the agent
    upsert_agent_manifest(
        pool,
        agent_name,
        version,
        "running",
        "promoter",
        Some("Example promotion worker that logs events"),
        Some(serde_json::json!({
            "sinex.agent.heartbeat": [{"type": "heartbeat"}]
        })),
        Some(serde_json::json!({
            "raw.events_feed_all": [{"note": "Subscribes to all events for demo purposes"}]
        })),
    )
    .await?;

    info!(agent_name = %agent_name, version = %version, "Agent registered");
    Ok(())
}

async fn emit_heartbeat(pool: PgPool, agent_name: String) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    
    loop {
        interval.tick().await;
        
        let heartbeat = AgentHeartbeat {
            agent_name: agent_name.clone(),
            status: "running".to_string(),
            uptime_seconds: 0, // TODO: track actual uptime
            events_processed_session: 0, // TODO: track actual events
            dlq_size: 0, // TODO: track DLQ size
            version: env!("CARGO_PKG_VERSION").to_string(),
        };

        match insert_raw_event(
            &pool,
            "sinex.agent.heartbeat",
            "heartbeat",
            &gethostname::gethostname().to_string_lossy(),
            serde_json::to_value(&heartbeat)?,
            Some(chrono::Utc::now()),
            Some(env!("CARGO_PKG_VERSION")),
            None,
        )
        .await
        {
            Ok(_) => {
                let _ = update_agent_heartbeat(&pool, &agent_name).await;
            }
            Err(e) => {
                warn!(error = %e, "Failed to emit heartbeat");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting sinex-promo-worker");

    // Create database pool
    let pool = sinex_db::create_pool(&args.database_url).await?;

    // Register the agent
    register_agent(&pool, &args.agent_name).await?;

    // Start heartbeat task
    let heartbeat_pool = pool.clone();
    let heartbeat_agent_name = args.agent_name.clone();
    task::spawn(async move {
        if let Err(e) = emit_heartbeat(heartbeat_pool, heartbeat_agent_name).await {
            error!(error = %e, "Heartbeat task failed");
        }
    });

    // Start metrics server
    let metrics_handle = task::spawn(async move {
        if let Err(e) = start_metrics_server(args.metrics_port).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    // Create processor
    let processor = Arc::new(ExampleProcessor {
        agent_name: args.agent_name.clone(),
        batch_size: args.batch_size,
        poll_interval: args.poll_interval,
    });

    // Create worker
    let worker_id = args.worker_id.unwrap_or_else(|| {
        format!(
            "{}-{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        )
    });
    
    let worker = Worker::new(pool, processor, worker_id);

    // Run worker until shutdown signal
    let worker_handle = task::spawn(async move {
        if let Err(e) = worker.run().await {
            error!(error = %e, "Worker failed");
        }
    });

    // Wait for shutdown signal
    signal::ctrl_c().await?;
    info!("Shutdown signal received");

    // Cancel tasks (they should handle cancellation gracefully)
    worker_handle.abort();
    metrics_handle.abort();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing() {
        let args = Args::try_parse_from([
            "sinex-promo-worker",
            "--database-url",
            "postgres://localhost/test",
            "--agent-name",
            "test-agent",
        ]);
        assert!(args.is_ok());
    }
}