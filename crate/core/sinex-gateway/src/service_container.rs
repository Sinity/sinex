//! Service container that holds all service instances

use camino::Utf8PathBuf;
use color_eyre::eyre::{Context, Result, WrapErr};
use sinex_db::create_pool;
use sinex_db::telemetry::telemetry::{SystemTelemetryEmitter, TelemetryAccumulator};
use sinex_satellite_sdk::annex::BlobManager;
use sinex_satellite_sdk::grpc_client::IngestClient;
use sinex_services::{AnalyticsService, ContentService, PkmService, SearchService};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Container holding all service instances
#[derive(Clone)]
pub struct ServiceContainer {
    pub analytics: Arc<AnalyticsService>,
    pub content: Arc<ContentService>,
    pub pkm: Arc<PkmService>,
    pub search: Arc<SearchService>,
    pub telemetry: Option<Arc<TelemetryAccumulator>>,
}

impl ServiceContainer {
    /// Create a new service container with the given database URL
    pub async fn new(database_url: Option<String>) -> Result<Self> {
        // Get database URL from parameter or environment
        let db_url = database_url
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .wrap_err("Database URL not provided and DATABASE_URL not set")?;

        // Create database pool
        let pool = create_pool(&db_url)
            .await
            .wrap_err("Failed to create database pool")?;

        // Create blob manager for content service
        let annex_path = Utf8PathBuf::from(
            std::env::var("SINEX_ANNEX_PATH").unwrap_or_else(|_| "/tmp/sinex-annex".to_string()),
        );

        // Ensure the annex directory exists
        std::fs::create_dir_all(&annex_path)
            .with_context(|| format!("Failed to create annex directory: {:?}", annex_path))?;

        let annex_config = sinex_satellite_sdk::annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };
        let blob_manager = Arc::new(
            BlobManager::new(annex_config, pool.clone())
                .wrap_err("Failed to create blob manager")?,
        );

        // Initialize telemetry
        let telemetry = if let Ok(ingest_socket) = std::env::var("SINEX_INGEST_SOCKET") {
            // Create channel for telemetry events
            let (tx, mut rx) = mpsc::unbounded_channel();

            // Spawn task to forward telemetry events to ingestd
            let ingest_socket_clone = ingest_socket.clone();
            tokio::spawn(async move {
                let mut ingest_client = match IngestClient::new(&ingest_socket_clone).await {
                    Ok(client) => client,
                    Err(e) => {
                        tracing::error!("Failed to create ingest client for telemetry: {}", e);
                        return;
                    }
                };

                let mut batch = Vec::new();
                let mut last_flush = std::time::Instant::now();

                while let Some(event) = rx.recv().await {
                    batch.push(event);

                    // Flush on batch size or timeout
                    if batch.len() >= 10 || last_flush.elapsed() > Duration::from_secs(5) {
                        if let Err(e) = ingest_client.ingest_batch(&batch).await {
                            tracing::warn!("Failed to send telemetry batch: {}", e);
                        }
                        batch.clear();
                        last_flush = std::time::Instant::now();
                    }
                }

                // Final flush
                if !batch.is_empty() {
                    if let Err(e) = ingest_client.ingest_batch(&batch).await {
                        tracing::warn!("Failed to send final telemetry batch: {}", e);
                    }
                }
            });

            let accumulator = TelemetryAccumulator::new("sinex-gateway")
                .with_event_sender(tx.clone())
                .with_interval(Duration::from_secs(300)); // 5 minutes

            // Set global telemetry
            sinex_db::telemetry::telemetry::set_global_telemetry(accumulator.clone()).await;

            // Spawn telemetry emitter
            accumulator.clone().spawn_emitter();

            // Also spawn system telemetry emitter
            let system_emitter = SystemTelemetryEmitter::new(tx);
            system_emitter.spawn_emitter();

            Some(Arc::new(accumulator))
        } else {
            None
        };

        // Initialize all services
        Ok(Self {
            analytics: Arc::new(AnalyticsService::new(pool.clone())),
            content: Arc::new(ContentService::new(pool.clone(), blob_manager)),
            pkm: Arc::new(PkmService::new(pool.clone())),
            search: Arc::new(SearchService::new(pool)),
            telemetry,
        })
    }
}
