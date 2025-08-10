//! Main sensd service implementation

use crate::{
    config::SensdConfig,
    job_manager::JobManager,
    sensors::{AppendStreamSensor, TreeWatchSensor},
    temporal_ledger::TemporalLedger,
};
use color_eyre::eyre::{eyre, Result};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Main sensd service
pub struct SensdService {
    config: SensdConfig,
    db_pool: PgPool,
    job_manager: Arc<JobManager>,
    temporal_ledger: Arc<TemporalLedger>,
    append_stream_sensor: Option<Arc<AppendStreamSensor>>,
    tree_watch_sensor: Option<Arc<TreeWatchSensor>>,
}

impl SensdService {
    /// Create new sensd service
    pub async fn new(config: SensdConfig) -> Result<Self> {
        info!("Initializing sensd service");

        // Connect to database
        let db_pool = PgPool::connect(&config.database_url).await?;
        info!("Connected to database");

        // Create temporal ledger
        let temporal_ledger =
            Arc::new(TemporalLedger::new(db_pool.clone(), config.temporal_ledger.clone()).await?);
        info!("Temporal ledger initialized");

        // Create job manager
        let job_manager = Arc::new(
            JobManager::new(
                db_pool.clone(),
                temporal_ledger.clone(),
                config.job_manager.clone(),
            )
            .await?,
        );
        info!("Job manager initialized");

        // Create sensors if enabled
        let append_stream_sensor = if config.sensors.enable_append_stream {
            info!("Initializing append_stream sensor");
            Some(Arc::new(AppendStreamSensor::new(
                temporal_ledger.clone(),
                config.sensors.clone(),
            )?))
        } else {
            None
        };

        let tree_watch_sensor = if config.sensors.enable_tree_watch {
            info!("Initializing tree_watch sensor");
            Some(Arc::new(TreeWatchSensor::new(
                temporal_ledger.clone(),
                config.sensors.clone(),
            )?))
        } else {
            None
        };

        Ok(Self {
            config,
            db_pool,
            job_manager,
            temporal_ledger,
            append_stream_sensor,
            tree_watch_sensor,
        })
    }

    /// Run the service
    pub async fn run(self) -> Result<()> {
        info!("Starting sensd service");

        // Start temporal ledger background worker
        let ledger_handle = {
            let ledger = self.temporal_ledger.clone();
            tokio::spawn(async move {
                if let Err(e) = ledger.run_background_worker().await {
                    error!("Temporal ledger worker error: {}", e);
                }
            })
        };

        // Start job manager
        let job_manager_handle = {
            let job_manager = self.job_manager.clone();
            let append_sensor = self.append_stream_sensor.clone();
            let tree_sensor = self.tree_watch_sensor.clone();

            tokio::spawn(async move {
                if let Err(e) = job_manager.run(append_sensor, tree_sensor).await {
                    error!("Job manager error: {}", e);
                }
            })
        };

        // Start gRPC server for MaterialSliceStream
        let grpc_handle = {
            let config = self.config.clone();
            let temporal_ledger = self.temporal_ledger.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::run_grpc_server(config, temporal_ledger).await {
                    error!("gRPC server error: {}", e);
                }
            })
        };

        // Wait for all tasks
        tokio::try_join!(ledger_handle, job_manager_handle, grpc_handle)?;

        Ok(())
    }

    /// Run gRPC server for MaterialSliceStream
    async fn run_grpc_server(
        config: SensdConfig,
        temporal_ledger: Arc<TemporalLedger>,
    ) -> Result<()> {
        // TODO: Implement gRPC server
        // This will provide the MaterialSliceStream interface to ingestors
        info!("gRPC server would run on port {}", config.grpc_port);

        // For now, just sleep
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    }
}
