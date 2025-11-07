//! Job manager for coordinating sensor acquisition tasks.
//!
//! Polls raw.sensor_jobs table, manages concurrency, and dispatches to sensors.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_core::{db::DbPool, types::Ulid};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::{
    stream_processor::{ProcessorHandles, ProcessorRuntimeState},
    SatelliteError, SatelliteResult,
};

/// Sensor type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SensorType {
    AppendStream,
    TreeWatch,
    Custom(String),
}

impl std::fmt::Display for SensorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SensorType::AppendStream => write!(f, "append_stream"),
            SensorType::TreeWatch => write!(f, "tree_watch"),
            SensorType::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl std::str::FromStr for SensorType {
    type Err = SatelliteError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "append_stream" => Ok(SensorType::AppendStream),
            "tree_watch" => Ok(SensorType::TreeWatch),
            other => Ok(SensorType::Custom(other.to_string())),
        }
    }
}

/// Sensor job record from raw.sensor_jobs table
#[derive(Debug, Clone)]
pub struct SensorJob {
    pub id: Ulid,
    pub sensor_type: String,
    pub target_uri: String,
    pub config: Value,
    pub status: String,
    pub priority: i32,
    pub updated_at: DateTime<Utc>,
}

/// Trait for sensors that can process jobs
#[async_trait]
pub trait SensorExecutor: Send + Sync {
    /// Process a sensor job and return the material ID on success
    async fn process_job(&self, job: &SensorJob) -> SatelliteResult<Ulid>;

    /// Get the sensor type this executor handles
    fn sensor_type(&self) -> SensorType;
}

/// Job manager configuration
#[derive(Debug, Clone)]
pub struct JobManagerConfig {
    /// Poll interval for checking new jobs (milliseconds)
    pub poll_interval_ms: u64,
    /// Maximum concurrent jobs
    pub max_concurrent_jobs: usize,
}

impl Default for JobManagerConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            max_concurrent_jobs: 10,
        }
    }
}

/// Job manager for coordinating sensor tasks
pub struct JobManager {
    db_pool: DbPool,
    config: JobManagerConfig,
    active_jobs: Arc<RwLock<Vec<Ulid>>>,
    executors: Arc<RwLock<Vec<Arc<dyn SensorExecutor>>>>,
}

impl JobManager {
    /// Create new job manager
    pub fn new(db_pool: DbPool, config: JobManagerConfig) -> Self {
        Self {
            db_pool,
            config,
            active_jobs: Arc::new(RwLock::new(Vec::new())),
            executors: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a job manager from processor handles
    pub fn from_handles(handles: &ProcessorHandles, config: JobManagerConfig) -> Self {
        Self::new(handles.db_pool().clone(), config)
    }

    /// Create a job manager from a processor runtime
    pub fn from_runtime(runtime: &ProcessorRuntimeState, config: JobManagerConfig) -> Self {
        Self::from_handles(runtime.handles(), config)
    }

    /// Register a sensor executor
    pub async fn register_executor(&self, executor: Arc<dyn SensorExecutor>) {
        self.executors.write().await.push(executor);
    }

    /// Run job manager main loop
    pub async fn run(self: Arc<Self>) -> SatelliteResult<()> {
        info!("Starting job manager");

        let poll_interval = tokio::time::Duration::from_millis(self.config.poll_interval_ms);
        let mut interval = tokio::time::interval(poll_interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.process_pending_jobs().await {
                error!("Error processing jobs: {}", e);
            }

            if let Err(e) = self.cleanup_completed_jobs().await {
                error!("Error cleaning up jobs: {}", e);
            }
        }
    }

    /// Process pending jobs
    async fn process_pending_jobs(&self) -> SatelliteResult<()> {
        let active_count = self.active_jobs.read().await.len();

        if active_count >= self.config.max_concurrent_jobs {
            debug!("Max concurrent jobs reached, skipping poll");
            return Ok(());
        }

        let pending_jobs = sqlx::query_as!(
            SensorJob,
            r#"
            SELECT
                id as "id: Ulid",
                sensor_type,
                target_uri,
                config,
                status,
                priority,
                updated_at
            FROM raw.sensor_jobs
            WHERE status = 'active'
            ORDER BY priority DESC, updated_at
            LIMIT $1
            "#,
            (self.config.max_concurrent_jobs - active_count) as i64
        )
        .fetch_all(&self.db_pool)
        .await?;

        for job in pending_jobs {
            debug!("Processing job: {} for {}", job.id, job.target_uri);

            self.update_job_status(&job.id, "paused").await?;
            self.active_jobs.write().await.push(job.id);

            let job_manager = Arc::new(JobManagerHandle {
                db_pool: self.db_pool.clone(),
                active_jobs: self.active_jobs.clone(),
                executors: self.executors.clone(),
            });

            tokio::spawn(async move {
                if let Err(e) = job_manager.execute_job(job).await {
                    error!("Job execution failed: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Update job status
    async fn update_job_status(&self, job_id: &Ulid, status: &str) -> SatelliteResult<()> {
        let db_status = match status {
            "running" | "paused" => "paused",
            "completed" | "failed" => "retired",
            _ => "active",
        };

        sqlx::query!(
            r#"
            UPDATE raw.sensor_jobs
            SET status = $2::text,
                updated_at = NOW()
            WHERE id = $1::ulid
            "#,
            *job_id as Ulid,
            db_status,
        )
        .execute(&self.db_pool)
        .await?;

        Ok(())
    }

    /// Clean up completed jobs
    async fn cleanup_completed_jobs(&self) -> SatelliteResult<()> {
        let mut active = self.active_jobs.write().await;
        let original_count = active.len();

        if original_count == 0 {
            return Ok(());
        }

        let still_running: Vec<Ulid> = sqlx::query_scalar!(
            r#"
            SELECT id as "id: Ulid"
            FROM raw.sensor_jobs
            WHERE id = ANY($1::ulid[])
            AND status IN ('active', 'paused')
            "#,
            &active.clone() as &[Ulid],
        )
        .fetch_all(&self.db_pool)
        .await?;

        *active = still_running;

        if active.len() < original_count {
            debug!(
                "Cleaned up {} completed jobs",
                original_count - active.len()
            );
        }

        Ok(())
    }
}

/// Handle for executing jobs (can be cloned and sent to spawned tasks)
struct JobManagerHandle {
    db_pool: DbPool,
    active_jobs: Arc<RwLock<Vec<Ulid>>>,
    executors: Arc<RwLock<Vec<Arc<dyn SensorExecutor>>>>,
}

impl JobManagerHandle {
    /// Execute a single job
    async fn execute_job(&self, job: SensorJob) -> SatelliteResult<()> {
        info!("Executing job {} for {}", job.id, job.target_uri);

        let sensor_type: SensorType = job.sensor_type.parse()?;

        let executors = self.executors.read().await;
        let executor = executors
            .iter()
            .find(|e| e.sensor_type() == sensor_type)
            .ok_or_else(|| {
                SatelliteError::Processing(format!("No executor for sensor type: {}", sensor_type))
            })?;

        let result = executor.process_job(&job).await;

        match result {
            Ok(material_id) => {
                info!(
                    "Job {} completed successfully, material: {}",
                    job.id, material_id
                );
                self.update_job_status(&job.id, "retired").await?;
            }
            Err(e) => {
                error!("Job {} failed: {}", job.id, e);
                self.update_job_error(&job.id, &e.to_string()).await?;
            }
        }

        self.active_jobs.write().await.retain(|id| *id != job.id);

        Ok(())
    }

    /// Update job status
    async fn update_job_status(&self, job_id: &Ulid, status: &str) -> SatelliteResult<()> {
        let db_status = match status {
            "running" | "paused" => "paused",
            "completed" | "failed" => "retired",
            _ => "active",
        };

        sqlx::query!(
            r#"
            UPDATE raw.sensor_jobs
            SET status = $2::text,
                updated_at = NOW()
            WHERE id = $1::ulid
            "#,
            *job_id as Ulid,
            db_status,
        )
        .execute(&self.db_pool)
        .await?;

        Ok(())
    }

    /// Update job error
    async fn update_job_error(&self, job_id: &Ulid, error: &str) -> SatelliteResult<()> {
        sqlx::query!(
            r#"
            UPDATE raw.sensor_jobs
            SET status = 'retired',
                updated_at = NOW()
            WHERE id = $1::ulid
            "#,
            *job_id as Ulid,
        )
        .execute(&self.db_pool)
        .await?;

        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_states (job_id, error_count)
            VALUES ($1::ulid, 1)
            ON CONFLICT (job_id)
            DO UPDATE SET error_count = sensor_states.error_count + 1,
                         updated_at = NOW()
            "#,
            *job_id as Ulid,
        )
        .execute(&self.db_pool)
        .await?;

        warn!("Job {} failed: {}", job_id, error);

        Ok(())
    }
}
