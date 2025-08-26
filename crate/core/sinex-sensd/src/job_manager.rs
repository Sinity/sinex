//! Job manager for sensd
//!
//! Manages sensor jobs, coordinating data acquisition tasks
//! and ensuring reliable capture of source materials.

use crate::{
    config::JobManagerConfig,
    sensors::{AppendStreamSensor, TreeWatchSensor},
    temporal_ledger::TemporalLedger,
};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_core::types::Ulid;
use sqlx::{PgPool, Type};
use std::{fmt, str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Sensor job status
#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[sqlx(type_name = "text")]
#[sqlx(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Sensor type enumeration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SensorType {
    AppendStream,
    TreeWatch,
}

impl fmt::Display for SensorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SensorType::AppendStream => write!(f, "append_stream"),
            SensorType::TreeWatch => write!(f, "tree_watch"),
        }
    }
}

impl From<String> for SensorType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "append_stream" => SensorType::AppendStream,
            "tree_watch" => SensorType::TreeWatch,
            _ => SensorType::AppendStream, // Default fallback
        }
    }
}

impl FromStr for SensorType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "append_stream" => Ok(SensorType::AppendStream),
            "tree_watch" => Ok(SensorType::TreeWatch),
            _ => Err(format!("Unknown sensor type: {}", s)),
        }
    }
}

/// Sensor job record (matches raw.sensor_jobs table)
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

/// Job manager
pub struct JobManager {
    db_pool: PgPool,
    temporal_ledger: Arc<TemporalLedger>,
    config: JobManagerConfig,
    active_jobs: Arc<RwLock<Vec<Ulid>>>,
}

impl JobManager {
    /// Create new job manager
    pub async fn new(
        db_pool: PgPool,
        temporal_ledger: Arc<TemporalLedger>,
        config: JobManagerConfig,
    ) -> Result<Self> {
        Ok(Self {
            db_pool,
            temporal_ledger,
            config,
            active_jobs: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Run job manager main loop
    pub async fn run(
        &self,
        append_sensor: Option<Arc<AppendStreamSensor>>,
        tree_sensor: Option<Arc<TreeWatchSensor>>,
    ) -> Result<()> {
        info!("Starting job manager");

        let poll_interval = tokio::time::Duration::from_millis(self.config.poll_interval_ms);
        let mut interval = tokio::time::interval(poll_interval);

        loop {
            interval.tick().await;

            // Check for new jobs
            if let Err(e) = self
                .process_pending_jobs(&append_sensor, &tree_sensor)
                .await
            {
                error!("Error processing jobs: {}", e);
            }

            // Clean up completed jobs
            if let Err(e) = self.cleanup_completed_jobs().await {
                error!("Error cleaning up jobs: {}", e);
            }
        }
    }

    /// Process pending jobs
    async fn process_pending_jobs(
        &self,
        append_sensor: &Option<Arc<AppendStreamSensor>>,
        tree_sensor: &Option<Arc<TreeWatchSensor>>,
    ) -> Result<()> {
        // Get current active job count
        let active_count = self.active_jobs.read().await.len();

        if active_count >= self.config.max_concurrent_jobs {
            debug!("Max concurrent jobs reached, skipping poll");
            return Ok(());
        }

        // Query for pending jobs (active status in the schema)
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

            // Mark job as running (using 'paused' as a running state)
            self.update_job_status(&job.id, "paused".to_string(), None)
                .await?;

            // Add to active jobs
            self.active_jobs.write().await.push(job.id);

            // Spawn job processor
            let job_manager = self.clone();
            let append_sensor = append_sensor.clone();
            let tree_sensor = tree_sensor.clone();

            tokio::spawn(async move {
                if let Err(e) = job_manager
                    .execute_job(job, append_sensor, tree_sensor)
                    .await
                {
                    error!("Job execution failed: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Execute a single job
    async fn execute_job(
        &self,
        job: SensorJob,
        append_sensor: Option<Arc<AppendStreamSensor>>,
        tree_sensor: Option<Arc<TreeWatchSensor>>,
    ) -> Result<()> {
        info!("Executing job {} for {}", job.id, job.target_uri);

        let sensor_type = SensorType::from_str(&job.sensor_type)
            .map_err(|e| eyre!("Invalid sensor type: {}", e))?;
        let result = match sensor_type {
            SensorType::AppendStream => {
                if let Some(sensor) = append_sensor {
                    sensor.process_job(&job, &self.temporal_ledger).await
                } else {
                    Err(eyre!("append_stream sensor not enabled"))
                }
            }
            SensorType::TreeWatch => {
                if let Some(sensor) = tree_sensor {
                    sensor.process_job(&job, &self.temporal_ledger).await
                } else {
                    Err(eyre!("tree_watch sensor not enabled"))
                }
            }
        };

        // Update job status based on result
        match result {
            Ok(material_id) => {
                info!(
                    "Job {} completed successfully, material: {}",
                    job.id, material_id
                );
                self.update_job_status(&job.id, "retired".to_string(), Some(material_id))
                    .await?;
            }
            Err(e) => {
                error!("Job {} failed: {}", job.id, e);
                self.update_job_error(&job.id, &e.to_string()).await?;
            }
        }

        // Remove from active jobs
        self.active_jobs.write().await.retain(|id| *id != job.id);

        Ok(())
    }

    /// Update job status
    async fn update_job_status(
        &self,
        job_id: &Ulid,
        status: String,
        _material_id: Option<Ulid>,
    ) -> Result<()> {
        // Map to valid status values: 'active', 'paused', 'retired'
        let db_status = match status.as_str() {
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

    /// Update job error (store error in sensor_states table)
    async fn update_job_error(&self, job_id: &Ulid, error: &str) -> Result<()> {
        // Update job status to retired
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

        // Update error count in sensor_states
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

        // Log the error since we can't store it in the table
        error!("Job {} failed: {}", job_id, error);

        Ok(())
    }

    /// Clean up completed jobs
    async fn cleanup_completed_jobs(&self) -> Result<()> {
        // Remove completed jobs from active list
        let mut active = self.active_jobs.write().await;
        let original_count = active.len();

        if original_count == 0 {
            return Ok(());
        }

        // Query to check which jobs are actually still active/paused
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

impl Clone for JobManager {
    fn clone(&self) -> Self {
        Self {
            db_pool: self.db_pool.clone(),
            temporal_ledger: self.temporal_ledger.clone(),
            config: self.config.clone(),
            active_jobs: self.active_jobs.clone(),
        }
    }
}
