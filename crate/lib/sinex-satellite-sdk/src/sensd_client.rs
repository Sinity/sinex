//! Client for submitting jobs to sensd
//!
//! Provides helpers for satellites to submit acquisition jobs to sensd
//! instead of directly capturing source material.
//!
//! Based on TARGET_canonical.md architecture:
//! - Jobs are persistent and stored in raw.sensor_jobs
//! - sensd continuously monitors based on job configuration
//! - Historical import works from last_successful_acquisition

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use serde_json::{json, Value};
use sinex_core::types::Ulid;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for sensd job submission
/// Maps to raw.sensor_jobs table schema from TARGET_canonical.md
#[derive(Debug, Clone)]
pub struct SensdJobConfig {
    /// Type of sensor pattern to use
    pub sensor_type: SensorType,
    /// Target URI/path for the sensor
    pub target_uri: String,
    /// Source identifier for material registry
    pub source_identifier: String,
    /// Acquisition mode configuration
    pub acquisition_mode: AcquisitionMode,
    /// Job-specific parameters
    pub parameters: Value,
    /// Owner identifier (satellite name)
    pub owner: String,
    /// Resource limits
    pub resource_limits: Option<ResourceLimits>,
    /// Job priority (higher = more important)
    pub priority: i32,
}

/// Sensor types from TARGET_canonical.md pattern catalog
#[derive(Debug, Clone)]
pub enum SensorType {
    /// append_stream: logs, sockets, JSONL
    AppendStream,
    /// tree_watch: filesystem monitoring
    TreeWatch,
    /// batched_pull: API pagination with cursor/ETag
    BatchedPull,
    /// replace_snapshot: full state snapshots (CSV/SQLite)
    ReplaceSnapshot,
    /// multi_file: directory drops
    MultiFile,
    /// db_snapshot: database backup API
    DbSnapshot,
}

impl SensorType {
    fn as_str(&self) -> &str {
        match self {
            SensorType::AppendStream => "append_stream",
            SensorType::TreeWatch => "tree_watch",
            SensorType::BatchedPull => "batched_pull",
            SensorType::ReplaceSnapshot => "replace_snapshot",
            SensorType::MultiFile => "multi_file",
            SensorType::DbSnapshot => "db_snapshot",
        }
    }
}

/// Acquisition mode for job
#[derive(Debug, Clone)]
pub enum AcquisitionMode {
    /// Continuous monitoring (persistent job)
    Continuous {
        /// How often to check for new data (ms)
        poll_interval_ms: u64,
    },
    /// One-time historical import
    Historical {
        /// Import data since this time
        since: Option<DateTime<Utc>>,
        /// Import data until this time
        until: Option<DateTime<Utc>>,
    },
    /// Hybrid: historical catch-up then continuous
    CatchUpThenContinuous {
        /// Start from this point in history
        catch_up_from: Option<DateTime<Utc>>,
        /// Then poll at this interval
        poll_interval_ms: u64,
    },
}

/// Resource limits for job execution
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Max memory usage in bytes
    pub max_memory_bytes: Option<i64>,
    /// Max CPU percentage
    pub max_cpu_percentage: Option<f64>,
    /// Max disk usage in bytes
    pub max_disk_bytes: Option<i64>,
    /// Timeout in seconds
    pub timeout_seconds: Option<i64>,
}

/// Client for interacting with sensd
pub struct SensdClient {
    db_pool: PgPool,
    satellite_name: String,
}

impl SensdClient {
    /// Create new sensd client
    pub fn new(db_pool: PgPool, satellite_name: impl Into<String>) -> Self {
        Self {
            db_pool,
            satellite_name: satellite_name.into(),
        }
    }

    /// Submit or get existing job for persistent monitoring
    ///
    /// For persistent sources (like Hyprland socket), this will:
    /// 1. Check if a job already exists for this source
    /// 2. If yes, return the existing job ID
    /// 3. If no, create a new persistent job
    pub async fn ensure_persistent_job(&self, config: SensdJobConfig) -> Result<Ulid> {
        // Check for existing job with same target_uri and sensor_type
        let existing = sqlx::query!(
            r#"
            SELECT id as "id: Ulid", status
            FROM raw.sensor_jobs
            WHERE target_uri = $1
            AND sensor_type = $2
            AND status IN ('active', 'paused')
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            config.target_uri,
            config.sensor_type.as_str(),
        )
        .fetch_optional(&self.db_pool)
        .await?;

        if let Some(job) = existing {
            info!(
                "Found existing {} job {} for {}",
                job.status, job.id, config.target_uri
            );
            return Ok(job.id);
        }

        // No existing job, create new one
        self.create_job(config).await
    }

    /// Create a new job
    pub async fn create_job(&self, config: SensdJobConfig) -> Result<Ulid> {
        let job_id = Ulid::new();

        let acquisition_mode_json = match &config.acquisition_mode {
            AcquisitionMode::Continuous { poll_interval_ms } => {
                json!({
                    "mode": "continuous",
                    "poll_interval_ms": poll_interval_ms,
                })
            }
            AcquisitionMode::Historical { since, until } => {
                json!({
                    "mode": "historical",
                    "since": since,
                    "until": until,
                })
            }
            AcquisitionMode::CatchUpThenContinuous {
                catch_up_from,
                poll_interval_ms,
            } => {
                json!({
                    "mode": "catch_up_then_continuous",
                    "catch_up_from": catch_up_from,
                    "poll_interval_ms": poll_interval_ms,
                })
            }
        };

        let resource_limits_json = config
            .resource_limits
            .as_ref()
            .map(|rl| {
                json!({
                    "max_memory_bytes": rl.max_memory_bytes,
                    "max_cpu_percentage": rl.max_cpu_percentage,
                    "max_disk_bytes": rl.max_disk_bytes,
                    "timeout_seconds": rl.timeout_seconds,
                })
            })
            .unwrap_or_else(|| json!({}));

        info!(
            "Creating sensd job {} for {} on {} ({})",
            job_id,
            config.sensor_type.as_str(),
            config.target_uri,
            config.source_identifier
        );

        // Insert job following TARGET_canonical.md schema
        // Note: The actual schema uses 'id' not 'job_id', and different columns
        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                id,
                sensor_type,
                target_uri,
                config,
                status,
                priority,
                updated_at
            ) VALUES ($1, $2, $3, $4, 'active', $5, $6)
            "#,
            job_id as Ulid,
            config.sensor_type.as_str(),
            config.target_uri,
            json!({
                "source_identifier": config.source_identifier,
                "acquisition_mode": acquisition_mode_json,
                "parameters": config.parameters,
                "owner": config.owner,
                "resource_limits": resource_limits_json,
            }),
            config.priority,
            Utc::now(),
        )
        .execute(&self.db_pool)
        .await?;

        debug!("Created job {} for {}", job_id, config.source_identifier);
        Ok(job_id)
    }

    /// Get or create persistent Unix socket monitoring job
    ///
    /// For sources like Hyprland socket, this ensures a single persistent job exists
    pub async fn ensure_unix_socket_monitor(
        &self,
        socket_path: &str,
        source_identifier: &str,
    ) -> Result<Ulid> {
        self.ensure_persistent_job(SensdJobConfig {
            sensor_type: SensorType::AppendStream,
            target_uri: socket_path.to_string(),
            source_identifier: source_identifier.to_string(),
            acquisition_mode: AcquisitionMode::Continuous {
                poll_interval_ms: 100, // 100ms for real-time sockets
            },
            parameters: json!({
                "buffer_size": 8192,
                "reconnect_on_error": true,
                "line_delimiter": "\n",
            }),
            owner: self.satellite_name.clone(),
            resource_limits: None,
            priority: 1000,
        })
        .await
    }

    /// Get or create persistent SQLite monitoring job (e.g., Atuin)
    pub async fn ensure_sqlite_monitor(
        &self,
        db_path: &str,
        source_identifier: &str,
    ) -> Result<Ulid> {
        self.ensure_persistent_job(SensdJobConfig {
            sensor_type: SensorType::DbSnapshot,
            target_uri: db_path.to_string(),
            source_identifier: source_identifier.to_string(),
            acquisition_mode: AcquisitionMode::CatchUpThenContinuous {
                catch_up_from: None,    // Will use last_successful_acquisition
                poll_interval_ms: 5000, // Check every 5 seconds
            },
            parameters: json!({
                "snapshot_mode": "incremental",
                "track_rowid": true,
            }),
            owner: self.satellite_name.clone(),
            resource_limits: None,
            priority: 500,
        })
        .await
    }

    /// Query for material captured since last processing
    ///
    /// This respects the last_successful_acquisition from sensor_states
    pub async fn query_new_material_since_last(
        &self,
        source_identifier: &str,
    ) -> Result<Vec<MaterialInfo>> {
        // Get last successful acquisition time from sensor_states
        // TODO: sensor_states table doesn't exist in current schema
        /*
        let last_acquisition = sqlx::query!(
            r#"
            SELECT ss.last_successful_acquisition
            FROM raw.sensor_states ss
            JOIN raw.sensor_jobs sj ON ss.job_id = sj.job_id
            WHERE sj.source_identifier = $1
            ORDER BY ss.last_successful_acquisition DESC NULLS LAST
            LIMIT 1
            "#,
            source_identifier,
        )
        .fetch_optional(&self.db_pool)
        .await?
        .and_then(|row| row.last_successful_acquisition);
        */
        // Until we persist last acquisition, default to 7 days ago
        let since = Utc::now() - chrono::Duration::days(7);

        self.query_new_material(source_identifier, Some(since))
            .await
    }

    /// Submit a one-time historical import job
    pub async fn submit_historical_import(
        &self,
        sensor_type: SensorType,
        target_uri: &str,
        source_identifier: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
    ) -> Result<Ulid> {
        self.create_job(SensdJobConfig {
            sensor_type,
            target_uri: target_uri.to_string(),
            source_identifier: source_identifier.to_string(),
            acquisition_mode: AcquisitionMode::Historical { since, until },
            parameters: json!({
                "import_mode": "bulk",
                "verify_completeness": true,
            }),
            owner: self.satellite_name.clone(),
            resource_limits: None,
            priority: 100, // Low priority for historical imports
        })
        .await
    }

    /// Query for new material from a specific source
    pub async fn query_new_material(
        &self,
        source_identifier: &str,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<MaterialInfo>> {
        let since = since.unwrap_or_else(|| Utc::now() - chrono::Duration::hours(1));

        let materials = sqlx::query!(
            r#"
            SELECT 
                id as "material_id: Ulid",
                staged_at as acquired_at,
                COALESCE((metadata->>'size_bytes')::bigint, 0) as "size_bytes!",
                material_kind as mime_type,
                metadata
            FROM raw.source_material_registry
            WHERE source_identifier = $1
            AND staged_at > $2
            -- TODO: Need to fix provenance check
            -- For now, just check if any events exist
            AND NOT EXISTS (
                SELECT 1 FROM core.events 
                WHERE source_event_ids IS NOT NULL
                LIMIT 1
            )
            ORDER BY staged_at DESC
            LIMIT 100
            "#,
            source_identifier,
            since,
        )
        .fetch_all(&self.db_pool)
        .await?;

        Ok(materials
            .into_iter()
            .map(|m| MaterialInfo {
                material_id: m.material_id,
                acquired_at: m.acquired_at,
                size_bytes: m.size_bytes,
                mime_type: Some(m.mime_type),
                metadata: m.metadata,
            })
            .collect())
    }

    /// List all jobs for a specific owner
    pub async fn list_jobs_for_owner(&self, _owner: &str) -> Result<Vec<JobStatus>> {
        // TODO: Fix query to match actual schema
        /*
        let jobs = sqlx::query!(
            r#"
            SELECT
                id as "job_id: Ulid",
                sensor_type,
                status,
                updated_at as created_at,
                NULL as "material_id?: Ulid",
                NULL as error_message
            FROM raw.sensor_jobs
            WHERE config->>'owner' = $1
            ORDER BY updated_at DESC
            LIMIT 100
            "#,
            owner,
        )
        .fetch_all(&self.db_pool)
        .await?;
        */
        let jobs: Vec<JobStatus> = vec![]; // Placeholder until schema is fixed

        Ok(jobs
            .into_iter()
            .map(|_j| JobStatus {
                job_id: Ulid::new(),
                sensor_type: String::new(),
                status: String::new(),
                created_at: Utc::now(),
                material_id: None,
                error_message: None,
            })
            .collect())
    }

    /// Wait for job completion
    pub async fn wait_for_job(&self, _job_id: Ulid, _timeout: Duration) -> Result<JobResult> {
        // TODO: Query needs to be updated to match actual schema
        /*
        let job_status = sqlx::query!(
            r#"
            SELECT status, NULL as "material_id?: Ulid", NULL as error_message
            FROM raw.sensor_jobs
            WHERE id = $1
            "#,
            job_id as Ulid,
        )
        .fetch_one(&self.db_pool)
        .await?;
        */
        // Placeholder until schema is fixed
        return Err(eyre!("sensor_jobs schema mismatch - needs updating"));

        /*
        #[allow(unreachable_code)]
        match job_status.status.as_str() {
            "completed" => {
                return Ok(JobResult::Completed {
                    material_id: job_status.material_id,
                });
            }
            "failed" => {
                return Ok(JobResult::Failed {
                    error: job_status
                        .error_message
                        .unwrap_or_else(|| "Unknown error".to_string()),
                });
            }
            "cancelled" => {
                return Ok(JobResult::Cancelled);
            }
            _ => {
                // Still pending or running
                if start.elapsed() > timeout {
                    return Err(eyre!("Job {} timed out", job_id));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        */
        // unreachable: placeholder early return above
        // Err(eyre!("wait_for_job not implemented"))
    }
}

/// Result of a sensd job
#[derive(Debug)]
pub enum JobResult {
    Completed { material_id: Option<Ulid> },
    Failed { error: String },
    Cancelled,
}

/// Job status information
#[derive(Debug, Clone)]
pub struct JobStatus {
    pub job_id: Ulid,
    pub sensor_type: String,
    pub status: String,
    pub created_at: chrono::DateTime<Utc>,
    pub material_id: Option<Ulid>,
    pub error_message: Option<String>,
}

/// Information about captured material
#[derive(Debug)]
pub struct MaterialInfo {
    pub material_id: Ulid,
    pub acquired_at: chrono::DateTime<Utc>,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub metadata: Value,
}

/// Trait for acknowledging direct capture when necessary
pub trait DirectCaptureAcknowledgment {
    /// Explicitly acknowledge that this component needs direct capture
    ///
    /// # Safety
    /// This should only be used when sensd truly cannot handle the capture,
    /// such as when deep integration with a library is required.
    fn acknowledge_direct_capture(&self, reason: &str) -> DirectCaptureToken {
        warn!(
            "DIRECT CAPTURE ACKNOWLEDGED: {} - This bypasses sensd!",
            reason
        );
        DirectCaptureToken {
            reason: reason.to_string(),
            acknowledged_at: Utc::now(),
        }
    }
}

/// Token proving direct capture was explicitly acknowledged
#[derive(Debug)]
pub struct DirectCaptureToken {
    pub reason: String,
    pub acknowledged_at: chrono::DateTime<Utc>,
}

impl DirectCaptureToken {
    /// Verify this token is valid and recent
    pub fn verify(&self) -> Result<()> {
        let age = Utc::now() - self.acknowledged_at;
        if age > chrono::Duration::hours(24) {
            return Err(eyre!(
                "Direct capture token expired. Re-acknowledge if still needed."
            ));
        }
        Ok(())
    }
}

/// Macro to enforce sensd usage by default  
/// Returns Result<(), SatelliteError> - must be used with ? operator
#[macro_export]
macro_rules! ensure_sensd_or_acknowledged {
    ($component:expr, $capture_type:expr) => {{
        use $crate::sensd_client::DirectCaptureAcknowledgment;
        use $crate::SatelliteError;

        // This will fail to compile if the component doesn't have a sensd_client field
        // or a direct_capture_token field
        if let Some(ref token) = $component.direct_capture_token {
            token.verify().map_err(|e| {
                SatelliteError::Configuration(format!(
                    "Direct capture token invalid for {}: {}",
                    stringify!($component),
                    e
                ))
            })?;
        } else if $component.sensd_client.is_none() {
            return Err(SatelliteError::Configuration(format!(
                "Component {} is attempting {} without sensd client or acknowledgment!",
                stringify!($component),
                $capture_type
            )));
        }

        Ok::<(), SatelliteError>(())
    }};
}
