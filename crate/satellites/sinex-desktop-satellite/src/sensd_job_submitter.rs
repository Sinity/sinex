//! sensd job submission for desktop satellite
//!
//! This module handles submitting acquisition jobs to sensd for desktop data sources
//! (clipboard, window manager) instead of direct capture.

use color_eyre::eyre::{eyre, Result};
use serde_json::json;
use sinex_satellite_sdk::sensd_client::{
    AcquisitionMode, JobStatus, ResourceLimits, SensdClient, SensdJobConfig, SensorType,
};
use sqlx::PgPool;
use tracing::{debug, info};

/// Desktop source types that submit to sensd
pub enum DesktopSource {
    Clipboard,
    WindowManager,
}

/// Submit desktop monitoring jobs to sensd
pub struct DesktopSensdSubmitter {
    sensd_client: SensdClient,
    satellite_name: String,
}

impl DesktopSensdSubmitter {
    /// Create new desktop sensd submitter
    pub async fn new(db_pool: PgPool, satellite_name: String) -> Result<Self> {
        let sensd_client = SensdClient::new(db_pool, satellite_name.clone());
        Ok(Self {
            sensd_client,
            satellite_name,
        })
    }

    /// Submit a job for clipboard monitoring
    pub async fn submit_clipboard_job(&self, poll_interval_secs: u64) -> Result<()> {
        info!("Submitting clipboard monitoring job to sensd");

        let config = SensdJobConfig {
            sensor_type: SensorType::AppendStream,
            target_uri: "unix:///tmp/sinex-clipboard.sock".to_string(),
            source_identifier: format!("{}_clipboard", self.satellite_name),
            acquisition_mode: AcquisitionMode::Continuous {
                poll_interval_ms: poll_interval_secs * 1000,
            },
            parameters: json!({
                "source_type": "clipboard",
                "buffer_size": 65536,
                "rotation_policy": {
                    "max_bytes": 10485760, // 10MB
                    "max_duration_secs": 3600, // 1 hour
                }
            }),
            owner: self.satellite_name.clone(),
            resource_limits: Some(ResourceLimits {
                max_memory_bytes: Some(104857600), // 100MB
                max_cpu_percentage: Some(10.0),
                max_disk_bytes: Some(1073741824), // 1GB
                timeout_seconds: None,            // No timeout for continuous
            }),
            priority: 5,
        };

        // Submit job (will check for existing job first)
        let job_id = self.sensd_client.ensure_persistent_job(config).await?;

        debug!("Clipboard monitoring job submitted with ID: {}", job_id);
        Ok(())
    }

    /// Submit a job for window manager monitoring
    pub async fn submit_window_manager_job(&self, wm_type: &str, socket_path: &str) -> Result<()> {
        info!("Submitting window manager monitoring job to sensd");

        let config = SensdJobConfig {
            sensor_type: SensorType::AppendStream,
            target_uri: format!("unix://{}", socket_path),
            source_identifier: format!("{}_{}_wm", self.satellite_name, wm_type),
            acquisition_mode: AcquisitionMode::Continuous {
                poll_interval_ms: 100, // 100ms for responsive WM events
            },
            parameters: json!({
                "source_type": "window_manager",
                "wm_type": wm_type,
                "event_types": [
                    "workspace_change",
                    "window_focus",
                    "window_open",
                    "window_close",
                    "window_move",
                    "window_resize"
                ],
                "buffer_size": 65536,
                "rotation_policy": {
                    "max_bytes": 10485760, // 10MB
                    "max_duration_secs": 1800, // 30 minutes
                }
            }),
            owner: self.satellite_name.clone(),
            resource_limits: Some(ResourceLimits {
                max_memory_bytes: Some(104857600), // 100MB
                max_cpu_percentage: Some(10.0),
                max_disk_bytes: Some(1073741824), // 1GB
                timeout_seconds: None,            // No timeout for continuous
            }),
            priority: 5,
        };

        // Submit job (will check for existing job first)
        let job_id = self.sensd_client.ensure_persistent_job(config).await?;

        debug!(
            "Window manager monitoring job submitted with ID: {}",
            job_id
        );
        Ok(())
    }

    /// Submit a job for historical data import
    pub async fn submit_historical_import(
        &self,
        source: DesktopSource,
        since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<()> {
        info!("Submitting historical import job to sensd for {:?}", source);

        let (target_uri, source_identifier, parameters) = match source {
            DesktopSource::Clipboard => (
                "/var/log/clipboard_history.jsonl".to_string(),
                format!("{}_clipboard_history", self.satellite_name),
                json!({
                    "source_type": "clipboard_history",
                    "format": "jsonl",
                }),
            ),
            DesktopSource::WindowManager => (
                "/var/log/window_manager_events.jsonl".to_string(),
                format!("{}_wm_history", self.satellite_name),
                json!({
                    "source_type": "wm_history",
                    "format": "jsonl",
                }),
            ),
        };

        let config = SensdJobConfig {
            sensor_type: SensorType::BatchedPull,
            target_uri,
            source_identifier,
            acquisition_mode: AcquisitionMode::Historical {
                since,
                until: Some(chrono::Utc::now()),
            },
            parameters,
            owner: self.satellite_name.clone(),
            resource_limits: Some(ResourceLimits {
                max_memory_bytes: Some(524288000), // 500MB for batch processing
                max_cpu_percentage: Some(50.0),
                max_disk_bytes: Some(5368709120), // 5GB
                timeout_seconds: Some(3600),      // 1 hour timeout
            }),
            priority: 3, // Lower priority for historical
        };

        let job_id = self.sensd_client.create_job(config).await?;

        debug!("Historical import job submitted with ID: {}", job_id);
        Ok(())
    }

    /// Check status of submitted jobs
    pub async fn check_job_status(&self) -> Result<Vec<JobStatus>> {
        let jobs = self
            .sensd_client
            .list_jobs_for_owner(&self.satellite_name)
            .await?;
        Ok(jobs)
    }
}
