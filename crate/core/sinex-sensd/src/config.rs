#![doc = include_str!("../doc/config.md")]

//! Configuration for sensd daemon.

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use sinex_core::types::validate_path;
use validator::Validate;

/// sensd daemon configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct SensdConfig {
    /// Database connection URL
    #[validate(length(min = 1))]
    #[builder(default = String::from("postgresql:///sinex_dev?host=/run/postgresql"))]
    pub database_url: String,

    /// Port for gRPC MaterialSliceStream service
    #[validate(range(min = 1024, max = 65535))]
    #[builder(default = 50052)]
    pub grpc_port: u16,

    /// Material storage path (for blobs)
    #[validate(length(min = 1))]
    #[validate(custom(
        function = "validate_material_path",
        message = "Invalid material storage path"
    ))]
    #[builder(default = default_material_path())]
    pub material_storage_path: String,

    /// Temporal ledger configuration
    #[builder(default = TemporalLedgerConfig::default())]
    pub temporal_ledger: TemporalLedgerConfig,

    /// Job manager configuration
    #[builder(default = JobManagerConfig::default())]
    pub job_manager: JobManagerConfig,

    /// Sensor configuration
    #[builder(default = SensorConfig::default())]
    pub sensors: SensorConfig,
}

/// Temporal ledger configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct TemporalLedgerConfig {
    /// Batch size for ledger writes
    #[validate(range(min = 1, max = 10000))]
    #[builder(default = 100)]
    pub batch_size: usize,

    /// Flush interval for ledger writes
    #[builder(default = 1000)]
    pub flush_interval_ms: u64,

    /// Maximum slice size in bytes
    #[validate(range(min = 1024))]
    #[builder(default = 10 * 1024 * 1024)] // 10MB
    pub max_slice_size: usize,
}

/// Job manager configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct JobManagerConfig {
    /// Poll interval for checking new jobs
    #[builder(default = 1000)]
    pub poll_interval_ms: u64,

    /// Maximum concurrent jobs
    #[validate(range(min = 1, max = 1000))]
    #[builder(default = 10)]
    pub max_concurrent_jobs: usize,

    /// Job timeout duration
    #[builder(default = 60000)] // 1 minute
    pub job_timeout_ms: u64,
}

/// Sensor configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, bon::Builder)]
#[builder(on(String, into))]
pub struct SensorConfig {
    /// Enable append_stream sensor
    #[builder(default = true)]
    pub enable_append_stream: bool,

    /// Enable tree_watch sensor
    #[builder(default = true)]
    pub enable_tree_watch: bool,

    /// Socket buffer size for append_stream
    #[validate(range(min = 1024))]
    #[builder(default = 65536)]
    pub socket_buffer_size: usize,

    /// File watcher debounce duration
    #[builder(default = 100)]
    pub tree_watch_debounce_ms: u64,
}

impl Default for SensdConfig {
    fn default() -> Self {
        Self {
            database_url: String::from("postgresql:///sinex_dev?host=/run/postgresql"),
            grpc_port: 50052,
            material_storage_path: String::from("/tmp/sinex/materials"),
            temporal_ledger: TemporalLedgerConfig::default(),
            job_manager: JobManagerConfig::default(),
            sensors: SensorConfig::default(),
        }
    }
}

impl Default for TemporalLedgerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_interval_ms: 1000,
            max_slice_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

impl Default for JobManagerConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            max_concurrent_jobs: 10,
            job_timeout_ms: 60000, // 1 minute
        }
    }
}

impl Default for SensorConfig {
    fn default() -> Self {
        Self {
            enable_append_stream: true,
            enable_tree_watch: true,
            socket_buffer_size: 65536,
            tree_watch_debounce_ms: 100,
        }
    }
}

impl SensdConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        // Start with a default config
        let mut config = Self::default();

        // Override with environment variables
        if let Ok(url) = std::env::var("DATABASE_URL") {
            config.database_url = url;
        }

        if let Some(port) = std::env::var("SENSD_GRPC_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
        {
            config.grpc_port = port;
        }

        if let Ok(path) = std::env::var("SENSD_MATERIAL_PATH") {
            config.material_storage_path = path;
        }

        config.validate()?;
        Ok(config)
    }
}

/// Default material storage path (validated)
fn default_material_path() -> String {
    let path = "/tmp/sinex/materials";
    match validate_path(path) {
        Ok(validated) => validated.to_string(),
        Err(_) => "/tmp/sinex/materials".to_string(), // Fallback to original if validation fails
    }
}

/// Custom validator for material storage path
fn validate_material_path(path: &str) -> Result<(), validator::ValidationError> {
    validate_path(path)
        .map(|_| ())
        .map_err(|_| validator::ValidationError::new("invalid_material_path"))
}
