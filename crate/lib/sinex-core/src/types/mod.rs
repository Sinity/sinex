//! Core types and constants for the Sinex system
//!
//! This crate provides the foundational types, error handling, and constants
//! that are used throughout the Sinex ecosystem.
//!
//! # Core Philosophy: Deep Oneness and Auditable Metacognition
//!
//! Sinex is built on four fundamental pillars that guide all architectural decisions:
//!
//! ## 1. Deep Oneness
//!
//! Dissolving artificial distinctions to reveal underlying unity:
//! - **One event stream** (`core.events`) - no separation between raw and synthesis
//! - **One processing primitive** (`StatefulStreamProcessor`) - all components are stream processors
//! - **One data lifecycle** (Stage → Replay → Synthesis → Curation → Action)
//!
//! ## 2. Declarative Core
//!
//! Logic as data, not code:
//! - System behavior described through configuration and patterns
//! - Imperative code reserved for inherently complex operations
//! - Evolution toward SQL-as-Automaton and Prompt-as-Automaton
//!
//! ## 3. Human-in-the-Loop
//!
//! Acknowledging imperfection, empowering users:
//! - Faithful recording of messy reality without premature cleverness
//! - Automated resolution where possible, human judgment when needed
//! - Users as final arbiters of meaning through curation
//!
//! ## 4. Auditable Metacognition
//!
//! Complete thought process preservation:
//! - Data provenance via `source_event_ids` chains
//! - Intent provenance via `core.operations_log`
//! - System remembers not just facts but why it changed its mind
//!
//! # Sentient Archive Vision
//!
//! Sinex transcends traditional data capture by implementing a "sentient archive" -
//! a system that not only captures but understands and participates in the user's
//! digital experience. Through its satellite constellation architecture and deep
//! philosophical principles, Sinex creates an external augmentation of human cognition.

pub mod constants;
pub mod domain;
pub mod error;
pub mod events;
pub mod ids;
pub mod ulid;
pub mod utils;
pub mod validation;

use chrono::{DateTime, Utc};
pub use error::{Result as SinexResult, SinexError};
pub use ids::Id;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::time::Duration;
pub use ulid::Ulid;
pub use utils::*;
pub use validation::{sanitize_filename_component, validate_json, validate_path, ValidationError};

// Re-export Result type alias for convenience
pub type Result<T> = std::result::Result<T, SinexError>;

// Re-export common types
pub type JsonValue = serde_json::Value;
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;
pub type DbPool = sqlx::PgPool;
pub type DbPoolRef<'a> = &'a sqlx::PgPool;

/// Timeout constants for various operations
pub mod timeouts {
    use super::Duration;

    /// Maximum time to wait for filesystem rename operations to complete
    pub const RENAME_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

    /// Interval for capturing terminal scrollback content  
    pub const KITTY_SCROLLBACK_INTERVAL: Duration = Duration::from_secs(180);

    /// Delay to apply when resources are exhausted to prevent overload
    pub const RESOURCE_EXHAUSTION_DELAY: Duration = Duration::from_secs(5);

    /// Default poll interval for terminal event sources
    pub const DEFAULT_TERMINAL_POLL_INTERVAL: Duration = Duration::from_millis(100);

    /// Timeout for database operations in tests
    pub const TEST_DATABASE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Default retry backoff initial delay
    pub const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(10);

    /// Maximum retry backoff delay
    pub const RETRY_MAX_DELAY: Duration = Duration::from_millis(1000);
}

/// Size and count limits for data validation
pub mod limits {
    /// Maximum allowed JSON nesting depth to prevent stack overflow
    pub const MAX_JSON_DEPTH: usize = 100;

    /// Maximum number of JSON elements to prevent memory exhaustion
    pub const MAX_JSON_ELEMENTS: usize = 50_000;

    /// JSON payload size threshold for applying processing delays
    pub const JSON_SIZE_THRESHOLD: usize = 1_000_000;

    /// Element count threshold for applying processing delays
    pub const ELEMENT_COUNT_THRESHOLD: usize = 100_000;

    /// Maximum event batch size for bulk operations
    pub const MAX_EVENT_BATCH_SIZE: usize = 1000;

    /// Maximum string length for source names
    pub const MAX_SOURCE_NAME_LENGTH: usize = 255;

    /// Maximum string length for event type names
    pub const MAX_EVENT_TYPE_LENGTH: usize = 255;

    /// Maximum string length for host names
    pub const MAX_HOST_NAME_LENGTH: usize = 255;
}

/// Channel and buffer size constants
pub mod buffers {
    /// Default channel buffer size for event streams
    pub const DEFAULT_EVENT_CHANNEL_SIZE: usize = 10_000;

    /// Small channel size for testing backpressure scenarios
    pub const TEST_SMALL_CHANNEL_SIZE: usize = 10;

    /// Large channel size for high-throughput scenarios
    pub const HIGH_THROUGHPUT_CHANNEL_SIZE: usize = 100_000;

    /// Default batch size for database insertions
    pub const DEFAULT_DB_BATCH_SIZE: usize = 100;

    /// Buffer size for notification channels
    pub const NOTIFICATION_CHANNEL_SIZE: usize = 100;
}

/// Retry and resilience constants
pub mod retry {
    /// Maximum number of retry attempts for failed operations
    pub const MAX_RETRY_ATTEMPTS: u32 = 3;

    /// Exponential backoff multiplier
    pub const BACKOFF_MULTIPLIER: u32 = 2;

    /// Maximum time to wait for work queue items
    pub const WORK_QUEUE_TIMEOUT_SECS: u64 = 60;

    /// Health check interval for monitoring
    pub const HEALTH_CHECK_INTERVAL_SECS: u64 = 30;
}

/// File system operation constants
pub mod filesystem {
    use super::Duration;

    /// Default interval for filesystem watch polls
    pub const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_millis(100);

    /// Interval for watching terminal socket files
    pub const TERMINAL_SOCKET_WATCH_INTERVAL: Duration = Duration::from_millis(500);

    /// Maximum file size for direct processing (10 MB)
    pub const MAX_DIRECT_PROCESS_SIZE: u64 = 10 * 1024 * 1024;

    /// Minimum free space required for operations (100 MB)
    pub const MIN_FREE_SPACE_BYTES: u64 = 100 * 1024 * 1024;

    /// Default permissions for created directories (0o755)
    pub const DEFAULT_DIR_PERMISSIONS: u32 = 0o755;

    /// Default permissions for created files (0o644)
    pub const DEFAULT_FILE_PERMISSIONS: u32 = 0o644;

    /// Buffer size for reading file contents (8KB)
    pub const FILE_READ_BUFFER_SIZE: usize = 8192;

    /// Maximum file size to process in memory (10MB)
    pub const MAX_IN_MEMORY_FILE_SIZE: usize = 10 * 1024 * 1024;

    /// Interval for cleanup operations in filesystem watcher
    pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

    /// Keep-alive interval for filesystem watcher main loop
    pub const WATCHER_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
}

/// Validation and integrity constants
pub mod validation_constants {
    /// Minimum length for non-empty string fields
    pub const MIN_STRING_LENGTH: usize = 1;

    /// Maximum depth for recursive validation
    pub const MAX_VALIDATION_DEPTH: usize = 10;

    /// Maximum number of validation errors to collect
    pub const MAX_VALIDATION_ERRORS: usize = 100;
}

/// Service communication constants
pub mod services {
    use super::Duration;

    /// Default gRPC connection timeout
    pub const GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Maximum gRPC message size (4 MB)
    pub const MAX_GRPC_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

    /// Heartbeat interval for service health checks
    pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

    /// Service startup grace period
    pub const STARTUP_GRACE_PERIOD: Duration = Duration::from_secs(10);
}

/// Redis stream constants
pub mod redis {
    use super::Duration;

    /// Default consumer group name for automata
    pub const DEFAULT_CONSUMER_GROUP: &str = "automata";

    /// Maximum pending entries before backpressure
    pub const MAX_PENDING_ENTRIES: u64 = 10_000;

    /// Claim timeout for stuck messages
    pub const CLAIM_TIMEOUT: Duration = Duration::from_secs(300);

    /// Block timeout for stream reads
    pub const BLOCK_TIMEOUT_MS: u64 = 1000;
}

// Result type alias is now re-exported from the error module

/// Status indicators for health checks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Service metadata for registration and discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub version: String,
    pub kind: ServiceKind,
    pub status: HealthStatus,
    pub started_at: Timestamp,
    pub metadata: HashMap<String, JsonValue>,
}

/// Types of services in the Sinex ecosystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceKind {
    Ingestor,
    Automaton,
    Gateway,
    Collector,
}

impl Display for ServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceKind::Ingestor => write!(f, "ingestor"),
            ServiceKind::Automaton => write!(f, "automaton"),
            ServiceKind::Gateway => write!(f, "gateway"),
            ServiceKind::Collector => write!(f, "collector"),
        }
    }
}

/// Common trait for components that can be health-checked
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    async fn check_health(&self) -> Result<HealthStatus>;
}

// ===== Metrics Types =====

/// Metrics entry for database storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsEntry {
    pub id: Ulid,
    pub metric_name: String,
    pub metric_type: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub namespace: String,
    pub subsystem: String,
}

impl MetricsEntry {
    pub fn new(
        metric_name: String,
        metric_type: String,
        value: f64,
        labels: HashMap<String, String>,
        namespace: String,
        subsystem: String,
    ) -> Self {
        Self {
            id: Ulid::new(),
            metric_name,
            metric_type,
            value,
            labels,
            timestamp: Utc::now(),
            namespace,
            subsystem,
        }
    }
}

/// Aggregated metrics data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsAggregation {
    pub count: u64,
    pub sum: f64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
}

/// Common trait for components that emit metrics
pub trait MetricsEmitter {
    fn emit_counter(&self, name: &str, value: u64, tags: &[(&str, &str)]);
    fn emit_gauge(&self, name: &str, value: f64, tags: &[(&str, &str)]);
    fn emit_histogram(&self, name: &str, value: f64, tags: &[(&str, &str)]);
}

/// Utility functions for working with paths
pub mod path_utils {

    use camino::{Utf8Path, Utf8PathBuf};

    /// Normalize a path by resolving . and .. components
    pub fn normalize_path(path: &Utf8Path) -> Utf8PathBuf {
        let mut parts = vec![];
        for component in path.as_str().split('/') {
            match component {
                ".." => {
                    parts.pop();
                }
                "." | "" => {}
                part => parts.push(part),
            }
        }
        if path.as_str().starts_with('/') {
            Utf8PathBuf::from("/".to_string() + &parts.join("/"))
        } else {
            Utf8PathBuf::from(parts.join("/"))
        }
    }

    /// Check if a path is safe (no directory traversal)
    pub fn is_safe_path(path: &Utf8Path) -> bool {
        !path.as_str().contains("..") && !path.as_str().contains("./")
    }
}

/// Utility functions for working with JSON
pub mod json_utils {
    use super::*;

    /// Count the total number of elements in a JSON value
    pub fn count_elements(value: &JsonValue) -> usize {
        match value {
            JsonValue::Object(map) => 1 + map.values().map(count_elements).sum::<usize>(),
            JsonValue::Array(arr) => 1 + arr.iter().map(count_elements).sum::<usize>(),
            _ => 1,
        }
    }

    /// Calculate the depth of a JSON value
    pub fn calculate_depth(value: &JsonValue) -> usize {
        match value {
            JsonValue::Object(map) => 1 + map.values().map(calculate_depth).max().unwrap_or(0),
            JsonValue::Array(arr) => 1 + arr.iter().map(calculate_depth).max().unwrap_or(0),
            _ => 1,
        }
    }

    /// Estimate the memory size of a JSON value
    pub fn estimate_size(value: &JsonValue) -> usize {
        match value {
            JsonValue::Null => 4,
            JsonValue::Bool(_) => 5,
            JsonValue::Number(_) => 8,
            JsonValue::String(s) => 8 + s.len(),
            JsonValue::Array(arr) => 24 + arr.iter().map(estimate_size).sum::<usize>(),
            JsonValue::Object(map) => {
                24 + map
                    .iter()
                    .map(|(k, v)| k.len() + estimate_size(v))
                    .sum::<usize>()
            }
        }
    }
}

/// Test utilities
#[cfg(test)]
pub mod test_utils {
    use super::*;

    /// Create a test database pool
    pub async fn create_test_pool() -> Result<DbPool> {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_test".to_string());

        sqlx::PgPool::connect(&database_url)
            .await
            .map_err(|e| SinexError::database(e.to_string()))
    }

    /// Generate a unique test identifier
    pub fn test_id(prefix: &str) -> String {
        format!("{}_{}", prefix, Ulid::new())
    }
}
