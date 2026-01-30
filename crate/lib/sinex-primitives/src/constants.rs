//! System-wide constants for limits, timeouts, and buffers.

use crate::units::{Bytes, Seconds};
use std::time::Duration;

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
    use super::Seconds;

    /// Maximum number of retry attempts for failed operations
    pub const MAX_RETRY_ATTEMPTS: u32 = 3;

    /// Exponential backoff multiplier
    pub const BACKOFF_MULTIPLIER: u32 = 2;

    /// Health check interval for monitoring
    pub const HEALTH_CHECK_INTERVAL_SECS: Seconds = Seconds::from_secs(30);
}

/// File system operation constants
pub mod filesystem {
    use super::{Bytes, Duration};

    /// Default interval for filesystem watch polls
    pub const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_millis(100);

    /// Interval for watching terminal socket files
    pub const TERMINAL_SOCKET_WATCH_INTERVAL: Duration = Duration::from_millis(500);

    /// Maximum file size for direct processing (10 MB)
    pub const MAX_DIRECT_PROCESS_SIZE: Bytes = Bytes::from_mebibytes(10);

    /// Minimum free space required for operations (100 MB)
    pub const MIN_FREE_SPACE_BYTES: Bytes = Bytes::from_mebibytes(100);

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
