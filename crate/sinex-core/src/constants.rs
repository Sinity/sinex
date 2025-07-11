//! Constants used throughout the Sinex system
//!
//! This module centralizes magic numbers, timeouts, and limits that were
//! previously hardcoded throughout the codebase.

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

    /// Buffer size for reading file contents
    pub const FILE_READ_BUFFER_SIZE: usize = 8192;

    /// Maximum file size to process in memory
    pub const MAX_IN_MEMORY_FILE_SIZE: usize = 10 * 1024 * 1024; // 10MB

    /// Default file permissions for created files
    pub const DEFAULT_FILE_PERMISSIONS: u32 = 0o644;

    /// Default directory permissions for created directories
    pub const DEFAULT_DIR_PERMISSIONS: u32 = 0o755;

    /// Interval for cleanup operations in filesystem watcher
    pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

    /// Keep-alive interval for filesystem watcher main loop
    pub const WATCHER_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeout_constants() {
        assert!(timeouts::RENAME_OPERATION_TIMEOUT > Duration::ZERO);
        assert!(timeouts::KITTY_SCROLLBACK_INTERVAL > timeouts::DEFAULT_TERMINAL_POLL_INTERVAL);
        assert!(timeouts::RETRY_MAX_DELAY > timeouts::RETRY_INITIAL_DELAY);
    }

    #[test]
    fn test_limit_constants() {
        // Verify our limits are reasonable - using runtime checks to avoid constant expression warnings
        let max_depth = limits::MAX_JSON_DEPTH;
        let max_elements = limits::MAX_JSON_ELEMENTS;
        let size_threshold = limits::JSON_SIZE_THRESHOLD;
        let batch_size = limits::MAX_EVENT_BATCH_SIZE;

        assert!(max_depth >= 16, "MAX_JSON_DEPTH should be at least 16");
        assert!(
            max_elements >= batch_size,
            "MAX_JSON_ELEMENTS should be at least MAX_EVENT_BATCH_SIZE"
        );
        assert!(
            size_threshold >= 1024,
            "JSON_SIZE_THRESHOLD should be at least 1024"
        );
    }

    #[test]
    fn test_buffer_constants() {
        // Verify buffer size relationships - using runtime checks to avoid constant expression warnings
        let default_size = buffers::DEFAULT_EVENT_CHANNEL_SIZE;
        let small_size = buffers::TEST_SMALL_CHANNEL_SIZE;
        let high_throughput_size = buffers::HIGH_THROUGHPUT_CHANNEL_SIZE;

        assert!(
            default_size >= small_size,
            "DEFAULT_EVENT_CHANNEL_SIZE should be at least TEST_SMALL_CHANNEL_SIZE"
        );
        assert!(
            high_throughput_size >= default_size,
            "HIGH_THROUGHPUT_CHANNEL_SIZE should be at least DEFAULT_EVENT_CHANNEL_SIZE"
        );
    }
}
