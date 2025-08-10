//! Configuration for replay operations
//!
//! This module provides configuration constants and structures for replay operations,
//! including cascade analysis, batch processing, and depth limits.

use serde::{Deserialize, Serialize};

/// Configuration for cascade analysis operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CascadeConfig {
    /// Maximum depth to traverse in cascade analysis
    pub max_depth: usize,
    /// Number of events to process in each batch
    pub batch_size: usize,
    /// Whether to use bloom filters for cycle detection
    pub use_bloom_filter: bool,
    /// Maximum memory usage in bytes before spilling to disk
    pub max_memory_bytes: usize,
    /// Timeout for analysis operations in seconds
    pub timeout_seconds: u64,
}

impl Default for CascadeConfig {
    fn default() -> Self {
        Self {
            max_depth: 100,
            batch_size: 1000,
            use_bloom_filter: true,
            max_memory_bytes: 100 * 1024 * 1024, // 100MB
            timeout_seconds: 300,                // 5 minutes
        }
    }
}

/// Configuration for batch replay execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Number of events to process in each batch
    pub batch_size: usize,
    /// Number of parallel workers for processing
    pub parallel_workers: usize,
    /// Whether to checkpoint after each batch
    pub checkpoint_after_batch: bool,
    /// Maximum retries for failed batches
    pub max_retries: u32,
    /// Delay between retries in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            parallel_workers: 4,
            checkpoint_after_batch: true,
            max_retries: 3,
            retry_delay_ms: 1000,
        }
    }
}

/// Configuration for the entire replay system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Cascade analysis configuration
    pub cascade: CascadeConfig,
    /// Batch processing configuration
    pub batch: BatchConfig,
    /// Whether to enforce invariants during replay
    pub enforce_invariants: bool,
    /// Whether to collect metrics during replay
    pub collect_metrics: bool,
    /// Whether to use advisory locks for coordination
    pub use_advisory_locks: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            cascade: CascadeConfig::default(),
            batch: BatchConfig::default(),
            enforce_invariants: true,
            collect_metrics: true,
            use_advisory_locks: true,
        }
    }
}

impl ReplayConfig {
    /// Create a configuration optimized for small datasets
    pub fn small_dataset() -> Self {
        Self {
            cascade: CascadeConfig {
                max_depth: 50,
                batch_size: 100,
                use_bloom_filter: false,
                max_memory_bytes: 10 * 1024 * 1024, // 10MB
                timeout_seconds: 60,
            },
            batch: BatchConfig {
                batch_size: 50,
                parallel_workers: 1,
                checkpoint_after_batch: false,
                max_retries: 1,
                retry_delay_ms: 100,
            },
            enforce_invariants: true,
            collect_metrics: false,
            use_advisory_locks: false,
        }
    }

    /// Create a configuration optimized for large datasets
    pub fn large_dataset() -> Self {
        Self {
            cascade: CascadeConfig {
                max_depth: 200,
                batch_size: 5000,
                use_bloom_filter: true,
                max_memory_bytes: 500 * 1024 * 1024, // 500MB
                timeout_seconds: 1800,               // 30 minutes
            },
            batch: BatchConfig {
                batch_size: 2000,
                parallel_workers: 8,
                checkpoint_after_batch: true,
                max_retries: 5,
                retry_delay_ms: 5000,
            },
            enforce_invariants: true,
            collect_metrics: true,
            use_advisory_locks: true,
        }
    }

    /// Create a configuration for testing
    pub fn test() -> Self {
        Self {
            cascade: CascadeConfig {
                max_depth: 10,
                batch_size: 10,
                use_bloom_filter: false,
                max_memory_bytes: 1024 * 1024, // 1MB
                timeout_seconds: 10,
            },
            batch: BatchConfig {
                batch_size: 5,
                parallel_workers: 1,
                checkpoint_after_batch: false,
                max_retries: 0,
                retry_delay_ms: 0,
            },
            enforce_invariants: false,
            collect_metrics: false,
            use_advisory_locks: false,
        }
    }
}
