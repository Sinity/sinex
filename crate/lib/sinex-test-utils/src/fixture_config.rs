//! Fixture Configuration
//!
//! Provides configurable settings for test fixtures via environment variables.

use once_cell::sync::Lazy;
use std::env;

/// Configuration for test fixtures
#[derive(Debug, Clone)]
pub struct FixtureConfig {
    /// Number of events in a small dataset
    pub small_dataset_size: usize,
    /// Number of events in a medium dataset
    pub medium_dataset_size: usize,
    /// Number of events in a large dataset
    pub large_dataset_size: usize,
    /// Default number of events in a user session
    pub user_session_event_count: usize,
    /// Default checkpoint interval
    pub checkpoint_interval: usize,
    /// Number of checkpoints to create in populated fixtures
    pub populated_checkpoints_count: usize,
    /// Batch size for bulk operations
    pub batch_insert_size: usize,
    /// Enable verbose fixture generation logging
    pub verbose: bool,
}

impl Default for FixtureConfig {
    fn default() -> Self {
        Self {
            small_dataset_size: 100,
            medium_dataset_size: 1_000,
            large_dataset_size: 10_000,
            user_session_event_count: 30,
            checkpoint_interval: 10,
            populated_checkpoints_count: 3,
            batch_insert_size: 1000,
            verbose: false,
        }
    }
}

impl FixtureConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Parse environment variables with sensible defaults
        if let Ok(val) = env::var("SINEX_TEST_SMALL_DATASET_SIZE") {
            if let Ok(size) = val.parse() {
                config.small_dataset_size = size;
            }
        }

        if let Ok(val) = env::var("SINEX_TEST_MEDIUM_DATASET_SIZE") {
            if let Ok(size) = val.parse() {
                config.medium_dataset_size = size;
            }
        }

        if let Ok(val) = env::var("SINEX_TEST_LARGE_DATASET_SIZE") {
            if let Ok(size) = val.parse() {
                config.large_dataset_size = size;
            }
        }

        if let Ok(val) = env::var("SINEX_TEST_USER_SESSION_EVENTS") {
            if let Ok(count) = val.parse() {
                config.user_session_event_count = count;
            }
        }

        if let Ok(val) = env::var("SINEX_TEST_CHECKPOINT_INTERVAL") {
            if let Ok(interval) = val.parse() {
                config.checkpoint_interval = interval;
            }
        }

        if let Ok(val) = env::var("SINEX_TEST_CHECKPOINT_COUNT") {
            if let Ok(count) = val.parse() {
                config.populated_checkpoints_count = count;
            }
        }

        if let Ok(val) = env::var("SINEX_TEST_BATCH_SIZE") {
            if let Ok(size) = val.parse() {
                config.batch_insert_size = size;
            }
        }

        config.verbose = env::var("SINEX_TEST_VERBOSE").is_ok();

        if config.verbose {
            eprintln!("Fixture configuration loaded:");
            eprintln!("  Small dataset: {} events", config.small_dataset_size);
            eprintln!("  Medium dataset: {} events", config.medium_dataset_size);
            eprintln!("  Large dataset: {} events", config.large_dataset_size);
            eprintln!("  User session: {} events", config.user_session_event_count);
            eprintln!("  Checkpoint interval: {}", config.checkpoint_interval);
            eprintln!("  Batch size: {}", config.batch_insert_size);
        }

        config
    }

    /// Get appropriate dataset size based on requested count
    pub fn get_dataset_size(&self, requested: usize) -> usize {
        match requested {
            0..=100 => self.small_dataset_size,
            101..=1000 => self.medium_dataset_size,
            _ => self.large_dataset_size,
        }
    }
}

/// Global fixture configuration
pub static FIXTURE_CONFIG: Lazy<FixtureConfig> = Lazy::new(FixtureConfig::from_env);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    fn test_default_config() {
        let config = FixtureConfig::default();
        assert_eq!(config.small_dataset_size, 100);
        assert_eq!(config.medium_dataset_size, 1_000);
        assert_eq!(config.large_dataset_size, 10_000);
        assert_eq!(config.user_session_event_count, 30);
        assert_eq!(config.checkpoint_interval, 10);
        assert!(!config.verbose);
    }

    #[sinex_test]
    fn test_dataset_size_selection() {
        let config = FixtureConfig::default();
        assert_eq!(config.get_dataset_size(50), 100);
        assert_eq!(config.get_dataset_size(500), 1_000);
        assert_eq!(config.get_dataset_size(5000), 10_000);
    }
}
