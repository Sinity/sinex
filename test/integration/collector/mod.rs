//! Collector integration tests
//!
//! This module contains integration tests for the unified collector system,
//! including configuration management, event source coordination, backpressure
//! handling, and hot reload functionality.

#![allow(dead_code)]
//!
//! # Test Coverage
//! - Basic collector startup and shutdown
//! - Event source registration and coordination
//! - Configuration loading and validation
//! - Hot reload of configuration and event sources
//! - Backpressure and flow control
//! - Multi-source coordination and timing

/// Backpressure and flow control tests
pub mod backpressure_test;

/// Basic collector functionality tests
pub mod basic_collector_test;

// Configuration management tests - consolidated to configuration_test.rs

/// Hot reload functionality tests
pub mod hot_reload_test;

/// Multi-source coordination tests
pub mod multi_source_coordination_test;

/// Common utilities for collector testing
pub mod utils {
    use crate::common::prelude::*;
    use serde_json::{json, Value};
    // use std::path::Path;

    /// Create a minimal collector configuration
    pub fn create_minimal_config() -> Value {
        json!({
            "database_url": "postgresql:///sinex_test",
            "event_sources": {},
            "worker_pool_size": 2,
            "shutdown_timeout_secs": 10
        })
    }

    /// Create collector configuration with specific event sources
    pub fn create_config_with_sources(sources: &[&str]) -> Value {
        let mut config = create_minimal_config();
        let mut event_sources = json!({});

        for source in sources {
            match *source {
                "fs" => {
                    event_sources["fs"] = json!({
                        "enabled": true,
                        "watch_patterns": ["/tmp/test/**/*"],
                        "ignore_patterns": ["*.tmp"]
                    });
                }
                "terminal" => {
                    event_sources["shell.kitty"] = json!({
                        "enabled": true,
                        "socket_path": "/tmp/test_terminal.sock"
                    });
                }
                "clipboard" => {
                    event_sources["clipboard"] = json!({
                        "enabled": true,
                        "poll_interval_ms": 1000
                    });
                }
                _ => {}
            }
        }

        config["event_sources"] = event_sources;
        config
    }

    /// Create a temporary configuration file
    pub async fn create_temp_config_file(config: &Value) -> Result<tempfile::NamedTempFile> {
        let temp_file = tempfile::NamedTempFile::new()?;
        tokio::fs::write(temp_file.path(), config.to_string()).await?;
        Ok(temp_file)
    }

    /// Wait for collector to be ready
    pub async fn wait_for_collector_ready(
        pool: &DbPool,
        timeout_secs: u64,
    ) -> Result<(), anyhow::Error> {
        crate::common::timing_optimization::wait_helpers::wait_for_condition(
            move || {
                let pool = pool.clone();
                async move {
                    // Check if collector has registered itself as an agent
                    let collector_registered = sqlx::query_scalar!(
                        "SELECT EXISTS(SELECT 1 FROM sinex_schemas.agent_manifests WHERE agent_name LIKE 'collector%')"
                    )
                    .fetch_one(&pool)
                    .await?
                    .unwrap_or(false);
                    Ok(collector_registered)
                }
            },
            timeout_secs
        ).await
    }

    /// Create test events to verify collector is processing
    pub fn create_test_events_batch(count: usize) -> Vec<RawEvent> {
        (0..count)
            .map(|i| {
                crate::common::event_builders::EventBuilder::generic(
                    "test_source",
                    "collector.test",
                )
                .payload(json!({ "index": i, "test_id": uuid::Uuid::new_v4() }))
                .build()
            })
            .collect()
    }
}
