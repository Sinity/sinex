// Satellite test utilities for integration testing
// Provides test handles for satellites, ingestd, and automata

use crate::common::prelude::*;
use std::path::PathBuf;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

/// Configuration for test ingestd instance
#[derive(Debug, Clone)]
pub struct TestIngestdConfig {
    pub socket_path: String,
    pub redis_url: String,
    pub database_url: String,
}

impl Default for TestIngestdConfig {
    fn default() -> Self {
        Self {
            socket_path: "/tmp/test-ingestd.sock".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            database_url: "postgresql:///sinex_test?host=/run/postgresql".to_string(),
        }
    }
}

/// Handle for a test ingestd process
pub struct TestIngestdHandle {
    pub socket_path: String,
    process: Option<Child>,
}

impl TestIngestdHandle {
    /// Stop the ingestd process
    pub async fn stop(&mut self) -> AnyhowResult<()> {
        if let Some(mut process) = self.process.take() {
            process.kill().await?;
        }
        Ok(())
    }
}

impl Drop for TestIngestdHandle {
    fn drop(&mut self) {
        // Best effort cleanup
        if let Some(mut process) = self.process.take() {
            let _ = process.start_kill();
        }
    }
}

/// Start a test ingestd instance with custom configuration
pub async fn start_test_ingestd_with_config(
    config: TestIngestdConfig,
) -> AnyhowResult<TestIngestdHandle> {
    // For now, return a mock handle
    // In a full implementation, this would start the actual ingestd process
    Ok(TestIngestdHandle {
        socket_path: config.socket_path,
        process: None,
    })
}

/// Handle for a test satellite process
pub struct TestSatelliteHandle {
    pub name: String,
    process: Option<Child>,
}

impl TestSatelliteHandle {
    /// Start a new test satellite
    pub async fn start(
        config: serde_json::Value,
        _pool: DbPool,
    ) -> AnyhowResult<Self> {
        // For now, return a mock handle
        Ok(Self {
            name: config.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("test-satellite")
                .to_string(),
            process: None,
        })
    }
    
    /// Stop the satellite process
    pub async fn stop(&mut self) -> AnyhowResult<()> {
        if let Some(mut process) = self.process.take() {
            process.kill().await?;
        }
        Ok(())
    }
}

/// Handle for a test automaton process
pub struct TestAutomatonHandle {
    pub name: String,
    process: Option<Child>,
}

impl TestAutomatonHandle {
    /// Start a new test automaton
    pub async fn start(
        automaton_type: &str,
        _pool: DbPool,
        _redis_url: &str,
    ) -> AnyhowResult<Self> {
        // For now, return a mock handle
        Ok(Self {
            name: format!("test-{}", automaton_type),
            process: None,
        })
    }
    
    /// Stop the automaton process
    pub async fn stop(&mut self) -> AnyhowResult<()> {
        if let Some(mut process) = self.process.take() {
            process.kill().await?;
        }
        Ok(())
    }
}

/// Create a test satellite configuration
pub fn create_test_satellite_config(
    service_name: &str,
    socket_path: &str,
) -> serde_json::Value {
    serde_json::json!({
        "name": service_name,
        "socket_path": socket_path,
        "batch_size": 10,
        "batch_timeout_ms": 100,
    })
}