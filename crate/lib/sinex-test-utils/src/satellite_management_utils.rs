// Satellite test utilities for integration testing
// Provides test handles for satellites, ingestd, and automata

use crate::Result;

use sinex_core::db::DbPool;
use tokio::process::Child;

// Re-export StreamMessage for convenience

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
    pub async fn stop(&mut self) -> Result<()> {
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
) -> Result<TestIngestdHandle> {
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
    pub async fn start(config: serde_json::Value, _pool: DbPool) -> Result<Self> {
        // For now, return a mock handle
        Ok(Self {
            name: config
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("test-satellite")
                .to_string(),
            process: None,
        })
    }

    /// Stop the satellite process
    pub async fn stop(&mut self) -> Result<()> {
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
    pub async fn start(automaton_type: &str, _pool: DbPool, _redis_url: &str) -> Result<Self> {
        // For now, return a mock handle
        Ok(Self {
            name: format!("test-{}", automaton_type),
            process: None,
        })
    }

    /// Stop the automaton process
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(mut process) = self.process.take() {
            process.kill().await?;
        }
        Ok(())
    }
}

/// Orchestrator for managing multiple satellites and automata
pub struct SatelliteOrchestrator {
    satellites: std::sync::Mutex<std::collections::HashMap<String, TestSatelliteHandle>>,
    automata: std::sync::Mutex<std::collections::HashMap<String, TestAutomatonHandle>>,
}

impl SatelliteOrchestrator {
    pub fn new() -> Self {
        Self {
            satellites: std::sync::Mutex::new(std::collections::HashMap::new()),
            automata: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn register_satellite(&self, name: &str, handle: TestSatelliteHandle) {
        self.satellites
            .lock()
            .unwrap()
            .insert(name.to_string(), handle);
    }

    pub fn register_automaton(&self, name: &str, handle: TestAutomatonHandle) {
        self.automata
            .lock()
            .unwrap()
            .insert(name.to_string(), handle);
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        // Shutdown all satellites
        let mut satellites = self.satellites.lock().unwrap();
        for (_, mut handle) in satellites.drain() {
            handle.stop().await?;
        }

        // Shutdown all automata
        let mut automata = self.automata.lock().unwrap();
        for (_, mut handle) in automata.drain() {
            handle.stop().await?;
        }

        Ok(())
    }
}

/// Create a test satellite configuration
pub fn build_test_satellite_config(service_name: &str, socket_path: &str) -> serde_json::Value {
    serde_json::json!({
        "name": service_name,
        "socket_path": socket_path,
        "batch_size": 10,
        "batch_timeout_ms": 100,
    })
}

// Comprehensive satellite management tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[sinex_test]
    async fn test_ingestd_config_default(_ctx: TestContext) -> Result<()> {
        let config = TestIngestdConfig::default();

        assert_eq!(config.socket_path, "/tmp/test-ingestd.sock");
        assert_eq!(config.redis_url, "redis://localhost:6379");
        assert_eq!(
            config.database_url,
            "postgresql:///sinex_test?host=/run/postgresql"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_handle_creation(_ctx: TestContext) -> Result<()> {
        let config = TestIngestdConfig {
            socket_path: "/tmp/custom-test.sock".to_string(),
            redis_url: "redis://custom:6379".to_string(),
            database_url: "postgresql:///custom_test".to_string(),
        };

        let handle = start_test_ingestd_with_config(config.clone()).await?;

        assert_eq!(handle.socket_path, config.socket_path);

        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_handle_stop(_ctx: TestContext) -> Result<()> {
        let config = TestIngestdConfig::default();
        let mut handle = start_test_ingestd_with_config(config).await?;

        // Should be able to stop without error
        handle.stop().await?;

        // Multiple stops should be ok
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_handle_creation(ctx: TestContext) -> Result<()> {
        let config = serde_json::json!({
            "name": "test-satellite",
            "source": "test",
            "buffer_size": 100,
        });

        let handle = TestSatelliteHandle::start(config.clone(), ctx.pool.clone()).await?;

        assert_eq!(handle.name, "test-satellite");

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_handle_stop(ctx: TestContext) -> Result<()> {
        let config = serde_json::json!({
            "name": "stop-test-satellite",
        });

        let mut handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;

        // Should be able to stop without error
        handle.stop().await?;

        // Multiple stops should be ok
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_handle_default_name(ctx: TestContext) -> Result<()> {
        // Config without name should use default
        let config = serde_json::json!({
            "source": "test",
        });

        let handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;

        assert_eq!(handle.name, "test-satellite");

        Ok(())
    }

    #[sinex_test]
    async fn test_automaton_handle_creation(_ctx: TestContext) -> Result<()> {
        let handle = TestAutomatonHandle {
            name: "test-automaton".to_string(),
            process: None,
        };

        assert_eq!(handle.name, "test-automaton");

        Ok(())
    }

    #[sinex_test]
    async fn test_automaton_handle_stop(_ctx: TestContext) -> Result<()> {
        let mut handle = TestAutomatonHandle {
            name: "stop-test".to_string(),
            process: None,
        };

        // Should be able to stop without error even with no process
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_orchestrator_creation(_ctx: TestContext) -> Result<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Initial state should be empty
        assert!(orchestrator.satellites.lock().unwrap().is_empty());
        assert!(orchestrator.automata.lock().unwrap().is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_orchestrator_register(ctx: TestContext) -> Result<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Register a satellite
        let config = serde_json::json!({
            "name": "orchestrated-satellite",
        });

        let handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;

        orchestrator.register_satellite("test", handle);

        // Should be registered
        assert_eq!(orchestrator.satellites.lock().unwrap().len(), 1);

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_orchestrator_shutdown(_ctx: TestContext) -> Result<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Register some handles
        let sat_handle = TestSatelliteHandle {
            name: "shutdown-test".to_string(),
            process: None,
        };

        let auto_handle = TestAutomatonHandle {
            name: "shutdown-auto".to_string(),
            process: None,
        };

        orchestrator.register_satellite("test", sat_handle);
        orchestrator.register_automaton("auto", auto_handle);

        // Shutdown should complete without error
        orchestrator.shutdown_all().await?;

        // All collections should be empty after shutdown
        assert!(orchestrator.satellites.lock().unwrap().is_empty());
        assert!(orchestrator.automata.lock().unwrap().is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_config_builder(_ctx: TestContext) -> Result<()> {
        let config = build_test_satellite_config("test-service", "/tmp/test.sock");

        assert_eq!(config["name"], "test-service");
        assert_eq!(config["socket_path"], "/tmp/test.sock");
        assert_eq!(config["batch_size"], 10);
        assert_eq!(config["batch_timeout_ms"], 100);

        Ok(())
    }

    #[sinex_test]
    async fn test_multiple_satellite_management(ctx: TestContext) -> Result<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Register multiple satellites
        for i in 0..5 {
            let config = serde_json::json!({
                "name": format!("satellite-{}", i),
            });

            let handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;
            orchestrator.register_satellite(&format!("sat-{}", i), handle);
        }

        // Should have all satellites registered
        assert_eq!(orchestrator.satellites.lock().unwrap().len(), 5);

        // Shutdown all
        orchestrator.shutdown_all().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_error_handling_in_shutdown(_ctx: TestContext) -> Result<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Even with no satellites/automata, shutdown should work
        orchestrator.shutdown_all().await?;

        Ok(())
    }

    #[sinex_test]
    fn test_ingestd_handle_drop() {
        // Test that drop doesn't panic even with no process
        let handle = TestIngestdHandle {
            socket_path: "/tmp/drop-test.sock".to_string(),
            process: None,
        };

        drop(handle); // Should not panic
    }

    #[sinex_test]
    fn test_orchestrator_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let orchestrator = Arc::new(SatelliteOrchestrator::new());
        let mut handles = vec![];

        // Spawn threads that register satellites
        for i in 0..10 {
            let orchestrator_clone = orchestrator.clone();
            let handle = thread::spawn(move || {
                let sat_handle = TestSatelliteHandle {
                    name: format!("thread-sat-{}", i),
                    process: None,
                };
                orchestrator_clone.register_satellite(&format!("key-{}", i), sat_handle);
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have all satellites
        assert_eq!(orchestrator.satellites.lock().unwrap().len(), 10);
    }
}
