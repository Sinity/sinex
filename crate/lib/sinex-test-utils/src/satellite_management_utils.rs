// Satellite test utilities for integration testing
// Provides test handles for satellites, ingestd, and automata

use crate::Result;
use crate::TestResult;

use camino::Utf8PathBuf;
use sinex_core::db::DbPool;
use sinex_core::types::error::SinexError;
use sinex_core::types::ulid::Ulid;
use sinex_ingestd::{config::IngestdConfig, service::IngestService};
use tokio::process::Child;
use tokio::task::JoinHandle;

// Re-export StreamMessage for convenience

/// Configuration for test ingestd instance
#[derive(Debug, Clone)]
pub struct TestIngestdConfig {
    pub nats_url: String,
    pub database_url: String,
    pub work_dir: Option<std::path::PathBuf>,
}

impl Default for TestIngestdConfig {
    fn default() -> Self {
        Self {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            database_url: "postgresql:///sinex_test?host=/run/postgresql".to_string(),
            work_dir: None,
        }
    }
}

/// Handle for a test ingestd process
pub struct TestIngestdHandle {
    pub stream_name: String,
    process: Option<Child>,
    service: Option<IngestService>,
    join_handle: Option<JoinHandle<Result<()>>>,
    _work_dir: Option<tempfile::TempDir>,
}

impl TestIngestdHandle {
    /// Stop the ingestd process
    pub async fn stop(&mut self) -> TestResult<()> {
        if let Some(service) = self.service.as_mut() {
            service.shutdown().await?;
        }

        if let Some(mut process) = self.process.take() {
            let _ = process.kill().await;
        }

        if let Some(join) = self.join_handle.take() {
            match join.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err.into()),
                Err(join_err) => {
                    return Err(
                        SinexError::service(format!("ingestd task join error: {join_err}")).into(),
                    )
                }
            }
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
) -> TestResult<TestIngestdHandle> {
    let work_dir_temp = match &config.work_dir {
        Some(_existing) => None,
        None => Some(
            tempfile::tempdir()
                .map_err(|e| SinexError::service(format!("failed to create temp work dir: {e}")))?,
        ),
    };

    let work_dir_path = config
        .work_dir
        .clone()
        .or_else(|| work_dir_temp.as_ref().map(|d| d.path().to_path_buf()))
        .ok_or_else(|| SinexError::service("failed to resolve ingestd work dir"))?;

    let work_dir = Utf8PathBuf::try_from(work_dir_path)
        .map_err(|e| SinexError::configuration(e.to_string()))?;

    let ingest_config = IngestdConfig::builder()
        .database_url(config.database_url.clone())
        .nats_url(config.nats_url.clone())
        .batch_size(1)
        .batch_timeout_secs(1)
        .validate_schemas(false)
        .skip_schema_sync(true)
        .work_dir(work_dir)
        .nats_stream_name(format!("sinex_test_events_{}", Ulid::new()))
        .build();

    let service = IngestService::new(ingest_config.clone()).await?;

    let mut service_runner = service.clone();
    let join_handle = tokio::spawn(async move { service_runner.run().await });

    // Verify service is ready by checking NATS stream exists
    let nats_client = async_nats::connect(&config.nats_url)
        .await
        .map_err(|e| SinexError::network(format!("Failed to connect to NATS: {e}")))?;
    let jetstream = async_nats::jetstream::new(nats_client);

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match jetstream.get_stream(&ingest_config.nats_stream_name).await {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .map_err(|_| SinexError::service("ingestd stream did not become ready"))?;

    Ok(TestIngestdHandle {
        stream_name: ingest_config.nats_stream_name,
        process: None,
        service: Some(service),
        join_handle: Some(join_handle),
        _work_dir: work_dir_temp,
    })
}

/// Handle for a test satellite process
pub struct TestSatelliteHandle {
    pub name: String,
    process: Option<Child>,
}

impl TestSatelliteHandle {
    /// Start a new test satellite
    pub async fn start(config: serde_json::Value, _pool: DbPool) -> TestResult<Self> {
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
    pub async fn stop(&mut self) -> TestResult<()> {
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
    pub async fn start(automaton_type: &str, _pool: DbPool, _redis_url: &str) -> TestResult<Self> {
        // For now, return a mock handle
        Ok(Self {
            name: format!("test-{automaton_type}"),
            process: None,
        })
    }

    /// Stop the automaton process
    pub async fn stop(&mut self) -> TestResult<()> {
        if let Some(mut process) = self.process.take() {
            process.kill().await?;
        }
        Ok(())
    }
}

/// Orchestrator for managing multiple satellites and automata
pub struct SatelliteOrchestrator {
    satellites: parking_lot::Mutex<std::collections::HashMap<String, TestSatelliteHandle>>,
    automata: parking_lot::Mutex<std::collections::HashMap<String, TestAutomatonHandle>>,
}

impl SatelliteOrchestrator {
    pub fn new() -> Self {
        Self {
            satellites: parking_lot::Mutex::new(std::collections::HashMap::new()),
            automata: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn register_satellite(&self, name: &str, handle: TestSatelliteHandle) {
        self.satellites.lock().insert(name.to_string(), handle);
    }

    pub fn register_automaton(&self, name: &str, handle: TestAutomatonHandle) {
        self.automata.lock().insert(name.to_string(), handle);
    }

    pub async fn shutdown_all(&self) -> TestResult<()> {
        // Shutdown all satellites
        let satellite_handles: Vec<_> = {
            let mut satellites = self.satellites.lock();
            satellites.drain().collect()
        };

        for (_, mut handle) in satellite_handles {
            handle.stop().await?;
        }

        // Shutdown all automata
        let automaton_handles: Vec<_> = {
            let mut automata = self.automata.lock();
            automata.drain().collect()
        };

        for (_, mut handle) in automaton_handles {
            handle.stop().await?;
        }

        Ok(())
    }
}

/// Create a test satellite configuration
pub fn build_test_satellite_config(service_name: &str) -> serde_json::Value {
    serde_json::json!({
        "name": service_name,
        "batch_size": 10,
        "batch_timeout_ms": 100,
    })
}

// Comprehensive satellite management tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::sinex_test;
    use crate::SinexError;

    #[sinex_test]
    async fn test_ingestd_config_default() -> TestResult<()> {
        let config = TestIngestdConfig::default();

        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        assert_eq!(
            config.database_url,
            "postgresql:///sinex_test?host=/run/postgresql"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_handle_creation(ctx: TestContext) -> TestResult<()> {
        use crate::nats::EphemeralNats;

        let nats = EphemeralNats::start().await?;
        let work_dir = tempfile::tempdir()
            .map_err(|e| SinexError::service(format!("failed to create temp work dir: {e}")))?;

        let config = TestIngestdConfig {
            nats_url: format!("nats://{}", nats.client_url()),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
        };

        let mut handle = start_test_ingestd_with_config(config.clone()).await?;

        assert!(!handle.stream_name.is_empty());
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_ingestd_handle_stop(ctx: TestContext) -> TestResult<()> {
        use crate::nats::EphemeralNats;

        let nats = EphemeralNats::start().await?;
        let work_dir = tempfile::tempdir()
            .map_err(|e| SinexError::service(format!("failed to create temp work dir: {e}")))?;

        let config = TestIngestdConfig {
            nats_url: format!("nats://{}", nats.client_url()),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
        };
        let mut handle = start_test_ingestd_with_config(config).await?;

        // Should be able to stop without error
        handle.stop().await?;

        // Multiple stops should be ok
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_handle_creation(ctx: TestContext) -> TestResult<()> {
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
    async fn test_satellite_handle_stop(ctx: TestContext) -> TestResult<()> {
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
    async fn test_satellite_handle_default_name(ctx: TestContext) -> TestResult<()> {
        // Config without name should use default
        let config = serde_json::json!({
            "source": "test",
        });

        let handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;

        assert_eq!(handle.name, "test-satellite");

        Ok(())
    }

    #[sinex_test]
    async fn test_automaton_handle_creation() -> TestResult<()> {
        let handle = TestAutomatonHandle {
            name: "test-automaton".to_string(),
            process: None,
        };

        assert_eq!(handle.name, "test-automaton");

        Ok(())
    }

    #[sinex_test]
    async fn test_automaton_handle_stop() -> TestResult<()> {
        let mut handle = TestAutomatonHandle {
            name: "stop-test".to_string(),
            process: None,
        };

        // Should be able to stop without error even with no process
        handle.stop().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_orchestrator_creation() -> TestResult<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Initial state should be empty
        assert!(orchestrator.satellites.lock().is_empty());
        assert!(orchestrator.automata.lock().is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_orchestrator_register(ctx: TestContext) -> TestResult<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Register a satellite
        let config = serde_json::json!({
            "name": "orchestrated-satellite",
        });

        let handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;

        orchestrator.register_satellite("test", handle);

        // Should be registered
        assert_eq!(orchestrator.satellites.lock().len(), 1);

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_orchestrator_shutdown() -> TestResult<()> {
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
        assert!(orchestrator.satellites.lock().is_empty());
        assert!(orchestrator.automata.lock().is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_satellite_config_builder() -> TestResult<()> {
        let config = build_test_satellite_config("test-service");

        assert_eq!(config["name"], "test-service");
        assert_eq!(config["batch_size"], 10);
        assert_eq!(config["batch_timeout_ms"], 100);

        Ok(())
    }

    #[sinex_test]
    async fn test_multiple_satellite_management(ctx: TestContext) -> TestResult<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Register multiple satellites
        for i in 0..5 {
            let config = serde_json::json!({
                "name": format!("satellite-{}", i),
            });

            let handle = TestSatelliteHandle::start(config, ctx.pool.clone()).await?;
            orchestrator.register_satellite(&format!("sat-{i}"), handle);
        }

        // Should have all satellites registered
        assert_eq!(orchestrator.satellites.lock().len(), 5);

        // Shutdown all
        orchestrator.shutdown_all().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_error_handling_in_shutdown() -> TestResult<()> {
        let orchestrator = SatelliteOrchestrator::new();

        // Even with no satellites/automata, shutdown should work
        orchestrator.shutdown_all().await?;

        Ok(())
    }

    #[sinex_test]
    fn test_ingestd_handle_drop() -> color_eyre::eyre::Result<()> {
        // Test that drop doesn't panic even with no process
        let handle = TestIngestdHandle {
            stream_name: "test-stream".to_string(),
            process: None,
            service: None,
            join_handle: None,
            _work_dir: None,
        };

        drop(handle); // Should not panic
        Ok(())
    }

    #[sinex_test]
    fn test_orchestrator_thread_safety() -> color_eyre::eyre::Result<()> {
        use std::sync::Arc;
        use std::thread;

        let orchestrator = Arc::new(SatelliteOrchestrator::new());
        let mut handles = vec![];

        // Spawn threads that register satellites
        for i in 0..10 {
            let orchestrator_clone = orchestrator.clone();
            let handle = thread::spawn(move || {
                let sat_handle = TestSatelliteHandle {
                    name: format!("thread-sat-{i}"),
                    process: None,
                };
                orchestrator_clone.register_satellite(&format!("key-{i}"), sat_handle);
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have all satellites
        assert_eq!(orchestrator.satellites.lock().len(), 10);
        Ok(())
    }
}
