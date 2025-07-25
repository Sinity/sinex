//! Mock Infrastructure - Comprehensive Service Simulation
//!
//! This module provides high-fidelity mocks for external dependencies, enabling
//! thorough testing of error conditions, performance characteristics, and edge cases
//! without requiring real services.
//!
//! # Available Mocks
//!
//! - **Filesystem**: In-memory filesystem with full POSIX semantics
//! - **Database**: Simulated database with transaction support
//! - **Redis**: Complete Redis command implementation
//! - **Network**: TCP/UDP simulation with latency/loss injection
//! - **Ingestd**: Event ingestion service mock
//! - **Satellite**: Event source simulation
//! - **Automaton**: Event processor mock
//!
//! # Usage Examples
//!
//! ## Basic Mock Usage
//! ```rust
//! let fs = ctx.mocks().filesystem();
//! fs.create_file("/test.txt", b"content").await?;
//! assert!(fs.exists("/test.txt").await);
//! ```
//!
//! ## Failure Injection
//! ```rust
//! let db = ctx.mocks()
//!     .database()
//!     .with_failure_rate(0.1)  // 10% failure rate
//!     .with_latency(Duration::from_millis(50))
//!     .with_pattern(FailurePattern::Burst { duration: Duration::from_secs(2) });
//! ```
//!
//! ## Network Simulation
//! ```rust
//! let net = ctx.mocks().network();
//! net.configure()
//!     .latency(Duration::from_millis(100))
//!     .packet_loss(0.05)
//!     .bandwidth_limit(1_000_000);  // 1MB/s
//! ```
//!
//! # Design Principles
//!
//! 1. **Realistic Behavior**: Mocks simulate real service behavior accurately
//! 2. **Controllable Chaos**: Inject failures, delays, and errors on demand
//! 3. **Performance**: Fast execution for rapid test feedback
//! 4. **Observability**: Track all operations for verification
//! 5. **Thread Safety**: Safe for concurrent test execution


pub mod failure_injector;
pub mod mock_automaton;
pub mod mock_database;
pub mod mock_event_sources;
pub mod mock_filesystem;
pub mod mock_ingestd;
pub mod mock_network;
pub mod mock_redis;
pub mod mock_satellite;

// Re-export main mock types
pub use failure_injector::FailurePattern;

use crate::TestContext;

/// Builder for accessing mock objects
pub struct MockBuilder<'a> {
    ctx: &'a TestContext,
}

impl<'a> MockBuilder<'a> {
    pub fn new(ctx: &'a TestContext) -> Self {
        Self { ctx }
    }
    
    pub fn filesystem(&self) -> mock_filesystem::MockFilesystem {
        mock_filesystem::MockFilesystem::new(mock_filesystem::MockFilesystemConfig::default())
    }
    
    pub fn database(&self) -> mock_database::MockDatabase {
        mock_database::MockDatabase::new(mock_database::MockDatabaseConfig::default())
    }
    
    pub fn redis(&self) -> mock_redis::MockRedis {
        mock_redis::MockRedis::new(mock_redis::MockRedisConfig::default())
    }
    
    pub fn satellite(&self, name: &str) -> mock_satellite::MockSatellite {
        use mock_satellite::MockSatelliteConfig;
        let mut config = MockSatelliteConfig::default();
        config.base_config.service_name = name.to_string();
        mock_satellite::MockSatellite::new(config)
    }
    
    pub fn automaton(&self, name: &str) -> mock_automaton::MockAutomaton {
        // Note: MockAutomaton requires more complex setup - this is a simplified version
        // In real tests, you'd need to provide actual pool and redis client
        todo!("MockAutomaton requires database pool and redis client")
    }
    
    pub fn ingestd(&self) -> mock_ingestd::MockIngestd {
        mock_ingestd::MockIngestd::new(
            "mock-ingestd".to_string(),
            mock_ingestd::MockIngestdConfig::default()
        )
    }
    
    pub fn network(&self) -> mock_network::MockNetwork {
        mock_network::MockNetwork::new(mock_network::MockNetworkConfig::default())
    }
}

// Comprehensive mock tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::path::Path;
    use std::time::Duration;
    
    #[sinex_test]
    async fn test_mock_builder_creation(ctx: TestContext) -> TestResult<()> {
        // Mock builder should be accessible from context
        let mocks = ctx.mocks();
        
        // Should create filesystem mock
        let fs = mocks.filesystem();
        assert!(fs.exists(Path::new("/")).await);
        
        // Should create database mock
        let _db = mocks.database();
        
        // Should create redis mock
        let redis = mocks.redis();
        let mut conn = redis.connect().await?;
        let result = conn.get::<String>("test").await?;
        assert!(result.is_none());
        
        // Should create satellite mock
        let sat = mocks.satellite("test-satellite");
        // MockSatellite created successfully
        
        // Should create ingestd mock
        let ingestd = mocks.ingestd();
        // MockIngestd created successfully
        
        // Should create network mock
        let net = mocks.network();
        assert!(!net.is_connected());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_isolation(ctx: TestContext) -> TestResult<()> {
        // Each test should get isolated mocks
        let fs1 = ctx.mocks().filesystem();
        let fs2 = ctx.mocks().filesystem();
        
        // Operations on one should not affect the other
        fs1.create_file(Path::new("/test1.txt"), b"content1").await?;
        
        assert!(fs1.exists(Path::new("/test1.txt")).await);
        assert!(!fs2.exists(Path::new("/test1.txt")).await);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_error_injection(ctx: TestContext) -> TestResult<()> {
        use mock_filesystem::MockFilesystemConfig;
        
        // Create filesystem with error injection
        let mut config = MockFilesystemConfig::default();
        config.permission_error_rate = 1.0; // Always fail
        
        let fs = mock_filesystem::MockFilesystem::new(config);
        
        // Should fail with permission error
        let result = fs.create_file(Path::new("/test.txt"), b"content").await;
        assert!(result.is_err());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_concurrency(ctx: TestContext) -> TestResult<()> {
        let fs = ctx.mocks().filesystem();
        
        // Concurrent operations should be safe
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let fs_clone = fs.clone();
                tokio::spawn(async move {
                    let path = format!("/concurrent_{}.txt", i);
                    fs_clone.create_file(Path::new(&path), format!("content_{}", i).as_bytes()).await
                })
            })
            .collect();
        
        let results = futures::future::join_all(handles).await;
        
        // All should succeed
        for result in results {
            assert!(result?.is_ok());
        }
        
        // All files should exist
        for i in 0..10 {
            let path = format!("/concurrent_{}.txt", i);
            assert!(fs.exists(Path::new(&path)).await);
        }
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_state_tracking(ctx: TestContext) -> TestResult<()> {
        let db = ctx.mocks().database();
        
        // Mock should track operations
        assert_eq!(db.query_count(), 0);
        
        let _ = db.execute("INSERT INTO test VALUES (1)").await?;
        let _ = db.execute("SELECT * FROM test").await?;
        
        assert_eq!(db.query_count(), 2);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_configuration(ctx: TestContext) -> TestResult<()> {
        use mock_network::MockNetworkConfig;
        
        // Should support custom configuration
        let mut config = MockNetworkConfig::default();
        config.latency = Duration::from_millis(100);
        config.packet_loss_rate = 0.1;
        
        let net = mock_network::MockNetwork::new(config);
        
        // Configuration should affect behavior
        let start = std::time::Instant::now();
        let _ = net.send_packet(b"test").await;
        let elapsed = start.elapsed();
        
        // Should have simulated latency
        assert!(elapsed >= Duration::from_millis(100));
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_failure_patterns(ctx: TestContext) -> TestResult<()> {
        use failure_injector::{FailurePattern, FailureInjector};
        
        let injector = FailureInjector::new();
        
        // Test different failure patterns
        injector.set_pattern(FailurePattern::Constant { rate: 0.5 });
        
        let mut failures = 0;
        for _ in 0..100 {
            if injector.should_fail().await {
                failures += 1;
            }
        }
        
        // Should be approximately 50%
        assert!(failures > 40 && failures < 60);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_lifecycle(ctx: TestContext) -> TestResult<()> {
        let sat = ctx.mocks().satellite("lifecycle-test");
        
        // Should start in stopped state
        assert!(!sat.is_running());
        
        // Should be startable
        sat.start().await?;
        assert!(sat.is_running());
        
        // Should be stoppable
        sat.stop().await?;
        assert!(!sat.is_running());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_event_generation(ctx: TestContext) -> TestResult<()> {
        let mut sat = ctx.mocks().satellite("event-gen");
        sat.start().await?;
        
        // Wait for satellite to generate events
        sat.wait_for_generation(5, 5).await?;
        
        // Should have generated events
        let events = sat.get_generated_events().await;
        assert!(events.len() >= 5);
        
        // Events should have proper structure
        for event in events.iter().take(5) {
            assert_eq!(event.source, "event-gen");
            assert!(!event.id.to_string().is_empty());
        }
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_redis_operations(ctx: TestContext) -> TestResult<()> {
        let redis = ctx.mocks().redis();
        let mut conn = redis.connect().await?;
        
        // Basic operations
        conn.set("key1", "value1").await?;
        let value = conn.get::<String>("key1").await?;
        assert_eq!(value, Some("value1".to_string()));
        
        // Test multiple keys
        conn.set("key2", "value2").await?;
        let value2 = conn.get::<String>("key2").await?;
        assert_eq!(value2, Some("value2".to_string()));
        
        // Lists
        redis.lpush("list1", "item1").await?;
        redis.lpush("list1", "item2").await?;
        
        let items = redis.lrange("list1", 0, -1).await?;
        assert_eq!(items.len(), 2);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_database_transactions(ctx: TestContext) -> TestResult<()> {
        let db = ctx.mocks().database();
        
        // Should support transactions
        let tx = db.begin().await?;
        
        tx.execute("INSERT INTO test VALUES (1)").await?;
        tx.execute("INSERT INTO test VALUES (2)").await?;
        
        // Before commit, main connection shouldn't see changes
        let result = db.query("SELECT COUNT(*) FROM test").await?;
        assert_eq!(result.len(), 0);
        
        tx.commit().await?;
        
        // After commit, should see changes
        let result = db.query("SELECT COUNT(*) FROM test").await?;
        assert_eq!(result.len(), 1);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_filesystem_operations(ctx: TestContext) -> TestResult<()> {
        let fs = ctx.mocks().filesystem();
        
        // Directory operations
        fs.create_dir(Path::new("/test_dir")).await?;
        assert!(fs.exists(Path::new("/test_dir")).await);
        assert!(fs.is_dir(Path::new("/test_dir")).await);
        
        // File operations
        let content = b"Hello, world!";
        fs.create_file(Path::new("/test_dir/file.txt"), content).await?;
        
        let read_content = fs.read_file(Path::new("/test_dir/file.txt")).await?;
        assert_eq!(read_content, content);
        
        // Metadata
        let metadata = fs.metadata(Path::new("/test_dir/file.txt")).await?;
        assert_eq!(metadata.len(), content.len() as u64);
        
        // Move operations
        fs.rename(Path::new("/test_dir/file.txt"), Path::new("/test_dir/renamed.txt")).await?;
        assert!(!fs.exists(Path::new("/test_dir/file.txt")).await);
        assert!(fs.exists(Path::new("/test_dir/renamed.txt")).await);
        
        // Delete operations
        fs.remove_file(Path::new("/test_dir/renamed.txt")).await?;
        assert!(!fs.exists(Path::new("/test_dir/renamed.txt")).await);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_network_simulation(ctx: TestContext) -> TestResult<()> {
        let net = ctx.mocks().network();
        
        // Connection simulation
        let mut conn = net.connect(std::net::SocketAddr::from(([127, 0, 0, 1], 80))).await?;
        
        // Data transfer
        conn.send(b"GET / HTTP/1.1\r\n\r\n").await?;
        
        // Receive simulation (would normally come from the other end)
        let mut buffer = [0u8; 1024];
        // In a real mock this would simulate receiving data
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mock_ingestd_grpc(ctx: TestContext) -> TestResult<()> {
        let ingestd = ctx.mocks().ingestd();
        ingestd.start().await?;
        
        // Should accept events
        let event = ctx.event()
            .source("test")
            .type_("test.event")
            .build();
        
        ingestd.ingest_event(&event).await?;
        
        // Should track ingested events
        assert_eq!(ingestd.event_count(), 1);
        
        // Should support batch ingestion
        let events: Vec<_> = (0..5)
            .map(|i| ctx.event()
                .source("batch")
                .type_("test.batch")
                .field("index", i)
                .build())
            .collect();
        
        ingestd.ingest_batch(&events).await?;
        assert_eq!(ingestd.event_count(), 6);
        
        Ok(())
    }
    
    #[test]
    fn test_mock_config_defaults() {
        use mock_filesystem::MockFilesystemConfig;
        use mock_network::MockNetworkConfig;
        use mock_database::MockDatabaseConfig;
        
        // All configs should have sensible defaults
        let fs_config = MockFilesystemConfig::default();
        assert_eq!(fs_config.max_files, 10000);
        assert_eq!(fs_config.permission_error_rate, 0.0);
        
        let net_config = MockNetworkConfig::default();
        assert_eq!(net_config.packet_loss_rate, 0.0);
        
        let db_config = MockDatabaseConfig::default();
        assert_eq!(db_config.max_connections, 100);
    }
}