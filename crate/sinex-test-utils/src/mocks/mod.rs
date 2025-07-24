// Mock implementations for satellite architecture testing
//
// Provides simplified, controllable versions of system components for testing:
// - Mock ingestd for gRPC event ingestion
// - Mock satellites for event generation
// - Mock automata for event processing
//
// These mocks are designed to be:
// - Fast and predictable for tests
// - Configurable for different scenarios
// - Compatible with real component interfaces


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