//! Mock implementations for satellite architecture testing
//!
//! Provides simplified, controllable versions of system components for testing:
//! - Mock ingestd for gRPC event ingestion
//! - Mock satellites for event generation
//! - Mock automata for event processing
//!
//! These mocks are designed to be:
//! - Fast and predictable for tests
//! - Configurable for different scenarios
//! - Compatible with real component interfaces

pub mod mock_ingestd;
pub mod mock_satellite;
pub mod mock_automaton;
pub mod mock_redis;
pub mod mock_database;
pub mod mock_filesystem;
pub mod mock_network;
pub mod failure_injector;

// Re-export main mock types
pub use mock_ingestd::{MockIngestd, MockIngestdConfig, MockIngestdBuilder};
pub use mock_satellite::{MockSatellite, MockSatelliteConfig};
pub use mock_automaton::{MockAutomaton, MockAutomatonConfig};
pub use mock_redis::{MockRedis, MockRedisConfig};
pub use mock_database::{MockDatabase, MockDatabaseConfig};
pub use mock_filesystem::{MockFilesystem, MockFilesystemConfig};
pub use mock_network::{MockNetwork, MockNetworkConfig};
pub use failure_injector::{FailureInjector, FailurePattern, FailureConfig};