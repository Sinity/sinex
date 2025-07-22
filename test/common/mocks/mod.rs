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
pub use mock_event_sources::{
    AtuinHistoryImporter, ClipboardMonitor, EventSourceContext, FilesystemMonitor,
    RedisClient, ShellHistoryMonitor, TerminalMonitor,
};
