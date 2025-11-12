// # Performance and Scale Testing Suite
//
// Comprehensive performance tests that verify system behavior under various
// load conditions and scale requirements. These tests focus on identifying
// performance bottlenecks, resource limitations, and scalability constraints.
//
// ## Test Categories
//
// - **Throughput Tests**: Maximum event processing rates
// - **Latency Tests**: Response time under various conditions
// - **Concurrent Load Tests**: Behavior under concurrent access
// - **Memory Usage Tests**: Memory consumption patterns
// - **Database Performance Tests**: Query performance and optimization
// - **JetStream Performance**: Publish/consume behaviour on the current bus
// - **Large Payload Handling**: End-to-end flow for sizeable messages
// - **Resource Exhaustion Tests**: Behavior at system limits
// - **Checkpoint Performance**: Persistence and recovery speed
// - **Bottleneck Identification**: Tools for identifying JetStream stress cases

use sinex_test_utils::TestResult;
use sinex_test_utils::prelude::*;

/// JetStream publish/consume performance tests
pub mod jetstream_performance_test;

/// Large payload handling tests
pub mod large_payload_test;

/// Checkpoint persistence and recovery tests
pub mod checkpoint_performance_test;

/// Resource exhaustion and limit tests
pub mod resource_exhaustion_test;

/// JetStream bottleneck identification tests
pub mod bottleneck_identification_test;
