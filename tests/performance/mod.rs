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
// - **Resource Exhaustion Tests**: Behavior at system limits
// - **Baseline Performance**: Establishes performance baselines
// - **Regression Detection**: Automated performance regression detection
// - **Bottleneck Identification**: Tools for identifying system bottlenecks

use color_eyre::eyre::Result;
use sinex_test_utils::prelude::*;

/// JetStream publish/consume performance tests
pub mod jetstream_performance_test;

/// Resource exhaustion and limit tests
pub mod resource_exhaustion_test;
