//! # Performance and Scale Testing Suite
//!
//! Comprehensive performance tests that verify system behavior under various
//! load conditions and scale requirements. These tests focus on identifying
//! performance bottlenecks, resource limitations, and scalability constraints.
//!
//! ## Test Categories
//!
//! - **Throughput Tests**: Maximum event processing rates
//! - **Latency Tests**: Response time under various conditions
//! - **Concurrent Load Tests**: Behavior under concurrent access
//! - **Memory Usage Tests**: Memory consumption patterns
//! - **Database Performance Tests**: Query performance and optimization
//! - **Stream Processing Performance**: Redis Streams throughput
//! - **Checkpoint Performance**: Persistence and recovery speed
//! - **Resource Exhaustion Tests**: Behavior at system limits

/// Throughput and latency performance tests
pub mod throughput_latency_test;

/// Concurrent load performance tests
pub mod concurrent_load_test;

/// Memory usage and resource consumption tests
pub mod memory_usage_test;

/// Database query performance tests
pub mod database_performance_test;

/// Redis Streams performance tests
pub mod stream_performance_test;

/// Checkpoint system performance tests
pub mod checkpoint_performance_test;

/// Resource exhaustion and limit tests
pub mod resource_exhaustion_test;

