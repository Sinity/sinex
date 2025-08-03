//! Main entry point for Health Aggregator using unified StatefulStreamProcessor
//!
//! This demonstrates the new pattern where automata implement StatefulStreamProcessor
//! directly and use the processor_main! macro for consistent CLI and lifecycle management.

mod unified_processor;

use unified_processor::HealthAggregator;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(HealthAggregator);