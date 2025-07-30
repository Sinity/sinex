//! Main entry point for Health Aggregator using unified StatefulStreamProcessor

use sinex_health_aggregator::HealthAggregator;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(HealthAggregator);
