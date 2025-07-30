//! Main entry point for Analytics Automaton using unified StatefulStreamProcessor

use sinex_analytics_automaton::AnalyticsProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(AnalyticsProcessor);
