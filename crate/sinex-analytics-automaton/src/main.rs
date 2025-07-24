//! Main entry point for Analytics Automaton using unified StatefulStreamProcessor

mod lib;

use lib::AnalyticsProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(AnalyticsProcessor);
