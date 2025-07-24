//! Search Service Automaton Binary
//!
//! Main entry point for Search Automaton using unified StatefulStreamProcessor

mod lib;

use lib::SearchProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(SearchProcessor);
