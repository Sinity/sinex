//! Search Service Automaton Binary
//!
//! Main entry point for Search Automaton using unified StatefulStreamProcessor

use sinex_search_automaton::SearchProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(SearchProcessor);
