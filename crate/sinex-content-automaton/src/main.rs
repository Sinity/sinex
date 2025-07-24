//! Content Service Automaton Binary
//!
//! Main entry point for Content Automaton using unified StatefulStreamProcessor

mod lib;

use lib::ContentProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(ContentProcessor);
