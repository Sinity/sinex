//! Content Service Automaton Binary
//!
//! Main entry point for Content Automaton using unified StatefulStreamProcessor

use sinex_content_automaton::ContentProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(ContentProcessor);
