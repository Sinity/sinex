//! Main entry point for Content Service using unified StatefulStreamProcessor

mod lib; // Keep legacy for now
mod unified_processor;

use unified_processor::ContentServiceProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(ContentServiceProcessor);