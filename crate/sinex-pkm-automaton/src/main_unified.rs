//! Main entry point for PKM Service using unified StatefulStreamProcessor

mod lib; // Keep legacy for now
mod unified_processor;

use unified_processor::PkmServiceProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(PkmServiceProcessor);