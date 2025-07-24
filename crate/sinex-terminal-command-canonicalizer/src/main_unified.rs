//! Main entry point for Terminal Command Canonicalizer using unified StatefulStreamProcessor

mod lib; // Keep legacy for now
mod unified_processor;

use unified_processor::TerminalCommandCanonicalizer;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(TerminalCommandCanonicalizer);