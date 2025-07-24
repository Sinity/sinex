//! Main entry point for PKM Automaton using unified StatefulStreamProcessor

mod lib;

use lib::PKMProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(PKMProcessor);
