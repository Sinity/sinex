//! Main entry point for PKM Automaton using unified StatefulStreamProcessor

use sinex_pkm_automaton::PKMProcessor;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(PKMProcessor);
