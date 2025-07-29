//! Main entry point for Terminal Command Canonicalizer using unified StatefulStreamProcessor

use sinex_terminal_command_canonicalizer::TerminalCommandCanonicalizer;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(TerminalCommandCanonicalizer);
