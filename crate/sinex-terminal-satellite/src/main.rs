//! Main binary for the unified terminal processor
//!
//! This uses the new StatefulStreamProcessor architecture with service/scan/explore subcommands.

use sinex_terminal_satellite::TerminalProcessor;

// Use the new unified architecture with macro
sinex_satellite_sdk::processor_main!(TerminalProcessor);