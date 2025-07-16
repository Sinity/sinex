//! Main binary for the unified desktop processor
//!
//! This uses the new StatefulStreamProcessor architecture with service/scan/explore subcommands.

use sinex_desktop_satellite::DesktopProcessor;

// Use the new unified architecture with macro
sinex_satellite_sdk::processor_main!(DesktopProcessor);