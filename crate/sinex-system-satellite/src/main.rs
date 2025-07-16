//! Main binary for the unified system processor
//!
//! This uses the new StatefulStreamProcessor architecture with service/scan/explore subcommands.

use sinex_system_satellite::SystemProcessor;

// Use the new unified architecture with macro
sinex_satellite_sdk::processor_main!(SystemProcessor);