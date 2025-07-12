//! Main binary for the unified filesystem processor
//!
//! This uses the new StatefulStreamProcessor architecture with service/scan/explore subcommands.

use sinex_fs_watcher::FilesystemProcessor;

// Use the new unified architecture with macro
sinex_satellite_sdk::processor_main!(FilesystemProcessor);