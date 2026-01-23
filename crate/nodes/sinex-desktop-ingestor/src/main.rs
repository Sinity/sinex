//! Main binary for the unified desktop processor
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_desktop_ingestor::DesktopProcessor;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_node_sdk::simple_ingestor::SimpleIngestorWrapper;

// Use the new unified architecture with macro
sinex_processor_runtime::processor_main!(
    SimpleIngestorWrapper<DesktopProcessor>,
    SimpleIngestorWrapper::new(DesktopProcessor::default())
);
