//! Main binary for the unified system processor
//!
//! This uses the new StatefulStreamProcessor architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_system_satellite::SystemProcessor;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use the new unified architecture with macro
sinex_satellite_sdk::processor_main!(SystemProcessor);
