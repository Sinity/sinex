//! Main binary for the unified filesystem processor
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_fs_watcher::FilesystemProcessor;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use the new unified architecture with macro
sinex_processor_runtime::processor_main!(FilesystemProcessor);
