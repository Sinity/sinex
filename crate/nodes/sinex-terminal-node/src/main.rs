//! Main binary for the unified terminal processor
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_terminal_node::TerminalProcessor;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use the new unified architecture with macro
sinex_processor_runtime::processor_main!(TerminalProcessor);
