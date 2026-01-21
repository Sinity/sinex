//! Modernized Main for Terminal Command Canonicalizer

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_processor_runtime::processor_main;
use sinex_terminal_command_canonicalizer::unified_processor::TerminalCommandCanonicalizerNode;

// Use the wrapped SimpleNode
processor_main!(TerminalCommandCanonicalizerNode);
