//! Modernized Main for Terminal Command Canonicalizer

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_node_sdk::{node_entrypoint, AutomatonNodeAdapter};
use sinex_terminal_command_canonicalizer::unified_node::TerminalCommandCanonicalizer;

node_entrypoint!(AutomatonNodeAdapter<TerminalCommandCanonicalizer>);
