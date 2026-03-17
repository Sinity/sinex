//! Main for Terminal Command Canonicalizer

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_node_sdk::node_entrypoint;
use sinex_terminal_command_canonicalizer::TerminalCommandCanonicalizerNode;

node_entrypoint!(TerminalCommandCanonicalizerNode);
