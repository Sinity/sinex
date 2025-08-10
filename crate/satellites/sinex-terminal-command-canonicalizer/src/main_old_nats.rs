//! Terminal Command Canonicalizer - Unified Main
//!
//! This automaton creates canonical command events as synthesis events based on terminal
//! command events from multiple sources (kitty, atuin, shell history).
//!
//! Uses the processor_main! macro and unified StatefulStreamProcessor architecture.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_macros::processor_main;
use sinex_terminal_command_canonicalizer::unified_processor::TerminalCommandCanonicalizer;

processor_main!(TerminalCommandCanonicalizer);