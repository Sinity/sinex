//! Modernized Main for PKM Automaton

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_processor_runtime::processor_main;
use sinex_pkm_automaton::PKMAutomatonNode;

processor_main!(PKMAutomatonNode);
