//! Binary entrypoint for the PKM Automaton using the unified processor runtime.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_pkm_automaton::PKMAutomaton;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Standardized CLI + lifecycle wiring
sinex_processor_runtime::processor_main!(PKMAutomaton);
