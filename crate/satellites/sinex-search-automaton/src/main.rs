//! Search Service Automaton Binary
//!
//! Main entry point for Search Automaton using unified StatefulStreamProcessor

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_search_automaton::SearchProcessor;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use the processor_main! macro for standardized CLI and lifecycle
sinex_satellite_sdk::processor_main!(SearchProcessor);
