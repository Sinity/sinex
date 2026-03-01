//! Modernized Main for Analytics Automaton

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_analytics_automaton::AnalyticsAutomaton;
use sinex_node_sdk::{AutomatonNodeAdapter, node_entrypoint};

node_entrypoint!(AutomatonNodeAdapter<AnalyticsAutomaton>);
