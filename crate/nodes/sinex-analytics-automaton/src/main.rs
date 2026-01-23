//! Modernized Main for Analytics Automaton

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_analytics_automaton::AnalyticsAutomatonNode;
use sinex_processor_runtime::processor_main;

processor_main!(AnalyticsAutomatonNode);
