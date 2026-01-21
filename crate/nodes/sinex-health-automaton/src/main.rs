//! Modernized Main for Health Aggregator

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_processor_runtime::processor_main;
use sinex_health_automaton::HealthAggregatorNode;

processor_main!(HealthAggregatorNode);
