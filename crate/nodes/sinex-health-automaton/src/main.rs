//! Binary entrypoint for the Health Aggregator using the unified processor runtime.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_health_automaton::HealthAggregator;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Standardized CLI + lifecycle wiring
sinex_processor_runtime::processor_main!(HealthAggregator);
