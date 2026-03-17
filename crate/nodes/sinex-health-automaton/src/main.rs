//! Main for Health Aggregator

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_health_automaton::HealthAggregatorNode;
use sinex_node_sdk::node_entrypoint;

node_entrypoint!(HealthAggregatorNode);
