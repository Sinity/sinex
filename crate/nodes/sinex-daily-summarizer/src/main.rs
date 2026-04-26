//! Main for Daily Summarizer

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_daily_summarizer::DailySummarizerNode;
use sinex_node_sdk::node_entrypoint;

node_entrypoint!(DailySummarizerNode);
