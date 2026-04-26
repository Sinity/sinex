//! Main binary for the terminal ingestor.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_node_sdk::IngestorNodeAdapter;
use sinex_terminal_ingestor::TerminalNode;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

sinex_node_sdk::node_entrypoint!(IngestorNodeAdapter<TerminalNode>);
