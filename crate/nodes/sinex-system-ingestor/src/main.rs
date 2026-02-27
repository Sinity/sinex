//! Main binary for the unified system node
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_system_ingestor::SystemNode;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Use the new unified architecture with macro
use sinex_node_sdk::IngestorNodeAdapter;

// Use the new unified architecture with macro
sinex_node_sdk::node_entrypoint!(
    IngestorNodeAdapter<SystemNode>,
    IngestorNodeAdapter::new(SystemNode::default())
);
