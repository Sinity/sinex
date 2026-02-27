//! Main binary for the unified desktop node
//!
//! This uses the new Node architecture with service/scan/explore subcommands.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_desktop_ingestor::DesktopNode;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_node_sdk::IngestorNodeAdapter;

// Use the new unified architecture with macro
sinex_node_sdk::node_entrypoint!(
    IngestorNodeAdapter<DesktopNode>,
    IngestorNodeAdapter::new(DesktopNode::default())
);
