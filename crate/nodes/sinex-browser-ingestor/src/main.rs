//! Main binary for the browser history ingestor.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;
use sinex_browser_ingestor::BrowserNode;
use sinex_node_sdk::IngestorNodeAdapter;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

sinex_node_sdk::node_entrypoint!(
    IngestorNodeAdapter<BrowserNode>,
    IngestorNodeAdapter::new(BrowserNode::default())
);
